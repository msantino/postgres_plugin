# -*- coding: utf-8 -*-
import logging
import os
import ast
import hashlib
from datetime import datetime
from tempfile import NamedTemporaryFile

from aws_plugin.hooks.aws_secrets_manager_hook import AwsSecretsManagerHook
from bash_plugin import BashHook
from airflow.utils import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import BaseOperator
from airflow.exceptions import AirflowException

logging = logging.getLogger(__name__)


class PostgresDumpOperator(BaseOperator):
    """
    Extract a PostgreSQL database dump to a temporary file, encrypt it (or not) using a public SSL certificate
    and send it to S3 bucket.
    A AWS credential using Secrets Manager service must be used to store database password.

    :param db_name: Database name to extract dump
    :type db_name: str
    :param secret_name: Secret name (path) from AWS Secrets Manager (ex: env/database/role/user)
    :type secret_name: str
    :param s3_bucket_name: S3 bucket on witch database dump will be stored.
    :type s3_bucket_name: str
    :param s3_key_name: Key name (file name/path) to use as dumpname on S3 bucket
    :type s3_key_name: str
    :param ssl_cert_path: Path to a public SSL certificate to use in dump encription.
    :type ssl_cert_path: str
    :return: None
    """

    template_fields = ['s3_key_name',]
    aws_secret_key = None
    s3_key_name = '{instance_identifier}/{db_name}/{year}/{month}/{dump_name}'
    dump_file_md5 = None

    @apply_defaults
    def __init__(self,
                 db_name,
                 secret_name,
                 s3_bucket_name,
                 aws_kms_key_arn,
                 dump_extra_parameters='',
                 *args, **kwargs):
        super(PostgresDumpOperator, self).__init__(*args, **kwargs)
        self.secret_name = secret_name
        self.db_name = db_name
        self.xcom_push_flag = False
        self.s3_bucket_name = s3_bucket_name
        self.aws_kms_key_arn = aws_kms_key_arn
        self.dump_extra_parameters = dump_extra_parameters

    def execute(self, context):

        logging.info('Looking for AWS Secret Manager key')
        secret_manager = AwsSecretsManagerHook(aws_secret_name=self.secret_name)
        self.aws_secret_key = ast.literal_eval(secret_manager.get_secret())

        # generate dumpfile from database
        dump_file_name = self.extract_dump()
        logging.info('Dump file: {}'.format(dump_file_name))

        # Calculate dump md5
        dump_file_md5 = self.calculate_file_hash(dump_file_name)

        # Encrypt file using public SSL cert
        encrypted_file = self.encrypt_dump(dump_file_name)
        logging.info('Encrypted file: {}'.format(encrypted_file))

        # Calculate encrypted md5
        encrypted_file_md5 = self.calculate_file_hash(encrypted_file)

        # list of files to save on S3
        files_to_transfer = [
            {'file': encrypted_file, 'ext': '.encrypted'},
            {'file': dump_file_md5, 'ext': '.md5'},
            {'file': encrypted_file_md5, 'ext': '.encrypted.md5'}
        ]

        # Send file to S3
        send_to_s3 = self.send_files_to_s3(files_to_transfer)
        logging.info('Send to S3 result: {}'.format(send_to_s3))

        # Clear files
        self.delete_file(dump_file_name)
        self.delete_file(encrypted_file)
        self.delete_file(dump_file_md5)
        self.delete_file(encrypted_file_md5)

        logging.info('Done.')

        return dump_file_name

    def extract_dump(self):
        """
        Extract pg_dump command

        :return: Dump file name
        :rtype: str
        """

        dump_file = NamedTemporaryFile('wb', prefix=self.task_id, delete=False)
        logging.info('Dump temporary file name: [{}]'.format(dump_file.name))

        # Set user password in ENV to use in bash command
        os.environ['PGPASSWORD'] = self.aws_secret_key['password']

        # Command to dump database
        bash_command = "pg_dump -v -Ft -h {host} -U {user} -d {dbname} -f {filename} {dump_extra_parameters}".format(
            host=self.aws_secret_key['host'],
            user=self.aws_secret_key['username'],
            dbname=self.db_name,
            password=self.aws_secret_key['host'],
            filename=dump_file.name,
            dump_extra_parameters=self.dump_extra_parameters
        )

        # Execute bash command to extract database dump
        self.execute_bash(bash_command)

        dump_file.flush()
        logging.info('Dump file size: {}Mb'.format(os.stat(dump_file.name).st_size >> 20))

        return dump_file.name

    def execute_bash(self, bash_command):
        """
        Use BashHook to execute bash command

        :return: Bash command return
        :rtype: str
        """
        return BashHook(bash_command=bash_command, task_id=self.task_id).execute()

    def send_files_to_s3(self, files_to_transfer):
        """
        Send dumpfile to S3

        :param files_to_transfer: Dumpfile location (path)
        :type files_to_transfer: list
        :return: S3 file uploaded
        :rtype: str
        """
        dest_s3 = S3Hook()
        self.s3_key_name = self.s3_key_name.format(
            instance_identifier=self.aws_secret_key['dbInstanceIdentifier'],
            db_name=self.db_name,
            year=datetime.now().strftime("%Y"),
            month=datetime.now().strftime("%m"),
            dump_name='dump_{}_{}.dmp'.format(self.db_name, datetime.now().strftime("%Y%m%d_%H%M%S"))
        )

        logging.info('Dump file name: [{}]'.format(self.s3_key_name))
        logging.info('Start transfer files to S3 bucket [{}]'.format(self.s3_bucket_name))
        # logging.info('File path on S3 [{}/{}]'.format(self.s3_bucket_name, self.s3_key_name))

        for file in files_to_transfer:

            key_name = '{}{}'.format(self.s3_key_name, file['ext'])
            logging.info('File {} as {}'.format(file['file'], key_name))
            dest_s3.load_file(file['file'],
                              key=key_name,
                              bucket_name=self.s3_bucket_name,
                              replace=True,
                              encrypt=False)

        # list s3 file
        files = dest_s3.list_keys(self.s3_bucket_name, prefix=self.s3_key_name)
        logging.info('S3 files: [{}]'.format(files))

        return files

    def encrypt_dump(self, dump_file_name):
        """
        Encrypt given dumpfile

        :param dump_file_name: Dumpfile location (path)
        :type dump_file_name: str
        :return: Encrypted file name (path)
        :rtype: str
        """

        encrypted_file = NamedTemporaryFile(prefix=self.task_id, delete=False)

        try:
            AWSKMSHook(aws_kms_key_arn=self.aws_kms_key_arn,
                       aws_conn_id='default',
                       task_id=self.task_id)\
                .encrypt_file(
                    source_file_name=dump_file_name,
                    target_file_name=encrypted_file.name
                )
        except Exception as e:
            logging.error('Error trying to fetch query: [{}]'.format(str(e)))
            raise AirflowException(str(e))

        encrypted_file.flush()

        return encrypted_file.name

    def calculate_file_hash(self, file_name):

        # Calculate hash md5 from dumpfile
        logging.info('Calculating MD5 hash for file {}'.format(file_name))
        dest_md5_file = NamedTemporaryFile('w', prefix=self.task_id, delete=False)
        try:
            file_md5 = self.md5sum(file_name)
            logging.info('File MD5: [{}]'.format(file_md5))
            dest_md5_file.write(file_md5)
            dest_md5_file.flush()
        except Exception as e:
            logging.error('Error trying to calculare md5 hash: [{}]'.format(str(e)))
            raise AirflowException(str(e))
        return dest_md5_file.name

    @staticmethod
    def delete_file(file_name):
        """
        Delete a file safely from a given path. It checks for a existing file before delete to prevent exceptions.

        :param file_name: File name (path) to be deleted
        :type file_name: str
        """

        # If file exists, delete it
        logging.info('Removing file safely: [{}]'.format(file_name))
        try:
            os.remove(file_name)
            logging.info('File removed successfully.')
        except OSError as e:
            logging.error("Error: {}".format(e.strerror))

    @staticmethod
    def md5sum(file_name):

        md5 = hashlib.md5()
        with open(file_name, 'rb') as f:
            for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
                md5.update(chunk)
        return md5.hexdigest()
