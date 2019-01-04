# -*- coding: utf-8 -*-
import logging
import gzip
from tempfile import NamedTemporaryFile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from postgres_plugin import PostgresWithSecretsManagerCredentialsHook


class PostgresToS3Operator(BaseOperator):

    template_fields = ('sql', 'dest_s3_key_name')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            dest_s3_bucket_name,
            dest_s3_key_name,
            src_postgres_conn,
            dest_s3_replace=False,
            dest_s3_encrypt=False,
            dest_s3_conn_id='postgres_default',
            delete_temporary_file=True,
            compress_file=False,
            *args, **kwargs):
        super(PostgresToS3Operator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.src_postgres_conn = src_postgres_conn
        self.dest_s3_conn_id = dest_s3_conn_id
        self.dest_s3_bucket_name = dest_s3_bucket_name
        self.dest_s3_key_name = dest_s3_key_name
        self.dest_s3_replace = dest_s3_replace
        self.dest_s3_encrypt = dest_s3_encrypt
        self.delete_temporary_file = delete_temporary_file
        self.compress_file = compress_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        logging.info('Extracting query: ' + str(self.sql))

        src_pgsql = PostgresWithSecretsManagerCredentialsHook(
            aws_conn_id=self.src_postgres_conn['aws_conn_id'],
            aws_secret_name=self.src_postgres_conn['aws_secret_name'],
            schema=self.src_postgres_conn['database']
        )
        src_conn = src_pgsql.get_conn()
        cursor = src_conn.cursor()

        dest_s3 = S3Hook(aws_conn_id=self.dest_s3_conn_id)

        with NamedTemporaryFile(mode='wb', delete=self.delete_temporary_file) as f_plain:

            logging.info('Starting COPY to [{}].'.format(f_plain.name))
            try:
                cursor.copy_to(f_plain, '({})'.format(self.sql), null='')
            except Exception as e:
                logging.error('Error trying to fetch query: [{}]'.format(str(e)))
                raise AirflowException(str(e))

            f_plain.flush()
            logging.info('File created.')

            f_compressed = NamedTemporaryFile(mode='wb', delete=self.delete_temporary_file)
            if self.compress_file:
                logging.info('Start to compress file [{}]'.format(f_compressed.name))
                with open(f_plain.name, 'rb') as src, gzip.open(f_compressed.name, 'wb') as dst:
                    dst.writelines(src)

            logging.info('Starting to transfer file to S3 bucket [{}]'.format(self.dest_s3_bucket_name))
            logging.info('File path on S3 [{}]'.format(self.dest_s3_key_name))
            dest_s3.load_file(f_compressed.name if self.compress_file else f_plain.name,
                              key=self.dest_s3_key_name,
                              bucket_name=self.dest_s3_bucket_name,
                              replace=self.dest_s3_replace,
                              encrypt=self.dest_s3_encrypt)

            # list s3 file
            files = dest_s3.list_keys(self.dest_s3_bucket_name, prefix=self.dest_s3_key_name)
            logging.info('S3 file: [{}]'.format(files))

            logging.info('Done.')


class S3ToPostgresOperator(BaseOperator):
    """
    Download a file from S3 bucket and import to PostgreSQL database via COPY command

    """

    template_fields = ('dest_table_name', 's3_key_name', 'pg_preoperator', 'pg_postoperator')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            postgres_conn,
            dest_table_name,
            dest_table_columns,
            s3_conn_id,
            s3_bucket_name,
            s3_key_name,
            pg_preoperator=None,
            pg_postoperator=None,
            *args, **kwargs):
        super(S3ToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn = postgres_conn
        self.dest_table_name = dest_table_name
        self.dest_table_columns = dest_table_columns
        self.pg_preoperator = pg_preoperator
        self.pg_postoperator = pg_postoperator
        self.s3_conn_id = s3_conn_id
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_name = s3_key_name
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):

        logging.info('Downloading file from S3 and inserting into PostgreSQL via COPY command')

        s3 = S3Hook(aws_conn_id=self.s3_conn_id)

        with NamedTemporaryFile(mode='w') as temp_file_s3:

            logging.info('S3 file to read: [{}/{}]'.format(self.s3_bucket_name, self.s3_key_name))
            logging.info('Writing S3 file to [{}].'.format(temp_file_s3.name))
            try:
                temp_file_s3.write(s3.read_key(self.s3_key_name, self.s3_bucket_name))
            except Exception as e:
                logging.error('Error: {}'.format(str(e)))
                logging.error('Error: {}'.format(str(type(e))))
                raise AirflowException('Error: {}'.format(str(e)))

            temp_file_s3.flush()

            with open(temp_file_s3.name, 'r') as file_read:

                # Skip first row
                next(file_read)

                pgsql = PostgresWithSecretsManagerCredentialsHook(
                    aws_conn_id=self.postgres_conn['aws_conn_id'],
                    aws_secret_name=self.postgres_conn['aws_secret_name'],
                    schema=self.postgres_conn['database']
                )
                pgsql_conn = pgsql.get_conn()
                pgsql_conn.autocommit = True
                pgsql_cursor = pgsql_conn.cursor()

                if self.pg_preoperator is not None:
                    logging.info('Running pg_preoperator query.')
                    pgsql.run(self.pg_preoperator)

                try:

                    logging.info('Start to copy file to database')

                    logging.info('Destination table: [{}]'.format(self.dest_table_name))
                    pgsql_cursor.copy_from(file=file_read,
                                           table=self.dest_table_name,
                                           columns=self.dest_table_columns)
                    pgsql_conn.commit()

                except Exception as e:
                    logging.error('Error trying to load file to db: [{}]'.format(str(e)))
                    raise AirflowException(str(e))

                if self.pg_postoperator is not None:
                    logging.info('Running pg_postoperator query [{}].'.format(self.pg_postoperator))
                    logging.info('Post operator result: {}'.format(pgsql.get_first(self.pg_postoperator)[0]))

                logging.info('File Uploaded to database.')

            logging.info('Done.')


