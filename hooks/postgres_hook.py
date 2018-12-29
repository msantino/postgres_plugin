# -*- coding: utf-8 -*-
import ast
import psycopg2
import psycopg2.extensions

from airflow.hooks.postgres_hook import PostgresHook as AirflowPostgresHook

from postgres_plugin.hooks.aws_secrets_manager_hook import AwsSecretsManagerHook


class PostgresHook(AirflowPostgresHook):
    """
    Interact with Postgres Using AWS Secrets Manager Credential

    :param aws_secret_name: reference to a aws secrets manager name
    :type aws_secret_name: string
    :param aws_conn_id: reference to a specific aws connection
    :type aws_conn_id: string
    :param schema: name of database which overwrite defined one in connection
    :type schema: string

    """
    conn_name_attr = 'aws_default'
    default_conn_name = 'aws_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.aws_conn_id = kwargs.pop("aws_conn_id", None)
        self.aws_secret_name = kwargs.pop("aws_secret_name", None)

    def get_conn(self):

        self.log.info('Looking for AWS Secret Manager key [{}]'.format(self.aws_secret_name))
        secret_manager = AwsSecretsManagerHook(
            aws_secret_name=self.aws_secret_name,
            aws_conn_id=self.aws_conn_id
        )

        aws_secret_key = ast.literal_eval(secret_manager.get_secret())

        self.log.info('Got key to database [{}] on host [{}:{}]'.format(
            self.schema or aws_secret_key['dbname'],
            aws_secret_key['host'],
            aws_secret_key['port']
        ))

        """
        Expected dict format based on automatic AWS Secrets Manager's Lambda rotation function:
        https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets-lambda-function-overview.html
        """
        conn_args = dict(
            host=aws_secret_key['host'],
            user=aws_secret_key['username'],
            password=aws_secret_key['password'],
            dbname=self.schema or aws_secret_key['dbname'],
            port=aws_secret_key['port'] or 5432)

        psycopg2_conn = psycopg2.connect(**conn_args)
        return psycopg2_conn

