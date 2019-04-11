# -*- coding: utf-8 -*-
"""
Execute some query on PostgreSQL database using Secrets Manager to retrieve database credentials.

"""
# pylint: disable=import-error,missing-docstring,too-few-public-methods
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from postgres_plugin.hooks.postgres_hook import PostgresWithSecretsManagerCredentialsHook


class PostgresWithSecretsManagerCredentialsOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database
    Using AWS Secrets Manager Credential to authenticate

    :param aws_secret_name: reference to a aws secrets manager name
    :type aws_secret_name: string
    :param aws_conn_id: reference to a specific aws connection
    :type aws_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'
    hook = None

    @apply_defaults
    def __init__(
            self,
            sql,
            aws_secret_name,
            aws_conn_id='aws_default',
            autocommit=False,
            parameters=None,
            database=None,
            host=None,
            **kwargs):
        super(PostgresWithSecretsManagerCredentialsOperator, self).__init__(**kwargs)
        self.sql = sql
        self.aws_conn_id = aws_conn_id
        self.aws_secret_name = aws_secret_name
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.host = host

    def execute(self, context):
        self.log.info('Executing query: {}'.format(self.sql))
        self.hook = PostgresWithSecretsManagerCredentialsHook(
            aws_conn_id=self.aws_conn_id,
            aws_secret_name=self.aws_secret_name,
            schema=self.database,
            host=self.host
        )
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)


class PostgresToPostgresOperator(BaseOperator):
    """
    Executes sql code in a Postgres database and inserts into another

    :param src_postgres_conn_id: reference to the source postgres database
    :type src_postgres_conn_id: string
    :param dest_postgress_conn_id: reference to the destination postgres database
    :type dest_postgress_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param parameters: a parameters dict that is substituted at query runtime.
    :type parameters: dict
    """

    template_fields = ('sql', 'parameters', 'pg_table', 'pg_preoperator', 'pg_postoperator')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            pg_table,
            src_postgres_conn,
            dest_postgres_conn,
            pg_preoperator=None,
            pg_postoperator=None,
            parameters=None,
            *args, **kwargs):
        super(PostgresToPostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.pg_table = pg_table
        self.src_postgres_conn = src_postgres_conn
        self.dest_postgres_conn = dest_postgres_conn
        self.pg_preoperator = pg_preoperator
        self.pg_postoperator = pg_postoperator
        self.parameters = parameters

    def execute(self, context):
        self.log.info('Executing: ' + str(self.sql))

        # Source Postgres connection info (from AWS Secrets Manager)
        src_pg = PostgresWithSecretsManagerCredentialsHook(
            aws_conn_id=self.src_postgres_conn['aws_conn_id'],
            aws_secret_name=self.src_postgres_conn['aws_secret_name'],
            schema=self.src_postgres_conn.pop("database", None),
            host=self.src_postgres_conn.pop("host", None)
        )

        # Destination Postgres connection info (from AWS Secrets Manager)
        dest_pg = PostgresWithSecretsManagerCredentialsHook(
            aws_conn_id=self.dest_postgres_conn['aws_conn_id'],
            aws_secret_name=self.dest_postgres_conn['aws_secret_name'],
            schema=self.src_postgres_conn.pop("database", None),
            host=self.src_postgres_conn.pop("host", None)
        )

        self.log.info("Transferring Postgres query results into other Postgres database.")
        conn = src_pg.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)

        if self.pg_preoperator:
            self.log.info("Executint Postgres preoperator")
            dest_pg.run(self.pg_preoperator)

        fetched_cursor = cursor.fetchall()
        total_rows = len(fetched_cursor)

        if total_rows == 0:
            self.log.info("No rows fetched")
            return

        self.log.info("Inserting rows into Postgres")
        dest_pg.insert_rows(table=self.pg_table, rows=fetched_cursor)

        if self.pg_postoperator:
            self.log.info("Running Postgres postoperator")
            dest_pg.run(self.pg_postoperator)

        self.log.info("Done.")
