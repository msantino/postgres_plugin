# -*- coding: utf-8 -*-
"""
Execute some query on PostgreSQL database using Secrets Manager to retrieve database credentials.

"""
# pylint: disable=import-error,missing-docstring,too-few-public-methods
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from postgres_plugin.hooks.postgres_hook import PostgresHook


class PostgresOperator(BaseOperator):
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
            **kwargs):
        super(PostgresOperator, self).__init__(**kwargs)
        self.sql = sql
        self.aws_conn_id = aws_conn_id
        self.aws_secret_name = aws_secret_name
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self):
        self.log.info('Executing query: {}'.format(self.sql))
        self.hook = PostgresHook(aws_conn_id=self.aws_conn_id,
                                 aws_secret_name=self.aws_secret_name,
                                 schema=self.database)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
