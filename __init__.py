# -*- coding: utf-8 -*-
"""
Plugin to store some usual PostgreSQL functions

"""
# pylint: disable=import-error,missing-docstring,too-few-public-methods
from airflow.plugins_manager import AirflowPlugin

from postgres_plugin.hooks.postgres_hook import PostgresWithSecretsManagerCredentialsHook

from postgres_plugin.operators.postgres_operator import PostgresWithSecretsManagerCredentialsOperator
from postgres_plugin.operators.postgres_operator import PostgresToPostgresOperator

from postgres_plugin.operators.postgres_to_s3_operator import PostgresToS3Operator
from postgres_plugin.operators.postgres_to_s3_operator import S3ToPostgresOperator

from postgres_plugin.operators.postgres_dump_operator import PostgresDumpOperator


class PostgresPlugin(AirflowPlugin):
    name = "postgres_plugin"
    operators = [PostgresWithSecretsManagerCredentialsOperator,
                 PostgresToPostgresOperator,
                 PostgresToS3Operator,
                 S3ToPostgresOperator,
                 PostgresDumpOperator]
    hooks = [PostgresWithSecretsManagerCredentialsHook]
