# -*- coding: utf-8 -*-
"""
Plugin to store some usual PostgreSQL functions

"""
# pylint: disable=import-error,missing-docstring,too-few-public-methods
from airflow.plugins_manager import AirflowPlugin

from postgres_plugin.operators.postgres_operator import PostgresOperator
from postgres_plugin.hooks.postgres_hook import PostgresHook


class PostgresPlugin(AirflowPlugin):
    name = "postgres_plugin"
    operators = [PostgresOperator]
    hooks = [PostgresHook]
