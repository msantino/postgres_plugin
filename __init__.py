from airflow.plugins_manager import AirflowPlugin

from postgres_plugin.operators.postgres_operator import PostgresOperator
from postgres_plugin.hooks.postgres_hook import PostgresHook
from postgres_plugin.hooks.aws_secrets_manager_hook import AwsSecretsManagerHook


class PostgresPlugin(AirflowPlugin):
    name = "postgres_plugin"
    operators = [PostgresOperator]
    hooks = [PostgresHook, AwsSecretsManagerHook]
