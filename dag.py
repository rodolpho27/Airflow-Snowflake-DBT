from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostegresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

