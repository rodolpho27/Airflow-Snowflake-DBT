from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostegresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,1,20),
    'email_on_retry':False,
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def postgres_to_snowflake_etl():
    table_names = ['veiculos','estados','cidades','concessionarias','vendedores','clientes','vendas']

    for table_name in table_names:
        @task(task_id=f'get_max_id{table_name}')
        def get_max_primary_key(table_name:str):
            with SnowflakeHook(Snowflake_conn_id='snowflake').get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f'SELECT MAX(ID_({table_name}) FROM {table_name})')
                    max_id = cursor.fetchone()[0]
                    return max_id if max_id is not None else 0