import os
import glob
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 23)
    
}

SILVER_SQL = "/opt/airflow/runner/jobs/sql/silver/{}/*.sql"
GOLD_SQL = "/opt/airflow/runner/jobs/sql/gold/*.sql"

with DAG(
    'transform_layers',
    default_args=default_args,
    schedule_interval=None,  # Set to None for on demand runs
    description='Transform layers of from bronze to silver and gold',
    max_active_tasks=1, # Limit the number of concurrent tasks because of device spec limitation, should be able to run parallel in proper cluster
    catchup=False
) as dag:
    start = EmptyOperator(task_id='start')

    # run bronze to silver transformations
    with TaskGroup("transform_bronze_to_silver") as transform_bronze_to_silver:
        for media in ['movies', 'series']:
            with TaskGroup(f"{media}_bronze_to_silver") as bronze_to_silver:
                for sql_file in glob.glob(SILVER_SQL.format(media)):
                    sql_filename = os.path.basename(sql_file).split('.')[0]
                    transform = BashOperator(
                        task_id=f"transform_{sql_filename}",
                        bash_command=f"trino --server=http://trino:8081 --catalog=iceberg --file={sql_file}",
                    )

    # run silver to gold transformations
    with TaskGroup("transform_silver_to_gold") as transform_silver_to_gold:
        for sql_file in glob.glob(GOLD_SQL):
            sql_filename = os.path.basename(sql_file).split('.')[0]
            transform = BashOperator(
                task_id=f"transform_{sql_filename}",
                bash_command=f"trino --server=http://trino:8081 --catalog=iceberg --file={sql_file}",
            )

    end = EmptyOperator(task_id='end')

    chain(start, transform_bronze_to_silver, transform_silver_to_gold, end)