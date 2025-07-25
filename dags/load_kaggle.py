import os
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

with DAG(
    'load_kaggle',
    default_args=default_args,
    schedule_interval=None,  # Set to None for on demand runs
    description='Load Kaggle dataset and save as Iceberg table',
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id='start')

    with TaskGroup("prepare_data") as download_and_prepare_data:
        # download kaggle dataset using curl
        download_kaggle_data = BashOperator(
            task_id='download',
            bash_command='curl -sL -o /tmp/tmdb-movies-and-series.zip https://www.kaggle.com/api/v1/datasets/download/edgartanaka1/tmdb-movies-and-series'
        )

        # quietly unzip the downloaded file
        unzip_kaggle_data = BashOperator(
            task_id='unzip',
            bash_command='unzip -qq /tmp/tmdb-movies-and-series.zip -d /tmp/tmdb-movies-and-series'
        )

        # remove zip file after extraction
        remove_zip_file = BashOperator(
            task_id='rm-zip',
            bash_command='rm /tmp/tmdb-movies-and-series.zip'
        )

        prepared = []
        for media in ['movies', 'series']:
            # convert list of json files to a single large newline delimited json
            with TaskGroup(f"{media}") as staging:
                convert_to_ndjson = BashOperator(
                    task_id=f'convert',
                    bash_command=f"find /tmp/tmdb-movies-and-series/{media}/{media} -maxdepth 1 -name '*.json' -print0 | xargs -0 jq -c . > /tmp/{media}.ndjson"
                )

                load_to_object_storage = BashOperator(
                    task_id='load_to_object_storage',
                    bash_command='/opt/airflow/runner/jobs/utils/upload_to_minio.sh ${BUCKET} ${FILE_PATH} ${OBJ_NAME} ${CONTENT_TYPE}',
                    env={
                        'FILE_PATH': f"/tmp/{media}.ndjson",
                        'CONTENT_TYPE': 'application/x-ndjson',
                        'OBJ_NAME': f"landing/{media}.ndjson",
                        'BUCKET': 'warehouse'
                    },
                    append_env=True,
                    retry_delay=60,
                    retries=3
                )

                chain(convert_to_ndjson, load_to_object_storage)

            prepared.append(staging)

        cleanup = BashOperator(
            task_id='cleanup',
            bash_command='rm -rf /tmp/tmdb-movies-and-series && rm -rf /tmp/*.ndjson',
            trigger_rule=TriggerRule.ALL_DONE  # Ensure cleanup runs regardless of previous task success
        )

        chain(download_kaggle_data,
              unzip_kaggle_data,
              [remove_zip_file] + prepared,
              cleanup)
    
    iceberg_tasks = []
    for media in ['movies', 'series']:
        load_to_iceberg = DockerOperator(
            task_id=f'load_{media}_to_iceberg',
            image='job-runner:latest',
            api_version='auto',
            auto_remove='success',
            command=f'python /jobs/jobs/python/extract_kaggle.py --filepath landing/{media}.ndjson --infer_rows 10000 --table_name {media} --write_mode append',
            docker_url='tcp://docker-proxy:2375',
            mount_tmp_dir=False,
            network_mode='iceberg_network',
            private_environment={
                'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
                'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', '')
            }
        )
        iceberg_tasks.append(load_to_iceberg)

    end = EmptyOperator(task_id='end')

    chain(start,
          download_and_prepare_data,
          iceberg_tasks,
          end)