import json
import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import yaml

airflow_home = os.getenv("AIRFLOW_HOME")
bucket_name = os.getenv("TF_VAR_BUCKET_NAME")
project_path = f"{airflow_home}/dags"

dim_tables = [
    "dim_digito_identificador",
    "dim_aeronaves",
    "dim_empresas",
]


with DAG(
    dag_id="StreamingDAG",
    schedule_interval="@once",
    start_date=datetime(year=2000, month=1, day=1),
    max_active_runs=1,
    default_args={"retries": 20},
) as dag:
    task_list = {}
    with open(f"{project_path}/schemas.yaml") as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)

    mock_dim_dates = SparkSubmitOperator(
        task_id="mock_dim_dates",
        application=f"{project_path}/spark/mock_dim_dates.py",
        total_executor_cores=1,
        executor_cores=1,
        num_executors=1,
        executor_memory="500M",
        application_args=[
            f"gs://{bucket_name}/raw_data/",
            f"gs://{bucket_name}/dw/",
            "2000-01-01",
            "2020-12-31",
        ],
    )

    fact_transform = SparkSubmitOperator(
        task_id="create_fact_aircraft_moviments",
        application=f"{project_path}/spark/transform_aircraft_moviments.py",
        name="create_fact_aircraft_moviments",
        total_executor_cores=1,
        executor_cores=1,
        num_executors=1,
        executor_memory="2G",
        application_args=[
            f"gs://{bucket_name}/raw_data/**/*.csv",
            f"gs://{bucket_name}/dw/fact_aircraft_moviments/",
            json.dumps(
                obj=config,
                separators=(",", ":"),
            ),
        ],
    )

    create_dim_aerodromos = SparkSubmitOperator(
        task_id="create_dim_aerodromos",
        application=f"{project_path}/spark/transform_dim_aerodromos.py",
        name="create_dim_aerodromos",
        total_executor_cores=1,
        executor_cores=1,
        num_executors=1,
        executor_memory="1G",
        application_args=[
            f"gs://{bucket_name}/raw_data/**/*.csv",
            f"gs://{bucket_name}/dw",
        ],
    )

    streaming_tasks = [fact_transform, create_dim_aerodromos]

    for table_name in dim_tables:
        task_id = f"create_{table_name}"
        application = f"{project_path}/spark/transform_raw_data.py"

        spark_task = SparkSubmitOperator(
            task_id=task_id,
            application=application,
            name=task_id,
            total_executor_cores=1,
            executor_cores=1,
            num_executors=1,
            executor_memory="1G",
            application_args=[
                f"gs://{bucket_name}/raw_data/**/*.csv",
                f"gs://{bucket_name}/dw",
                table_name,
            ],
        )
        streaming_tasks.append(spark_task)

    mock_dim_dates >> streaming_tasks
