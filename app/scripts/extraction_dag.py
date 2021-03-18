import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from dotenv import load_dotenv

load_dotenv()

airflow_home = os.getenv("AIRFLOW_HOME")
dataset_name = os.getenv("TF_VAR_DATASET_ID")
bucket_name = os.getenv("TF_VAR_BUCKET_NAME")

project_path = f"{airflow_home}/dags/app/scripts"
tmp_path = f"{project_path}/data/tmp"
raw_path = f"{project_path}/data/raw"
tmp_filepath = "{tmp_path}/{year}-{month}.zip"
raw_filepath = "{raw_path}/basica{year}-{month}.txt"

# base_url = "https://www.anac.gov.br/assuntos/setor-regulado/empresas/envio-de-informacoes/microdados"
base_url = "https://www.gov.br/anac/pt-br/assuntos/regulados/empresas-aereas/envio-de-informacoes/microdados"
request_raw_data_fmt = "curl {base_url}/basica{year}-{month}.zip -o {tmp_filepath}"
unzip_file_fmt = "unzip -o {tmp_filepath} -d {raw_path}"


with DAG(
    dag_id="ExtractionDAG",
    schedule_interval="@monthly",
    start_date=datetime(year=2000, month=1, day=1),
    max_active_runs=1,
    default_args={"retries": 20},
) as dag:
    request_raw_data = BashOperator(
        task_id="request_raw_data",
        bash_command=request_raw_data_fmt.format(
            base_url=base_url,
            year="{{ execution_date.year }}",
            month="{{ execution_date.strftime('%m') }}",
            tmp_filepath=tmp_filepath.format(
                tmp_path=tmp_path,
                year="{{ execution_date.year }}",
                month="{{ execution_date.strftime('%m') }}",
            ),
        ),
    )

    unzip_file = BashOperator(
        task_id="unzip_file",
        bash_command=unzip_file_fmt.format(
            year="{{ execution_date.year }}",
            month="{{ execution_date.strftime('%m') }}",
            raw_path=raw_path,
            tmp_filepath=tmp_filepath.format(
                tmp_path=tmp_path,
                year="{{ execution_date.year }}",
                month="{{ execution_date.strftime('%m') }}",
            ),
        ),
    )

    raw_upload = PythonOperator(
        task_id="raw_upload",
        python_callable=GCSHook().upload,
        op_kwargs={
            "bucket_name": bucket_name,
            "object_name": (
                "raw_data/{{ execution_date.year }}/"
                "{{ execution_date.strftime('%m') }}.csv"
            ),
            "filename": raw_filepath.format(
                raw_path=raw_path,
                year="{{ execution_date.year }}",
                month="{{ execution_date.strftime('%m') }}",
            ),
        },
    )

    request_raw_data >> unzip_file >> raw_upload
