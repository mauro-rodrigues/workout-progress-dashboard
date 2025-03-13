import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def run_prod_models():
    # using BashOperator was causing issues with not marking the task as failed when running into errors
    cmd = [
        "/bin/bash", "-c",
        "source /opt/venv_pydbt/bin/activate && cd /root/etls/workout_journal_dbt/ && dbt run --select models/prod/*"
    ]
    
    process = subprocess.run(cmd, shell=False, check=True, text=True, capture_output=True)
    
    print("DBT Output:", process.stdout)
    print("DBT Errors:", process.stderr)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 14),
    "retries": 1
}

RunProdModels = DAG(
    "run_prod_models",
    default_args=default_args,
    schedule_interval="0 22 * * *",
    catchup=False
)

WaitForRunIntermediateModels = ExternalTaskSensor(
    task_id="wait_for_run_intermediate_models",
    external_dag_id="run_intermediate_models",
    external_task_id="run_intermediate_models_task",
    mode="poke",
    timeout=1200,
    poke_interval=20,
    dag=RunProdModels
)

RunProdModelsTask = PythonOperator(
    task_id="run_prod_models_task",
    python_callable=run_prod_models,
    dag=RunProdModels
)

WaitForRunIntermediateModels >> RunProdModelsTask
