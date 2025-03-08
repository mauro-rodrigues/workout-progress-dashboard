import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def run_intermediate_models():
    # using BashOperator was causing issues with not marking the task as failed when running into errors
    cmd = [
        "/bin/bash", "-c",
        "source /opt/venv_pydbt/bin/activate && cd /root/etls/workout_journal_dbt/ && dbt run --select models/intermediate/*"
    ]
    
    process = subprocess.run(cmd, shell=False, check=True, text=True, capture_output=True)
    
    print("DBT Output:", process.stdout)
    print("DBT Errors:", process.stderr)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 14),
    "retries": 1
}

RunIntermediateModels = DAG(
    "run_intermediate_models",
    default_args=default_args,
    schedule_interval="0 22 * * 0",
    catchup=False
)

WaitForLoadWorkoutInformation = ExternalTaskSensor(
    task_id="wait_for_load_workout_information",
    external_dag_id="load_workout_information",
    external_task_id="elt_workout_sheet",
    mode="poke",
    timeout=1200,
    poke_interval=20,
    dag=RunIntermediateModels
)

RunIntermediateModelsTask = PythonOperator(
    task_id="run_intermediate_models_task",
    python_callable=run_intermediate_models,
    dag=RunIntermediateModels
)

WaitForLoadWorkoutInformation >> RunIntermediateModelsTask
