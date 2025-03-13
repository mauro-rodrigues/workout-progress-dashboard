import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_workout_journal():
    # using BashOperator was causing issues with not marking the task as failed when running into errors
    cmd = [
        "/bin/bash", "-c",
        "source /opt/venv_pydbt/bin/activate && python /root/etls/workout_journal/workout_journal.py"
    ]
    
    try:
        process = subprocess.run(cmd, shell=False, check=True, text=True, capture_output=True)
        # check if the process returned a non-zero code
        if process.returncode != 0:
            raise Exception(f"Script failed with return code {process.returncode}")
        print("Script Output:", process.stdout)
        print("Script Errors:", process.stderr)
    except subprocess.CalledProcessError as e:
        # this will ensure that any subprocess errors are caught and reported by Airflow
        print(f"Error running script: {e}")
        raise Exception(f"Error in running workout journal: {e}")
    except Exception as e:
        # catch any other exceptions and raise them
        print(f"General error: {e}")
        raise e


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 14),
    "retries": 3
}

LoadWorkoutInformation = DAG(
    "load_workout_information",
    default_args=default_args,
    schedule_interval="0 22 * * *",
    catchup=False
)

ELTWorkoutSheet = PythonOperator(
    task_id="elt_workout_sheet",
    python_callable=run_workout_journal,
    dag=LoadWorkoutInformation
)

ELTWorkoutSheet