import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def kill_streamlit():
    # killing it directly with a bash command returns a -15, which airflow interprets as the task itself failing
    try:
        subprocess.run(["pkill", "-f", "streamlit run"], check=False)
        print("Streamlit process killed")
    except Exception as e:
        print(f"Failed to kill Streamlit: {e}")


def restart_streamlit():
    # Popen starts the process without waiting for it to finish
    # start_new_session=True detaches Streamlit from Airflow’s process, so it doesn’t get killed when the task finishes
    subprocess.Popen(
        ["/opt/venv_pystreamlit/bin/python", "-m", "streamlit", "run", "/root/dashboard/dashboard.py", 
         "--server.port=8501"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True
    )
    print("Streamlit restarted successfully")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 14),
    "retries": 3
}

RestartStreamlitProcess = DAG(
    'restart_streamlit_process',
    default_args=default_args,
    schedule_interval="0 22 * * 0",
    catchup=False,
)

WaitForRunProdModels = ExternalTaskSensor(
    task_id="wait_for_run_prod_models",
    external_dag_id="run_prod_models",
    external_task_id="run_prod_models_task",
    mode="poke",
    timeout=1200,
    poke_interval=20,
    dag=RestartStreamlitProcess
)

KillStreamlit = PythonOperator(
    task_id='kill_streamlit',
    python_callable=kill_streamlit,
    dag=RestartStreamlitProcess,
)

RestartStreamlit = PythonOperator(
    task_id='restart_streamlit',
    python_callable=restart_streamlit,
    dag=RestartStreamlitProcess,
)

WaitForRunProdModels >> KillStreamlit >> RestartStreamlit
