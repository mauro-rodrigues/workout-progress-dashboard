#!/bin/bash

# activate the airflow virtual environment
. /opt/venv_pyairflow/bin/activate

# upgrade the Airflow DB (perhaps use migrate here)
airflow db upgrade

# check if the admin user already exists, and if not, create it
if ! airflow users list | grep -q 'admin'; then
    echo "Creating Airflow admin user..."
    airflow users create \
      --username ${AIRFLOW_USER} \
      --password ${AIRFLOW_PASSWORD} \
      --firstname Airflow \
      --lastname Admin \
      --role Admin \
      --email ${AIRFLOW_EMAIL}
else
    echo "Admin user already exists, skipping creation."
fi

# disable remote logging completely (logs are not required for this project)
sed -i 's/^remote_logging = .*/remote_logging = False/' /root/airflow/airflow.cfg

# reduce scheduler polling to once every 6 hours (these polls add up when it comes to Supabase egress)
sed -i 's/^scheduler_heartbeat_sec = .*/scheduler_heartbeat_sec = 300/' /root/airflow/airflow.cfg  # 5 minutes
sed -i 's/^min_file_process_interval = .*/min_file_process_interval = 21600/' /root/airflow/airflow.cfg  # 6 hours

# reduce orphaned task checking frequency
sed -i 's/^orphaned_tasks_check_interval = .*/orphaned_tasks_check_interval = 21600/' /root/airflow/airflow.cfg  # 6 hours

# cleanup old XCom records weekly
sed -i 's/^xcom_cleanup_interval = .*/xcom_cleanup_interval = 604800/' /root/airflow/airflow.cfg  # 7 days

echo "Optimized airflow.cfg to reduce database egress."

# start the Airflow Scheduler in the background
echo "Starting Airflow scheduler..."
airflow scheduler &

# activate the Streamlit virtual environment
. /opt/venv_pystreamlit/bin/activate

# start the Streamlit dashboard and wait, so that the process can be killed and rerun without stopping the container
# usually the first thing to run gets a PID of 1, which causes the container to stop if you kill it
# and streamlit needs to be restarted when the DAGs are executed
# this way I avoid having to use a refresh button on the dashboard which can be misused
# or having a cache timeout, which would refresh the data several times per day
# when the DAGs only run once per day, which would make no sense
echo "Starting Streamlit dashboard..."
exec streamlit run /root/dashboard/dashboard.py --server.port=8501 &
wait