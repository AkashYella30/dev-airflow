from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import json
import logging
import os
import base64
from include.helper import *

logger = logging.getLogger(__name__)

# Load configuration
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
config = read_config_file(config_path, logger)

ENV = get_env()
DAG_ID = f"poc_demo_finalv1_{ENV}"
SEPARATOR = '-' * 50

default_args = {    
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='POC Demo Final',
    schedule_interval='@once',
    start_date=days_ago(0),
    max_active_runs=1,
    tags=['poc'],
)

def display_config_vars():
    """Display configuration variables for debugging."""
    logger.info(SEPARATOR)
    logger.info(f"ENV: {ENV}")
    logger.info(f"config: {config}")
    logger.info(SEPARATOR)


def extract_file_names(**kwargs):
    """Process messages (no branching needed)."""
    ti = kwargs["ti"]
    messages = ti.xcom_pull(task_ids="wait_for_message")
    file_list = []

    for message in messages:
        data = json.loads(base64.b64decode(message["message"]["data"]).decode("utf-8"))
        if (filename := data.get("name")) and (bucket := data.get("bucket")):
            file_list.append(f"gs://{bucket}/{filename}")

    if not file_list:
        logger.error("No valid files in messages (unexpected!)")
        raise ValueError("PubSub messages contained no valid files")

    ti.xcom_push(key="file_list", value=file_list)

def get_overrides(**kwargs):
    """
    Prepare Cloud Run job overrides.
    Includes fallback command when no files are present.
    """
    ti = kwargs["ti"]
    file_list = ti.xcom_pull(task_ids="extract_file_names", key="file_list") or []
    
    command = ["python", "main.py"]
    if file_list:
        command.extend(file_list)
        logger.info(f"Preparing to process files: {file_list}")
    else:
        command.append("--no-files")
        logger.info("No files to process - using fallback command")

    return {
        "container_overrides": [{
            "name": "python-cloud-run-job",
            "args": command,
            "env": [{"name": "ENVIRONMENT", "value": ENV}],
        }],
        "task_count": 1,
        "timeout": "300s" if file_list else "60s",
    }

# Define tasks
display_config_vars_task = PythonOperator(    
    task_id='display_config_vars',
    python_callable=display_config_vars,
    dag=dag,
)

wait_for_message_task = PubSubPullSensor(
    task_id='wait_for_message',
    project_id=config[ENV]["project_id"],
    subscription=config[ENV]['subscription'],
    max_messages=config[ENV]['pubsub_max_messages'],
    gcp_conn_id=config[ENV]['gcp_conn_id'],
    poke_interval=config[ENV]['pubsub_poke_interval'],
    ack_messages=True,
    mode="reschedule",
    dag=dag,
)

extract_file_names_task = PythonOperator(
    task_id="extract_file_names",
    python_callable=extract_file_names,
    dag=dag,
)

prepare_overrides = PythonOperator(
    task_id="prepare_overrides",
    python_callable=get_overrides,
    dag=dag,
)

trigger_self = TriggerDagRunOperator(
    task_id='trigger_self',
    trigger_dag_id=DAG_ID,
    wait_for_completion=False,
    reset_dag_run=True,
    dag=dag,
)

trigger_cloud_run = CloudRunExecuteJobOperator(
    task_id="trigger_cloud_run",
    project_id=config[ENV]["project_id"],
    # region=config[ENV]["region"],
    # job_name=config[ENV]["job_name"],
    gcp_conn_id=config[ENV]['gcp_conn_id'],
    region="us-central1",
    job_name="python-cloud-run-job",

    overrides=prepare_overrides.output,
    dag=dag,
)

# # Set up dependencies
# display_config_vars_task >> wait_for_message_task >> extract_file_names_task
# extract_file_names_task >> [prepare_overrides, trigger_self]
# prepare_overrides >> trigger_cloud_run


display_config_vars_task >> wait_for_message_task >> extract_file_names_task >> prepare_overrides >> trigger_cloud_run>>trigger_self