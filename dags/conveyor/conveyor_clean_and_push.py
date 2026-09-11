from datetime import datetime, timedelta, timezone

from airflow import DAG
from conveyor.operators import ConveyorContainerOperatorV2

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2026, 1, 1, tzinfo=timezone.utc),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="conveyor_clean_and_push",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["capstonellm", "s3", "conveyor"],
) as dag:
    ConveyorContainerOperatorV2(
        task_id="clean_and_push",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        cmds=["python3", "-m", "capstonellm.tasks.clean_all"],
        arguments=["--env", "production"],
    )