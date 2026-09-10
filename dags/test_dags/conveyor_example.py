from datetime import datetime, timedelta

from airflow import DAG
from conveyor.operators import (
    ConveyorContainerOperatorV2,
    ConveyorSparkSubmitOperatorV2,
)

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "docker_operator_dag_<name>",
    default_args=default_args,
    catchup=False,
) as dag:
    ConveyorContainerOperatorV2(
        dag=dag,
        task_id="clean",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        cmds=["your_container_command"],
        arguments=["your_container_arguments"],
    )