import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")


with DAG(
	dag_id="clean_and_push",
	start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
	schedule="@daily",
	catchup=False,
	tags=["capstonellm", "s3"],
) as dag:
	clean_and_push = DockerOperator(
		task_id="clean_and_push",
		image="capstonellm:local",
		command=[
			"--env",
			"production",
			"--tag",
			"python-polars",
			"--output",
			"s3://dataminded-academy-capstone-llm-data/cleaned/python-polars",
		],
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		auto_remove="force",
		environment={
			"AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
			"AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
			"AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
		},
	)
