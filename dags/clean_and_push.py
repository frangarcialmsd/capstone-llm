import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
SOURCE_BUCKET = "dataminded-academy-capstone-llm-data"


@task
def source_tags():
	import boto3

	client = boto3.client("s3", region_name=AWS_DEFAULT_REGION)
	paginator = client.get_paginator("list_objects_v2")
	datasets = {}
	for page in paginator.paginate(
		Bucket=SOURCE_BUCKET,
		Prefix="input/",
	):
		for object_data in page.get("Contents", []):
			key = object_data["Key"]
			if key.endswith("/questions.json"):
				dataset = key.removeprefix("input/").removesuffix("/questions.json")
				datasets.setdefault(dataset, set()).add("questions")
			elif key.endswith("/answers.json"):
				dataset = key.removeprefix("input/").removesuffix("/answers.json")
				datasets.setdefault(dataset, set()).add("answers")

	tags = sorted(
		dataset
		for dataset, files in datasets.items()
		if files == {"questions", "answers"} and "," not in dataset
	)

	return [
		[
			"--env",
			"production",
			"--tag",
			tag,
			"--output",
			f"s3://{SOURCE_BUCKET}/cleaned/{tag}",
		]
		for tag in tags
	]


with DAG(
	dag_id="clean_and_push",
	start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
	schedule="@daily",
	catchup=False,
	tags=["capstonellm", "s3"],
) as dag:
	clean_and_push = DockerOperator.partial(
		task_id="clean_and_push",
		image="capstonellm:local",
		docker_url="unix://var/run/docker.sock",
		network_mode="bridge",
		mount_tmp_dir=False,
		auto_remove="force",
		environment={
			"AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
			"AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
			"AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
		},
	).expand(command=source_tags())
