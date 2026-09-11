import argparse

import boto3
from pyspark.sql import SparkSession

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
from capstonellm.tasks.clean import clean


def source_tags(bucket: str) -> list[str]:
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    datasets = {}

    for page in paginator.paginate(Bucket=bucket, Prefix="input/"):
        for object_data in page.get("Contents", []):
            key = object_data["Key"]
            if key.endswith("/questions.json"):
                dataset = key.removeprefix("input/").removesuffix("/questions.json")
                datasets.setdefault(dataset, set()).add("questions")
            elif key.endswith("/answers.json"):
                dataset = key.removeprefix("input/").removesuffix("/answers.json")
                datasets.setdefault(dataset, set()).add("answers")

    return sorted(
        dataset
        for dataset, files in datasets.items()
        if files == {"questions", "answers"} and "," not in dataset
    )


def clean_all(spark: SparkSession, environment: str, output_root: str):
    tags = source_tags(llm_bucket)
    print(f"Discovered tags: {', '.join(tags)}")

    for tag in tags:
        clean(
            spark,
            environment=environment,
            tag=tag,
            output=f"{output_root.rstrip('/')}/{tag}",
        )


def main():
    parser = argparse.ArgumentParser(description="Clean every complete dataset in S3")
    parser.add_argument("--env", default="production")
    parser.add_argument(
        "--output-root",
        default=f"s3://{llm_bucket}/cleaned",
        help="Root output path; one directory is created per discovered tag",
    )
    args = parser.parse_args()

    spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    with ClosableSparkSession("capstone_llm", spark_config=spark_config) as spark:
        clean_all(spark, args.env, args.output_root)


if __name__ == "__main__":
    main()