import argparse
import logging

from pyspark.sql import SparkSession

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    input_prefix = (
        f"s3a://{llm_bucket}/input/{tag}"
        if environment != "local"
        else "."
    )

    questions = (
        spark.read
        .option("multiLine", True)
        .json(f"{input_prefix}/questions.json")
        .selectExpr("explode(items) as question")
        .select("question.*")
    )

    answers = (
        spark.read
        .option("multiLine", True)
        .json(f"{input_prefix}/answers.json")
        .selectExpr("explode(items) as answer")
        .select("answer.*")
    )

    cleaned = (
        questions.join(answers, on="question_id", how="inner")
        .select(
            "question_id",
            questions.title.alias("title"),
            questions.body.alias("question"),
            questions.link.alias("link"),
            answers.answer_id.alias("answer_id"),
            answers.body.alias("answer"),
        )
    )

    cleaned.show(5, truncate=False,st)
    print("Rows:", cleaned.count())

    return cleaned

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
