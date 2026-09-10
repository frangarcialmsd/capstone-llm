import argparse
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, when

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)


def write_per_question(cleaned, output: str):
    parsed = urlparse(output)
    if parsed.scheme in ("s3", "s3a"):
        import boto3

        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/")
        if not bucket:
            raise ValueError("S3 output path must include a bucket")
        if not prefix:
            raise ValueError("S3 output path must include a prefix")
        client = boto3.client("s3")
        for row in cleaned.toJSON().toLocalIterator():
            document = json.loads(row)
            key = f"{prefix}/{document['question_id']}.json"
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=(json.dumps(document) + "\n").encode("utf-8"),
                ContentType="application/json",
            )
        return

    if parsed.scheme:
        raise ValueError(f"Unsupported output path scheme: {parsed.scheme}")

    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)
    for row in cleaned.toJSON().toLocalIterator():
        document = json.loads(row)
        with (output_path / f"{document['question_id']}.json").open("w", encoding="utf-8") as file:
            json.dump(document, file)
            file.write("\n")

def clean(
    spark: SparkSession,
    environment: str,
    tag: str,
    output: str | None = None,
    limit: int | None = None,
    input_path: str = ".",
):
    input_prefix = (
        f"s3a://{llm_bucket}/input/{tag}"
        if environment != "local"
        else input_path
    )

    questions = (
        spark.read
        .option("multiLine", True)
        .json(f"{input_prefix}/questions.json")
        .selectExpr("explode(items) as question")
        .select("question.*")
    )
    if limit is not None:
        questions = questions.limit(limit)

    answers = (
        spark.read
        .option("multiLine", True)
        .json(f"{input_prefix}/answers.json")
        .selectExpr("explode(items) as answer")
        .select("answer.*")
    )

    cleaned = (
        questions.join(answers, on="question_id", how="left")
        .groupBy(
            "question_id",
            questions.title,
            questions.body,
            questions.link,
        )
        .agg(
            collect_list(
                when(
                    answers.answer_id.isNotNull(),
                    struct(
                        answers.answer_id.alias("answer_id"),
                        answers.body.alias("answer"),
                    ),
                )
            ).alias("answers")
        )
        .select(
            "question_id",
            questions.title.alias("title"),
            questions.body.alias("question"),
            questions.link.alias("link"),
            "answers",
        )
    )

    cleaned.show(5, truncate=False)
    print("Rows:", cleaned.count())
    if output:
        write_per_question(cleaned, output)

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
    parser.add_argument(
        "-o", "--output", dest="output", help="output path for joined JSON files",
        default=None, required=False
    )
    parser.add_argument(
        "--limit", type=int, help="maximum number of questions to process",
        default=None, required=False
    )
    parser.add_argument(
        "--input", dest="input_path", help="local directory containing questions.json and answers.json",
        default=".", required=False
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
        clean(session, args.env, args.tag, args.output, args.limit, args.input_path)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag, args.output, args.limit, args.input_path)


if __name__ == "__main__":
    main()
