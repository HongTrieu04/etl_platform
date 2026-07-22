import logging
import os
import sys

from pyspark.sql import SparkSession


def configure_logging(app_name: str) -> None:
    """Configure basic logging for the Spark application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stdout
    )


def build_spark(app_name: str) -> SparkSession:
    """Create a SparkSession with settings ready for MinIO and Delta."""
    configure_logging(app_name)
    logger = logging.getLogger(app_name)
    logger.info("Creating SparkSession for %s...", app_name)

    minio_access_key = os.getenv("MINIO_ROOT_USER", "")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    logger.info("SparkSession created.")
    return spark
