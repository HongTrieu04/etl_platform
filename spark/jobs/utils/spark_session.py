import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    """Create a SparkSession with settings ready for MinIO and Delta."""
    minio_access_key = os.getenv("MINIO_ROOT_USER", "")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
