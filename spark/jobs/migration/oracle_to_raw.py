from datetime import datetime

from pyspark.sql import functions as F

from jobs.utils.spark_session import build_spark


def main() -> None:
    spark = build_spark("migration_oracle_to_raw")

    # Placeholder extraction frame. Replace with JDBC extraction from Oracle.
    df = spark.createDataFrame(
        [(1, "sample", datetime.utcnow())],
        ["id", "name", "extracted_at"],
    )

    partition_value = datetime.utcnow().strftime("%Y-%m-%d")
    output_path = f"s3a://raw/source_table/partition={partition_value}"

    df.withColumn("ingestion_date", F.lit(partition_value)).write.mode("append").parquet(output_path)
    spark.stop()


if __name__ == "__main__":
    main()
