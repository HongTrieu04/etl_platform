from jobs.utils.spark_session import build_spark


def main() -> None:
    spark = build_spark("raw_to_stg")
    source_path = "s3a://raw/source_table"
    target_path = "s3a://stg/source_table"

    # Placeholder transformation for STG zone.
    df = spark.read.parquet(source_path)
    df.dropDuplicates().write.mode("overwrite").parquet(target_path)
    spark.stop()


if __name__ == "__main__":
    main()
