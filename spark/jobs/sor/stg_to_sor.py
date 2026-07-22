from jobs.utils.spark_session import build_spark


def main() -> None:
    spark = build_spark("stg_to_sor")
    source_path = "s3a://stg/source_table"
    target_path = "s3a://sor/source_table"

    # Placeholder SOR load. Replace with merge/upsert semantics per table.
    df = spark.read.parquet(source_path)
    df.write.mode("overwrite").parquet(target_path)
    spark.stop()


if __name__ == "__main__":
    main()
