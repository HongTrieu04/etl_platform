from datetime import datetime

from pyspark.sql import functions as F

from jobs.utils.spark_session import build_spark


def main() -> None:
    spark = build_spark("sor_reconcile")
    source_path = "s3a://sor/source_table"
    target_path = "s3a://reconcile/date"

    # Placeholder reconciliation summary (row count snapshot).
    df = spark.read.parquet(source_path)
    summary = df.agg(F.count("*").alias("row_count")).withColumn("run_date", F.lit(datetime.utcnow().date().isoformat()))
    summary.write.mode("append").json(target_path)
    spark.stop()


if __name__ == "__main__":
    main()
