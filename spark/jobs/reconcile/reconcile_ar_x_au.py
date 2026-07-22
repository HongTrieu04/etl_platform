"""
Reconcile job: Compare Oracle target vs MinIO SOR for AR_X_AU.
"""

import logging
from typing import List
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, current_timestamp

from jobs.utils.spark_session import build_spark
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.oracle_reader import OracleReader
from jobs.utils.minio_writer import write_parquet
from jobs.utils.hash_function import dataframe_checksum
from jobs.common.constants import TABLE_GAM_TDY, SOR_AR_X_AU
from jobs.config.job_config import JobConfig


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        logger.info(f"Starting Reconcile job for {SOR_AR_X_AU}. ETL Date: {args.etl_date}")

        spark = build_spark("reconcile_ar_x_au")
        config = JobConfig()
        oracle_reader = OracleReader(spark)
        
        # Read from Oracle
        logger.info(f"Reading source data from Oracle table: {TABLE_GAM_TDY}")
        df_oracle = oracle_reader.read_table(TABLE_GAM_TDY, partition_column=None, num_partitions=4)
        
        # Map Oracle column to match SOR for simplistic reconciliation checks
        df_oracle_mapped = df_oracle.select(col("ACID").alias("ACCOUNT_ID"))
        
        # Read from MinIO SOR
        sor_bucket = config.sor_bucket
        sor_path = f"s3a://{sor_bucket}/{SOR_AR_X_AU}/"
        logger.info(f"Reading target data from MinIO: {sor_path}")
        df_sor = spark.read.parquet(sor_path)
        
        results: List[Row] = []

        # 1. Row Count
        count_oracle = df_oracle.count()
        count_sor = df_sor.count()
        count_diff = count_oracle - count_sor
        status = "PASS" if count_diff == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Row Count",
            STATUS=status,
            DETAILS=f"Oracle: {count_oracle}, SOR: {count_sor}, Diff: {count_diff}",
            CHECK_TIMESTAMP=None
        ))

        # 2. Duplicate Key
        duplicate_count = df_sor.groupBy("ACCOUNT_ID").count().filter(col("count") > 1).count()
        status = "PASS" if duplicate_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Duplicate Key",
            STATUS=status,
            DETAILS=f"Found {duplicate_count} duplicate ACCOUNT_IDs",
            CHECK_TIMESTAMP=None
        ))

        # 3. Null Key
        null_count = df_sor.filter(col("ACCOUNT_ID").isNull()).count()
        status = "PASS" if null_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Null Key",
            STATUS=status,
            DETAILS=f"Found {null_count} null ACCOUNT_IDs",
            CHECK_TIMESTAMP=None
        ))

        # 4. Hash Checksum
        oracle_hash = dataframe_checksum(df_oracle_mapped, ["ACCOUNT_ID"])
        sor_hash = dataframe_checksum(df_sor, ["ACCOUNT_ID"])
        status = "PASS" if oracle_hash == sor_hash else "FAIL"
        results.append(Row(
            CHECK_NAME="Hash Checksum",
            STATUS=status,
            DETAILS=f"Oracle Hash: {oracle_hash}, SOR Hash: {sor_hash}",
            CHECK_TIMESTAMP=None
        ))

        # 5. Missing Records (Oracle has but SOR doesn't)
        missing_count = df_oracle_mapped.join(df_sor, on="ACCOUNT_ID", how="left_anti").count()
        status = "PASS" if missing_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Missing Records",
            STATUS=status,
            DETAILS=f"Found {missing_count} records in Oracle missing in SOR",
            CHECK_TIMESTAMP=None
        ))

        # 6. Extra Records (SOR has but Oracle doesn't)
        extra_count = df_sor.join(df_oracle_mapped, on="ACCOUNT_ID", how="left_anti").count()
        status = "PASS" if extra_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Extra Records",
            STATUS=status,
            DETAILS=f"Found {extra_count} extra records in SOR not in Oracle",
            CHECK_TIMESTAMP=None
        ))

        # Create Summary DataFrame
        df_summary = spark.createDataFrame(results)
        df_summary = df_summary.withColumn("CHECK_TIMESTAMP", current_timestamp())

        # Log results
        logger.info("Reconciliation Results:")
        for row in df_summary.collect():
            logger.info(f"[{row.STATUS}] {row.CHECK_NAME}: {row.DETAILS}")

        # Write to Reconcile Zone
        reconcile_bucket = config.reconcile_bucket
        output_path = f"{SOR_AR_X_AU}/"
        logger.info(f"Writing summary to s3a://{reconcile_bucket}/{output_path}")
        write_parquet(df_summary, reconcile_bucket, output_path, mode='overwrite')
        
        logger.info(f"Successfully completed Reconcile job for {SOR_AR_X_AU}")

    except Exception as e:
        logger.error(f"Reconcile job failed: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    main()
