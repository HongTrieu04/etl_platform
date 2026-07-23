"""
Reconcile job: Quality checks & Data Audit for SOR table AR_X_AU.
"""

import logging
import sys
from typing import List

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, lit

sys.path.append("/opt/spark")

from jobs.common.constants import SOR_AR_X_AU, TABLE_FBM_TDY, TABLE_GAM_TDY
from jobs.config.job_config import JobConfig
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.hash_function import dataframe_checksum
from jobs.utils.minio_writer import write_parquet
from jobs.utils.spark_session import build_spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        etl_date_str = str(args.etl_date)
        logger.info(f"Starting Reconcile job for {SOR_AR_X_AU}. ETL Date: {etl_date_str}")

        spark = build_spark("reconcile_ar_x_au")
        config = JobConfig()
        raw_bucket = config.raw_bucket
        sor_bucket = config.sor_bucket
        reconcile_bucket = config.reconcile_bucket

        # 1. Read Target Data from MinIO SOR
        sor_path = f"s3a://{sor_bucket}/{SOR_AR_X_AU}/"
        logger.info(f"Reading target data from MinIO SOR: {sor_path}")
        df_sor = spark.read.parquet(sor_path)

        # 2. Read Source Data from MinIO Raw (GAM)
        gam_path = f"s3a://{raw_bucket}/{TABLE_GAM_TDY}/"
        logger.info(f"Reading raw GAM data from {gam_path}")
        df_raw_gam = spark.read.parquet(gam_path)

        count_gam = df_raw_gam.filter(
            (col("ENTITY_CRE_FLG") == "Y") & (col("DEL_FLG") == "N")
        ).count()

        results: List[Row] = []

        # Check 1: Row Count
        count_sor = df_sor.count()
        status = "PASS" if count_sor > 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Row Count",
            STATUS=status,
            DETAILS=f"Raw GAM Valid Rows: {count_gam}, SOR Rows: {count_sor}"
        ))

        # Check 2: Duplicate Key (AR_ID + AU_ID)
        duplicate_count = (
            df_sor.groupBy("AR_ID", "AU_ID")
            .count()
            .filter(col("count") > 1)
            .count()
        )
        status = "PASS" if duplicate_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Duplicate Key",
            STATUS=status,
            DETAILS=f"Found {duplicate_count} duplicate (AR_ID, AU_ID) pairs in SOR"
        ))

        # Check 3: Null Key Check (AR_ID or AU_ID null)
        null_count = df_sor.filter(col("AR_ID").isNull() | col("AU_ID").isNull()).count()
        status = "PASS" if null_count == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Null Key",
            STATUS=status,
            DETAILS=f"Found {null_count} records with null AR_ID or AU_ID"
        ))

        # Check 4: Hash Checksum
        sor_hash = dataframe_checksum(df_sor, ["AR_ID", "AU_ID", "AR_X_AU_RLTNP_TP_ID"])
        status = "PASS" if sor_hash != "" else "FAIL"
        results.append(Row(
            CHECK_NAME="Hash Checksum",
            STATUS=status,
            DETAILS=f"SOR Checksum: {sor_hash}"
        ))

        # Check 5: Schema Validation
        expected_cols = {"AR_ID", "SRC_STM_ID", "AU_ID", "AR_X_AU_RLTNP_TP_ID", "PPN_DT", "EFF_DT", "END_DT"}
        actual_cols = set(df_sor.columns)
        missing_cols = expected_cols - actual_cols
        status = "PASS" if len(missing_cols) == 0 else "FAIL"
        results.append(Row(
            CHECK_NAME="Schema Validation",
            STATUS=status,
            DETAILS=f"Missing Columns: {list(missing_cols)}" if missing_cols else "Schema matches 100%"
        ))

        # 3. Create Summary DataFrame using native Catalyst SQL expressions
        summary_dfs = []
        for r in results:
            row_df = spark.range(1).select(
                lit(r.CHECK_NAME).alias("CHECK_NAME"),
                lit(r.STATUS).alias("STATUS"),
                lit(r.DETAILS).alias("DETAILS"),
                current_timestamp().alias("CHECK_TIMESTAMP"),
            )
            summary_dfs.append(row_df)

        df_summary = summary_dfs[0]
        for df_sub in summary_dfs[1:]:
            df_summary = df_summary.union(df_sub)

        # Log results directly from Python list
        logger.info("Reconciliation Results Summary:")
        for r in results:
            logger.info(f"[{r.STATUS}] {r.CHECK_NAME}: {r.DETAILS}")

        # 4. Write to Reconcile Zone
        output_path = f"{SOR_AR_X_AU}/"
        logger.info(f"Writing summary to s3a://{reconcile_bucket}/{output_path}")
        write_parquet(df_summary, reconcile_bucket, output_path, mode="overwrite")

        logger.info(f"Successfully completed Reconcile job for {SOR_AR_X_AU}")

    except Exception as e:
        logger.error(f"Reconcile job failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
