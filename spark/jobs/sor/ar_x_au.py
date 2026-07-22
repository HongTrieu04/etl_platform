"""
Transform TBAADM_GAM_TDY -> AR_X_AU
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

from jobs.utils.spark_session import build_spark
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.minio_writer import write_parquet
from jobs.utils.hash_function import hash_columns
from jobs.common.constants import TABLE_GAM_TDY, SOR_ZONE, SOR_AR_X_AU
from jobs.config.job_config import JobConfig


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        logger.info(f"Starting SOR transformation job for {SOR_AR_X_AU}. ETL Date: {args.etl_date}")

        spark = build_spark("sor_ar_x_au")
        config = JobConfig()
        raw_bucket = config.raw_bucket
        sor_bucket = config.sor_bucket

        # ──────────────────────────────────────────────────────────────
        # MAPPING BLOCK A — Replace with actual SQL mapping logic
        # ──────────────────────────────────────────────────────────────
        input_path = f"s3a://{raw_bucket}/{TABLE_GAM_TDY}/"
        logger.info(f"Reading raw data from {input_path}")
        df_raw = spark.read.parquet(input_path)

        # Apply DataFrame API transformations
        block_a = df_raw.select(
            col("ACID").alias("ACCOUNT_ID"),
            col("FOESSION").alias("FOESSION_ID"),
            col("GL_SUB_HEAD_CODE").alias("GL_CODE"),
            col("ACCT_NAME").alias("ACCOUNT_NAME"),
            col("SOL_ID").alias("BRANCH_CODE"),
            col("CLR_BAL_AMT").alias("BALANCE"),
            col("ACCT_OPN_DATE").alias("OPEN_DATE"),
            col("SCHM_CODE").alias("SCHEME_CODE"),
            col("SCHM_TYPE").alias("SCHEME_TYPE"),
            col("ACCT_CRNCY_CODE").alias("CURRENCY_CODE"),
            col("ACCT_CLS_FLG"),
            col("DEL_FLG"),
            col("ENTITY_CRE_FLG")
        )

        block_a = block_a.withColumn(
            "ACCOUNT_STATUS",
            when(col("ACCT_CLS_FLG") == "Y", lit("CLOSED"))
            .when(col("DEL_FLG") == "Y", lit("DELETED"))
            .otherwise(lit("ACTIVE"))
        ).withColumn(
            "ACCOUNT_CATEGORY",
            when(col("SCHM_TYPE") == "SBA", lit("SAVINGS"))
            .when(col("SCHM_TYPE") == "CAA", lit("CURRENT"))
            .when(col("SCHM_TYPE") == "LAA", lit("LOAN"))
            .otherwise(lit("OTHER"))
        ).filter(col("DEL_FLG") != "Y")

        block_a = block_a.drop("ACCT_CLS_FLG", "DEL_FLG", "SCHM_TYPE")
        
        # ──────────────────────────────────────────────────────────────
        # MAPPING BLOCK B — Replace with actual SQL mapping logic
        # ──────────────────────────────────────────────────────────────
        block_b = block_a.filter(col("ENTITY_CRE_FLG") == "Y")
        
        logger.info("Unioning block_a and block_b")
        df_union = block_a.unionByName(block_b, allowMissingColumns=True)

        # Add ETL_DATE and HASH_KEY
        df_transformed = df_union.withColumn("ETL_DATE", lit(args.etl_date))
        df_final = hash_columns(df_transformed, ["ACCOUNT_ID", "GL_CODE", "BALANCE", "CURRENCY_CODE"], output_col="HASH_KEY")
        df_final = df_final.drop("ENTITY_CRE_FLG")

        # Write to SOR Zone
        output_path = f"{SOR_AR_X_AU}/"
        logger.info(f"Writing transformed data to s3a://{sor_bucket}/{output_path}")
        write_parquet(df_final, sor_bucket, output_path, mode='overwrite')
        
        logger.info(f"Successfully completed SOR transformation job for {SOR_AR_X_AU}")

    except Exception as e:
        logger.error(f"SOR transformation job failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
