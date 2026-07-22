"""
Skeleton job for SOR Job B.
"""

import logging
from pyspark.sql.functions import lit

import sys
sys.path.append("/opt/spark")

from jobs.utils.spark_session import build_spark
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.minio_writer import write_parquet
from jobs.common.constants import TABLE_FBM_TDY, SOR_ZONE
from jobs.config.job_config import JobConfig


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        logger.info(f"Starting SOR Job B. ETL Date: {args.etl_date}")

        spark = build_spark("sor_job_b")
        config = JobConfig()
        raw_bucket = config.raw_bucket
        sor_bucket = config.sor_bucket

        input_path = f"s3a://{raw_bucket}/{TABLE_FBM_TDY}/"
        logger.info(f"Reading raw data from {input_path}")
        df_raw = spark.read.parquet(input_path)

        # TODO: Implement actual mapping logic
        df_transformed = df_raw.withColumn("ETL_DATE", lit(args.etl_date))

        output_path = "JOB_B_OUTPUT/"
        logger.info(f"Writing transformed data to s3a://{sor_bucket}/{output_path}")
        write_parquet(df_transformed, sor_bucket, output_path, mode='overwrite')
        
        logger.info("Successfully completed SOR Job B")

    except Exception as e:
        logger.error(f"SOR Job B failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
