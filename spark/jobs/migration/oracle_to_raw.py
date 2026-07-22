"""
Migration job: Oracle to Raw (MinIO).
Reads data from Oracle via JDBC and writes to Raw Zone in MinIO.
"""

import time
import logging
from typing import List, Dict, Any

from pyspark.sql.functions import lit, current_timestamp

from jobs.utils.spark_session import build_spark
from jobs.utils.cli_parser import parse_etl_args
from jobs.utils.oracle_reader import OracleReader
from jobs.utils.minio_writer import write_parquet
from jobs.common.constants import TABLE_GAM_TDY, TABLE_FBM_TDY, RAW_ZONE
from jobs.config.job_config import JobConfig


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        args = parse_etl_args()
        logger.info(f"Starting Oracle to Raw migration job. ETL Date: {args.etl_date}, Run Type: {args.run_type}")

        spark = build_spark("migration_oracle_to_raw")
        oracle_reader = OracleReader(spark)
        config = JobConfig()
        raw_bucket = config.raw_bucket

        tables_to_migrate: List[str] = [TABLE_GAM_TDY, TABLE_FBM_TDY]
        failures: List[Dict[str, Any]] = []

        for table_name in tables_to_migrate:
            logger.info(f"Starting migration for table: {table_name}")
            start_time = time.time()
            try:
                # Read from Oracle
                df = oracle_reader.read_table(table_name, partition_column=None, num_partitions=4)

                # Add metadata columns
                df_enriched = df.withColumn('ETL_DATE', lit(args.etl_date)) \
                                .withColumn('INGESTION_TIMESTAMP', current_timestamp())

                # Write to MinIO
                output_path = f"{table_name}/"
                write_parquet(df_enriched, raw_bucket, output_path, mode='overwrite', partition_by=None)

                # Log metrics
                row_count = df_enriched.count()
                duration = time.time() - start_time
                logger.info(f"Successfully migrated {table_name}. Row count: {row_count}. Duration: {duration:.2f} seconds.")

            except Exception as e:
                logger.error(f"Failed to migrate {table_name}: {e}", exc_info=True)
                failures.append({
                    'table': table_name,
                    'error': str(e),
                    'duration': time.time() - start_time
                })

        if failures:
            error_msg = f"Migration failed for {len(failures)} tables: " + ", ".join([f"{f['table']} ({f['error']})" for f in failures])
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.info("Oracle to Raw migration completed successfully for all tables.")

    except Exception as e:
        logger.error(f"Migration job failed: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    main()
