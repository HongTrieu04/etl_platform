"""MinIO / S3A Parquet writer utility."""
import logging
from typing import Optional, Sequence

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_parquet(
    df: DataFrame,
    bucket: str,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[Sequence[str]] = None,
) -> str:
    """Write a DataFrame to MinIO as Parquet.

    Args:
        df: PySpark DataFrame to write.
        bucket: MinIO bucket name (e.g. 'raw', 'sor').
        path: Sub-path inside the bucket (e.g. 'TBAADM_GAM_TDY').
        mode: Write mode ('overwrite', 'append', 'error', 'ignore').
        partition_by: Optional list of columns to partition by.

    Returns:
        The full S3A output path.
    """
    output_path = f"s3a://{bucket}/{path}"
    logger.info("Writing Parquet → %s (mode=%s)", output_path, mode)

    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(output_path)

    row_count = df.count()
    logger.info("Written %d rows → %s", row_count, output_path)
    return output_path
