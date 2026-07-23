"""Hash helper functions for ETL data integrity checks.

Replaces Oracle FUNCTION_HASH02 with a PySpark equivalent.
"""
import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def hash_columns(
    df: DataFrame,
    columns: List[str],
    output_col: str = "HASH_VALUE",
    algorithm: str = "sha2",
) -> DataFrame:
    """Compute a hash over the given columns.

    Null values are coalesced to empty string before hashing so that
    the result is deterministic regardless of null positions.

    Args:
        df: Input DataFrame.
        columns: List of column names to include in the hash.
        output_col: Name of the output hash column.
        algorithm: 'sha2' (SHA-256) or 'md5'.

    Returns:
        DataFrame with an additional hash column.
    """
    logger.info("Hashing %d columns → %s (algo=%s)", len(columns), output_col, algorithm)

    coalesced = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    concat_expr = F.concat_ws("||", *coalesced)

    if algorithm == "md5":
        hash_expr = F.md5(concat_expr)
    else:
        hash_expr = F.sha2(concat_expr, 256)

    return df.withColumn(output_col, hash_expr)


def function_hash02(prefix: str, col_expr):
    """Simulate Oracle FUNCTION_HASH02(prefix, value).

    Concatenates prefix string with the column expression value and computes SHA-256 hash.
    """
    val_str = F.coalesce(col_expr.cast("string"), F.lit(""))
    return F.sha2(F.concat(F.lit(prefix), val_str), 256)


def dataframe_checksum(df: DataFrame, columns: List[str]) -> str:
    """Compute a 100% memory-safe, ultra-fast distributed checksum for any size DataFrame.

    Uses native Spark xxhash64 and distributed sum aggregation.

    Args:
        df: Input DataFrame.
        columns: Columns to include in the per-row hash.

    Returns:
        Deterministic checksum string.
    """
    row_count = df.count()
    if row_count == 0:
        return "empty_dataframe"

    coalesced = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    hash_sum = df.select(F.sum(F.xxhash64(*coalesced))).collect()[0][0] or 0

    checksum_str = f"rows:{row_count}|hash_sum:{hash_sum}"
    logger.info("DataFrame checksum (over %d columns, %d rows): %s", len(columns), row_count, checksum_str)
    return checksum_str
