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

    # Coalesce nulls and cast to string
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
    """Compute an aggregate checksum for the entire DataFrame in a memory-safe, distributed way.

    Args:
        df: Input DataFrame.
        columns: Columns to include in the per-row hash.

    Returns:
        Deterministic SHA-256 checksum string.
    """
    hashed_df = hash_columns(df, columns, output_col="_row_hash_", algorithm="md5")
    # Convert first 15 hex characters of MD5 to BigInt number and sum distributedly
    num_col = F.conv(F.substring(F.col("_row_hash_"), 1, 15), 16, 10).cast("decimal(38,0)")
    
    row_count = df.count()
    sum_val = hashed_df.select(F.sum(num_col)).collect()[0][0] or 0
    
    checksum_str = f"cnt:{row_count}|sum:{sum_val}"
    result = hashed_df.select(F.sha2(F.lit(checksum_str), 256)).collect()[0][0]

    logger.info("DataFrame checksum (over %d columns, %d rows): %s", len(columns), row_count, result)
    return result or ""
