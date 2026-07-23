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
    """Compute an aggregate checksum for the entire DataFrame.

    Hashes each row, then XORs/concatenates for a single summary value.

    Args:
        df: Input DataFrame.
        columns: Columns to include in the per-row hash.

    Returns:
        Single hex string representing the aggregate checksum.
    """
    hashed_df = hash_columns(df, columns, output_col="_row_hash_")
    result = hashed_df.agg(
        F.sha2(F.concat_ws("|", F.sort_array(F.collect_list("_row_hash_"))), 256).alias("checksum")
    ).collect()[0]["checksum"]

    logger.info("DataFrame checksum (over %d columns): %s", len(columns), result)
    return result or ""
