"""Command-line argument parser for ETL Spark jobs."""
import argparse
import logging
from datetime import datetime
from typing import NamedTuple

logger = logging.getLogger(__name__)


class ETLArgs(NamedTuple):
    """Parsed ETL job arguments."""
    etl_date: str       # format YYYYMMDD
    run_type: str       # FULL or DELTA
    blocks: str         # e.g. "7,8" or "all"


def parse_etl_args(description: str = "ETL Spark Job") -> ETLArgs:
    """Parse standard ETL command-line arguments.

    Args:
        description: Description shown in --help output.

    Returns:
        ETLArgs with etl_date, run_type, and blocks.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--etl-date",
        type=str,
        default=datetime.now().strftime("%Y%m%d"),
        help="ETL processing date in YYYYMMDD format (default: today).",
    )
    parser.add_argument(
        "--run-type",
        type=str,
        choices=["FULL", "DELTA"],
        default="FULL",
        help="Run type: FULL or DELTA (default: FULL).",
    )
    parser.add_argument(
        "--blocks",
        type=str,
        default="all",
        help="Comma-separated list of blocks to run, e.g. '7,8' or '7' (default: all).",
    )

    args = parser.parse_args()
    result = ETLArgs(etl_date=args.etl_date, run_type=args.run_type, blocks=args.blocks)
    logger.info("ETL args → etl_date=%s, run_type=%s, blocks=%s", result.etl_date, result.run_type, result.blocks)
    return result
