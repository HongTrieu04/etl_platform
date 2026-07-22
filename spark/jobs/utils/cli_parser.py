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


def parse_etl_args(description: str = "ETL Spark Job") -> ETLArgs:
    """Parse standard ETL command-line arguments.

    Args:
        description: Description shown in --help output.

    Returns:
        ETLArgs with etl_date and run_type.
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

    args = parser.parse_args()
    result = ETLArgs(etl_date=args.etl_date, run_type=args.run_type)
    logger.info("ETL args → etl_date=%s, run_type=%s", result.etl_date, result.run_type)
    return result
