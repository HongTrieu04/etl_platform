"""Oracle JDBC reader utility for Spark ETL jobs."""
import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

JDBC_DRIVER = "oracle.jdbc.OracleDriver"


class OracleReader:
    """Read data from Oracle via JDBC.

    Connection parameters are read from environment variables:
    - ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE_NAME
    - ORACLE_APP_USER, ORACLE_APP_USER_PASSWORD
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._host = os.getenv("ORACLE_HOST", "localhost")
        self._port = os.getenv("ORACLE_PORT", "1521")
        self._service = os.getenv("ORACLE_SERVICE_NAME", "XEPDB1")
        self._user = os.getenv("ORACLE_APP_USER", "")
        self._password = os.getenv("ORACLE_APP_USER_PASSWORD", "")
        self._jdbc_url = f"jdbc:oracle:thin:@//{self._host}:{self._port}/{self._service}"
        logger.info("OracleReader initialized → %s (user=%s)", self._jdbc_url, self._user)

    @property
    def jdbc_url(self) -> str:
        return self._jdbc_url

    def read_table(
        self,
        table_name: str,
        partition_column: Optional[str] = None,
        num_partitions: int = 4,
        fetch_size: int = 10000,
    ) -> DataFrame:
        """Read a full table from Oracle via JDBC.

        Args:
            table_name: Fully qualified table name (e.g. TBAADM_GAM_TDY).
            partition_column: Column to use for parallel reads (numeric/date).
            num_partitions: Number of parallel JDBC partitions.
            fetch_size: JDBC fetch size per round-trip.

        Returns:
            PySpark DataFrame.
        """
        logger.info("Reading table %s from Oracle ...", table_name)

        reader = (
            self._spark.read.format("jdbc")
            .option("url", self._jdbc_url)
            .option("dbtable", table_name)
            .option("user", self._user)
            .option("password", self._password)
            .option("driver", JDBC_DRIVER)
            .option("fetchsize", str(fetch_size))
        )

        if partition_column:
            # Fetch bounds for partitioned read
            bounds_df = self._spark.read.format("jdbc") \
                .option("url", self._jdbc_url) \
                .option("user", self._user) \
                .option("password", self._password) \
                .option("driver", JDBC_DRIVER) \
                .option("dbtable", f"(SELECT MIN({partition_column}) AS min_val, MAX({partition_column}) AS max_val FROM {table_name})") \
                .load()

            bounds = bounds_df.collect()[0]
            lower = int(bounds["min_val"]) if bounds["min_val"] is not None else 0
            upper = int(bounds["max_val"]) if bounds["max_val"] is not None else 0

            logger.info("Partitioned read on %s: lower=%d, upper=%d, partitions=%d",
                        partition_column, lower, upper, num_partitions)

            reader = (
                reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", str(lower))
                .option("upperBound", str(upper))
                .option("numPartitions", str(num_partitions))
            )

        df = reader.load()
        row_count = df.count()
        logger.info("Table %s loaded: %d rows", table_name, row_count)
        return df

    def read_query(self, query: str, fetch_size: int = 10000) -> DataFrame:
        """Execute a custom SQL query against Oracle.

        Args:
            query: SQL query string (will be wrapped as subquery).
            fetch_size: JDBC fetch size.

        Returns:
            PySpark DataFrame.
        """
        logger.info("Executing custom query on Oracle ...")
        dbtable = f"({query})"
        df = (
            self._spark.read.format("jdbc")
            .option("url", self._jdbc_url)
            .option("dbtable", dbtable)
            .option("user", self._user)
            .option("password", self._password)
            .option("driver", JDBC_DRIVER)
            .option("fetchsize", str(fetch_size))
            .load()
        )
        logger.info("Custom query returned %d rows", df.count())
        return df
