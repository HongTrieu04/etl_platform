"""Centralized job configuration for the ETL platform.

All connection parameters are read from environment variables
set via docker-compose.yml / .env.
"""
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class JobConfig:
    """Immutable configuration container for ETL jobs."""

    # Source system
    source_system: str = "oracle"

    # MinIO buckets
    raw_bucket: str = "raw"
    stg_bucket: str = "stg"
    sor_bucket: str = "sor"
    reconcile_bucket: str = "reconcile"

    # Oracle connection
    oracle_host: str = field(default_factory=lambda: os.getenv("ORACLE_HOST", "localhost"))
    oracle_port: str = field(default_factory=lambda: os.getenv("ORACLE_PORT", "1521"))
    oracle_service_name: str = field(default_factory=lambda: os.getenv("ORACLE_SERVICE_NAME", "XEPDB1"))
    oracle_user: str = field(default_factory=lambda: os.getenv("ORACLE_APP_USER", ""))
    oracle_password: str = field(default_factory=lambda: os.getenv("ORACLE_APP_USER_PASSWORD", ""))

    @property
    def jdbc_url(self) -> str:
        """Build Oracle JDBC connection URL."""
        return f"jdbc:oracle:thin:@//{self.oracle_host}:{self.oracle_port}/{self.oracle_service_name}"

    @classmethod
    def from_env(cls) -> "JobConfig":
        """Create a JobConfig from current environment variables."""
        return cls()
