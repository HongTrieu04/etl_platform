"""Shared constants for the ETL platform."""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"

# Data zone names (MinIO buckets)
RAW_ZONE = "raw"
STG_ZONE = "stg"
SOR_ZONE = "sor"
RECONCILE_ZONE = "reconcile"

# Source table names
TABLE_GAM_TDY = "TBAADM_GAM_TDY"
TABLE_FBM_TDY = "TBAADM_FBM_TDY"

# SOR table names
SOR_AR_X_AU = "AR_X_AU"

# JDBC
JDBC_DRIVER_CLASS = "oracle.jdbc.OracleDriver"
