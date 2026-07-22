from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"

RAW_ZONE = "raw"
STG_ZONE = "stg"
SOR_ZONE = "sor"
RECONCILE_ZONE = "reconcile"
