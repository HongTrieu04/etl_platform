from dataclasses import dataclass


@dataclass(frozen=True)
class JobConfig:
    source_system: str = "oracle"
    raw_bucket: str = "raw"
    stg_bucket: str = "stg"
    sor_bucket: str = "sor"
    reconcile_bucket: str = "reconcile"
