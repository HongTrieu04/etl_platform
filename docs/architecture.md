# ETL Platform Architecture

## End-to-end flow

Oracle -> MinIO raw -> Spark transform -> MinIO stg -> Spark transform -> MinIO sor -> Reconcile outputs

## Service interaction principles

- All services communicate over one internal Docker network.
- Service discovery uses container service names (oracle, postgres, minio, spark-master, airflow-webserver).
- No intra-service localhost dependency.

## Why this layout is production-like

- Isolated metadata database for Airflow (PostgreSQL).
- Separation of storage zones in object store buckets.
- Spark split into master and worker role with easy scale-out.
- Dedicated init containers/scripts for idempotent bootstrap tasks.
- Secrets and runtime settings centralized in .env.
- Log paths are separated from source code paths.

## Scale path

- Increase workers: docker compose up -d --scale spark-worker=3.
- Introduce extra DAGs for table-specific loads without changing base infrastructure.
- Replace local object storage endpoint by enterprise S3-compatible gateway with only env and spark config updates.
