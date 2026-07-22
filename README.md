# Local Data Platform ETL Project (Production-like)

## 1. System architecture

This project simulates a realistic ETL platform for banking-style workloads:

Oracle (source) -> MinIO raw -> Spark ETL -> MinIO stg -> Spark ETL -> MinIO sor -> Reconcile

Airflow orchestrates each stage as independent DAGs and can be extended to table-level pipelines.

## 2. Why this architecture is optimized

- Better than single-container mockups: every core concern is separated (compute, metadata, storage, orchestration).
- LocalExecutor + PostgreSQL gives realistic Airflow behavior while still lightweight for local development.
- MinIO bucket bootstrap is automatic and idempotent (no manual setup).
- Spark image is preloaded with common data engineering dependencies (PySpark, JDBC, S3A stack, Delta, boto3).
- All variables are centralized in .env for security and maintainability.

## 3. Project structure

project-root/

- docker/
- compose/
- spark/
- jobs/
- shared/
- oracle/
- init/
- airflow/
- dags/
- plugins/
- logs/
- config/
- minio/
- init/
- bucket/
- scripts/
- sql/
- config/
- data/
- raw/
- stg/
- sor/
- reconcile/
- docs/
- .env
- docker-compose.yml
- README.md

Implemented workspace layout:

- docker/spark/Dockerfile: Spark runtime image with ETL dependencies.
- docker/airflow/Dockerfile: Airflow runtime image with Spark/MinIO/Oracle providers.
- oracle/sql/init: auto-run Oracle initialization SQL scripts.
- minio/init/init-buckets.sh: auto-create required buckets and baseline paths.
- spark/config: Spark runtime configuration and logging.
- spark/jobs: ETL code skeleton by zone.
- airflow/dags: DAG skeletons for migration/stg/sor/reconcile.
- airflow/plugins, airflow/config, airflow/logs: extension, config, and logs.
- data/raw, data/stg, data/sor, data/reconcile: local data sandbox.
- logs: host-level log separation.
- docs/architecture.md: architecture reference.

## 4. Services and roles

- oracle: source operational database.
- postgres: Airflow metadata database.
- minio: object storage for data lake zones.
- minio-mc: one-shot init service for bucket bootstrap.
- spark-master: Spark cluster coordinator.
- spark-worker: Spark worker node (scalable).
- airflow-init: DB migration + admin bootstrap.
- airflow-webserver: Airflow UI and API.
- airflow-scheduler: DAG scheduling engine.
- airflow-triggerer: async trigger processing.

## 5. Network model

- Single bridge network: etl-network.
- Services communicate by service name, not localhost.
- Example endpoints:
  - oracle:1521
  - postgres:5432
  - minio:9000
  - spark-master:7077
  - airflow-webserver:8080

## 6. Environment variables

All variables are in .env.

Key groups:

- Oracle credentials and service.
- PostgreSQL metadata credentials.
- MinIO root user/password and bucket list.
- Airflow admin and security settings.
- Spark ports and worker sizing.

For team onboarding:

1. Copy .env.example to .env
2. Set passwords and keys
3. Start stack

## 7. Start / stop / reset

PowerShell helpers:

- Start: scripts/start.ps1
- Stop: scripts/stop.ps1
- Reset (destroy volumes + rebuild): scripts/reset.ps1

Direct Docker Compose commands:

- Start and build:
  docker compose up -d --build

- Stop only:
  docker compose down

- Full reset:
  docker compose down -v --remove-orphans

## 8. Health checks

Health checks are configured for:

- Oracle
- PostgreSQL
- MinIO
- Spark master
- Spark worker
- Airflow webserver
- Airflow scheduler
- Airflow triggerer

Startup dependencies use condition-based gating where useful:

- MinIO client waits for healthy MinIO.
- Spark waits for Oracle and MinIO.
- Airflow init waits for PostgreSQL, MinIO, Spark.
- Airflow services wait for successful Airflow init.

## 9. Oracle initialization

- Folder: oracle/sql/init
- Any .sql in this folder is auto-executed on first Oracle boot.
- Use file ordering like:
  - 00_init_schema.sql
  - 01_create_source_tables.sql
  - 02_seed_data.sql

To add tables later, just drop SQL files in this folder and recreate Oracle volume when needed.

## 10. MinIO data lake zones

Buckets auto-created:

- raw
- stg
- sor
- reconcile
- logs
- tmp

Conventions:

- raw/table_name/partition=yyyy-mm-dd/
- stg/table_name/partition=yyyy-mm-dd/
- sor/table_name/partition=yyyy-mm-dd/
- reconcile/date=yyyy-mm-dd/

## 11. Spark usage

Submit sample job:

docker compose exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/migration/oracle_to_raw.py

Scale workers:

docker compose up -d --scale spark-worker=3

Mounted paths on Spark containers:

- /opt/spark/jobs
- /opt/spark/shared
- /opt/bitnami/spark/conf
- /opt/spark/logs
- /opt/spark/data

## 12. Airflow usage

Airflow UI:

- URL: http://localhost:${AIRFLOW_WEB_PORT}
- Username/password from .env

Skeleton DAGs:

- airflow/dags/migration_dag.py
- airflow/dags/stg_dag.py
- airflow/dags/sor_dag.py
- airflow/dags/reconcile_dag.py

Current DAGs are intentional skeletons (EmptyOperator chain). Replace each stage task with Spark submit logic when your ETL code is ready.

Add new DAG:

1. Put .py file under airflow/dags
2. Wait for scheduler refresh
3. Validate import in Airflow UI

## 13. Access commands

Open Oracle SQL shell:

docker compose exec oracle sqlplus system/${ORACLE_PASSWORD}@localhost:1521/${ORACLE_SERVICE_NAME}

PostgreSQL shell:

docker compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

MinIO client shell:

docker compose run --rm minio-mc mc ls local

Upload sample file to raw zone:

docker compose run --rm minio-mc mc cp /minio/init/init-buckets.sh local/raw/table_name/partition=2026-07-22/sample.txt

Spark master shell:

docker compose exec spark-master bash

Airflow scheduler logs:

docker compose logs -f airflow-scheduler

## 14. Debugging tips

- Service status:
  docker compose ps

- View health transitions:
  docker inspect --format='{{json .State.Health}}' <container_name>

- Check Oracle startup logs:
  docker compose logs -f oracle

- Check bucket bootstrap:
  docker compose logs minio-mc

- Validate DAG loading:
  docker compose logs airflow-scheduler | findstr /i "dag"

## 15. Logging and persistence

- Database persistence:
  - oracle_data volume
  - postgres_data volume
- Object persistence:
  - minio_data volume
- File logs:
  - airflow/logs
  - logs/spark
  - logs/airflow
  - logs/minio
  - logs/oracle

This keeps runtime artifacts separated from source and avoids accidental data loss on restart.

## 16. Next extension ideas

- Add table-level config-driven ingestion from Oracle.
- Add Great Expectations or Deequ validation in reconcile stage.
- Add Airflow Connections and Variables bootstrap script.
- Split compose into base + override profiles for CI and dev.
