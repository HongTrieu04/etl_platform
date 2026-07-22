#!/bin/sh
set -eu

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"

mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "$MINIO_BUCKETS" | tr ',' '\n' | while IFS= read -r bucket; do
  if [ -n "$bucket" ]; then
    mc mb --ignore-existing "$MINIO_ALIAS/$bucket"
    mc anonymous set none "$MINIO_ALIAS/$bucket"
    echo "Initialized bucket: $bucket"
  fi
done

# Pre-create a production-like data lake layout.
mc mb --ignore-existing "$MINIO_ALIAS/raw/table_name/partition"
mc mb --ignore-existing "$MINIO_ALIAS/stg/table_name/partition"
mc mb --ignore-existing "$MINIO_ALIAS/sor/table_name/partition"
mc mb --ignore-existing "$MINIO_ALIAS/reconcile/date"

echo "MinIO bootstrap completed"
