#!/bin/sh
set -e

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"

mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in raw stg sor reconcile logs tmp; do
  mc mb --ignore-existing "$MINIO_ALIAS/$bucket" || true
  mc anonymous set none "$MINIO_ALIAS/$bucket" || true
  echo "Initialized bucket: $bucket"
done

echo "MinIO bootstrap completed"
