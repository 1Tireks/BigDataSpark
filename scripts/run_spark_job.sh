#!/usr/bin/env bash
set -euo pipefail

JOB="${1:?Usage: $0 <etl_star_schema|etl_clickhouse_reports>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

PACKAGES="org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3"

docker compose exec spark /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  "/jobs/${JOB}.py"
