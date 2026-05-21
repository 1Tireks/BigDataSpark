#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker compose exec clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS marts"

"$SCRIPT_DIR/truncate_star.sh"
"$SCRIPT_DIR/run_spark_job.sh" etl_star_schema
"$SCRIPT_DIR/drop_clickhouse_marts.sh"
"$SCRIPT_DIR/run_spark_job.sh" etl_clickhouse_reports

echo "ETL pipeline finished."
