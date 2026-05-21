#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

TABLES=(
  mart_product_sales
  mart_customer_sales
  mart_time_sales
  mart_store_sales
  mart_supplier_sales
  mart_product_quality
)

for t in "${TABLES[@]}"; do
  docker compose exec clickhouse clickhouse-client -q "DROP TABLE IF EXISTS marts.${t}"
done

echo "ClickHouse mart tables dropped."
