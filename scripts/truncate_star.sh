#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

docker compose exec -T postgres psql -U postgres -d bigdata_lab <<'SQL'
TRUNCATE TABLE
    fact_sales,
    dim_customer,
    dim_seller,
    dim_product,
    dim_pet,
    dim_store,
    dim_supplier,
    dim_pet_category,
    dim_product_category,
    dim_country
RESTART IDENTITY CASCADE;
SQL

echo "Star schema tables truncated."
