#!/usr/bin/env bash
# Post-event archival for one raw partition: export to Parquet, then
# detach and drop it from PostgreSQL to free disk space.
#
# Uses DuckDB for the CSV->Parquet conversion instead of a PostgreSQL
# Parquet extension — one fewer thing to install and trust on srv-db.
#
# Usage: ./archive_partition.sh <schema.partition_table> <archive_dir>
# Example: ./archive_partition.sh raw.chat_messages_raw_2026_09_04 /archive

set -euo pipefail

PARTITION="${1:?usage: archive_partition.sh <schema.partition_table> <archive_dir>}"
ARCHIVE_DIR="${2:?usage: archive_partition.sh <schema.partition_table> <archive_dir>}"
PARENT_SCHEMA="${PARTITION%%.*}"
BARE_TABLE="${PARTITION#*.}"

: "${PGDATABASE:=zevent}"

mkdir -p "$ARCHIVE_DIR"
CSV_PATH="${ARCHIVE_DIR}/${BARE_TABLE}.csv"
PARQUET_PATH="${ARCHIVE_DIR}/${BARE_TABLE}.parquet"

echo "==> Exporting ${PARTITION} to ${CSV_PATH}"
psql -d "$PGDATABASE" -c "\copy (SELECT * FROM ${PARTITION}) TO '${CSV_PATH}' WITH CSV HEADER"

echo "==> Converting to Parquet: ${PARQUET_PATH}"
duckdb -c "COPY (SELECT * FROM read_csv_auto('${CSV_PATH}')) TO '${PARQUET_PATH}' (FORMAT PARQUET);"

echo "==> Verifying row counts match before dropping anything"
CSV_ROWS=$(($(wc -l < "$CSV_PATH") - 1))
PARQUET_ROWS=$(duckdb -csv -noheader -c "SELECT count(*) FROM '${PARQUET_PATH}';")
if [ "$CSV_ROWS" -ne "$PARQUET_ROWS" ]; then
    echo "Row count mismatch: CSV=${CSV_ROWS} Parquet=${PARQUET_ROWS} — not touching PostgreSQL" >&2
    exit 1
fi
echo "==> ${PARQUET_ROWS} rows confirmed in ${PARQUET_PATH}"

rm "$CSV_PATH"

echo "==> Detaching and dropping ${PARTITION} (parent: ${PARENT_SCHEMA})"
PARENT_TABLE=$(echo "$BARE_TABLE" | sed -E 's/_[0-9]{4}_[0-9]{2}_[0-9]{2}$//')
psql -d "$PGDATABASE" -c "ALTER TABLE ${PARENT_SCHEMA}.${PARENT_TABLE} DETACH PARTITION ${PARTITION};"
psql -d "$PGDATABASE" -c "DROP TABLE ${PARTITION};"

echo "==> Done: ${PARTITION} archived to ${PARQUET_PATH} and dropped"
