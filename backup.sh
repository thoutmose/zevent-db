#!/usr/bin/env bash
# Take a base backup on srv-db. Run once before the event starts, and
# optionally again mid-event for a shorter restore path — WAL archiving
# (postgresql.tuning.conf) covers everything in between either backup
# and "now".
#
# Usage: ./backup.sh [archive_dir]   (default: /archive)

set -euo pipefail

ARCHIVE_DIR="${1:-/archive}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BASE_DIR="${ARCHIVE_DIR}/base/${STAMP}"

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=5432}"
: "${PGUSER:=zevent_user}"
: "${PGDATABASE:=zevent}"

mkdir -p "$BASE_DIR"

echo "==> pg_basebackup -> ${BASE_DIR}"
pg_basebackup -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
    -D "$BASE_DIR" -Fp -Xs -P -c fast

echo "==> Base backup complete: ${BASE_DIR}"
echo "    WAL segments since this backup are in ${ARCHIVE_DIR}/wal/"
echo "    Verify restorability with: ./restore_test.sh ${BASE_DIR}"

# Note for anyone testing this by hand: never combine a data-modifying
# statement with `SELECT pg_switch_wal();` in one `psql -c "A; B;"` call.
# psql -c sends multi-statement strings as a single implicit transaction,
# so the switch can land the segment boundary BETWEEN the write and its
# commit record — the commit record then sits in the next (unarchived)
# segment, and a restore silently loses that "committed" row. Run them
# as separate psql invocations, which is what real traffic does anyway
# (every NiFi batch commit is its own connection/transaction).
