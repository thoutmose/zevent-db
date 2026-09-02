#!/usr/bin/env bash
# Proves a base backup + archived WAL are actually restorable, by doing
# a real point-in-time recovery into a throwaway instance and comparing
# row counts against the live database. Run this after every backup.sh,
# not just once — an unrestored backup is a hope, not a strategy.
#
# Usage: ./restore_test.sh <base_backup_dir> [archive_dir] [restore_port]
# Example: ./restore_test.sh /archive/base/20260904T180000Z /archive 5433
#
# Requires pg_ctl/postgres/psql in PATH (same major version as source),
# and PGHOST/PGPORT/PGUSER/PGDATABASE pointing at the live database to
# compare against (defaults: 127.0.0.1:5432, zevent_user, zevent).

set -euo pipefail

BASE_BACKUP_DIR="${1:?usage: restore_test.sh <base_backup_dir> [archive_dir] [restore_port]}"
ARCHIVE_DIR="${2:-/archive}"
RESTORE_PORT="${3:-5433}"

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=5432}"
: "${PGUSER:=zevent_user}"
: "${PGDATABASE:=zevent}"

RESTORE_DIR=$(mktemp -d)
trap 'pg_ctl -D "$RESTORE_DIR" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$RESTORE_DIR"' EXIT

echo "==> Restoring ${BASE_BACKUP_DIR} into throwaway instance at ${RESTORE_DIR}"
cp -a "${BASE_BACKUP_DIR}/." "$RESTORE_DIR/"
chmod 700 "$RESTORE_DIR"
rm -f "$RESTORE_DIR/postmaster.pid"
touch "$RESTORE_DIR/recovery.signal"
cat >> "$RESTORE_DIR/postgresql.auto.conf" <<EOF
restore_command = 'cp ${ARCHIVE_DIR}/wal/%f %p'
port = ${RESTORE_PORT}
EOF

echo "==> Starting recovery (replaying archived WAL)"
pg_ctl -D "$RESTORE_DIR" -l "$RESTORE_DIR/restore.log" start -w

echo "==> Waiting for recovery to finish and instance to promote"
for i in $(seq 1 60); do
    STATE=$(psql -h 127.0.0.1 -p "$RESTORE_PORT" -U "$PGUSER" -d "$PGDATABASE" -tAc "SELECT pg_is_in_recovery();" 2>/dev/null || true)
    [ "$STATE" = "f" ] && break
    sleep 1
done
if [ "$STATE" != "f" ]; then
    echo "FAIL: recovery did not complete within 60s (last state: '${STATE}')" >&2
    tail -30 "$RESTORE_DIR/restore.log" >&2
    exit 1
fi

echo "==> Comparing row counts: live vs restored (source-of-truth tables only)"
FAILED=0
for TABLE in raw.chat_messages_raw raw.user_events_raw; do
    LIVE=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -tAc "SELECT count(*) FROM ${TABLE};")
    RESTORED=$(psql -h 127.0.0.1 -p "$RESTORE_PORT" -U "$PGUSER" -d "$PGDATABASE" -tAc "SELECT count(*) FROM ${TABLE};")
    if [ "$LIVE" = "$RESTORED" ]; then
        echo "    OK  ${TABLE}: ${LIVE} rows match"
    else
        echo "    MISMATCH ${TABLE}: live=${LIVE} restored=${RESTORED}" >&2
        FAILED=1
    fi
done

if [ "$FAILED" -ne 0 ]; then
    echo "FAIL: restore is missing data — check archive_command is actually running on the live instance" >&2
    exit 1
fi

echo "==> PASS: backup at ${BASE_BACKUP_DIR} restores cleanly and matches live data"
