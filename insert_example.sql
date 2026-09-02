-- High-throughput ingestion pattern for the 10,000 TPS sustained target
-- (see README's "Scaling to 10k TPS" section). Replaces the older plain
-- multi-row INSERT ... ON CONFLICT pattern, which caps out around
-- ~5,000 rows/s batched: every row there pays full per-statement parse/
-- plan overhead plus three-index maintenance directly on the WAL-logged
-- target table.
--
-- Pattern: COPY (no WAL, no indexes, no per-row statement parsing) into
-- a shared UNLOGGED staging table, then move each batch into the real
-- table with one bulk INSERT ... SELECT ... ON CONFLICT DO NOTHING.
-- Index maintenance on the real table still happens, but once per batch
-- instead of once per row, against only two indexes instead of three
-- (see schema.sql).
--
-- created_at MUST be the source event's own timestamp (from the Twitch/
-- Zevent payload), never now()/clock_timestamp() at insert time — it is
-- part of the uniqueness constraint, so a value that changes between
-- retries defeats idempotency and lets a replayed batch insert twice.
-- ingested_at (on the real table only) is the one place "when did we
-- write this" belongs; it defaults to now() and is deliberately outside
-- the constraint.
--
-- All three steps run in ONE transaction (BEGIN ... COMMIT), which is
-- what makes a NiFi retry safe: normal MVCC/rollback semantics apply to
-- UNLOGGED tables during live operation, so a crash or timeout before
-- COMMIT rolls back the staging insert along with everything else —
-- there's nothing left over to clean up before retrying the same
-- batch_id. NiFi should issue these as one multi-statement transaction
-- per batch, not three separate round trips.
--
-- For real NiFi flows, step 1 is a COPY (PutDatabaseRecord in COPY mode,
-- or an equivalent \copy), not INSERT — shown here as multi-row INSERT
-- only because a plain .sql file can't drive \copy or a real client-side
-- COPY stream.

BEGIN;

-- 1. Bulk-load the batch into the shared staging table.
INSERT INTO raw.chat_messages_staging (batch_id, row_number, user_id, username, message, created_at)
VALUES
    ('11111111-1111-1111-1111-111111111111', 1, 42, 'alice', 'hype!!',   '2026-09-04 18:00:05+02'),
    ('11111111-1111-1111-1111-111111111111', 2, 43, 'bob',   'poggers', '2026-09-04 18:00:06+02');

-- 2. Merge this batch into the real (WAL-logged, partitioned) table.
--    batch_id scopes the merge to just this batch — staging is shared
--    across concurrent NiFi flows, not truncated wholesale per batch.
INSERT INTO raw.chat_messages_raw (batch_id, row_number, user_id, username, message, created_at)
SELECT batch_id, row_number, user_id, username, message, created_at
FROM raw.chat_messages_staging
WHERE batch_id = '11111111-1111-1111-1111-111111111111'
ON CONFLICT (batch_id, row_number, created_at) DO NOTHING;

-- 3. Clear this batch's rows from staging so it doesn't grow unbounded.
DELETE FROM raw.chat_messages_staging
WHERE batch_id = '11111111-1111-1111-1111-111111111111';

COMMIT;

-- raw.user_events_raw follows the identical three-step pattern against
-- raw.user_events_staging.
