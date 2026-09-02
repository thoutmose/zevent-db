-- Zevent Data Engineering — Database layer (PostgreSQL)
-- Layers: raw (bronze) -> staging -> intermediate -> marts
-- Run as: psql -d zevent -f schema.sql

-- ============================================================
-- SCHEMAS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS int;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- LAYER 1 — RAW (bronze): NiFi writes here, one row per event.
--
-- Partitioned by created_at (plain column, not an expression) because
-- PostgreSQL requires the partition key column(s) to appear in any
-- UNIQUE constraint on a partitioned table — using a plain column keeps
-- that requirement simple to satisfy and keeps partition pruning exact.
--
-- Idempotency: (batch_id, row_number) is what actually guarantees "this
-- exact row, from this exact NiFi batch, was inserted once." created_at
-- rides along in the constraint only to satisfy the partitioning rule
-- above — it isn't semantically part of the uniqueness.
-- ============================================================

CREATE TABLE raw.chat_messages_raw (
    id           BIGSERIAL,
    batch_id     UUID NOT NULL,
    row_number   INT  NOT NULL,
    user_id      BIGINT,
    username     VARCHAR(50),
    message      TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number, created_at)
) PARTITION BY RANGE (created_at);

-- BRIN, not btree: rows arrive roughly in time order within a partition,
-- so BRIN's coarse per-block min/max summary filters almost as well as a
-- btree here and costs far less to maintain per insert — at a 10k
-- rows/s sustained target that maintenance cost is paid on every single
-- row. Partition pruning already handles most time filtering; this
-- covers filtering *within* one partition.
CREATE INDEX ON raw.chat_messages_raw USING BRIN (created_at);

-- No standalone user_id index: nothing queries raw by user_id directly —
-- per-user analytics come from stg/int (marts.top_users,
-- int.user_hourly_stats), which are populated from stg after ingestion,
-- not per-row. Dropping it removes a third btree from every insert's
-- index-maintenance cost.

CREATE TABLE raw.user_events_raw (
    id           BIGSERIAL,
    batch_id     UUID NOT NULL,
    row_number   INT  NOT NULL,
    user_id      BIGINT,
    event_type   VARCHAR(50),
    payload      JSONB,
    created_at   TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX ON raw.user_events_raw USING BRIN (created_at);
-- Same reasoning as chat_messages_raw above: no standalone user_id index.

-- Catch-all partitions: a clock-skewed row or a run that overshoots the
-- planned window lands here instead of failing the whole NiFi batch.
-- Never leave data here for long — it means the partition plan was wrong
-- for that timestamp and should be extended (see partitions.sql).
CREATE TABLE raw.chat_messages_raw_default PARTITION OF raw.chat_messages_raw DEFAULT;
CREATE TABLE raw.user_events_raw_default PARTITION OF raw.user_events_raw DEFAULT;

-- ============================================================
-- Ingestion staging: UNLOGGED tables NiFi COPYs into before each batch
-- is merged into the real (WAL-logged) raw tables. This is the main
-- lever for the 10,000 TPS sustained target — see insert_example.sql
-- for the full pattern and README's "Scaling to 10k TPS" section for
-- why plain multi-row INSERT stopped being enough.
--
-- Shared, not per-session TEMP: PgBouncer transaction pooling can hand a
-- client a different backend between statements, so a session-local
-- TEMP table isn't reliably visible across a COPY and the merge that
-- follows it. batch_id scopes concurrent batches sharing this table.
--
-- UNLOGGED is safe here specifically because every batch that reaches
-- COMMIT has, in that same transaction, already been merged into the
-- WAL-logged real table and cleared from staging — a crash truncates
-- staging on recovery, but nothing committed was ever left only in
-- staging.
-- ============================================================

CREATE UNLOGGED TABLE IF NOT EXISTS raw.chat_messages_staging (
    batch_id    UUID NOT NULL,
    row_number  INT  NOT NULL,
    user_id     BIGINT,
    username    VARCHAR(50),
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS chat_messages_staging_batch_id_idx
    ON raw.chat_messages_staging (batch_id);

CREATE UNLOGGED TABLE IF NOT EXISTS raw.user_events_staging (
    batch_id    UUID NOT NULL,
    row_number  INT  NOT NULL,
    user_id     BIGINT,
    event_type  VARCHAR(50),
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS user_events_staging_batch_id_idx
    ON raw.user_events_staging (batch_id);

-- ============================================================
-- Partition management: one function, called for both raw tables,
-- instead of hand-writing per-table CREATE TABLE statements.
--
-- Daily, not hourly: the Zevent 2025 baseline (327 streams, 55h,
-- 296K avg / 752K peak viewers) works out to ~15-18M chat messages for
-- the whole event at a realistic 1.5 msg/min per 100 viewers. That's
-- ~6M rows/day, which a single partition handles fine — hourly slices
-- (55 partitions) added partition-management overhead an order of
-- magnitude past what this volume needs. 3 daily partitions for the
-- 55h window is the simpler thing that still works.
-- ============================================================

CREATE OR REPLACE FUNCTION raw.create_daily_partitions(
    p_table      REGCLASS,
    p_start      TIMESTAMPTZ,
    p_end        TIMESTAMPTZ
) RETURNS SETOF TEXT AS $$
DECLARE
    v_schema     TEXT;
    v_table      TEXT;
    v_slot_start TIMESTAMPTZ;
    v_slot_end   TIMESTAMPTZ;
    v_part_name  TEXT;
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_table
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_table;

    v_slot_start := date_trunc('day', p_start);
    WHILE v_slot_start < p_end LOOP
        v_slot_end  := v_slot_start + interval '1 day';
        v_part_name := format('%s_%s', v_table, to_char(v_slot_start, 'YYYY_MM_DD'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
            v_schema, v_part_name, v_schema, v_table, v_slot_start, v_slot_end
        );

        RETURN NEXT format('%I.%I', v_schema, v_part_name);
        v_slot_start := v_slot_end;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION raw.create_daily_partitions IS
    'Creates daily partitions of p_table for [p_start, p_end). Idempotent: safe to re-run, existing partitions are skipped.';

-- ============================================================
-- LAYER 2 — STAGING: deduped, cleaned, typed. ~1-2M rows.
-- Truncate + repopulate (or incremental, see refresh functions below).
-- ============================================================

CREATE TABLE stg.chat_messages (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    username    VARCHAR(50) NOT NULL,
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON stg.chat_messages (user_id);
CREATE INDEX ON stg.chat_messages (created_at);

CREATE OR REPLACE FUNCTION stg.refresh_chat_messages() RETURNS BIGINT AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    TRUNCATE stg.chat_messages;

    INSERT INTO stg.chat_messages (id, user_id, username, message, created_at)
    SELECT DISTINCT ON (user_id, created_at, message)
        id, user_id, username, trim(message), created_at
    FROM raw.chat_messages_raw
    WHERE user_id IS NOT NULL
      AND message IS NOT NULL
      AND trim(message) <> ''
    ORDER BY user_id, created_at, message, id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- LAYER 3 — INTERMEDIATE: joins/aggregations. ~2M rows.
-- ============================================================

CREATE TABLE int.user_hourly_stats (
    user_id       BIGINT NOT NULL,
    hour_bucket   TIMESTAMPTZ NOT NULL,
    message_count BIGINT NOT NULL,
    first_message TIMESTAMPTZ NOT NULL,
    last_message  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, hour_bucket)
);

CREATE INDEX ON int.user_hourly_stats (hour_bucket);

CREATE OR REPLACE FUNCTION int.refresh_user_hourly_stats() RETURNS BIGINT AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    TRUNCATE int.user_hourly_stats;

    INSERT INTO int.user_hourly_stats (user_id, hour_bucket, message_count, first_message, last_message)
    SELECT
        user_id,
        date_trunc('hour', created_at) AS hour_bucket,
        count(*),
        min(created_at),
        max(created_at)
    FROM stg.chat_messages
    GROUP BY user_id, date_trunc('hour', created_at);

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- LAYER 4 — MARTS: dashboard-ready. Small.
-- ============================================================

CREATE TABLE marts.top_users (
    rank            INT NOT NULL,
    user_id         BIGINT NOT NULL,
    username        VARCHAR(50) NOT NULL,
    total_messages  BIGINT NOT NULL,
    active_hours    INT NOT NULL,
    PRIMARY KEY (rank)
);

CREATE OR REPLACE FUNCTION marts.refresh_top_users(p_limit INT DEFAULT 100) RETURNS BIGINT AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    TRUNCATE marts.top_users;

    INSERT INTO marts.top_users (rank, user_id, username, total_messages, active_hours)
    SELECT
        row_number() OVER (ORDER BY sum(h.message_count) DESC),
        h.user_id,
        (SELECT username FROM stg.chat_messages c WHERE c.user_id = h.user_id LIMIT 1),
        sum(h.message_count),
        count(*)
    FROM int.user_hourly_stats h
    GROUP BY h.user_id
    ORDER BY sum(h.message_count) DESC
    LIMIT p_limit;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Convenience: run the whole staging -> intermediate -> marts chain.
-- ============================================================

CREATE OR REPLACE FUNCTION marts.refresh_all(p_top_n INT DEFAULT 100) RETURNS TABLE(step TEXT, rows_affected BIGINT) AS $$
BEGIN
    RETURN QUERY SELECT 'stg.chat_messages', stg.refresh_chat_messages();
    RETURN QUERY SELECT 'int.user_hourly_stats', int.refresh_user_hourly_stats();
    RETURN QUERY SELECT 'marts.top_users', marts.refresh_top_users(p_top_n);
END;
$$ LANGUAGE plpgsql;
