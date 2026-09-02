-- Create daily partitions for the raw layer covering the event window.
--
-- Usage:
--   psql -d zevent -v start_ts="'2026-09-04 18:00:00+02'" -v hours=55 -f partitions.sql
--
-- `hours` sets the window length (55h = the event); the function still
-- slices that window into daily (not hourly) partitions internally.
--
-- Re-runnable: raw.create_daily_partitions() skips partitions that
-- already exist, so re-running with a wider window just adds the delta.

\if :{?start_ts}
\else
\set start_ts '''2026-09-04 18:00:00+02'''
\endif

\if :{?hours}
\else
\set hours 55
\endif

SELECT raw.create_daily_partitions(
    'raw.chat_messages_raw'::regclass,
    :start_ts::timestamptz,
    :start_ts::timestamptz + (:hours || ' hours')::interval
);

SELECT raw.create_daily_partitions(
    'raw.user_events_raw'::regclass,
    :start_ts::timestamptz,
    :start_ts::timestamptz + (:hours || ' hours')::interval
);
