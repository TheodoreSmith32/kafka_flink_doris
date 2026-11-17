-- Init session: definisi source (5 topic) dan sink (2 topic matang).
-- Dipakai dengan:  sql-client.sh -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/01_xxx.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'app-activity-join';
SET 'table.exec.source.idle-timeout' = '10 s';  -- cegah watermark macet jika ada partisi kosong

-- =========================== SOURCES ===========================
CREATE TEMPORARY TABLE app_login (
  event_id STRING, user_id STRING, session_id STRING,
  device STRING, ip STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'app_login',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-app-login',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE app_activity (
  event_id STRING, user_id STRING, session_id STRING,
  page STRING, action STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'app_activity',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-app-activity',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE app_transaction (
  event_id STRING, user_id STRING, session_id STRING,
  txn_id STRING, txn_type STRING, amount BIGINT, status STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'app_transaction',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-app-transaction',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE app_error (
  event_id STRING, user_id STRING, session_id STRING,
  error_code STRING, error_message STRING, page STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'app_error',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-app-error',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE app_logout (
  event_id STRING, user_id STRING, session_id STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'app_logout',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-app-logout',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

-- ============================ SINKS ============================
-- Interval join hanya menghasilkan INSERT (append-only) -> connector 'kafka' biasa cukup.
CREATE TEMPORARY TABLE enriched_error_context (
  user_id STRING, session_id STRING,
  page STRING, activity_action STRING, activity_time TIMESTAMP(3),
  error_code STRING, error_message STRING, error_time TIMESTAMP(3),
  seconds_to_error BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'enriched_error_context',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json', 'json.timestamp-format.standard' = 'SQL'
);

CREATE TEMPORARY TABLE enriched_transaction_journey (
  user_id STRING, session_id STRING,
  device STRING, login_time TIMESTAMP(3),
  page STRING, activity_time TIMESTAMP(3),
  txn_id STRING, txn_type STRING, amount BIGINT, status STRING, txn_time TIMESTAMP(3),
  seconds_to_txn BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'enriched_transaction_journey',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json', 'json.timestamp-format.standard' = 'SQL'
);

-- Sink window aggregation (hasil TUMBLE bersifat append-only -> connector 'kafka' biasa)
CREATE TEMPORARY TABLE page_views_per_minute (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  page STRING, views BIGINT, unique_users BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'page_views_per_minute',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json', 'json.timestamp-format.standard' = 'SQL'
);

CREATE TEMPORARY TABLE txn_summary_per_minute (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  status STRING, txn_count BIGINT, total_amount BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'txn_summary_per_minute',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json', 'json.timestamp-format.standard' = 'SQL'
);

-- Sink alert: satu topic dengan skema seragam untuk semua jenis alert
-- (mudah dimuat ke SATU tabel Doris, dibedakan lewat kolom alert_type).
CREATE TEMPORARY TABLE alerts (
  alert_type STRING,        -- ERROR_SPIKE | FAILED_TXN_STREAK
  severity   STRING,        -- MEDIUM | HIGH | CRITICAL
  user_id    STRING,        -- NULL untuk alert tingkat sistem
  subject    STRING,        -- error_code atau session/user terkait
  metric     BIGINT,        -- nilai pemicu (jumlah error / jumlah gagal)
  detail     STRING,
  start_time TIMESTAMP(3),
  end_time   TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'alerts',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json', 'json.timestamp-format.standard' = 'SQL'
);
