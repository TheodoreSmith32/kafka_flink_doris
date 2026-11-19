-- USE CASE 6: Alert transaksi gagal beruntun (MATCH_RECOGNIZE / deteksi pola)
-- Alert bila satu user mengalami 3 transaksi FAILED berturut-turut dalam 2 menit
-- (tanpa ada transaksi lain di antaranya).
INSERT INTO alerts
SELECT
  'FAILED_TXN_STREAK'                                  AS alert_type,
  'CRITICAL'                                           AS severity,
  user_id,
  user_id                                              AS subject,
  fail_count                                           AS metric,
  CONCAT('3 transaksi gagal beruntun, total nominal ', CAST(total_amount AS STRING)) AS detail,
  first_fail_time                                      AS start_time,
  last_fail_time                                       AS end_time
FROM app_transaction
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES
    FIRST(F.event_time)  AS first_fail_time,
    LAST(F.event_time)   AS last_fail_time,
    COUNT(F.txn_id)      AS fail_count,
    SUM(F.amount)        AS total_amount
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (F{3}) WITHIN INTERVAL '2' MINUTE
  DEFINE F AS F.status = 'FAILED'
);
