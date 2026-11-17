-- USE CASE 4: Ringkasan transaksi per menit (tumbling window)
-- "Berapa transaksi SUCCESS vs FAILED tiap menit, dan total nominalnya?"
INSERT INTO txn_summary_per_minute
SELECT
  window_start,
  window_end,
  status,
  COUNT(*)      AS txn_count,
  SUM(amount)   AS total_amount
FROM TABLE(
  TUMBLE(TABLE app_transaction, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, status;
