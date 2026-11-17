-- USE CASE 2: Transaction journey (3 tabel: login -> activity -> transaction)
-- "Transaksi ini dilakukan dari device apa, dan lewat halaman apa?"
-- Interval join bisa dirantai: hasil join pertama masih punya event-time (a.event_time),
-- jadi aman dijoin lagi. Syarat: kolom waktu yang dipakai tetap time attribute.
--   login  -> activity    : aktivitas 0 s .. 5 menit setelah login
--   activity -> transaction : transaksi 0 s .. 30 s setelah aktivitas
-- Filter halaman finansial agar hasilnya tidak meledak.
INSERT INTO enriched_transaction_journey
SELECT
  t.user_id,
  t.session_id,
  l.device,
  l.event_time                                       AS login_time,
  a.page,
  a.event_time                                       AS activity_time,
  t.txn_id, t.txn_type, t.amount, t.status,
  t.event_time                                       AS txn_time,
  TIMESTAMPDIFF(SECOND, a.event_time, t.event_time)  AS seconds_to_txn
FROM app_activity AS a
JOIN app_login AS l
  ON  a.session_id = l.session_id
  AND a.event_time BETWEEN l.event_time AND l.event_time + INTERVAL '5' MINUTE
JOIN app_transaction AS t
  ON  a.session_id = t.session_id
  AND t.event_time BETWEEN a.event_time AND a.event_time + INTERVAL '30' SECOND
WHERE a.page IN ('/transfer', '/payment');
