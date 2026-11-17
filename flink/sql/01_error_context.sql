-- USE CASE 1: Error context
-- "Saat error terjadi, user sedang ada di halaman/aktivitas apa?"
-- Join: app_activity (a) x app_error (e) pada session_id,
--       error muncul 0 s .. 30 s SETELAH aktivitas.
INSERT INTO enriched_error_context
SELECT
  a.user_id,
  a.session_id,
  a.page,
  a.action                                        AS activity_action,
  a.event_time                                    AS activity_time,
  e.error_code,
  e.error_message,
  e.event_time                                    AS error_time,
  TIMESTAMPDIFF(SECOND, a.event_time, e.event_time) AS seconds_to_error
FROM app_activity AS a
JOIN app_error AS e
  ON  a.session_id = e.session_id
  AND e.event_time BETWEEN a.event_time AND a.event_time + INTERVAL '30' SECOND;
