-- USE CASE 5: Alert lonjakan error (window aggregation + HAVING)
-- Alert bila >= 5 error dengan error_code yang sama terjadi dalam 1 menit.
INSERT INTO alerts
SELECT
  'ERROR_SPIKE'                                                       AS alert_type,
  'HIGH'                                                              AS severity,
  CAST(NULL AS STRING)                                                AS user_id,
  error_code                                                          AS subject,
  COUNT(*)                                                            AS metric,
  CONCAT('error_code=', error_code, ' muncul ', CAST(COUNT(*) AS STRING), 'x dalam 1 menit') AS detail,
  window_start                                                        AS start_time,
  window_end                                                          AS end_time
FROM TABLE(
  TUMBLE(TABLE app_error, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, error_code
HAVING COUNT(*) >= 5;
