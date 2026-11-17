-- USE CASE 3: Page views per menit (tumbling window)
-- "Halaman mana yang paling banyak dibuka tiap menit, dan oleh berapa user unik?"
-- Window tumbling 1 menit, tidak saling tumpang tindih. Baris keluar saat window ditutup watermark.
INSERT INTO page_views_per_minute
SELECT
  window_start,
  window_end,
  page,
  COUNT(*)                  AS views,
  COUNT(DISTINCT user_id)   AS unique_users
FROM TABLE(
  TUMBLE(TABLE app_activity, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, page;
