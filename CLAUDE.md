# CLAUDE.md

Proyek portfolio/belajar: pipeline streaming **Kafka → Flink SQL → (Doris)** untuk log aplikasi.
Tujuan akhir: **alert**. Topic hasil Flink nantinya dimuat ke Apache Doris (belum dibuat).
Detail lengkap use case dan cara menjalankan ada di `README.md`.

## Bahasa
Pengguna berbahasa Indonesia. Balas, tulis README, dan komentar kode dalam bahasa Indonesia.

## Struktur
- `docker-compose.yml`: Kafka (KRaft, apache/kafka 3.9), Kafka Connect, Kafka UI, Flink 1.20 (1 JobManager + 1 TaskManager, **2 slot**). Network `data-net` disiapkan untuk Doris.
- `produce_data/producer.py`: generator data dummy (`confluent_kafka`) ke 5 topic mentah.
- `flink/sql/`: `00_init.sql` (DDL 5 source + sink) dan `01`–`06` (satu file per use case).
- `flink/lib/flink-sql-connector-kafka.jar`: di-mount ke container Flink.
- `requirements.txt` dan `.venv/` ada di **root** (bukan di `produce_data/`).

## Konvensi
- Kafka dari host: `localhost:9092`. Dari container (Flink/Doris): `kafka:19092`.
- Topic mentah: `app_login`, `app_activity`, `app_transaction`, `app_error`, `app_logout`. Kunci join: `session_id`.
- `event_time` berformat `yyyy-MM-dd HH:mm:ss.SSS` (UTC), watermark 5 detik.
- Semua tabel Flink dibuat `TEMPORARY` di `00_init.sql`; tiap run wajib `-i 00_init.sql`.
- Sink memakai connector `kafka` biasa (hasil append-only). Semua alert ke satu topic `alerts` dengan skema seragam (supaya satu tabel Doris).
- Tiap use case berdiri sendiri, tidak ada dependensi antar file SQL.
- Jalankan producer dari root: `python produce_data/producer.py` (opsi `--spike-every 60` memicu skenario alert).

## Use case
1. `01_error_context`: interval join activity ⨝ error
2. `02_transaction_journey`: interval join berantai login → activity → transaction
3. `03_page_views_window`: tumbling window page views
4. `04_txn_summary_window`: tumbling window ringkasan transaksi
5. `05_alert_error_spike`: window + `HAVING`, alert lonjakan error
6. `06_alert_failed_txn_streak`: `MATCH_RECOGNIZE`, 3 transaksi gagal beruntun

Belum ada **window join** (hanya window aggregation).

## Status dan catatan penting
- Producer sudah diuji dan jalan. **File SQL Flink belum pernah dijalankan/diuji** (akses Docker dari sesi Claude ditolak: `permission denied` di docker.sock). Anggap sintaksnya belum terbukti.
- Bagian paling berisiko: use case 6 (tipe `MEASURES`/time attribute) dan use case 2 (join berantai).
- TaskManager hanya 2 slot, jadi maksimal ±2 job aktif bersamaan. Source `earliest-offset`, jadi menjalankan ulang job menghasilkan duplikat di topic hasil.
- Belum ada: Doris (compose, Routine Load), repo belum punya commit.
- `kafka-python` di mesin pengguna rusak (Python 3.10); selalu pakai `confluent_kafka`.
