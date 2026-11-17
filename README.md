# Kafka → Flink SQL → (Doris): App Activity Interval Join

Proyek belajar untuk membangun pipeline streaming dari log aplikasi. Beberapa topic Kafka
di-*join* dengan **Flink SQL (interval join)** menjadi topic "matang" (enriched) yang
nantinya bisa diteruskan ke Apache Doris.

```
produce_data/producer.py ──► Kafka (5 topic mentah) ──► Flink SQL interval join ──► Kafka (topic enriched_*) ──► (Doris)
```

> Status: Kafka, Flink, dan generator data sudah ada. Integrasi Doris belum dibuat.

## Struktur folder

```
.
├── docker-compose.yml          # Kafka (KRaft), Kafka Connect, Kafka UI, Flink JM + TM
├── requirements.txt            # dependensi Python untuk producer
├── .venv/                      # virtualenv (dibuat sendiri, tidak di-commit)
├── produce_data/
│   └── producer.py             # generator data dummy -> Kafka
└── flink/
    ├── lib/                    # flink-sql-connector-kafka.jar (di-mount ke container)
    └── sql/
        ├── 00_init.sql             # DDL 5 source + 2 sink
        ├── 01_error_context.sql    # Use case 1
        ├── 02_transaction_journey.sql  # Use case 2
        ├── 03_page_views_window.sql    # Use case 3 (window)
        ├── 04_txn_summary_window.sql   # Use case 4 (window)
        ├── 05_alert_error_spike.sql    # Use case 5 (alert, window + HAVING)
        └── 06_alert_failed_txn_streak.sql  # Use case 6 (alert, MATCH_RECOGNIZE)
```

## Topic sumber

| Topic | Event | Field utama |
|---|---|---|
| `app_login` | User berhasil login | `device`, `ip` |
| `app_activity` | User membuka halaman / beraktivitas | `page`, `action` |
| `app_transaction` | User melakukan transaksi | `txn_id`, `txn_type`, `amount`, `status` |
| `app_error` | Error saat aplikasi digunakan | `error_code`, `error_message`, `page` |
| `app_logout` | User logout | - |

Semua event punya `event_id`, `user_id`, `session_id`, dan `event_time`
(UTC, format `yyyy-MM-dd HH:mm:ss.SSS`, dipakai sebagai event time + watermark 5 detik di Flink).
Kunci join antar topic adalah `session_id`.

## Use case Flink SQL

### Use case 1 — Error context

**Pertanyaan bisnis:** saat error terjadi, user sedang berada di halaman / aktivitas apa?

- **Join:** `app_activity` ⨝ `app_error` pada `session_id`
- **Interval:** error muncul 0–30 detik setelah aktivitas
- **Output:** topic `enriched_error_context`

| Kolom output | Keterangan |
|---|---|
| `user_id`, `session_id` | identitas user dan sesi |
| `page`, `activity_action`, `activity_time` | aktivitas sebelum error |
| `error_code`, `error_message`, `error_time` | detail error |
| `seconds_to_error` | selisih detik aktivitas → error |

```sql
INSERT INTO enriched_error_context
SELECT a.user_id, a.session_id, a.page, a.action AS activity_action,
       a.event_time AS activity_time, e.error_code, e.error_message,
       e.event_time AS error_time,
       TIMESTAMPDIFF(SECOND, a.event_time, e.event_time) AS seconds_to_error
FROM app_activity AS a
JOIN app_error AS e
  ON  a.session_id = e.session_id
  AND e.event_time BETWEEN a.event_time AND a.event_time + INTERVAL '30' SECOND;
```

### Use case 2 — Transaction journey

**Pertanyaan bisnis:** transaksi ini dilakukan dari device apa, lewat halaman apa, dan
berapa lama setelah aktivitas tersebut?

- **Join (berantai, 3 topic):** `app_login` → `app_activity` → `app_transaction` pada `session_id`
- **Interval:** aktivitas 0–5 menit setelah login; transaksi 0–30 detik setelah aktivitas
- **Filter:** hanya halaman `/transfer` dan `/payment`
- **Output:** topic `enriched_transaction_journey`

| Kolom output | Keterangan |
|---|---|
| `user_id`, `session_id` | identitas user dan sesi |
| `device`, `login_time` | dari `app_login` |
| `page`, `activity_time` | dari `app_activity` |
| `txn_id`, `txn_type`, `amount`, `status`, `txn_time` | dari `app_transaction` |
| `seconds_to_txn` | selisih detik aktivitas → transaksi |

Query lengkap ada di [`flink/sql/02_transaction_journey.sql`](flink/sql/02_transaction_journey.sql).

### Use case 3 — Page views per menit (window aggregation)

**Pertanyaan bisnis:** halaman mana yang paling banyak dibuka tiap menit, dan oleh berapa user unik?

- **Teknik:** tumbling window 1 menit pada `app_activity` (`TUMBLE` + `GROUP BY`)
- **Output:** topic `page_views_per_minute`

| Kolom output | Keterangan |
|---|---|
| `window_start`, `window_end` | batas jendela 1 menit |
| `page` | halaman |
| `views` | jumlah event aktivitas |
| `unique_users` | jumlah user unik |

```sql
INSERT INTO page_views_per_minute
SELECT window_start, window_end, page,
       COUNT(*) AS views, COUNT(DISTINCT user_id) AS unique_users
FROM TABLE(TUMBLE(TABLE app_activity, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, page;
```

### Use case 4 — Ringkasan transaksi per menit (window aggregation)

**Pertanyaan bisnis:** berapa transaksi `SUCCESS` vs `FAILED` tiap menit, dan total nominalnya?

- **Teknik:** tumbling window 1 menit pada `app_transaction`
- **Output:** topic `txn_summary_per_minute` (`window_start`, `window_end`, `status`, `txn_count`, `total_amount`)

Query lengkap: [`flink/sql/04_txn_summary_window.sql`](flink/sql/04_txn_summary_window.sql).

### Use case 5 — Alert lonjakan error (window + `HAVING`)

**Pertanyaan bisnis:** apakah satu jenis error tiba-tiba meledak?

- **Teknik:** tumbling window 1 menit pada `app_error`, dikelompokkan per `error_code`, lalu `HAVING COUNT(*) >= 5`
- **Output:** topic `alerts` dengan `alert_type = ERROR_SPIKE`, `severity = HIGH`

```sql
INSERT INTO alerts
SELECT 'ERROR_SPIKE', 'HIGH', CAST(NULL AS STRING), error_code, COUNT(*),
       CONCAT('error_code=', error_code, ' muncul ', CAST(COUNT(*) AS STRING), 'x dalam 1 menit'),
       window_start, window_end
FROM TABLE(TUMBLE(TABLE app_error, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, error_code
HAVING COUNT(*) >= 5;
```

### Use case 6 — Alert transaksi gagal beruntun (`MATCH_RECOGNIZE`)

**Pertanyaan bisnis:** adakah user yang gagal bertransaksi 3 kali berturut-turut?

- **Teknik:** deteksi pola urutan event (`MATCH_RECOGNIZE`) per `user_id`, pola `F F F`
  (3 transaksi `FAILED` berurutan) dalam 2 menit
- **Output:** topic `alerts` dengan `alert_type = FAILED_TXN_STREAK`, `severity = CRITICAL`

Query lengkap: [`flink/sql/06_alert_failed_txn_streak.sql`](flink/sql/06_alert_failed_txn_streak.sql).

### Skema topic `alerts` (untuk Doris)

Use case 5 dan 6 menulis ke **satu topic** dengan skema seragam, sehingga cukup satu tabel di Doris:

| Kolom | Keterangan |
|---|---|
| `alert_type` | `ERROR_SPIKE` / `FAILED_TXN_STREAK` |
| `severity` | `MEDIUM` / `HIGH` / `CRITICAL` |
| `user_id` | user terkait (NULL untuk alert tingkat sistem) |
| `subject` | `error_code` atau `user_id` |
| `metric` | nilai pemicu (jumlah error / jumlah gagal) |
| `detail` | deskripsi singkat |
| `start_time`, `end_time` | rentang waktu kejadian (`yyyy-MM-dd HH:mm:ss.SSS`) |

### Interval join vs window aggregation vs pattern

| | Interval join (use case 1–2) | Window aggregation (use case 3–5) | MATCH_RECOGNIZE (use case 6) |
|---|---|---|---|
| Fungsi | mencocokkan 2 event yang berkaitan | meringkas banyak event per periode | mendeteksi pola urutan event |
| Output | 1 baris per pasangan event | 1 baris per window per grup | 1 baris per pola yang cocok |
| Cocok untuk | enrichment, konteks, durasi | metrik, dashboard, alert ambang batas | alert pola perilaku |

### Catatan

- Satu error/transaksi bisa cocok dengan beberapa aktivitas dalam interval, sehingga satu
  event sumber dapat menghasilkan beberapa baris. Persempit interval atau dedup bila perlu.
- Interval join hanya menghasilkan *insert* (append-only), jadi sink memakai connector `kafka` biasa.
- File SQL ini belum diuji penuh di Flink. Join berantai di use case 2 adalah bagian yang
  paling mungkin perlu penyesuaian.

## Cara menjalankan

### 1. Jalankan infrastruktur

```bash
docker compose up -d
```

| Service | URL |
|---|---|
| Kafka (dari host) | `localhost:9092` |
| Kafka UI | http://localhost:8080 |
| Flink Web UI | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |

Pastikan file `flink/lib/flink-sql-connector-kafka.jar` ada (di-mount ke container Flink).

### 2. Jalankan producer data dummy (Python + venv)

```bash
# dari root proyek
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python produce_data/producer.py                          # terus-menerus, 2 sesi/detik
python produce_data/producer.py --rate 5 --duration 120  # 5 sesi/detik selama 2 menit
python produce_data/producer.py --create-topics-only     # hanya membuat topic
python produce_data/producer.py --spike-every 60         # picu skenario alert tiap 60 detik (use case 5 & 6)
```

`--spike-every N` menyuntikkan, tiap N detik, 12 error `E500` dalam ±5 detik (memicu use case 5)
dan 1 user dengan 3 transaksi `FAILED` berturut-turut (memicu use case 6).

Producer membuat kelima topic otomatis (3 partisi). Opsi lain: `--bootstrap`, `--users`,
`--think-time`, `--txn-prob`, `--fail-prob`, `--error-prob`, `--seed`
(lihat `python produce_data/producer.py --help`).

Satu sesi menghasilkan event berurutan dengan `user_id`/`session_id` yang sama:
`login → activity (n kali) → [transaction] → [error] → logout`.
Transaksi yang gagal selalu diikuti `app_error`.

### 3. Jalankan job Flink SQL

Biarkan producer berjalan, lalu di terminal lain:

```bash
# Use case 1
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/01_error_context.sql

# Use case 2
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/02_transaction_journey.sql

# Use case 3 dan 4 (window)
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/03_page_views_window.sql
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/04_txn_summary_window.sql

# Use case 5 dan 6 (alert) -- jalankan producer dengan --spike-every 60
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/05_alert_error_spike.sql
docker exec -it flink-jobmanager ./bin/sql-client.sh \
  -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/06_alert_failed_txn_streak.sql
```

Pantau job di http://localhost:8081, dan lihat hasilnya di Kafka UI pada topic
`enriched_error_context`, `enriched_transaction_journey`, `page_views_per_minute`, dan
`txn_summary_per_minute`, dan `alerts`.
Biarkan producer jalan minimal 1–2 menit karena watermark baru maju saat ada event baru.
Untuk window 1 menit, baris pertama baru muncul setelah window ditutup (±1 menit + 5 detik watermark).

## Rencana berikutnya

- Menambah use case lain: transaksi gagal + penyebabnya (`app_transaction` ⨝ `app_error`),
  durasi sesi (`app_login` ⨝ `app_logout`).
- Mengalirkan topic hasil (`enriched_*`, `page_views_per_minute`, `txn_summary_per_minute`, `alerts`)
  ke Apache Doris (Routine Load atau Flink Doris connector).
  Network `data-net` di `docker-compose.yml` sudah disiapkan untuk ini.
