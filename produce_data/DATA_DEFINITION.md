# Definisi Data: 5 Topic Mentah

Dokumen ini menjelaskan isi 5 topic mentah yang dihasilkan `producer.py`, yaitu data yang nanti di-*enrich* oleh Flink.
Contoh pesan di bawah diambil langsung dari Kafka (hasil jalan producer sungguhan).

## Gambaran umum

Satu **sesi** user menghasilkan event yang saling berhubungan lewat `user_id` dan `session_id`:

```
app_login ──▶ app_activity (2..n kali) ──▶ [app_transaction] ──▶ [app_error] ──▶ app_logout
                                              (hanya di /transfer, /payment)   (jika txn gagal / acak)
```

- `[...]` artinya opsional, tidak setiap sesi punya.
- **`session_id` adalah kunci join** antar topic. `user_id` dipakai untuk pola per-user (use case 6).
- Format JSON, satu pesan = satu event. Tidak ada key Kafka, jadi sebaran partisi acak (3 partisi).

## Kolom yang ada di semua topic

| Kolom | Tipe Flink | Keterangan |
|---|---|---|
| `event_id` | STRING | UUID unik per event |
| `user_id` | STRING | `user_0001` .. `user_0050` (50 user unik secara default) |
| `session_id` | STRING | UUID per sesi, sama untuk semua event dalam satu sesi |
| `event_time` | TIMESTAMP(3) | Waktu event, UTC, format `yyyy-MM-dd HH:mm:ss.SSS`. Dipakai sebagai event time, watermark 5 detik |

## 1. `app_login`: user masuk aplikasi

Pertama kali muncul dalam sesi. Menandai awal sesi.

| Kolom | Tipe | Nilai / contoh |
|---|---|---|
| `device` | STRING | `android`, `ios`, `web` |
| `ip` | STRING | `10.x.x.x` (acak) |

```json
{"event_id": "1f07a9c0-5a1b-4480-bbb5-ceff44a84451", "user_id": "user_0050", "session_id": "9fd4d26d-68a3-411a-ae84-1e6f6a73fa3f", "device": "ios", "ip": "10.125.238.158", "event_time": "2026-11-19 02:51:44.899"}
```

## 2. `app_activity`: user membuka halaman / berinteraksi

Setiap sesi punya 2 sampai `--max-activities` aktivitas. Jarak antar aktivitas 1 sampai `--think-time` detik.

| Kolom | Tipe | Nilai / contoh |
|---|---|---|
| `page` | STRING | `/home`, `/balance`, `/transfer`, `/payment`, `/history`, `/profile`, `/promo` |
| `action` | STRING | `view`, `click`, `scroll` |

```json
{"event_id": "220910f6-1e70-47df-b62b-c352edc57823", "user_id": "user_0032", "session_id": "f754e3dd-2d72-4356-a33f-1fb0f1dad7f4", "page": "/payment", "action": "click", "event_time": "2026-11-19 02:51:46.660"}
```

## 3. `app_transaction`: transaksi keuangan

Hanya muncul setelah aktivitas di `/transfer` atau `/payment` (peluang `--txn-prob`). Sebagian gagal (peluang `--fail-prob`).

| Kolom | Tipe | Nilai / contoh |
|---|---|---|
| `txn_id` | STRING | UUID transaksi |
| `txn_type` | STRING | `transfer`, `payment`, `topup` |
| `amount` | BIGINT | 10.000 sampai 5.000.000 (kelipatan 1.000) |
| `status` | STRING | `SUCCESS` atau `FAILED` |

```json
{"event_id": "6de21dbb-4870-408c-aadb-034e830aa238", "user_id": "user_0032", "session_id": "f754e3dd-2d72-4356-a33f-1fb0f1dad7f4", "txn_id": "88276751-e062-4f5b-aab9-2b309812777c", "txn_type": "transfer", "amount": 3060000, "status": "SUCCESS", "event_time": "2026-11-19 02:51:48.370"}
```

## 4. `app_error`: error aplikasi

Muncul dalam dua kondisi: (a) setelah transaksi `FAILED`, dan (b) acak di halaman mana pun (peluang `--error-prob`).

| Kolom | Tipe | Nilai / contoh |
|---|---|---|
| `error_code` | STRING | `E101` (timeout core banking), `E204` (saldo kurang), `E301` (token sesi tidak valid), `E500` (internal server error) |
| `error_message` | STRING | teks sesuai kode di atas |
| `page` | STRING | halaman tempat error terjadi (nilai sama seperti `app_activity.page`) |

```json
{"event_id": "9facfce3-6787-4d1c-8208-1474c27e9162", "user_id": "user_0039", "session_id": "ba229426-7fc4-4e4d-b64d-5a7dacbb9e71", "error_code": "E500", "error_message": "internal server error", "page": "/transfer", "event_time": "2026-11-19 02:51:55.267"}
```

## 5. `app_logout`: user keluar

Event terakhir sesi. Tidak punya kolom tambahan selain kolom umum.

```json
{"event_id": "e530f920-84e4-4ea4-948e-2670d9ae4b4b", "user_id": "user_0032", "session_id": "f754e3dd-2d72-4356-a33f-1fb0f1dad7f4", "event_time": "2026-11-19 02:51:55.166"}
```

## Satu sesi utuh (contoh mengikuti `session_id` yang sama)

Sesi `f754e3dd-...` milik `user_0032`:

| Waktu | Topic | Isi singkat |
|---|---|---|
| 02:51:45.352 | `app_login` | device `web` |
| 02:51:46.660 | `app_activity` | `/payment`, `click` |
| 02:51:48.370 | `app_transaction` | `transfer`, 3.060.000, `SUCCESS` |
| 02:51:55.166 | `app_logout` | |

Inilah yang digabung Flink. Contoh use case 2 (`02_transaction_journey`) menyatukan login + activity + transaction jadi satu baris di topic `enriched_transaction_journey`:

```json
{"user_id":"user_0032","session_id":"f754e3dd-...","device":"web","login_time":"2026-11-19 02:51:45.352","page":"/payment","activity_time":"2026-11-19 02:51:46.66","txn_id":"88276751-...","txn_type":"transfer","amount":3060000,"status":"SUCCESS","txn_time":"2026-11-19 02:51:48.37","seconds_to_txn":1}
```

## Peta topic mentah → use case Flink

| Topic mentah | Dipakai oleh use case |
|---|---|
| `app_login` | 2 (journey) |
| `app_activity` | 1 (error context), 2 (journey), 3 (page views) |
| `app_transaction` | 2 (journey), 4 (ringkasan transaksi), 6 (gagal beruntun) |
| `app_error` | 1 (error context), 5 (lonjakan error) |
| `app_logout` | belum dipakai oleh use case mana pun |

## Skenario alert (opsi `--spike-every N`)

Setiap N detik producer menyuntikkan dua skenario untuk memicu alert:
- **Lonjakan error:** 12 error `E500` di `/payment` dalam sekitar 5 detik, dengan `session_id` acak. Memicu use case 5.
- **Gagal beruntun:** satu user dengan 3 transaksi `FAILED` berturut-turut. Memicu use case 6.

Event dari skenario ini tidak punya login/activity pendamping, jadi tidak ikut tergabung di use case 1 dan 2.
