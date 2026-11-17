#!/usr/bin/env python3
"""Generator data dummy app activity -> Kafka.

Setiap "sesi" user menghasilkan event yang saling berhubungan (user_id & session_id sama),
supaya bisa di-join dengan Flink SQL interval join:

    app_login -> app_activity (n kali) -> [app_transaction] -> [app_error] -> app_logout

event_time = waktu nyata saat event dikirim (UTC, format 'yyyy-MM-dd HH:mm:ss.SSS'),
sehingga cocok langsung dipakai sebagai TIMESTAMP(3) + watermark di Flink.

Contoh:
    python produce_data/producer.py                       # jalan terus, ~2 sesi/detik
    python produce_data/producer.py --rate 5 --duration 120
    python produce_data/producer.py --create-topics-only
    python produce_data/producer.py --spike-every 60      # picu skenario alert berkala
"""
import argparse
import heapq
import itertools
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = ["app_login", "app_activity", "app_transaction", "app_error", "app_logout"]

PAGES = ["/home", "/balance", "/transfer", "/payment", "/history", "/profile", "/promo"]
DEVICES = ["android", "ios", "web"]
TXN_TYPES = ["transfer", "payment", "topup"]
ERRORS = [
    ("E101", "timeout calling core banking"),
    ("E204", "insufficient balance"),
    ("E301", "invalid session token"),
    ("E500", "internal server error"),
]


def now_str() -> str:
    # UTC, format yang bisa di-parse Flink: TIMESTAMP(3)
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def make_session(user_id: str, args) -> list:
    """Bangun daftar (delay_detik, topic, builder) untuk satu sesi."""
    session_id = str(uuid.uuid4())
    events = []
    t = 0.0

    def ev(topic, **fields):
        base = {"event_id": str(uuid.uuid4()), "user_id": user_id, "session_id": session_id}
        base.update(fields)
        return topic, base

    events.append((t, *ev("app_login", device=random.choice(DEVICES),
                          ip=f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}")))

    page = "/home"
    for _ in range(random.randint(2, args.max_activities)):
        t += random.uniform(1, args.think_time)
        page = random.choice(PAGES)
        events.append((t, *ev("app_activity", page=page, action=random.choice(["view", "click", "scroll"]))))

        # Transaksi muncul setelah aktivitas di halaman finansial
        if page in ("/transfer", "/payment") and random.random() < args.txn_prob:
            t += random.uniform(1, 5)
            failed = random.random() < args.fail_prob
            events.append((t, *ev("app_transaction",
                                  txn_id=str(uuid.uuid4()),
                                  txn_type=random.choice(TXN_TYPES),
                                  amount=random.randint(10, 5000) * 1000,
                                  status="FAILED" if failed else "SUCCESS")))
            if failed:
                code, msg = random.choice(ERRORS)
                t += random.uniform(0.5, 3)
                events.append((t, *ev("app_error", error_code=code, error_message=msg, page=page)))

        # Error acak di halaman mana pun
        if random.random() < args.error_prob:
            code, msg = random.choice(ERRORS)
            t += random.uniform(0.2, 2)
            events.append((t, *ev("app_error", error_code=code, error_message=msg, page=page)))

    t += random.uniform(1, args.think_time)
    events.append((t, *ev("app_logout")))
    return events


def make_alert_scenarios(users) -> list:
    """Skenario pemicu alert (untuk demo), format sama dengan make_session.

    1) Lonjakan error : 12 error E500 dalam ~5 detik  -> use case 5 (>= 5 error/menit)
    2) Gagal beruntun : 1 user, 3 transaksi FAILED berturut-turut -> use case 6
    """
    events = []

    for _ in range(12):
        events.append((random.uniform(0, 5), "app_error", {
            "event_id": str(uuid.uuid4()), "user_id": random.choice(users),
            "session_id": str(uuid.uuid4()),
            "error_code": "E500", "error_message": "internal server error", "page": "/payment",
        }))

    user_id, session_id = random.choice(users), str(uuid.uuid4())
    t = 0.0
    for _ in range(3):
        t += random.uniform(2, 5)
        events.append((t, "app_transaction", {
            "event_id": str(uuid.uuid4()), "user_id": user_id, "session_id": session_id,
            "txn_id": str(uuid.uuid4()), "txn_type": "transfer",
            "amount": random.randint(100, 5000) * 1000, "status": "FAILED",
        }))
    return events


def create_topics(bootstrap: str, partitions: int):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in TOPICS if t not in existing]
    if not new:
        print("Semua topic sudah ada.")
        return
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print(f"Topic dibuat: {topic}")
        except Exception as e:  # noqa: BLE001
            print(f"Gagal membuat {topic}: {e}", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap (default: localhost:9092)")
    p.add_argument("--rate", type=float, default=2.0, help="sesi baru per detik (default 2)")
    p.add_argument("--users", type=int, default=50, help="jumlah user unik (default 50)")
    p.add_argument("--duration", type=float, default=0, help="lama jalan dalam detik, 0 = tanpa batas")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--max-activities", type=int, default=6)
    p.add_argument("--think-time", type=float, default=5, help="jeda maks antar aktivitas (detik)")
    p.add_argument("--txn-prob", type=float, default=0.5, help="peluang transaksi di halaman finansial")
    p.add_argument("--fail-prob", type=float, default=0.3, help="peluang transaksi gagal (+ error)")
    p.add_argument("--error-prob", type=float, default=0.1, help="peluang error acak per aktivitas")
    p.add_argument("--create-topics-only", action="store_true")
    p.add_argument("--spike-every", type=float, default=0,
                   help="setiap N detik picu skenario alert (lonjakan error + gagal beruntun), 0 = mati")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    create_topics(args.bootstrap, args.partitions)
    if args.create_topics_only:
        return

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    users = [f"user_{i:04d}" for i in range(1, args.users + 1)]

    stop = False

    def handle_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    counter = itertools.count()  # tie-breaker heap
    heap = []  # (due_ts, n, topic, payload)
    sent = {t: 0 for t in TOPICS}
    start = time.time()
    next_session = start
    last_report = start

    next_spike = start + args.spike_every if args.spike_every else None

    print(f"Mengirim ke {args.bootstrap} ... Ctrl+C untuk berhenti")
    while not stop:
        now = time.time()
        if args.duration and now - start >= args.duration:
            break

        # Jadwalkan sesi baru sesuai rate
        while now >= next_session:
            user = random.choice(users)
            for delay, topic, payload in make_session(user, args):
                heapq.heappush(heap, (next_session + delay, next(counter), topic, payload))
            next_session += random.expovariate(args.rate)

        # Picu skenario alert secara berkala (jika --spike-every diisi)
        if next_spike and now >= next_spike:
            for delay, topic, payload in make_alert_scenarios(users):
                heapq.heappush(heap, (now + delay, next(counter), topic, payload))
            print("  >> skenario alert dipicu (lonjakan error E500 + 3 transaksi gagal beruntun)")
            next_spike += args.spike_every

        # Kirim event yang sudah jatuh tempo
        while heap and heap[0][0] <= now:
            _, _, topic, payload = heapq.heappop(heap)
            payload["event_time"] = now_str()
            producer.produce(topic, key=payload["user_id"], value=json.dumps(payload))
            sent[topic] += 1

        producer.poll(0)

        if now - last_report >= 5:
            print("  terkirim:", ", ".join(f"{k}={v}" for k, v in sent.items()), f"| antrian={len(heap)}")
            last_report = now
        time.sleep(0.05)

    # Kirim sisa event supaya sesi tidak menggantung tanpa logout
    print("Menutup... mengirim sisa event")
    while heap:
        _, _, topic, payload = heapq.heappop(heap)
        payload["event_time"] = now_str()
        producer.produce(topic, key=payload["user_id"], value=json.dumps(payload))
        sent[topic] += 1
    producer.flush(10)
    print("Selesai:", sent)


if __name__ == "__main__":
    main()
