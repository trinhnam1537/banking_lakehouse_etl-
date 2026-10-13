"""Giao dịch thẻ kiểu Sparkov -> Kafka topic card.authorizations (+ chargeback trễ).

    python -m simulators.cards.stream generate --days 7 --speed 0          # 7 ngày nghiệp vụ, nhanh nhất
    python -m simulators.cards.stream generate --live --rate 5             # giờ THẬT, ~5 giao dịch/giây
    python -m simulators.cards.stream replay data/sparkov/fraudTrain.csv   # phát lại bộ Kaggle

Bộ Kaggle (1,85 triệu giao dịch 2019-2020, 1.000 khách Mỹ, sinh bằng Sparkov):
    kaggle datasets download -d kartik2112/fraud-detection -p data/sparkov --unzip

Chế độ `generate` tái hiện cách Sparkov sinh dữ liệu (hồ sơ khách theo tuổi/giới,
nhóm merchant, số tiền phân phối gamma, giờ giao dịch theo hồ sơ, gian lận theo đợt)
nhưng CHỦ THẺ LÀ KHÁCH CORE BANKING: cc_num = core.card.card_number, họ tên/giới/
ngày sinh lấy từ core.client. Nhờ vậy luồng thẻ nối được về khách hàng và tài khoản.
Không đọc được corebank thì tự sinh 1.000 chủ thẻ bằng Faker.

Giữ đúng tên cột của Sparkov (trans_date_trans_time, cc_num, merchant, category, amt,
first, last, gender, street, city, state, zip, lat, long, city_pop, job, dob, trans_num,
unix_time, merch_lat, merch_long) để mã xử lý viết cho bộ Kaggle dùng lại được.
Khác Sparkov: cc_num là số thẻ ĐÃ CHE (PCI DSS) kèm card_token = md5(số thẻ) để nối
về core.card; bỏ tiền tố "fraud_" vô nghĩa ở tên merchant; is_fraud KHÔNG đi vào Kafka
mà thành file chargeback landing/_inbox/cards/labels/ sau --label-delay-days ngày.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import random
import sys
import time
from typing import Dict, List

from ..common import SIM_EPOCH, LabelSink, corebank_conn, kafka_producer, stable_id

TOPIC = "card.authorizations"
# (nhom, ty trong, trung vi so tien, ty le giao dich dem)
CATEGORIES = [("grocery_pos", .14, 60, .05), ("gas_transport", .12, 45, .10), ("home", .10, 55, .03),
              ("shopping_pos", .09, 70, .04), ("kids_pets", .09, 40, .03), ("shopping_net", .07, 65, .20),
              ("entertainment", .07, 45, .15), ("food_dining", .07, 35, .20), ("personal_care", .07, 30, .05),
              ("health_fitness", .06, 40, .03), ("misc_pos", .06, 50, .05), ("misc_net", .05, 60, .25),
              ("grocery_net", .03, 50, .10), ("travel", .03, 120, .15)]
# Vung lanh tho Sec (chu the la khach core banking Berka)
LAT, LON = (48.6, 51.0), (12.2, 18.8)


def _pick(rng, items, w):
    return rng.choices(items, w)[0]


def holders(rng: random.Random) -> List[Dict]:
    from faker import Faker

    fake = Faker("cs_CZ")
    try:
        conn = corebank_conn()
        cur = conn.cursor()
        cur.execute("""SELECT k.card_number, c.full_name, c.gender, c.birth_date, c.address, c.client_id
                       FROM core.card k JOIN core.disp d USING (disp_id) JOIN core.client c USING (client_id)
                       WHERE k.status = 'ACTIVE'""")
        rows = cur.fetchall()
        conn.close()
    except Exception as e:  # noqa: BLE001
        print(f"(khong doc duoc corebank: {e}) -> sinh 1.000 chu the bang Faker")
        rows = []
        for i in range(1000):
            fake.seed_instance(i)
            g = rng.choice("FM")
            rows.append((str(4 * 10**15 + i), fake.name_female() if g == "F" else fake.name_male(), g,
                         fake.date_of_birth(minimum_age=18, maximum_age=85), fake.address(), None))
    out = []
    for cc, name, g, dob, addr, cid in rows:
        r = random.Random(cc)
        fake.seed_instance(int(cc[-9:]))
        first, _, last = name.partition(" ")
        out.append({"cc_num": cc, "first": first, "last": last or first, "gender": g, "dob": str(dob),
                    "street": (addr or "").split(",")[0], "city": fake.city(), "state": "CZ",
                    "zip": fake.postcode(), "lat": round(r.uniform(*LAT), 4), "long": round(r.uniform(*LON), 4),
                    "city_pop": int(r.lognormvariate(9, 1.5)), "job": fake.job(), "client_id": cid,
                    "rate": r.uniform(0.5, 3.0),  # giao dich/ngay
                    "night_owl": r.random() < 0.2})
    return out


def merchants(rng: random.Random, n: int = 800) -> List[Dict]:
    from faker import Faker

    fake = Faker("cs_CZ")
    fake.seed_instance(800)
    out = []
    for i in range(n):
        cat = _pick(rng, CATEGORIES, [c[1] for c in CATEGORIES])
        out.append({"merchant": fake.company(), "category": cat[0], "median": cat[2], "night": cat[3],
                    "merch_lat": round(rng.uniform(*LAT), 6), "merch_long": round(rng.uniform(*LON), 6)})
    return out


def card_token(pan: str) -> str:
    """Token = md5(số thẻ) dạng hex — cùng công thức với entry corebank_card
    (`md5(card_number)` trong Postgres), nên hai luồng nối được mà không ai giữ PAN."""
    import hashlib

    return hashlib.md5(pan.encode()).hexdigest()


def mask(pan: str) -> str:
    return pan[:6] + "*" * (len(pan) - 10) + pan[-4:]


def event(h: Dict, m: Dict, ts: dt.datetime, amt: float) -> Dict:
    return {
        "trans_date_trans_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
        # PCI DSS: so the day du KHONG roi xuong Kafka/raw. cc_num giu ten cot Sparkov
        # nhung la ban che; card_token la khoa noi ve core.card.
        "cc_num": mask(h["cc_num"]), "card_token": card_token(h["cc_num"]), "merchant": m["merchant"], "category": m["category"], "amt": round(amt, 2),
        "first": h["first"], "last": h["last"], "gender": h["gender"], "street": h["street"],
        "city": h["city"], "state": h["state"], "zip": h["zip"], "lat": h["lat"], "long": h["long"],
        "city_pop": h["city_pop"], "job": h["job"], "dob": h["dob"],
        "trans_num": stable_id("card", h["cc_num"], ts.isoformat(), m["merchant"], amt, n=32),
        "unix_time": int(ts.replace(tzinfo=dt.timezone.utc).timestamp()),
        "merch_lat": m["merch_lat"], "merch_long": m["merch_long"],
    }


def day_events(rng: random.Random, d: dt.date, hs: List[Dict], ms: List[Dict]):
    """(ts, event, is_fraud) cho một ngày, đã sắp theo thời gian."""
    out = []
    w = [1 - m["night"] for m in ms]
    for h in hs:
        for _ in range(_poisson(rng, h["rate"])):
            m = _pick(rng, ms, w)
            night = h["night_owl"] and rng.random() < 0.3
            hour = rng.randint(0, 5) if night else int(min(23, max(6, rng.gauss(15, 4))))
            ts = dt.datetime.combine(d, dt.time(hour, rng.randint(0, 59), rng.randint(0, 59)))
            amt = rng.gammavariate(2.0, m["median"] / 1.7)
            out.append((ts, event(h, m, ts, amt), 0))
    # Gian lan theo dot: the bi lo -> 3-8 giao dich dem, online, so tien lon, xa nha
    for h in rng.sample(hs, min(len(hs), _poisson(rng, len(hs) * 0.0015))):
        ts = dt.datetime.combine(d, dt.time(rng.randint(0, 3), rng.randint(0, 59)))
        for _ in range(rng.randint(3, 8)):
            ts += dt.timedelta(minutes=rng.randint(2, 50))
            m = rng.choice([x for x in ms if x["category"] in ("shopping_net", "misc_net", "grocery_pos")])
            out.append((ts, event(h, m, ts, rng.uniform(250, 1300)), 1))
    out.sort(key=lambda x: x[0])
    return out


def _poisson(rng, lam):
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def cmd_generate(args) -> None:
    rng = random.Random(args.seed)
    hs, ms = holders(rng), merchants(rng)
    print(f"{len(hs)} chu the, {len(ms)} merchant")
    producer = None if args.dry else kafka_producer("sim-cards")
    labels = LabelSink("cards", ["trans_num", "is_fraud"], args.label_delay_days, dry=args.dry)
    d = dt.date.today() if args.live else (args.start or SIM_EPOCH)
    end = d + dt.timedelta(days=args.days - 1)
    sent = 0
    while args.live or d <= end:
        evs = day_events(rng, d, hs, ms)
        wall0, sim0 = time.monotonic(), dt.datetime.combine(d, dt.time())
        for ts, ev, fraud in evs:
            if args.live:  # gio that: cho toi dung thoi diem, dong dau thoi gian hien tai
                now = dt.datetime.utcnow()
                if ts > now:
                    time.sleep(min((ts - now).total_seconds(), 1 / max(args.rate, 0.1)))
            elif args.speed > 0:
                lag = (ts - sim0).total_seconds() / args.speed - (time.monotonic() - wall0)
                if lag > 0:
                    time.sleep(lag)
            if producer is not None:
                producer.produce(TOPIC, key=ev["card_token"], value=json.dumps(ev, ensure_ascii=False))
                producer.poll(0)
            labels.add(d, (ev["trans_num"], fraud))
            sent += 1
        labels.advance(d)
        print(f"{d}: {len(evs)} giao dich, {sum(f for *_, f in evs)} gian lan")
        d += dt.timedelta(days=1)
    if producer is not None:
        producer.flush(60)
    labels.close()
    print(f"Da phat {sent:,} giao dich vao {TOPIC}")


def cmd_replay(args) -> None:
    producer = None if args.dry else kafka_producer("sim-cards-replay")
    labels = LabelSink("cards", ["trans_num", "is_fraud"], args.label_delay_days, dry=args.dry)
    shift = None
    n = 0
    with open(args.path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row.pop("", None)  # cot chi so khong ten cua file Kaggle
            ts = dt.datetime.strptime(row["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S")
            if shift is None:
                shift = dt.datetime.combine(SIM_EPOCH, dt.time()) - dt.datetime.combine(ts.date(), dt.time())
            ts += shift
            fraud = row.pop("is_fraud")
            row["merchant"] = row["merchant"].removeprefix("fraud_")
            pan = row["cc_num"]
            row["cc_num"], row["card_token"] = mask(pan), card_token(pan)
            row["trans_date_trans_time"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            row["unix_time"] = int(ts.replace(tzinfo=dt.timezone.utc).timestamp())
            labels.add(ts.date(), (row["trans_num"], fraud))
            if producer is not None:
                producer.produce(TOPIC, key=row["card_token"], value=json.dumps(row, ensure_ascii=False))
                producer.poll(0)
            n += 1
            if args.limit and n >= args.limit:
                break
    if producer is not None:
        producer.flush(60)
    labels.close()
    print(f"Da phat lai {n:,} giao dich")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--days", type=int, default=1)
    g.add_argument("--start", type=dt.date.fromisoformat)
    g.add_argument("--speed", type=float, default=0, help="giay nghiep vu / giay that; 0 = nhanh nhat")
    g.add_argument("--live", action="store_true")
    g.add_argument("--rate", type=float, default=5)
    g.add_argument("--seed", type=int, default=42)
    r = sub.add_parser("replay")
    r.add_argument("path")
    r.add_argument("--limit", type=int, default=0)
    for p in (g, r):
        p.add_argument("--label-delay-days", type=int, default=20)
        p.add_argument("--dry", action="store_true")
    args = ap.parse_args(argv)
    {"generate": cmd_generate, "replay": cmd_replay}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
