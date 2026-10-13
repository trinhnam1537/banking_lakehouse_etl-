"""Nạp lịch sử hệ thống cho vay vào Postgres lending.

    python -m simulators.lending.seed                    # file Kaggle nếu có, không thì tổng hợp
    python -m simulators.lending.seed --applicants 5000
    python -m simulators.lending.seed --synthetic        # bắt buộc tổng hợp

Bộ Kaggle (cần tài khoản + đồng ý luật cuộc thi, KHÔNG được phân phối lại):
    kaggle competitions download -c home-credit-default-risk -p data/home-credit
    -> giải nén để có data/home-credit/application_train.csv, bureau.csv, ...

Chế độ Kaggle LẤY MẪU THEO SK_ID_CURR rồi kéo mọi dòng con của đúng các hồ sơ đó:
giữ toàn vẹn tham chiếu xuống 6 bảng con mà giảm khối lượng hàng chục lần
(bureau_balance gốc 27 triệu dòng). File được đọc theo luồng, không nạp vào RAM.

Cả hai chế độ:
  * application_date: ngày trong năm trước SIM_EPOCH; status = DISBURSED
    (application_train của Kaggle là các khoản đã giải ngân)
  * customer_ref: ~70% hồ sơ gán cho khách core banking (nếu corebank đã seed),
    còn lại là khách chưa có tài khoản — ánh xạ GIẢ LẬP, tất định theo seed
  * TARGET -> file nhãn landing/_inbox/lending/labels/ (không vào DB nguồn)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import random
import sys
import time
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set

from ..common import DATA_DIR, SIM_EPOCH, corebank_conn, csv_gz, inbox_key, lending_conn, put_inbox
from . import generator as g

TABLES = ["application", "bureau", "bureau_balance", "previous_application",
          "pos_cash_balance", "credit_card_balance", "installments_payments"]
KAGGLE_FILES = {"bureau": "bureau.csv", "bureau_balance": "bureau_balance.csv",
                "previous_application": "previous_application.csv",
                "pos_cash_balance": "POS_CASH_balance.csv",
                "credit_card_balance": "credit_card_balance.csv",
                "installments_payments": "installments_payments.csv"}


def table_columns(cur, table: str) -> List[str]:
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'lending' AND table_name = %s
                     AND column_name NOT IN ('id', 'created_at', 'updated_at')
                   ORDER BY ordinal_position""", (table,))
    return [r[0] for r in cur.fetchall()]


class Copier:
    """Gom dict theo bảng, COPY mỗi 50.000 dòng."""

    def __init__(self, cur):
        self.cur = cur
        self.cols = {t: table_columns(cur, t) for t in TABLES}
        self.buf: Dict[str, List[Dict]] = {t: [] for t in TABLES}
        self.count: Dict[str, int] = {t: 0 for t in TABLES}

    def add(self, table: str, row: Dict) -> None:
        self.buf[table].append(row)
        if len(self.buf[table]) >= 50_000:
            self.flush()  # xa CA cac bang theo thu tu FK, khong chi bang nay

    def flush(self, table: Optional[str] = None) -> None:
        # Thu tu FK: cha truoc con
        for t in ([table] if table else TABLES):
            rows = self.buf[t]
            if not rows:
                continue
            cols = self.cols[t]
            out = io.StringIO()
            w = csv.writer(out, lineterminator="\n")
            for r in rows:
                w.writerow(["\\N" if r.get(c) in (None, "") else
                            (json.dumps(r[c]) if c == "extra" else r[c]) for c in cols])
            out.seek(0)
            self.cur.copy_expert(f"COPY lending.{t} ({', '.join(cols)}) FROM STDIN "
                                 f"WITH (FORMAT csv, NULL '\\N')", out)
            self.count[t] += len(rows)
            self.buf[t] = []


def core_clients() -> List[int]:
    try:
        conn = corebank_conn()
        cur = conn.cursor()
        cur.execute("SELECT client_id FROM core.client ORDER BY client_id")
        ids = [r[0] for r in cur.fetchall()]
        conn.close()
        return ids
    except Exception as e:  # noqa: BLE001
        print(f"(khong doc duoc corebank: {e}) -> customer_ref = NULL")
        return []


def stamp_app(rng: random.Random, row: Dict, clients: List[int], used: Set[int]) -> None:
    row["application_date"] = SIM_EPOCH - dt.timedelta(days=rng.randint(1, 365))
    row["status"] = "DISBURSED"
    free = [c for c in clients if c not in used] if clients and rng.random() < 0.7 else []
    if free:
        c = rng.choice(free)
        used.add(c)
        row["customer_ref"] = c


def _lower(row: Dict[str, str]) -> Dict[str, str]:
    return {k.lower(): (v if v != "" else None) for k, v in row.items()}


def _stream(path) -> Iterator[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield _lower(row)


def seed_kaggle(cp: Copier, n: int, rng: random.Random, clients: List[int]) -> List[Sequence]:
    base = DATA_DIR / "home-credit"
    app_cols = set(cp.cols["application"])
    # Lan 1: chon mau SK_ID_CURR (reservoir sampling — khong giu ca file trong RAM)
    sample: List[Dict] = []
    for i, row in enumerate(_stream(base / "application_train.csv")):
        if len(sample) < n:
            sample.append(row)
        else:
            j = rng.randint(0, i)
            if j < n:
                sample[j] = row
    labels, used, ids = [], set(), set()
    for row in sample:
        target = row.pop("target")
        app = {k: v for k, v in row.items() if k in app_cols}
        app["extra"] = {k: v for k, v in row.items() if k not in app_cols and v is not None}
        stamp_app(rng, app, clients, used)
        cp.add("application", app)
        ids.add(row["sk_id_curr"])
        labels.append((row["sk_id_curr"], target))
    cp.flush("application")

    bureau_ids: Set[str] = set()
    for t in ["bureau", "bureau_balance", "previous_application", "pos_cash_balance",
              "credit_card_balance", "installments_payments"]:
        cols = set(cp.cols[t])
        for row in _stream(base / KAGGLE_FILES[t]):
            if t == "bureau_balance":
                if row["sk_id_bureau"] not in bureau_ids:
                    continue
            elif row.get("sk_id_curr") not in ids:
                continue
            if t == "bureau":
                bureau_ids.add(row["sk_id_bureau"])
            cp.add(t, {k: v for k, v in row.items() if k in cols})
        cp.flush(t)
        print(f"  {t}: {cp.count[t]}")
    return labels


def seed_synthetic(cp: Copier, n: int, rng: random.Random, clients: List[int]) -> List[Sequence]:
    labels, used = [], set()
    next_bureau, next_prev = 5_000_000, 1_000_000
    for i in range(n):
        sk = 100_002 + i
        app = g.application(rng, sk)
        stamp_app(rng, app, clients, used)
        cp.add("application", app)
        labels.append((sk, int(rng.random() < g.default_prob(app))))
        b_rows, bb = g.bureau(rng, sk, next_bureau)
        next_bureau += len(b_rows)
        prevs, pos, cc, inst = g.previous(rng, sk, next_prev)
        next_prev += len(prevs)
        for t, rows in (("bureau", b_rows), ("bureau_balance", bb), ("previous_application", prevs),
                        ("pos_cash_balance", pos), ("credit_card_balance", cc),
                        ("installments_payments", inst)):
            for r in rows:
                cp.add(t, r)
        if i % 1000 == 999:
            cp.flush()  # giu thu tu FK: application + bureau truoc bureau_balance
    return labels


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--applicants", type=int, default=5000)
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--truncate", action="store_true")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--no-labels", action="store_true", help="khong ghi file nhan len S3")
    args = ap.parse_args(argv)

    t0 = time.time()
    conn = lending_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM lending.application")
    if cur.fetchone()[0] and not args.truncate:
        print("lending.application da co du lieu. Them --truncate de nap lai.")
        return 1
    if args.truncate:
        cur.execute("TRUNCATE " + ", ".join(f"lending.{t}" for t in TABLES) + " CASCADE")

    rng = random.Random(args.seed)
    clients = core_clients()
    cp = Copier(cur)
    kaggle = (DATA_DIR / "home-credit" / "application_train.csv").exists() and not args.synthetic
    print("Che do:", "Kaggle (lay mau)" if kaggle else "tong hop (generator.py)")
    labels = (seed_kaggle if kaggle else seed_synthetic)(cp, args.applicants, rng, clients)
    cp.flush()
    conn.commit()
    for t in TABLES:
        print(f"{t:24s} {cp.count[t]:>9,}")

    if not args.no_labels:
        day = SIM_EPOCH - dt.timedelta(days=1)
        # Nhan vo no "tinh den" ngay truoc SIM_EPOCH: mot file cho toan bo lich su
        put_inbox(inbox_key("lending", "labels", day, f"labels_{day:%Y%m%d}.csv.gz"),
                  csv_gz(["sk_id_curr", "target", "label_source"],
                         [(sk, t, "kaggle" if kaggle else "synthetic") for sk, t in labels]))
    print(f"Xong trong {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
