"""IBM AML (NeurIPS 2023) -> Kafka (giao dịch) + inbox S3 (tài khoản, mẫu rửa tiền, nhãn trễ).

    # 0. (không có bộ Kaggle) sinh bộ tổng hợp CÙNG ĐỊNH DẠNG file Kaggle
    python -m simulators.aml.replay synth --transactions 200000

    # 1. dữ liệu tĩnh: danh sách tài khoản + file mẫu rửa tiền -> landing/_inbox/aml/
    python -m simulators.aml.replay static --dataset HI-Small

    # 2. phát lại giao dịch vào topic aml.interbank.transfers
    python -m simulators.aml.replay stream --dataset HI-Small --speed 600   # 1 giây thật = 10 phút
    python -m simulators.aml.replay stream --dataset SYN --speed 0          # nhanh nhất có thể

Bộ Kaggle: kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml
-p data/ibm-aml --unzip  (dùng HI-Small: ~5 triệu giao dịch, 10 ngày 09/2022).

Những gì producer làm thay cho "hệ thống chuyển tiền liên ngân hàng":
  * ĐỔI TÊN CỘT: file gốc có HAI cột "Account" (nguồn và đích) — đọc bằng tên cột thì
    cột sau đè cột trước, mất tài khoản nguồn mà không báo lỗi.
  * txn_id tất định = hash(nội dung giao dịch, lần xuất hiện thứ n): file gốc không có mã
    giao dịch; mã này để silver khử trùng và để nối với nhãn + file mẫu.
  * Dời thời gian về SIM_EPOCH (giữ nguyên khoảng cách giữa các giao dịch).
  * TÁCH NHÃN: "Is Laundering" không đi vào Kafka. Nhãn được ghi thành file
    landing/_inbox/aml/labels/dt=<ngày>/ sau --label-delay-days ngày nghiệp vụ.
  * Key Kafka = tài khoản nguồn: mọi giao dịch của một tài khoản vào cùng partition,
    giữ thứ tự theo tài khoản.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import re
import sys
import time
from collections import Counter
from typing import Dict, Iterator, List, Tuple

from ..common import DATA_DIR, SIM_EPOCH, LabelSink, csv_gz, inbox_key, kafka_producer, ndjson_gz, put_inbox, stable_id

TOPIC = "aml.interbank.transfers"
TS_FMT = "%Y/%m/%d %H:%M"
COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
        "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
PATTERN_RE = re.compile(r"^BEGIN LAUNDERING ATTEMPT - ([A-Z-]+)(.*)$")


def paths(dataset: str):
    base = DATA_DIR / ("ibm-aml-synthetic" if dataset == "SYN" else "ibm-aml")
    return (base / f"{dataset}_Trans.csv", base / f"{dataset}_accounts.csv", base / f"{dataset}_Patterns.txt")


def content_key(row: List[str]) -> str:
    return "|".join(row[:10])


def iter_trans(path) -> Iterator[Tuple[Dict, str]]:
    """(bản ghi đã đổi tên cột, txn_id). Đọc theo VỊ TRÍ cột, không theo tên."""
    seen: Counter = Counter()
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
        if len(header) != 11:
            raise SystemExit(f"{path}: can 11 cot, gap {len(header)}: {header}")
        for row in r:
            k = content_key(row)
            seen[k] += 1
            yield dict(zip(COLS, row)), stable_id("aml", k, seen[k])


# ---------------- static ----------------


def cmd_static(args) -> None:
    _, acc_path, pat_path = paths(args.dataset)
    day = SIM_EPOCH
    with open(acc_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        next(r)
        rows = list(r)
    put_inbox(inbox_key("aml", "accounts", day, f"{args.dataset}_accounts.csv.gz"),
              csv_gz(["bank_name", "bank_id", "account_number", "entity_id", "entity_name"], rows), args.dry)

    attempts, cur, attempt_no = [], None, 0
    with open(pat_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            m = PATTERN_RE.match(line)
            if m:
                attempt_no += 1
                cur = {"attempt_id": f"{args.dataset}-{attempt_no:05d}", "typology": m.group(1),
                       "typology_detail": m.group(2).strip(" :") or None}
                continue
            if line.startswith("END LAUNDERING ATTEMPT") or not line:
                cur = None if line.startswith("END") else cur
                continue
            if cur is not None:
                row = next(csv.reader([line]))
                rec = dict(zip(COLS, row))
                # Lan xuat hien thu 1: giao dich trong file mau gan nhu luon duy nhat theo noi dung
                attempts.append({**cur, "txn_id": stable_id("aml", content_key(row), 1),
                                 **{k: rec[k] for k in COLS if k != "is_laundering"}})
    put_inbox(inbox_key("aml", "patterns", day, f"{args.dataset}_patterns.ndjson.gz"), ndjson_gz(attempts), args.dry)
    print(f"accounts={len(rows)} pattern_rows={len(attempts)} attempts={attempt_no}")


# ---------------- stream ----------------


def cmd_stream(args) -> None:
    trans_path, _, _ = paths(args.dataset)
    producer = None if args.dry else kafka_producer("sim-aml-replay")
    labels = LabelSink("aml", ["txn_id", "is_laundering"], args.label_delay_days, dry=args.dry)
    shift = None
    sim_prev = None
    wall0 = time.monotonic()
    sim0 = None
    n = 0
    for rec, txn_id in iter_trans(trans_path):
        ts = dt.datetime.strptime(rec["timestamp"], TS_FMT)
        if shift is None:
            shift = dt.datetime.combine(SIM_EPOCH, dt.time()) - dt.datetime.combine(ts.date(), dt.time())
            sim0 = ts
        sim_ts = ts + shift
        if sim_prev and sim_ts.date() != sim_prev.date():
            labels.advance(sim_ts.date())
        sim_prev = sim_ts

        if args.speed > 0:  # giu nhip thoi gian, nen theo --speed
            due = (ts - sim0).total_seconds() / args.speed
            lag = due - (time.monotonic() - wall0)
            if lag > 0:
                time.sleep(lag)

        value = {"txn_id": txn_id, "event_ts": sim_ts.isoformat() + "Z", "source_ts": rec["timestamp"],
                 **{k: rec[k] for k in COLS if k not in ("timestamp", "is_laundering")}}
        labels.add(sim_ts.date(), (txn_id, rec["is_laundering"]))
        if producer is not None:
            producer.produce(TOPIC, key=rec["from_account"], value=json.dumps(value))
            producer.poll(0)
        n += 1
        if args.limit and n >= args.limit:
            break
        if n % 100_000 == 0:
            print(f"  {n:,} giao dich, gio nghiep vu {sim_ts}")
    if producer is not None:
        producer.flush(60)
    labels.close()
    print(f"Da phat {n:,} giao dich vao {TOPIC}")


# ---------------- synth ----------------

CURRENCIES = [("US Dollar", .55), ("Euro", .20), ("UK Pound", .05), ("Yuan", .04), ("Yen", .03),
              ("Rupee", .03), ("Canadian Dollar", .02), ("Australian Dollar", .02), ("Swiss Franc", .02),
              ("Ruble", .01), ("Mexican Peso", .01), ("Brazil Real", .01), ("Saudi Riyal", .005),
              ("Shekel", .005), ("Bitcoin", .01)]
RATE = {"US Dollar": 1, "Euro": .93, "UK Pound": .79, "Yuan": 7.1, "Yen": 145, "Rupee": 83,
        "Canadian Dollar": 1.36, "Australian Dollar": 1.53, "Swiss Franc": .88, "Ruble": 92,
        "Mexican Peso": 17.5, "Brazil Real": 4.9, "Saudi Riyal": 3.75, "Shekel": 3.7, "Bitcoin": 0.000038}
FORMATS = [("ACH", .30), ("Cheque", .22), ("Credit Card", .20), ("Cash", .12), ("Wire", .08),
           ("Reinvestment", .06), ("Bitcoin", .02)]
TYPOLOGIES = ["FAN-OUT", "FAN-IN", "CYCLE", "SCATTER-GATHER", "GATHER-SCATTER"]


def _pick(rng, table):
    x = rng.random() * sum(p for _, p in table)
    for v, p in table:
        x -= p
        if x <= 0:
            return v
    return table[-1][0]


def cmd_synth(args) -> None:
    rng = random.Random(args.seed)
    out = DATA_DIR / "ibm-aml-synthetic"
    out.mkdir(parents=True, exist_ok=True)
    banks = [f"{rng.randint(10, 99999):05d}" if i else "00070" for i in range(args.banks)]
    accounts = []  # (bank_id, account, entity_id, entity_name)
    for e in range(args.entities):
        kind = rng.choice(["Corporation", "Partnership", "Sole Proprietorship", "Person"])
        for _ in range(rng.choice([1, 1, 1, 2, 3])):
            accounts.append((rng.choice(banks), f"{rng.getrandbits(36):09X}", f"{e:07X}", f"{kind} #{e}"))
    weights = [rng.paretovariate(1.2) for _ in accounts]
    start = dt.datetime(2022, 9, 1)
    span = args.days * 86400

    def tx(ts, a, b, amount, fmt, laundering):
        cur = _pick(rng, CURRENCIES) if fmt != "Bitcoin" else "Bitcoin"
        cur_to = cur if rng.random() < .97 else _pick(rng, CURRENCIES)
        paid = round(amount * RATE[cur], 2)
        recv = round(amount * RATE[cur_to], 2)
        return [ts.strftime(TS_FMT), a[0], a[1], b[0], b[1], f"{recv:.2f}", cur_to, f"{paid:.2f}", cur, fmt,
                str(int(laundering))]

    rows: List[List[str]] = []
    for _ in range(args.transactions):
        a = rng.choices(accounts, weights)[0]
        fmt = _pick(rng, FORMATS)
        b = a if fmt == "Reinvestment" else rng.choices(accounts, weights)[0]
        ts = start + dt.timedelta(seconds=rng.randint(0, span))
        rows.append(tx(ts, a, b, rng.lognormvariate(7, 1.6), fmt, False))

    patterns: List[Tuple[str, List[List[str]]]] = []
    for _ in range(max(1, int(args.transactions * args.laundering_ratio / 6))):
        typ = rng.choice(TYPOLOGIES)
        ts = start + dt.timedelta(seconds=rng.randint(0, span - 5 * 86400))
        hub = rng.choice(accounts)
        k = rng.randint(3, 7)
        others = rng.sample(accounts, k + 1)
        amount = rng.lognormvariate(9, 1)
        fmt = rng.choice(["ACH", "Wire", "Cheque", "Cash", "Bitcoin"])
        step = lambda: dt.timedelta(minutes=rng.randint(5, 600))  # noqa: E731
        att: List[List[str]] = []
        if typ == "FAN-OUT":
            for o in others[:k]:
                ts += step(); att.append(tx(ts, hub, o, amount / k, fmt, True))
        elif typ == "FAN-IN":
            for o in others[:k]:
                ts += step(); att.append(tx(ts, o, hub, amount / k, fmt, True))
        elif typ == "CYCLE":
            ring = [hub] + others[:k]
            for i, o in enumerate(ring):
                ts += step(); att.append(tx(ts, o, ring[(i + 1) % len(ring)], amount * .97 ** i, fmt, True))
        elif typ == "SCATTER-GATHER":
            sink = others[k]
            for o in others[:k]:
                ts += step(); att.append(tx(ts, hub, o, amount / k, fmt, True))
            for o in others[:k]:
                ts += step(); att.append(tx(ts, o, sink, amount / k * .98, fmt, True))
        else:  # GATHER-SCATTER
            for o in others[:k]:
                ts += step(); att.append(tx(ts, o, hub, amount / k, fmt, True))
            for o in rng.sample(accounts, k):
                ts += step(); att.append(tx(ts, hub, o, amount / k * .98, fmt, True))
        detail = f": Max {k}-degree Fan-Out" if typ == "FAN-OUT" else (f":  Max {k + 1} hops" if typ == "CYCLE" else "")
        patterns.append((typ + detail, att))
        rows.extend(att)

    rows.sort(key=lambda r: r[0])
    header = ["Timestamp", "From Bank", "Account", "To Bank", "Account", "Amount Received", "Receiving Currency",
              "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]
    with open(out / "SYN_Trans.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    with open(out / "SYN_accounts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"])
        w.writerows([(f"Bank #{b}", b, acc, eid, name) for b, acc, eid, name in accounts])
    with open(out / "SYN_Patterns.txt", "w", encoding="utf-8") as f:
        for typ, att in patterns:
            f.write(f"BEGIN LAUNDERING ATTEMPT - {typ}\n")
            csv.writer(f, lineterminator="\n").writerows(att)
            f.write(f"END LAUNDERING ATTEMPT - {typ.split(':')[0]}\n\n")
    n_bad = sum(r[-1] == "1" for r in rows)
    print(f"{out}: {len(rows):,} giao dich ({n_bad} rua tien, 1/{len(rows) // max(n_bad, 1)}), "
          f"{len(accounts):,} tai khoan, {len(patterns)} vu")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("synth")
    s.add_argument("--transactions", type=int, default=200_000)
    s.add_argument("--entities", type=int, default=20_000)
    s.add_argument("--banks", type=int, default=30)
    s.add_argument("--days", type=int, default=10)
    s.add_argument("--laundering-ratio", type=float, default=0.001)
    s.add_argument("--seed", type=int, default=2023)
    for name in ("static", "stream"):
        p = sub.add_parser(name)
        p.add_argument("--dataset", default="HI-Small", help="HI-Small | LI-Small | ... | SYN (bo tong hop)")
        p.add_argument("--dry", action="store_true", help="khong ghi S3/Kafka, chi in")
    st = sub.choices["stream"]
    st.add_argument("--speed", type=float, default=0, help="giay nghiep vu / giay that; 0 = nhanh nhat")
    st.add_argument("--label-delay-days", type=int, default=14)
    st.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)
    {"synth": cmd_synth, "static": cmd_static, "stream": cmd_stream}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
