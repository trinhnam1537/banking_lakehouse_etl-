"""PaySim (ví điện tử, có nhãn gian lận) -> file cuối ngày (EOD) của đối tác trên S3.

    python -m simulators.paysim.eod synth --rows 300000      # khi chưa tải bộ Kaggle
    python -m simulators.paysim.eod publish                  # mọi ngày, liền một mạch
    python -m simulators.paysim.eod publish --days 3 --pace 30  # 3 ngày, mỗi 30 giây một file

Bộ Kaggle:
    kaggle datasets download -d ntnu-testimon/paysim1 -p data/paysim --unzip
    -> data/paysim/PS_20174392719_1491204439457_log.csv (6.362.620 dòng, 744 giờ)

Mô phỏng một đối tác ví điện tử gửi file đối soát mỗi cuối ngày:
  landing/_inbox/paysim/eod/dt=YYYY-MM-DD/paysim_eod_YYYYMMDD.csv.gz

  * step (giờ 1..744) -> ngày = SIM_EPOCH + (step-1)//24, giờ trong ngày = (step-1)%24
  * txn_ref: PaySim KHÔNG có mã giao dịch -> producer cấp mã tất định "PS" + số dòng gốc
    (đối tác thật luôn có mã đối soát). Silver khử trùng theo txn_ref.
  * isFraud / isFlaggedFraud bị TÁCH khỏi file EOD -> landing/_inbox/paysim/labels/
    sau --label-delay-days ngày (khiếu nại/hoàn tiền đến muộn).
  * File nhỏ theo ngày (~15 MB nén) thay vì một file 470 MB.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import random
import sys
import time
from typing import List

from ..common import DATA_DIR, SIM_EPOCH, LabelSink, csv_gz, inbox_key, put_inbox

KAGGLE = "paysim/PS_20174392719_1491204439457_log.csv"
SYNTH = "paysim-synthetic/PS_synthetic_log.csv"
HEADER = ["txn_ref", "txn_ts", "step", "type", "amount", "name_orig", "old_balance_orig",
          "new_balance_orig", "name_dest", "old_balance_dest", "new_balance_dest"]


def source_path():
    for rel in (KAGGLE, SYNTH):
        if (DATA_DIR / rel).exists():
            return DATA_DIR / rel
    raise SystemExit(f"Khong thay {DATA_DIR / KAGGLE} hay {DATA_DIR / SYNTH}. Chay `synth` truoc.")


def cmd_publish(args) -> None:
    path = source_path()
    print(f"Nguon: {path}")
    labels = LabelSink("paysim", ["txn_ref", "is_fraud", "is_flagged_fraud"], args.label_delay_days, args.dry)
    day_rows: List[list] = []
    cur_day, published = None, 0

    def flush(day: dt.date):
        nonlocal published
        put_inbox(inbox_key("paysim", "eod", day, f"paysim_eod_{day:%Y%m%d}.csv.gz"), csv_gz(HEADER, day_rows), args.dry)
        published += 1
        labels.advance(day)

    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for line_no, row in enumerate(r, 1):
            step = int(row["step"])
            day = SIM_EPOCH + dt.timedelta(days=(step - 1) // 24)
            if cur_day and day != cur_day:
                flush(cur_day)
                day_rows = []
                if args.days and published >= args.days:
                    break
                if args.pace:
                    time.sleep(args.pace)
            cur_day = day
            rng = random.Random(line_no)
            ts = dt.datetime.combine(day, dt.time((step - 1) % 24, rng.randint(0, 59), rng.randint(0, 59)))
            ref = f"PS{line_no:09d}"
            day_rows.append([ref, ts.isoformat() + "Z", step, row["type"], row["amount"], row["nameOrig"],
                             row["oldbalanceOrg"], row["newbalanceOrig"], row["nameDest"],
                             row["oldbalanceDest"], row["newbalanceDest"]])
            labels.add(day, (ref, row["isFraud"], row["isFlaggedFraud"]))
        else:
            if day_rows:
                flush(cur_day)
    labels.close()
    print(f"Da gui {published} file EOD")


def cmd_synth(args) -> None:
    """Sinh file theo ĐÚNG định dạng Kaggle, phân phối gần PaySim:
    CASH_OUT 35% · PAYMENT 34% · CASH_IN 22% · TRANSFER 8% · DEBIT 1%; gian lận ~0,13%
    theo mẫu gốc: TRANSFER rút sạch tài khoản nạn nhân sang tài khoản trung gian, rồi
    CASH_OUT ngay; chỉ TRANSFER > 200.000 bị gắn isFlaggedFraud. Giờ thưa dần về cuối
    tháng như bản gốc. Như PaySim: nameOrig gần như không lặp, merchant (M...) không có số dư."""
    rng = random.Random(args.seed)
    out = DATA_DIR / SYNTH
    out.parent.mkdir(parents=True, exist_ok=True)
    types = [("CASH_OUT", .35), ("PAYMENT", .34), ("CASH_IN", .22), ("TRANSFER", .08), ("DEBIT", .01)]
    dests = [f"C{rng.randint(10**8, 10**10)}" for _ in range(max(1000, args.rows // 3))]
    rows = []
    steps = [s for s in range(1, 745) for _ in range(int(40 * (1.0 if s < 400 else 0.08)) + 1)]

    def bal(x):
        return f"{x:.2f}"

    while len(rows) < args.rows:
        step = rng.choice(steps)
        if rng.random() < 0.0013 / 2:  # mot vu = 2 giao dich
            amount = round(rng.lognormvariate(12, 1.2), 2)
            victim, mule = f"C{rng.randint(10**8, 10**10)}", rng.choice(dests)
            flagged = int(amount > 200_000 and rng.random() < 0.01)
            rows.append([step, "TRANSFER", bal(amount), victim, bal(amount), "0.00", mule, "0.00", "0.00", 1, flagged])
            rows.append([step, "CASH_OUT", bal(amount), mule, bal(amount), "0.00", rng.choice(dests),
                         "0.00", bal(amount), 1, 0])
            continue
        t = None
        x = rng.random()
        for v, p in types:
            x -= p
            if x <= 0:
                t = v
                break
        t = t or "CASH_OUT"
        amount = round(rng.lognormvariate(11 if t in ("TRANSFER", "CASH_OUT") else 9, 1.0), 2)
        old = round(rng.lognormvariate(10, 2), 2) if rng.random() < .67 else 0.0
        new = max(0.0, old - amount) if t != "CASH_IN" else old + amount
        orig = f"C{rng.randint(10**8, 10**10)}"
        if t == "PAYMENT":
            dest, od, nd = f"M{rng.randint(10**8, 10**10)}", 0.0, 0.0
        else:
            dest = rng.choice(dests)
            od = round(rng.lognormvariate(11, 2), 2) if rng.random() < .6 else 0.0
            nd = od + amount if t != "CASH_IN" else max(0.0, od - amount)
        rows.append([step, t, bal(amount), orig, bal(old), bal(new), dest, bal(od), bal(nd), 0, 0])
    rows.sort(key=lambda r: r[0])
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest",
                    "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"])
        w.writerows(rows)
    print(f"{out}: {len(rows):,} dong, {sum(r[9] for r in rows)} gian lan")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("synth")
    s.add_argument("--rows", type=int, default=300_000)
    s.add_argument("--seed", type=int, default=2017)
    p = sub.add_parser("publish")
    p.add_argument("--days", type=int, default=0, help="0 = tat ca")
    p.add_argument("--pace", type=float, default=0, help="giay cho giua hai file")
    p.add_argument("--label-delay-days", type=int, default=30)
    p.add_argument("--dry", action="store_true")
    args = ap.parse_args(argv)
    {"synth": cmd_synth, "publish": cmd_publish}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
