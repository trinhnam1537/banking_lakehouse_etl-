"""Sinh hoạt động core banking mới, từng ngày nghiệp vụ, ghi THẲNG vào Postgres.

    python -m simulators.corebank.simulate --days 30         # 30 ngày tiếp theo
    python -m simulators.corebank.simulate --until 2023-03-31
    python -m simulators.corebank.simulate --live --sleep 60 # mỗi phút một ngày, chạy mãi

Ngày bắt đầu = ngày sau giao dịch cuối cùng trong core.trans, nhưng không sớm hơn
SIM_EPOCH (mặc định 2023-01-01; lịch sử Berka đã dời kết thúc 2022-12-31).

Mỗi ngày, các hành vi bám theo mẫu của chính Berka (mã tiếng Séc giữ nguyên):

  giao dịch thường    số lượng ~ Poisson(nhịp lịch sử của TỪNG tài khoản)
                      VKLAD nộp tiền mặt | VYBER rút tiền mặt | VYBER KARTOU rút thẻ
                      PREVOD NA UCET chuyển đi | PREVOD Z UCTU nhận về
  ngày 10             lương/lương hưu (PREVOD Z UCTU, k_symbol DUCHOD cho khách > 60 tuổi)
  ngày 15             lệnh định kỳ core.standing_order (SIPO, POJISTNE, UVER, LEASING)
  ngày 20             trả nợ vay (UVER). Thiếu tiền -> loan.status C->D; đủ lại D->C;
                      hết kỳ hạn -> A (đủ) / B (từng quá hạn)
  cuối tháng          lãi UROK (dư dương), phí SLUZBY, lãi phạt SANKC. UROK (dư âm)
  rải rác             khách mới (client + account + disp OWNER, có thể kèm thẻ),
                      đổi điện thoại/địa chỉ, khoá thẻ  -> các bảng có updated_at thay đổi

Mọi thứ đi qua UPDATE/INSERT thường nên trigger updated_at chạy đúng như hệ thống
thật — tầng sourcing CDC1 bắt được mà không biết đây là dữ liệu giả lập.
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import math
import random
import sys
import time
from typing import Dict, List, Tuple

from ..common import SIM_EPOCH, corebank_conn

OPS = [  # (operation, trans_type, xac suat, trung vi so tien CZK)
    ("VYBER", "VYDAJ", 0.42, 2500),
    ("PREVOD NA UCET", "VYDAJ", 0.20, 3500),
    ("VKLAD", "PRIJEM", 0.18, 6000),
    ("PREVOD Z UCTU", "PRIJEM", 0.10, 8000),
    ("VYBER KARTOU", "VYDAJ", 0.10, 1800),
]
OVERDRAFT = -5000  # khong cho chi qua muc nay
BANKS = ["AB", "CD", "EF", "GH", "IJ", "KL", "MN", "OP", "QR", "ST", "UV", "WX", "YZ"]


def _amount(rng: random.Random, median: float) -> float:
    return round(max(50.0, rng.lognormvariate(math.log(median), 0.8)), 0)


class Bank:
    def __init__(self, conn, seed: int):
        self.conn = conn
        self.cur = conn.cursor()
        self.rng = random.Random(seed)
        self._load()

    def _load(self) -> None:
        c = self.cur
        c.execute("""
            SELECT a.account_id, a.balance::float, a.district_id,
                   coalesce(r.per_day, 0.15),
                   EXISTS (SELECT 1 FROM core.disp d JOIN core.card k USING (disp_id)
                           WHERE d.account_id = a.account_id AND k.status = 'ACTIVE'),
                   (SELECT min(cl.birth_date) FROM core.disp d JOIN core.client cl USING (client_id)
                    WHERE d.account_id = a.account_id AND d.disp_type = 'OWNER')
            FROM core.account a
            LEFT JOIN (SELECT account_id,
                              count(*) / greatest(1, max(trans_date) - min(trans_date))::float AS per_day
                       FROM core.trans GROUP BY account_id) r USING (account_id)
            WHERE a.status = 'ACTIVE'""")
        self.acc: Dict[int, dict] = {
            r[0]: {"bal": r[1], "district": r[2], "rate": min(r[3], 2.0), "card": r[4], "birth": r[5]}
            for r in c.fetchall()
        }
        c.execute("SELECT district_id, avg_salary FROM core.district")
        self.salary = dict(c.fetchall())
        c.execute("SELECT (SELECT coalesce(max(client_id), 0) FROM core.client), "
                  "(SELECT coalesce(max(account_id), 0) FROM core.account), "
                  "(SELECT coalesce(max(disp_id), 0) FROM core.disp), "
                  "(SELECT coalesce(max(card_id), 0) FROM core.card)")
        self.next_client, self.next_account, self.next_disp, self.next_card = \
            [v + 1 for v in c.fetchone()]

    # ---------- một ngày ----------

    def day(self, d: dt.date) -> Dict[str, int]:
        rng = self.rng
        rows: List[Tuple] = []
        touched = set()
        last_dom = calendar.monthrange(d.year, d.month)[1]

        def book(acc_id, ttype, op, amount, k_symbol=None, bank=None, counter=None):
            a = self.acc[acc_id]
            signed = amount if ttype == "PRIJEM" else -amount
            if signed < 0 and a["bal"] + signed < OVERDRAFT and k_symbol not in ("UVER", "SLUZBY", "SANKC. UROK"):
                return False
            a["bal"] = round(a["bal"] + signed, 2)
            secs = rng.randint(7 * 3600, 20 * 3600)
            booked = dt.datetime.combine(d, dt.time(), tzinfo=dt.timezone.utc) + dt.timedelta(seconds=secs)
            rows.append((acc_id, d, booked, ttype, op, amount, a["bal"], k_symbol, bank, counter))
            touched.add(acc_id)
            return True

        for acc_id, a in self.acc.items():
            for _ in range(_poisson(rng, a["rate"])):
                op, ttype, _, med = _choice(rng, [o for o in OPS if a["card"] or o[0] != "VYBER KARTOU"])
                bank = counter = None
                if op.startswith("PREVOD"):
                    bank, counter = rng.choice(BANKS), rng.randint(10**7, 10**8 - 1)
                book(acc_id, ttype, op, _amount(rng, med), None, bank, counter)

            if d.day == 10 and acc_id % 10 < 7:  # 70% tai khoan co thu nhap dinh ky
                age = (d - a["birth"]).days // 365 if a["birth"] else 40
                base = self.salary.get(a["district"], 9000) * (0.6 if age >= 60 else 1.0)
                book(acc_id, "PRIJEM", "PREVOD Z UCTU", round(base * rng.uniform(0.8, 1.4)),
                     "DUCHOD" if age >= 60 else None, rng.choice(BANKS), rng.randint(10**7, 10**8 - 1))

            if d.day == last_dom:
                if a["bal"] > 0:
                    book(acc_id, "PRIJEM", None, round(a["bal"] * 0.001, 2) or 0.01, "UROK")
                elif a["bal"] < 0:
                    book(acc_id, "VYDAJ", "VYBER", round(-a["bal"] * 0.02, 2) or 0.01, "SANKC. UROK")
                book(acc_id, "VYDAJ", "VYBER", 14.6, "SLUZBY")

        stats = {"trans": 0, "orders": 0, "loan_payments": 0, "new_clients": 0, "profile_updates": 0}
        c = self.cur
        if d.day == 15:
            c.execute("SELECT order_id, account_id, bank_to, account_to, amount::float, k_symbol "
                      "FROM core.standing_order WHERE active")
            for oid, acc_id, bank_to, acc_to, amt, ks in c.fetchall():
                if acc_id in self.acc and book(acc_id, "VYDAJ", "PREVOD NA UCET", amt, ks, bank_to, acc_to):
                    stats["orders"] += 1

        if d.day == 20:
            c.execute("SELECT loan_id, account_id, granted_date, duration, payments::float, status "
                      "FROM core.loan WHERE status IN ('C', 'D')")
            for lid, acc_id, granted, duration, pay, status in c.fetchall():
                if acc_id not in self.acc:
                    continue
                end = _add_months(granted, duration)
                if d >= end:
                    c.execute("UPDATE core.loan SET status = %s WHERE loan_id = %s",
                              ("A" if status == "C" else "B", lid))
                    continue
                ok = self.acc[acc_id]["bal"] >= pay
                book(acc_id, "VYDAJ", "PREVOD NA UCET", pay, "UVER")
                stats["loan_payments"] += 1
                new = "C" if ok else "D"
                if new != status:
                    c.execute("UPDATE core.loan SET status = %s WHERE loan_id = %s", (new, lid))

        if rows:
            from psycopg2.extras import execute_values

            execute_values(c, """INSERT INTO core.trans (account_id, trans_date, booked_at, trans_type,
                                 operation, amount, balance, k_symbol, bank, counter_account) VALUES %s""",
                           rows, page_size=5000)
            execute_values(c, """UPDATE core.account a SET balance = v.bal FROM (VALUES %s) v(id, bal)
                                 WHERE a.account_id = v.id""",
                           [(i, self.acc[i]["bal"]) for i in touched], page_size=5000)
        stats["trans"] = len(rows)
        stats["new_clients"] = self._new_clients(d)
        stats["profile_updates"] = self._profile_updates(d)
        self.conn.commit()
        return stats

    def _new_clients(self, d: dt.date) -> int:
        from faker import Faker

        rng, c = self.rng, self.cur
        n = _poisson(rng, max(1.0, len(self.acc) * 0.003 / 30 * 10))
        fake = Faker("cs_CZ")
        for _ in range(n):
            cid, aid, did = self.next_client, self.next_account, self.next_disp
            self.next_client, self.next_account, self.next_disp = cid + 1, aid + 1, did + 1
            fake.seed_instance(cid)
            g = rng.choice("FM")
            birth = d - dt.timedelta(days=rng.randint(18 * 365, 80 * 365))
            district = rng.choice(list(self.salary))
            month = birth.month + (50 if g == "F" else 0)
            c.execute("""INSERT INTO core.client (client_id, full_name, national_id, gender, birth_date,
                         district_id, email, phone, address) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                      (cid, fake.name_female() if g == "F" else fake.name_male(),
                       f"{birth:%y}{month:02d}{birth:%d}/{cid % 10000:04d}", g, birth, district,
                       fake.free_email(), fake.phone_number(), fake.address().replace("\n", ", ")))
            c.execute("INSERT INTO core.account (account_id, district_id, frequency, opened_date) "
                      "VALUES (%s,%s,%s,%s)", (aid, district, "POPLATEK MESICNE", d))
            c.execute("INSERT INTO core.disp (disp_id, client_id, account_id, disp_type) "
                      "VALUES (%s,%s,%s,'OWNER')", (did, cid, aid))
            has_card = rng.random() < 0.3
            if has_card:
                from .seed import luhn_card

                kid = self.next_card
                self.next_card += 1
                c.execute("INSERT INTO core.card (card_id, disp_id, card_type, issued_date, card_number) "
                          "VALUES (%s,%s,'classic',%s,%s)", (kid, did, d, luhn_card(kid)))
            self.acc[aid] = {"bal": 0.0, "district": district, "rate": 0.15, "card": has_card, "birth": birth}
        return n

    def _profile_updates(self, d: dt.date) -> int:
        from faker import Faker

        fake = Faker("cs_CZ")
        fake.seed_instance(d.toordinal())
        c = self.cur
        c.execute("SELECT client_id FROM core.client WHERE status = 'ACTIVE' ORDER BY random() LIMIT %s",
                  (self.rng.randint(0, 8),))
        ids = [r[0] for r in c.fetchall()]
        for cid in ids:
            if self.rng.random() < 0.5:
                c.execute("UPDATE core.client SET phone = %s WHERE client_id = %s", (fake.phone_number(), cid))
            else:
                c.execute("UPDATE core.client SET address = %s WHERE client_id = %s",
                          (fake.address().replace("\n", ", "), cid))
        if self.rng.random() < 0.2:  # thinh thoang khoa mot the (mat the)
            c.execute("UPDATE core.card SET status = 'BLOCKED' WHERE card_id = "
                      "(SELECT card_id FROM core.card WHERE status = 'ACTIVE' ORDER BY random() LIMIT 1)")
        return len(ids)


def _poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def _choice(rng: random.Random, ops):
    total = sum(o[2] for o in ops)
    x = rng.random() * total
    for o in ops:
        x -= o[2]
        if x <= 0:
            return o
    return ops[-1]


def _add_months(d: dt.date, months: int) -> dt.date:
    y, m = divmod(d.month - 1 + months, 12)
    y, m = d.year + y, m + 1
    return dt.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def next_day(conn) -> dt.date:
    cur = conn.cursor()
    cur.execute("SELECT max(trans_date) FROM core.trans")
    last = cur.fetchone()[0]
    if last is None:
        raise SystemExit("core.trans rong. Chay simulators.corebank.seed truoc.")
    return max(last + dt.timedelta(days=1), SIM_EPOCH)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--days", type=int, default=1)
    g.add_argument("--until", type=dt.date.fromisoformat)
    ap.add_argument("--live", action="store_true", help="chay mai, moi --sleep giay mot ngay")
    ap.add_argument("--sleep", type=float, default=60.0)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)

    conn = corebank_conn()
    d = next_day(conn)
    bank = Bank(conn, seed=args.seed * 100_000 + d.toordinal())
    end = args.until or (d + dt.timedelta(days=args.days - 1))
    while args.live or d <= end:
        t0 = time.time()
        s = bank.day(d)
        print(f"{d}  " + "  ".join(f"{k}={v}" for k, v in s.items()) + f"  ({time.time() - t0:.1f}s)")
        d += dt.timedelta(days=1)
        if args.live:
            time.sleep(args.sleep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
