"""Nạp lịch sử core banking: Berka (MariaDB công khai của CTU) -> Postgres corebank.

    python -m simulators.corebank.seed                      # toàn bộ 4.500 tài khoản
    python -m simulators.corebank.seed --limit-accounts 300 # bản nhỏ để thử nhanh
    python -m simulators.corebank.seed --sanction-hits 5    # gieo 5 khách trùng tên cấm vận
    python -m simulators.corebank.seed --if-empty           # máy nguồn AWS: chỉ nạp khi DB rỗng

Schema `core` chưa có thì seed tự áp `schema.sql` (cạnh file này). Stack local đã áp sẵn
qua docker-entrypoint-initdb.d; máy nguồn trên AWS thì không — Postgres ở đó do repo hạ tầng
dựng, schema thuộc về repo này.

Biến đổi so với bản gốc (mọi thứ GIẢ LẬP đều tất định theo id -> chạy lại ra y hệt):

  * Dời ngày +24 năm: 1993-1998 -> 2017-2022. Bội số của 4 nên 29/02/1996 vẫn hợp lệ
    (2020), và ngày trong tháng giữ nguyên (lãi cuối tháng vẫn rơi cuối tháng).
    Ngày sinh dời cùng để tuổi khách không đổi.
  * Họ tên (Faker cs_CZ theo giới tính), số định danh kiểu rodné číslo, email,
    điện thoại, địa chỉ: Berka đã ẩn danh nên không có; thiếu chúng thì bài toán
    sàng lọc cấm vận không có gì để so.
  * Số thẻ 16 chữ số hợp lệ Luhn — simulator giao dịch thẻ dùng làm cc_num, nhờ
    đó luồng thẻ nối được về khách hàng core banking.
  * booked_at: Berka chỉ có ngày; giờ được sinh tất định trong 07:00-20:00.
  * `--sanction-hits N`: thay tên N khách bằng tên NGƯỜI trong danh sách cấm vận
    OpenSanctions. Đáp án ghi ra data/ground_truth/sanction_hits.csv (KHÔNG ghi vào
    hệ thống nguồn) để đánh giá bước screening.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import pathlib
import random
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence

from ..common import DATA_DIR, corebank_conn

YEAR_SHIFT = 24
CTU = dict(host="relational.fel.cvut.cz", port=3306, user="guest",
           password="ctu-relational", database="financial")
SANCTIONS_CSV = "https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv"


def shift(d: Optional[dt.date]) -> Optional[dt.date]:
    return d.replace(year=d.year + YEAR_SHIFT) if d else None


def _rng(*key) -> random.Random:
    return random.Random(int(hashlib.md5("|".join(map(str, key)).encode()).hexdigest()[:12], 16))


def luhn_card(card_id: int) -> str:
    r = _rng("card", card_id)
    body = "4" + "".join(str(r.randint(0, 9)) for _ in range(14))
    total = 0
    for i, ch in enumerate(reversed(body)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            n = n - 9 if n > 9 else n
        total += n
    return body + str((10 - total % 10) % 10)


def rodne_cislo(birth: dt.date, gender: str, client_id: int) -> str:
    month = birth.month + (50 if gender == "F" else 0)
    return f"{birth:%y}{month:02d}{birth:%d}/{client_id % 10000:04d}"


def _copy(cur, table: str, cols: Sequence[str], rows: Iterable[Sequence]) -> int:
    """COPY FROM STDIN theo lô 50.000 dòng: nhanh hơn INSERT hàng chục lần."""
    total, buf, w = 0, io.StringIO(), None
    w = csv.writer(buf, lineterminator="\n")
    sql = f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"

    def flush():
        buf.seek(0)
        cur.copy_expert(sql, buf)
        buf.seek(0)
        buf.truncate()

    n = 0
    for row in rows:
        w.writerow(["\\N" if v is None else v for v in row])
        n += 1
        if n == 50_000:
            flush()
            total += n
            n = 0
    if n:
        flush()
        total += n
    return total


def sanctioned_people(n: int) -> List[str]:
    """Đọc theo luồng file cấm vận, dừng khi đủ n NGƯỜI (không tải cả 74 MB)."""
    import urllib.request

    names: List[str] = []
    req = urllib.request.Request(SANCTIONS_CSV, headers={"User-Agent": "FinGraph simulator"})
    with urllib.request.urlopen(req, timeout=60) as r:
        text = io.TextIOWrapper(r, encoding="utf-8", newline="")
        for row in csv.DictReader(text):
            if row["schema"] == "Person" and row["name"] and len(row["name"]) < 60:
                names.append(row["name"])
                if len(names) >= n * 20:  # lay du rong roi chon ngau nhien tat dinh
                    break
    return _rng("sanction-pick").sample(names, min(n, len(names)))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--limit-accounts", type=int, default=0, help="0 = tat ca 4.500")
    ap.add_argument("--sanction-hits", type=int, default=0)
    ap.add_argument("--truncate", action="store_true", help="xoa du lieu cu trong core.* truoc")
    ap.add_argument("--if-empty", action="store_true",
                    help="DB da co du lieu thi thoat 0 (timer tren may nguon goi moi ngay)")
    args = ap.parse_args(argv)

    import pymysql
    from faker import Faker

    t0 = time.time()
    dst = corebank_conn()
    cur = dst.cursor()

    cur.execute("SELECT to_regclass('core.account')")
    if cur.fetchone()[0] is None:
        schema = pathlib.Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        cur.execute(schema)
        dst.commit()
        print("Da tao schema core (schema.sql)")

    cur.execute("SELECT count(*) FROM core.account")
    if cur.fetchone()[0] and not args.truncate:
        if args.if_empty:
            print("core.account da co du lieu -> bo qua seed (--if-empty).")
            return 0
        print("core.account da co du lieu. Them --truncate de nap lai.")
        return 1
    if args.truncate:
        cur.execute("TRUNCATE core.trans, core.standing_order, core.loan, core.card, core.disp, "
                    "core.client, core.account, core.district CASCADE")

    # Mo ket noi CTU SAU kiem tra: --if-empty chay moi ngay khong duoc phu thuoc server CTU
    src = pymysql.connect(**CTU, connect_timeout=20, cursorclass=pymysql.cursors.DictCursor)
    scur = src.cursor()

    scur.execute("SELECT account_id FROM account ORDER BY account_id")
    accounts = [r["account_id"] for r in scur.fetchall()]
    if args.limit_accounts:
        accounts = accounts[: args.limit_accounts]
    acc_set = set(accounts)
    in_acc = ",".join(map(str, accounts))

    # ---- district ----
    scur.execute("SELECT * FROM district")
    n = _copy(cur, "core.district", [
        "district_id", "district_name", "region", "inhabitants", "munic_lt_500", "munic_500_1999",
        "munic_2000_9999", "munic_ge_10000", "cities", "urban_ratio", "avg_salary",
        "unemployment_95", "unemployment_96", "entrepreneurs_per_1000", "crimes_95", "crimes_96"],
        ([r["district_id"]] + [r[f"A{i}"] for i in range(2, 17)] for r in scur.fetchall()))
    print(f"district {n}")

    # ---- account ----
    scur.execute(f"SELECT * FROM account WHERE account_id IN ({in_acc})")
    n = _copy(cur, "core.account", ["account_id", "district_id", "frequency", "opened_date"],
              ((r["account_id"], r["district_id"], r["frequency"], shift(r["date"]))
               for r in scur.fetchall()))
    print(f"account {n}")

    # ---- client (qua disp) ----
    scur.execute(f"SELECT * FROM disp WHERE account_id IN ({in_acc})")
    disps = scur.fetchall()
    client_ids = sorted({d["client_id"] for d in disps})
    scur.execute(f"SELECT * FROM client WHERE client_id IN ({','.join(map(str, client_ids))})")
    fake = Faker("cs_CZ")
    hits: Dict[int, str] = {}
    if args.sanction_hits:
        pick = _rng("sanction-clients").sample(client_ids, args.sanction_hits)
        hits = dict(zip(pick, sanctioned_people(args.sanction_hits)))

    def clients():
        for r in scur.fetchall():
            cid, g = r["client_id"], r["gender"]
            fake.seed_instance(cid)
            name = hits.get(cid) or (fake.name_female() if g == "F" else fake.name_male())
            birth = shift(r["birth_date"])
            yield (cid, name, rodne_cislo(birth, g, cid), g, birth, r["district_id"],
                   fake.free_email(), fake.phone_number(), fake.address().replace("\n", ", "))

    n = _copy(cur, "core.client", ["client_id", "full_name", "national_id", "gender", "birth_date",
                                   "district_id", "email", "phone", "address"], clients())
    print(f"client {n}")
    n = _copy(cur, "core.disp", ["disp_id", "client_id", "account_id", "disp_type"],
              ((d["disp_id"], d["client_id"], d["account_id"], d["type"]) for d in disps))
    print(f"disp {n}")

    disp_ids = {d["disp_id"] for d in disps}
    scur.execute("SELECT * FROM card")
    n = _copy(cur, "core.card", ["card_id", "disp_id", "card_type", "issued_date", "card_number"],
              ((r["card_id"], r["disp_id"], r["type"], shift(r["issued"]), luhn_card(r["card_id"]))
               for r in scur.fetchall() if r["disp_id"] in disp_ids))
    print(f"card {n}")

    scur.execute(f"SELECT * FROM loan WHERE account_id IN ({in_acc})")
    n = _copy(cur, "core.loan", ["loan_id", "account_id", "granted_date", "amount", "duration",
                                 "payments", "status"],
              ((r["loan_id"], r["account_id"], shift(r["date"]), r["amount"], r["duration"],
                r["payments"], r["status"]) for r in scur.fetchall()))
    print(f"loan {n}")

    scur.execute(f"SELECT * FROM `order` WHERE account_id IN ({in_acc})")
    n = _copy(cur, "core.standing_order", ["order_id", "account_id", "bank_to", "account_to",
                                           "amount", "k_symbol"],
              ((r["order_id"], r["account_id"], r["bank_to"], r["account_to"], r["amount"],
                (r["k_symbol"] or "").strip() or None) for r in scur.fetchall()))
    print(f"standing_order {n}")

    # ---- trans: doc dan tu CTU, COPY theo lo ----
    sscur = src.cursor(pymysql.cursors.SSDictCursor)
    sscur.execute("SELECT * FROM trans ORDER BY trans_id")

    def trans():
        for r in sscur:
            if r["account_id"] not in acc_set:
                continue
            d = shift(r["date"])
            secs = 7 * 3600 + _rng("t", r["trans_id"]).randint(0, 13 * 3600)
            booked = dt.datetime.combine(d, dt.time()) + dt.timedelta(seconds=secs)
            yield (r["trans_id"], r["account_id"], d, booked.isoformat() + "+00:00", r["type"],
                   r["operation"], r["amount"], r["balance"], r["k_symbol"], r["bank"], r["account"])

    n = _copy(cur, "core.trans", ["trans_id", "account_id", "trans_date", "booked_at", "trans_type",
                                  "operation", "amount", "balance", "k_symbol", "bank",
                                  "counter_account"], trans())
    print(f"trans {n}")

    # So du hien tai = so du sau giao dich cuoi cung cua moi tai khoan
    cur.execute("""
        UPDATE core.account a SET balance = t.balance
        FROM (SELECT DISTINCT ON (account_id) account_id, balance
              FROM core.trans ORDER BY account_id, trans_date DESC, trans_id DESC) t
        WHERE a.account_id = t.account_id""")
    dst.commit()

    if hits:
        out = DATA_DIR / "ground_truth"
        out.mkdir(parents=True, exist_ok=True)
        with open(out / "sanction_hits.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["client_id", "injected_name"])
            w.writerows(sorted(hits.items()))
        print(f"Dap an screening: {out / 'sanction_hits.csv'} ({len(hits)} khach)")

    print(f"Xong trong {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
