"""Sinh hoạt động mới của hệ thống cho vay, từng ngày nghiệp vụ.

    python -m simulators.lending.simulate --days 30
    python -m simulators.lending.simulate --live --sleep 60

Mỗi ngày:
  * hồ sơ mới (status SUBMITTED), ~0,2% số hồ sơ hiện có
  * hồ sơ SUBMITTED >= 2 ngày -> APPROVED / REJECTED (UPDATE -> updated_at đổi)
  * hồ sơ APPROVED >= 1 ngày  -> DISBURSED + một hợp đồng trong previous_application
  * hợp đồng đang chạy, đúng ngày kỳ hạn: một dòng installments_payments + pos_cash_balance
  * thỉnh thoảng bureau đổi credit_day_overdue (tin CIC cập nhật)
  * khoản giải ngân đủ 90 ngày: quyết định vỡ nợ -> nhãn trễ (landing/_inbox/lending/labels/)

Ngày bắt đầu = ngày sau application_date lớn nhất, không sớm hơn SIM_EPOCH.
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
import time

from ..common import SIM_EPOCH, LabelSink, lending_conn
from . import generator as g


def _next_day(cur) -> dt.date:
    cur.execute("SELECT max(application_date) FROM lending.application")
    last = cur.fetchone()[0]
    if last is None:
        raise SystemExit("lending.application rong. Chay simulators.lending.seed truoc.")
    return max(last + dt.timedelta(days=1), SIM_EPOCH)


def simulate_day(conn, rng: random.Random, d: dt.date, labels: LabelSink) -> dict:
    cur = conn.cursor()
    s = {"new": 0, "decided": 0, "disbursed": 0, "instalments": 0, "bureau_updates": 0}

    cur.execute("SELECT count(*), max(sk_id_curr) FROM lending.application")
    total, max_id = cur.fetchone()
    for i in range(max(1, int(total * 0.002 * rng.uniform(0.5, 1.5)))):
        app = g.application(rng, max_id + 1 + i)
        app.update(application_date=d, status="SUBMITTED")
        cols = list(app)
        cur.execute(f"INSERT INTO lending.application ({', '.join(cols)}) VALUES "
                    f"({', '.join(['%s'] * len(cols))})", [app[c] for c in cols])
        s["new"] += 1

    cur.execute("""UPDATE lending.application
                   SET status = CASE WHEN coalesce(ext_source_2, .5) < 0.25 THEN 'REJECTED' ELSE 'APPROVED' END
                   WHERE status = 'SUBMITTED' AND application_date <= %s""", (d - dt.timedelta(days=2),))
    s["decided"] = cur.rowcount

    cur.execute("""SELECT sk_id_curr, amt_credit, amt_annuity, name_contract_type FROM lending.application
                   WHERE status = 'APPROVED' AND application_date <= %s""", (d - dt.timedelta(days=3),))
    approved = cur.fetchall()
    cur.execute("SELECT coalesce(max(sk_id_prev), 1000000) FROM lending.previous_application")
    next_prev = cur.fetchone()[0] + 1
    for sk, credit, annuity, ctype in approved:
        cnt = max(6, round(float(credit) / max(float(annuity or 1), 1)))
        cur.execute("""INSERT INTO lending.previous_application (sk_id_prev, sk_id_curr, name_contract_type,
                       amt_annuity, amt_application, amt_credit, amt_goods_price, name_contract_status,
                       days_decision, name_payment_type, code_reject_reason, name_client_type,
                       cnt_payment, name_yield_group)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,'Approved',0,'Cash through the bank','XAP','New',%s,'middle')""",
                    (next_prev, sk, ctype, annuity, credit, credit, credit, cnt))
        cur.execute("UPDATE lending.application SET status = 'DISBURSED' WHERE sk_id_curr = %s", (sk,))
        next_prev += 1
        s["disbursed"] += 1

    # Ky han: hop dong giai ngan trong SIM (days_decision = 0). Ngay giai ngan = ngay nop + 3
    # (SUBMITTED -> 2 ngay APPROVED -> 1 ngay DISBURSED); khong dung updated_at vi do la gio THAT.
    cur.execute("""SELECT p.sk_id_prev, p.sk_id_curr, p.amt_annuity, p.cnt_payment, a.application_date + 3,
                          coalesce(a.ext_source_2, .5), coalesce(a.ext_source_3, .5), a.code_gender
                   FROM lending.previous_application p JOIN lending.application a USING (sk_id_curr)
                   WHERE a.status = 'DISBURSED' AND p.days_decision = 0 AND a.application_date >= %s""",
                (SIM_EPOCH,))
    for pid, sk, annuity, cnt, disbursed, e2, e3, gender in cur.fetchall():
        age = (d - disbursed).days
        if age <= 0 or age % 30 != 0 or age // 30 > cnt:
            continue
        k = age // 30
        late = rng.random() < 0.08
        cur.execute("""INSERT INTO lending.installments_payments (sk_id_prev, sk_id_curr, num_instalment_version,
                       num_instalment_number, days_instalment, days_entry_payment, amt_instalment, amt_payment)
                       VALUES (%s,%s,1,%s,%s,%s,%s,%s)""",
                    (pid, sk, k, float(age), float(age + (rng.randint(1, 30) if late else 0)), annuity, annuity))
        cur.execute("""INSERT INTO lending.pos_cash_balance (sk_id_prev, sk_id_curr, months_balance, cnt_instalment,
                       cnt_instalment_future, name_contract_status, sk_dpd, sk_dpd_def)
                       VALUES (%s,%s,0,%s,%s,%s,%s,0)""",
                    (pid, sk, cnt, cnt - k, "Completed" if k == cnt else "Active", rng.randint(1, 30) if late else 0))
        s["instalments"] += 1
        if age == 90:
            p = g.default_prob({"ext_source_2": float(e2), "ext_source_3": float(e3), "code_gender": gender})
            labels.add(d, (sk, int(rng.random() < p), "simulated"))

    cur.execute("""UPDATE lending.bureau SET credit_day_overdue = %s, days_credit_update = 0
                   WHERE sk_id_bureau IN (SELECT sk_id_bureau FROM lending.bureau
                                          WHERE credit_active = 'Active' ORDER BY random() LIMIT %s)""",
                (rng.choice([0, 0, 15, 30]), rng.randint(0, 5)))
    s["bureau_updates"] = cur.rowcount
    conn.commit()
    labels.advance(d)
    return s


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sleep", type=float, default=60.0)
    ap.add_argument("--label-delay-days", type=int, default=7)
    ap.add_argument("--dry-labels", action="store_true", help="in nhan ra man hinh, khong ghi S3")
    ap.add_argument("--seed", type=int, default=13)
    args = ap.parse_args(argv)

    conn = lending_conn()
    d = _next_day(conn.cursor())
    rng = random.Random(args.seed * 100_000 + d.toordinal())
    labels = LabelSink("lending", ["sk_id_curr", "target", "label_source"], args.label_delay_days,
                       dry=args.dry_labels)
    end = d + dt.timedelta(days=args.days - 1)
    while args.live or d <= end:
        t0 = time.time()
        s = simulate_day(conn, rng, d, labels)
        print(f"{d}  " + "  ".join(f"{k}={v}" for k, v in s.items()) + f"  ({time.time() - t0:.1f}s)")
        d += dt.timedelta(days=1)
        if args.live:
            time.sleep(args.sleep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
