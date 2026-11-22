"""FinGraph Portal: ứng dụng downstream cho người dùng cuối của lakehouse.

Chạy:  streamlit run apps/portal/app.py   (từ thư mục gốc repo ETL; xem apps/portal/README.md)

Mỗi persona là một role AWS: portal tạo boto3.Session(profile_name=...) của role đó và chỉ làm những gì
role đó được phép. Phân quyền dữ liệu (che cột PII, lọc dòng của khách) do Lake Formation quyết.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.portal import data as D  # noqa: E402

st.set_page_config(page_title="FinGraph Portal", page_icon="🏦", layout="wide")


# ------------------------------------------------------------------ phiên làm việc theo persona

@st.cache_resource(show_spinner=False)
def session_for(profile: str):
    import boto3

    return boto3.Session(profile_name=profile, region_name=D.REGION)


@st.cache_data(ttl=600, show_spinner="Đang truy vấn Athena…")
def q(profile: str, workgroup: str, sql: str) -> pd.DataFrame:
    return pd.DataFrame(D.run_sql(session_for(profile), workgroup, sql))


def query(p: D.Persona, sql: str) -> pd.DataFrame | None:
    try:
        return q(p.profile, p.workgroup, sql)
    except D.QueryError as e:
        st.warning(str(e))
    except Exception as e:  # credential hết hạn, mạng…
        st.error(f"{type(e).__name__}: {str(e)[:300]}")
    return None


def num(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


PERSONAS = D.personas()
with st.sidebar:
    st.title("🏦 FinGraph")
    key = st.radio("Vai trò", list(PERSONAS), format_func=lambda k: PERSONAS[k].label)
    P = PERSONAS[key]
    st.caption(f"Profile AWS `{P.profile}` · workgroup `{P.workgroup}`")
    try:
        ident = session_for(P.profile).client("sts").get_caller_identity()["Arn"].split("/")[-2]
        st.success(f"Đăng nhập: {ident}")
    except Exception as e:
        st.error(f"Chưa có credential cho `{P.profile}`: chạy scripts/assume-all.ps1 (repo hạ tầng).\n\n{str(e)[:160]}")
        st.stop()
    st.divider()
    st.caption("Dữ liệu cá nhân được che và lọc theo vai trò bởi AWS Lake Formation, không phải bởi ứng dụng.")


# ------------------------------------------------------------------ trang

def page_overview():
    st.header("Tổng quan dữ liệu")
    st.write("Các bảng vai trò này được thấy (metadata đã lọc theo quyền Lake Formation).")
    try:
        t = pd.DataFrame(D.visible_tables(session_for(P.profile)))
    except Exception as e:
        st.error(str(e)[:300])
        return
    if t.empty:
        st.info("Không thấy bảng nào.")
        return
    c1, c2 = st.columns(2)
    c1.metric("Database", t["database"].nunique())
    c2.metric("Bảng", len(t))
    st.dataframe(t, use_container_width=True, hide_index=True)
    if P.ops:
        st.subheader("Lần chạy Glue gần nhất")
        try:
            runs = pd.DataFrame(D.glue_job_runs(session_for(P.profile)))
            if not runs.empty:
                bad = (runs["state"] == "FAILED").sum()
                st.metric("Lần chạy lỗi (trong danh sách)", int(bad))
                st.dataframe(runs, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(str(e)[:300])


def page_customer360():
    st.header("Khách hàng 360 & gợi ý sản phẩm")
    cid = st.text_input("Mã khách (client_id, Berka)", "2")
    if not cid.strip().isdigit():
        st.info("Nhập client_id là số.")
        return
    prof = query(P, D.sql_client_latest(cid))
    if prof is None or prof.empty:
        st.info("Không có dữ liệu gold cho khách này.")
        return
    r = prof.iloc[0].to_dict()
    c = st.columns(5)
    c[0].metric("Tháng gần nhất", r["month"])
    c[1].metric("Tuổi / giới", f"{r['age']} / {r['gender']}")
    c[2].metric("Vùng", r["region"])
    c[3].metric("Số sản phẩm", r["n_products"])
    c[4].metric("Số dư cuối tháng", f"{float(r['end_balance'] or 0):,.0f} CZK")
    owned = [n for p, (f, n) in D.PRODUCTS.items() if str(r.get(f)).lower() in ("true", "1")]
    st.write("**Đang sở hữu:** " + (", ".join(owned) if owned else "chưa có sản phẩm nào"))

    left, right = st.columns([3, 2])
    with left:
        h = query(P, D.sql_client_history(cid))
        if h is not None and not h.empty:
            h = num(h, ["end_balance", "inflow", "outflow"])
            st.line_chart(h.set_index("month")[["end_balance"]], height=240)
    with right:
        st.subheader("Gợi ý sản phẩm tiếp theo")
        rates = query(P, D.sql_segment_rates(r["region"], r["age"]))
        recs = D.recommend(r, rates.to_dict("records") if rates is not None else [])
        if not recs:
            st.info("Khách đã có mọi sản phẩm trong danh mục.")
        for x in recs:
            lift = f" · lift ×{x['lift']:.1f}" if x["lift"] else ""
            st.write(f"**{x['name']}**: {x['score']:.1%} khách tương tự mua trong kỳ tới{lift}")
            st.caption(f"Căn cứ: {x['basis']} ({x['n_segment']} dòng train, cùng vùng và nhóm tuổi)")
        st.caption("Baseline thống kê trên track_a_pretrain.uc2_next_product (tập train). Chỉ gợi ý khoản vay và "
                   "thẻ: nhãn bảo hiểm / hộ gia đình của Berka bị lệch (docs/RECSYS.md mục 3.2).")

    page_mbd_reco()

    st.subheader("Quan hệ trong đồ thị tri thức")
    u = query(P, D.sql_customer_uid(cid))
    if u is None or u.empty:
        st.info("Khách chưa có trong track_a_onto.entity.")
        return
    from agent.tools import AthenaBackend, Tools

    tools = Tools(AthenaBackend(P.workgroup, session=session_for(P.profile)))
    out = tools.call("customer_360", {"uid": u.iloc[0]["uid"], "as_of": None})
    if out.get("_is_error"):
        st.warning(out.get("error"))
    else:
        st.dataframe(pd.DataFrame(out.get("rows", [])), use_container_width=True, hide_index=True)


def page_mbd_reco():
    with st.expander("Gợi ý bằng model (LightGBM, khách MBD)", expanded=False):
        why = D.recsys_available()
        if why:
            st.info(why)
            return
        st.caption("Khách của bộ MBD (client_id dạng mã băm), khác khách Berka ở trên. Điểm dùng để XẾP HẠNG; "
                   "p_calibrated đã hiệu chỉnh lại tỉ lệ lấy mẫu âm. Lý do lấy từ SHAP.")
        mid = st.text_input("client_id MBD", key="mbd_cid")
        if mid and st.button("Chấm điểm", key="mbd_go"):
            try:
                with st.spinner("Đang tính đặc trưng qua Athena và chấm điểm…"):
                    r = D.recommend_mbd(session_for(P.profile), P.workgroup, mid, k=3)
            except (ValueError, D.QueryError) as e:
                st.warning(str(e))
                return
            if r.empty:
                st.info("Không có khách này trong silver_mbd.")
            else:
                st.dataframe(r, use_container_width=True, hide_index=True,
                             column_config={c: st.column_config.NumberColumn(format="%.3f")
                                            for c in ["score", "p_calibrated"]})


def page_aml():
    st.header("Điều tra AML")
    from agent.tools import AthenaBackend, NeptuneBackend, Tools

    s = session_for(P.profile)
    tools = Tools(AthenaBackend(P.workgroup, session=s), graph=NeptuneBackend(session=s) if P.graph else None)
    st.subheader("Hàng đợi duyệt khớp danh sách cấm vận")
    conf = st.slider("Độ tin cậy tối thiểu", 0.6, 0.9, 0.7, 0.01)
    out = tools.call("sanctions_review", {"uid": None, "min_confidence": conf, "limit": 50})
    if out.get("_is_error"):
        st.warning(out.get("error"))
    else:
        st.dataframe(pd.DataFrame(out.get("rows", [])), use_container_width=True, hide_index=True)
        st.caption("Nhãn khách là 'Khách N' (không PII); evidence chỉ là cờ khớp tên, ngày sinh, quốc gia.")

    st.subheader("Liên hệ giữa hai thực thể (Neptune)")
    if not P.graph:
        st.info("Vai trò này không gọi được Neptune; dùng vai trò Data engineer hoặc trợ lý AI.")
        return
    a, b = st.columns(2)
    ua, ub = a.text_input("uid A"), b.text_input("uid B")
    hops = st.slider("Số bước tối đa", 1, 4, 3)
    if ua and ub and st.button("Tìm đường liên hệ"):
        out = tools.call("explain_link", {"uid_a": ua.strip(), "uid_b": ub.strip(), "max_hops": hops})
        st.json(out)


def page_fraud():
    st.header("Gian lận thẻ: chất lượng rule")
    s = query(P, D.SQL_FRAUD_SUMMARY)
    if s is None or s.empty:
        st.info("Chưa có bảng đánh giá (gold.card_fraud_eval).")
        return
    s = num(s, ["n", "n_fraud", "tp", "fp", "fn", "precision", "recall", "f1", "alert_rate"])
    c1, c2 = st.columns(2)
    lv = c1.radio("Đơn vị đánh giá", ["txn", "card_day"], horizontal=True,
                  format_func={"txn": "từng giao dịch", "card_day": "thẻ × ngày"}.get)
    sv = c2.radio("Mức cảnh báo", ["any", "high"], horizontal=True,
                  format_func={"any": "mọi mức", "high": "chỉ mức cao"}.get)
    s = s[(s["level"] == lv) & (s["severity"] == sv)].drop(columns=["level", "severity"])
    st.caption(f"Kỳ đánh giá {s['eval_from'].iloc[0]} → {s['eval_to'].iloc[0]} · tập {s['universe'].iloc[0]}")
    st.dataframe(s, use_container_width=True, hide_index=True,
                 column_config={c: st.column_config.NumberColumn(format="%.3f")
                                for c in ["precision", "recall", "f1", "alert_rate"]})
    d = query(P, D.SQL_FRAUD_DAILY)
    if d is not None and not d.empty:
        d = num(d, ["precision", "recall"])
        metric = st.radio("Chỉ số theo ngày", ["precision", "recall"], horizontal=True)
        st.line_chart(d.pivot_table(index="txn_date", columns="rule", values=metric), height=260)
        st.caption("Theo ngày, đơn vị từng giao dịch.")
    st.info("Cảnh báo realtime (vài giây): Grafana trên máy nguồn, dashboard \"Gian lận thẻ: realtime\". "
            "Mở bằng SSM port forward 3000 (repo hạ tầng docs/STREAMING.md mục 6.3).")


def page_sql():
    st.header("Truy vấn tự phục vụ")
    st.caption("Chỉ SELECT/WITH. Cột hoặc dòng vai trò không được xem sẽ bị Lake Formation từ chối hoặc lọc bỏ.")
    sql = st.text_area("SQL", "SELECT region, count(*) AS khach FROM track_a_gold.berka_client_month "
                              "WHERE month = (SELECT max(month) FROM track_a_gold.berka_client_month) GROUP BY 1 ORDER BY 2 DESC",
                       height=120)
    if st.button("Chạy"):
        try:
            safe = D.readonly_sql(sql)
        except ValueError as e:
            st.error(str(e))
            return
        df = query(P, safe)
        if df is not None:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Tải CSV", df.to_csv(index=False).encode("utf-8"), "ket_qua.csv")


def page_assistant():
    st.header("Trợ lý AI")
    st.caption("Trả lời từ dữ liệu, mỗi ý kèm trích dẫn [uid:…]/[triple:…] tra ngược được. "
               "Chạy bằng quyền của vai trò hiện tại.")
    provider = st.selectbox("Nhà cung cấp model", [os.environ.get("FINGRAPH_PROVIDER", "bedrock-runtime"),
                                                    "anthropic"], index=0)
    if "chat" not in st.session_state:
        st.session_state.chat = []
    for m in st.session_state.chat:
        st.chat_message(m["role"]).markdown(m["content"])
    qn = st.chat_input("Ví dụ: Khách 2 đang giữ sản phẩm nào?")
    if not qn:
        return
    st.session_state.chat.append({"role": "user", "content": qn})
    st.chat_message("user").markdown(qn)
    from agent import loop as L
    from agent.tools import AthenaBackend, NeptuneBackend, Tools

    s = session_for(P.profile)
    tools = Tools(AthenaBackend(P.workgroup, session=s), graph=NeptuneBackend(session=s) if P.graph else None)
    with st.chat_message("assistant"):
        try:
            client, model = L.make_client(provider)
            ans = L.ask(qn, tools, client, model, provider=provider)
            text = ans.text + ("" if ans.grounded else "\n\n_⚠ Câu trả lời không qua được kiểm tra trích dẫn._")
        except Exception as e:
            text = (f"Chưa gọi được model ({type(e).__name__}: {str(e)[:200]}). "
                    "Bedrock cần form use case Anthropic (docs/AI-AGENT.md).")
        st.markdown(text)
    st.session_state.chat.append({"role": "assistant", "content": text})


def page_my_account():
    st.header("Tài khoản của tôi")
    st.caption("Bạn chỉ thấy dữ liệu của chính mình: Lake Formation lọc dòng theo mã khách của vai trò.")
    acc = query(P, D.SQL_MY_ACCOUNTS)
    if acc is not None:
        st.subheader("Tài khoản")
        st.dataframe(acc, use_container_width=True, hide_index=True)
    bal = query(P, D.SQL_MY_BALANCE)
    if bal is not None and not bal.empty:
        bal = num(bal, ["net", "end_balance"])
        st.subheader("Số dư cuối tháng")
        st.line_chart(bal.set_index("month")[["end_balance"]], height=220)
    tr = query(P, D.SQL_MY_TRANS)
    if tr is not None:
        st.subheader("50 giao dịch gần nhất")
        st.dataframe(tr, use_container_width=True, hide_index=True)
    ln = query(P, D.SQL_MY_LOANS)
    if ln is not None and not ln.empty:
        st.subheader("Khoản vay")
        st.dataframe(ln, use_container_width=True, hide_index=True)


# ------------------------------------------------------------------ hệ gợi ý v2: người dùng cuối

def _reco_event(client_id, row, event_type: str, surface: str):
    """Ghi sự kiện cho đánh giá online (recsys/v2/online.py). Impression chỉ ghi một lần mỗi phiên."""
    from recsys.v2.online import RecoEvent

    key = ("imp", str(client_id), row["product"], surface)
    if event_type == "impression":
        if key in st.session_state:
            return
        st.session_state[key] = True
    D.reco_event_log().write(RecoEvent(
        client_id=str(client_id), product=str(row["product"]), event_type=event_type, variant=str(row["variant"]),
        model_version=str(row["model_version"]), rank=int(row["rank"]), p_shown=float(row["p_calibrated"]),
        surface=surface, actor=P.key))


def _reco_cards(client_id, df: pd.DataFrame, surface: str, customer: bool):
    for _, r in df.iterrows():
        _reco_event(client_id, r, "impression", surface)
        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"**{int(r['rank'])}. {r['product_label']}**")
            c2.metric("Khả năng quan tâm", f"{float(r['p_calibrated']):.1%}")
            st.caption(("Vì sao bạn thấy gợi ý này: " if customer else "Lý do (SHAP): ") + str(r["reasons"] or "-"))
            if not customer and r.get("pitch"):
                st.caption(f"Gợi ý cách nói chuyện: {r['pitch']}")
            b1, b2, b3 = st.columns(3)
            if customer:
                labels = (("Tôi quan tâm", "accept"), ("Ẩn gợi ý này", "dismiss"), ("Xem chi tiết", "click"))
            else:
                labels = (("Khách đồng ý", "accept"), ("Khách không quan tâm", "dismiss"), ("Đã giới thiệu", "click"))
            for col, (text, ev) in zip((b1, b2, b3), labels):
                if col.button(text, key=f"{surface}_{client_id}_{r['product']}_{ev}"):
                    _reco_event(client_id, r, ev, surface)
                    st.toast("Đã ghi nhận" + (": sẽ không chào lại sản phẩm này trong 60 ngày" if ev == "dismiss" else ""))


def page_reco_rm():
    st.header("Gợi ý sản phẩm cho khách (RM)")
    st.caption("Mô hình v2 (LightGBM multi-task + hiệu chỉnh xác suất), chấm tháng mới nhất, đã áp luật đủ điều kiện và "
               "chính sách liên hệ. Khách được chia ổn định vào nhóm thử nghiệm A/B: nhóm control nhận gợi ý theo độ "
               "phổ biến. Mọi thao tác dưới đây được ghi để đo hiệu quả online (trang Giám sát gợi ý).")
    cid = st.text_input("Mã khách Berka (client_id)", value="2", key="reco_cid")
    if not cid:
        return
    try:
        sql = D.sql_reco_client(cid)
    except ValueError as e:
        st.warning(str(e))
        return
    df = query(P, sql)
    if df is None:
        return
    if df.empty:
        st.info("Không có gợi ý cho khách này: đã giữ mọi sản phẩm, không qua luật đủ điều kiện, đang trong thời gian "
                "chờ sau khi từ chối, hoặc khả năng quá thấp (không gợi ý còn hơn gợi ý tồi).")
        return
    df = num(df, ["rank", "p_calibrated", "expected_value"])
    st.caption(f"Nhóm thử nghiệm: **{df.variant.iat[0]}** · mô hình `{df.model_version.iat[0]}` · "
               f"tháng {df.scored_month.iat[0]}")
    _reco_cards(cid, df, "portal_rm", customer=False)


def page_reco_campaign():
    st.header("Chiến dịch: chọn khách cho một sản phẩm")
    st.caption("Xếp khách theo xác suất đã hiệu chỉnh: tổng xác suất của danh sách = số khách kỳ vọng sẽ nhận. "
               "So với chọn ngẫu nhiên cùng số khách để thấy giá trị của mô hình.")
    from recsys.v2.config import BERKA

    c1, c2, c3 = st.columns(3)
    prod = c1.selectbox("Sản phẩm", D.RECO_PRODUCTS, format_func=lambda c: BERKA.product(c).label_vi)
    n = c2.number_input("Số khách liên hệ", 10, 5000, 200, step=50)
    cost = c3.number_input(f"Chi phí một lần liên hệ ({BERKA.currency})", 0.0, 10000.0, float(BERKA.contact_cost))
    size = query(P, D.sql_campaign_size(prod))
    top = query(P, D.sql_campaign(prod, int(n)))
    if size is None or top is None or top.empty:
        return
    size = num(size, ["n", "expected"])
    top = num(top, ["p_calibrated", "expected_value"])
    pool, pool_exp = float(size.n.iat[0]), float(size.expected.iat[0] or 0)
    exp = float(top.p_calibrated.sum())
    rnd = pool_exp / pool * len(top) if pool else 0
    value = BERKA.product(prod).value
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Khách đủ điều kiện", f"{pool:,.0f}")
    m2.metric("Kỳ vọng nhận (mô hình)", f"{exp:,.1f}", f"{(exp / rnd - 1):+.0%} so với ngẫu nhiên" if rnd else None)
    m3.metric("Doanh thu kỳ vọng", f"{exp * value:,.0f}")
    m4.metric("ROI kỳ vọng", f"{(exp * value - len(top) * cost) / max(len(top) * cost, 1e-9):.1f}x")
    st.dataframe(top, use_container_width=True, hide_index=True,
                 column_config={"p_calibrated": st.column_config.NumberColumn(format="%.4f")})
    st.download_button("Tải danh sách (CSV)", top.to_csv(index=False).encode("utf-8"), f"chien_dich_{prod}.csv")
    st.caption("Giá trị sản phẩm và chi phí liên hệ là tham số giả định (recsys/v2/config.py).")


def page_reco_monitor():
    st.header("Giám sát hệ gợi ý")
    card = D.reco_model_card("berka")
    if not card:
        st.info("Chưa có mô hình trong registry: chạy `python -m recsys.v2.run all --domain berka`.")
        return
    st.subheader("Model card")
    m = card.get("metrics", {})
    tr = m.get("test_ranking", {})
    c = st.columns(4)
    c[0].metric("Phiên bản", card.get("version", "-"))
    c[1].metric("MAP@3 (test)", f"{tr.get('map@3', 0):.3f}")
    c[2].metric("Hit@1 (test)", f"{tr.get('hit@1', 0):.3f}")
    c[3].metric("Xếp hạng theo", card.get("rank_with", "-"))
    st.caption(f"Huấn luyện {card.get('trained_at', '')} · train {card.get('train_months')} · valid "
               f"{card.get('valid_months')} · ECE sau hiệu chỉnh {m.get('calibration_ece_after')} · PSI {m.get('psi')}")
    st.subheader("Đo online từ nhật ký sự kiện")
    from recsys.v2.online import online_metrics, srm, two_proportion

    ev = D.reco_event_log().read()
    if not ev:
        st.info("Chưa có sự kiện: mở trang Gợi ý cho khách / Ưu đãi cho bạn và bấm phản hồi để sinh dữ liệu.")
    else:
        om = online_metrics(ev)
        st.dataframe(pd.DataFrame(om["rows"]), use_container_width=True, hide_index=True)
        pu = om["per_user"]
        a, b = pu.get("control", {}), pu.get("treatment", {})
        if a and b:
            t = two_proportion(a["users_accept"], a["users"], b["users_accept"], b["users"])
            s = srm(a["users"], b["users"])
            st.write(f"Tỉ lệ khách nhận: control {t['rate_a']:.2%} vs treatment {t['rate_b']:.2%}, "
                     f"p = {t['p_value']:.3f}; SRM p = {s['p_value']:.3f} "
                     f"({'đạt' if s['ok'] else 'HỎNG: không đọc kết quả'}).")
            st.caption("Cần đủ cỡ mẫu trước khi kết luận (docs/RECSYS-V2.md mục 6); "
                       "đừng dừng thử nghiệm khi vừa thấy p < 0,05.")
    st.subheader("Báo cáo đánh giá offline")
    rep = ROOT / "recsys" / "v2" / "reports" / "berka" / "offline.md"
    if rep.exists():
        with st.expander("Mở báo cáo đầy đủ"):
            st.markdown(rep.read_text(encoding="utf-8"))


def page_my_offers():
    st.header("Ưu đãi dành cho bạn")
    st.caption("Gợi ý dựa trên giao dịch và sản phẩm bạn đang dùng tại ngân hàng. Dữ liệu không chia sẻ cho bên thứ "
               "ba. Bạn có thể ẩn một gợi ý: chúng tôi sẽ không chào lại sản phẩm đó trong 60 ngày.")
    df = query(P, D.SQL_MY_RECO)
    if df is None:
        st.info("Vai trò của bạn chưa được cấp quyền xem gợi ý (Lake Formation, repo hạ tầng: customer_gold_tables).")
        return
    if df.empty:
        st.info("Hiện chưa có ưu đãi phù hợp với bạn.")
        return
    df = num(df, ["rank", "p_calibrated", "expected_value"])
    _reco_cards("me", df.head(2), "portal_customer", customer=True)


def page_reco_pilot():
    """Pilot người dùng thật (docs/RECSYS-PILOT.md): so sánh mù hai danh sách gợi ý, rồi chấm lời giải thích."""
    import json
    import time as _t

    from recsys.v2 import pilot as PL
    from recsys.v2.config import BERKA

    st.header("Đánh giá mù hệ gợi ý (pilot)")
    st.caption("Bạn đóng vai cán bộ quan hệ khách hàng. Mỗi bài: xem hồ sơ khách, chọn danh sách gợi ý A hay B phù hợp "
               "hơn (hai danh sách do hai cách khác nhau tạo ra, thứ tự ngẫu nhiên), rồi chấm lời giải thích. "
               "Không có đáp án đúng sai: chúng tôi đo hệ thống, không đo bạn.")
    if not PL.TASKS.exists():
        st.info("Chưa có bài: chạy `python -m recsys.v2.pilot build --participants 8`.")
        return
    tasks = pd.read_parquet(PL.TASKS)
    who = st.text_input("Mã người tham gia (vd P01)", key="pilot_who").strip().upper()
    if not who:
        return
    mine = PL.assignment(tasks.task_id.tolist(), who)
    done = {r["task_id"] for r in PL.read_responses() if r["participant"] == who}
    todo = [t for t in mine if t not in done]
    st.progress(len(done & set(mine)) / len(mine), text=f"Đã làm {len(done & set(mine))} / {len(mine)} bài")
    if not todo:
        st.success("Bạn đã làm xong. Cảm ơn! Mời bạn điền bảng hỏi SUS (docs/RECSYS-PILOT.md, phụ lục).")
        return
    t = tasks.set_index("task_id").loc[todo[0]]
    side = PL.v2_side(who, todo[0])
    lists = {"v2": json.loads(t.list_v2), "pop": json.loads(t.list_pop)}
    show = {"A": lists["v2"] if side == "A" else lists["pop"], "B": lists["pop"] if side == "A" else lists["v2"]}
    st.subheader(f"Bài {todo[0]}")
    prof = json.loads(t.profile)
    cols = st.columns(4)
    for i, (k, v) in enumerate(prof.items()):
        cols[i % 4].metric(k, f"{v:,.0f}" if isinstance(v, (int, float)) and k != "Nam" else
                           ("có" if v == 1 else "không") if k == "Nam" else str(v))
    t0_key = f"pilot_t0_{who}_{todo[0]}"
    st.session_state.setdefault(t0_key, _t.time())
    c1, c2 = st.columns(2)
    for col, name in ((c1, "A"), (c2, "B")):
        with col.container(border=True):
            st.markdown(f"**Danh sách {name}**")
            for r, code in enumerate(show[name], 1):
                st.write(f"{r}. {BERKA.product(code).label_vi}")
    stage = f"pilot_stage_{who}_{todo[0]}"
    if not st.session_state.get(stage):
        with st.form(f"pilot_choose_{todo[0]}"):
            choice = st.radio("Danh sách nào phù hợp với khách này hơn?", ["A", "B", "Như nhau"], horizontal=True)
            pitch = st.selectbox("Nếu chỉ được chào MỘT sản phẩm, bạn chào sản phẩm nào?",
                                 sorted(set(lists["v2"]) | set(lists["pop"])),
                                 format_func=lambda c: BERKA.product(c).label_vi)
            if st.form_submit_button("Tiếp"):
                st.session_state[stage] = {"choice": "same" if choice == "Như nhau" else choice, "pitch": pitch}
                st.rerun()
        return
    st.info(f"Mô hình đưa ra lý do cho gợi ý **{BERKA.product(t.top_v2).label_vi}**: {t.reason_v2 or '-'}")
    with st.form(f"pilot_reason_{todo[0]}"):
        useful = st.slider("Lý do này giúp bạn nói chuyện với khách đến mức nào? (1 = không giúp, 5 = rất giúp)", 1, 5, 3)
        fits = st.radio("Lý do có khớp với hồ sơ khách ở trên không?", ["Có", "Một phần", "Không"], horizontal=True)
        comment = st.text_input("Góp ý (không bắt buộc)")
        if st.form_submit_button("Lưu và sang bài tiếp"):
            s = st.session_state.pop(stage)
            PL.write_response(PL.Response(
                participant=who, task_id=todo[0], choice=s["choice"], v2_side=side, reason_useful=int(useful),
                reason_fits={"Có": "yes", "Một phần": "partly", "Không": "no"}[fits], pitch=s["pitch"],
                seconds=round(_t.time() - st.session_state.pop(t0_key, _t.time()), 1), comment=comment))
            st.rerun()


if P.customer:
    pages = {"Tài khoản của tôi": page_my_account, "Ưu đãi cho bạn": page_my_offers}
else:
    pages = {"Tổng quan": page_overview, "Khách hàng 360": page_customer360, "Điều tra AML": page_aml,
             "Gợi ý cho khách (RM)": page_reco_rm, "Chiến dịch gợi ý": page_reco_campaign,
             "Giám sát gợi ý": page_reco_monitor, "Đánh giá mù (pilot)": page_reco_pilot,
             "Gian lận thẻ": page_fraud, "Truy vấn SQL": page_sql, "Trợ lý AI": page_assistant}
# Chỉ chạy trang đang mở: st.tabs chạy MỌI tab mỗi lần tương tác (≈ 10 truy vấn Athena, 30 s).
with st.sidebar:
    page = st.radio("Trang", list(pages), key=f"page_{P.key}")
pages[page]()
