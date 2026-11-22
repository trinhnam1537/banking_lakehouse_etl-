"""Hệ gợi ý v2: mô hình, đánh giá offline, thống kê online, tầng phục vụ. Dữ liệu tổng hợp, không cần AWS."""

import datetime as dt

import pytest

# recsys/ khong chay tren Glue (3.9): CI chi cai recsys/requirements.txt o job 3.12, job 3.9 bo qua.
lightgbm = pytest.importorskip("lightgbm")  # import truoc numpy/pandas (thu tu import tren Windows)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from recsys.v2 import evaluate as EV  # noqa: E402
from recsys.v2 import online as ON  # noqa: E402
from recsys.v2 import serve as SV  # noqa: E402
from recsys.v2.config import BERKA, Domain, Product  # noqa: E402
from recsys.v2.data import Dataset, matrices  # noqa: E402
from recsys.v2.model import Isotonic, Ranker  # noqa: E402

TOY = Domain("toy", [Product("a", "A", "fin:A", 100.0), Product("b", "B", "fin:B", 10.0)], "y_", "USD", 1.0,
             min_p_show=0.0)


def toy_dataset(n=4000, seed=0) -> Dataset:
    rng = np.random.default_rng(seed)
    x1, x2 = rng.normal(size=n), rng.normal(size=n)
    df = pd.DataFrame({"client_id": np.arange(n) % 1000, "x1": x1, "x2": x2,
                       "month": np.repeat(["2022-01", "2022-02", "2022-03", "2022-04"], n // 4)})
    df["y_a"] = (rng.random(n) < 1 / (1 + np.exp(-(2 * x1 - 3)))).astype(int)    # a phụ thuộc x1
    df["y_b"] = (rng.random(n) < 1 / (1 + np.exp(-(2 * x2 - 3)))).astype(int)    # b phụ thuộc x2
    df["elig_a"], df["elig_b"] = True, rng.random(n) < 0.8
    df["split"] = np.select([df.month <= "2022-02", df.month == "2022-03"], ["train", "valid"], "test")
    return Dataset(TOY, df, ["x1", "x2"], {"a": {}, "b": {}})


@pytest.fixture(scope="module")
def toy():
    ds = toy_dataset()
    return ds, Ranker.fit(ds, log=lambda *_: None)


# ------------------------------------------------------------------------------------- mô hình
def test_ranker_hoc_dung_tin_hieu_va_hieu_chinh(toy):
    ds, m = toy
    te = ds.split("test")
    Y, E = matrices(ds, te)
    P = m.predict(ds, te)
    assert (P[~E] == 0).all()                                         # không đủ điều kiện -> 0
    assert EV.ranking(Y, P, E)["map@2"] > EV.ranking(Y, np.ones_like(P), E)["map@2"]   # K = 2 san pham
    cal = EV.calibration(Y[E[:, 0], 0], P[E[:, 0], 0])
    assert abs(cal["mean_p"] - cal["rate"]) < 0.03 and cal["brier"] < cal["brier_ref"]


def test_shap_giai_thich_dung_dac_trung(toy):
    ds, m = toy
    te = ds.split("test").reset_index(drop=True)
    hi = te.sort_values("x1", ascending=False).head(20).reset_index(drop=True)
    why = m.contributions(ds, hi, np.zeros(len(hi), int))
    assert sum(w[0][0] == "x1" for w in why if w) >= 15                # sản phẩm a: x1 là lý do chính


def test_luu_nap_cho_cung_ket_qua(toy, tmp_path):
    ds, m = toy
    m.meta["version"] = "v-test"
    m.save(tmp_path, "v-test")
    m2 = Ranker.load(tmp_path, "toy")
    te = ds.split("test")
    assert np.allclose(m.predict(ds, te), m2.predict(ds, te), atol=1e-9)


def test_isotonic_don_dieu():
    rng = np.random.default_rng(1)
    p = rng.random(2000)
    iso = Isotonic().fit(p, (rng.random(2000) < p ** 2).astype(float))
    q = iso(np.linspace(0, 1, 50))
    assert (np.diff(q) >= -1e-12).all()


# ------------------------------------------------------------------------------------- đánh giá offline
def test_ranking_ndcg_va_map():
    Y = np.array([[1, 0, 0], [0, 1, 0]])
    perfect = EV.ranking(Y, Y.astype(float))
    assert perfect["map@3"] == perfect["ndcg@3"] == perfect["hit@1"] == 1.0
    worst = EV.ranking(Y, np.array([[0, 1, 2], [2, 0, 1]], float))
    assert worst["hit@1"] == 0 and worst["ndcg@3"] == pytest.approx(0.5)


def test_doi_chung_ngau_nhien_va_do_kho():
    # dòng 0: 1 sản phẩm đủ điều kiện (tầm thường); dòng 1: 4 đủ điều kiện, 1 được mua; dòng 2: không ai mua (bỏ)
    Y = np.array([[1, 0, 0, 0], [0, 0, 1, 0], [0, 0, 0, 0]])
    E = np.array([[1, 0, 0, 0], [1, 1, 1, 1], [1, 1, 1, 1]], bool)
    el = EV.eligibility(Y, E)
    assert el["rows"] == 2 and el["eligible_dist"] == {1: 1, 4: 1}
    assert el["trivial_share"] == 0.5
    assert el["random_hit1_expected"] == pytest.approx((1 + 0.25) / 2)
    r = EV.random_ranking(Y, E, draws=400)
    assert r["hit@1"] == pytest.approx(0.625, abs=0.05)
    assert EV.nontrivial(E).tolist() == [False, True, True]


def test_calibration_hoan_hao_ece_gan_0():
    rng = np.random.default_rng(0)
    p = rng.random(50000) * 0.2
    y = (rng.random(50000) < p).astype(int)
    assert EV.calibration(y, p)["ece"] < 0.005
    assert EV.calibration(y, p * 3)["ece"] > 0.05


def test_psi_va_beyond_accuracy():
    rng = np.random.default_rng(0)
    a = rng.normal(size=10000)
    assert EV.psi(a, rng.normal(size=10000)) < 0.02 and EV.psi(a, rng.normal(1, 1, 10000)) > 0.25
    S = np.tile([0.9, 0.1, 0.0], (100, 1))
    b = EV.beyond_accuracy(S, np.ones_like(S, bool))
    assert b["coverage"] == pytest.approx(1 / 3) and b["personalization"] == 0.0


def test_off_policy_ips_dr_gan_gia_tri_that():
    rng = np.random.default_rng(0)
    n, K = 20000, 3
    true_p = np.array([0.05, 0.10, 0.20])
    Y = (rng.random((n, K)) < true_p).astype(int)
    E = np.ones((n, K), bool)
    target = np.full(n, 2)
    r = EV.off_policy(Y, E, target, np.tile(true_p, (n, 1)), pop=np.array([0.3, 0.2, 0.1]), eps=0.3)
    assert abs(r["truth"] - 0.2) < 0.01
    assert abs(r["bias"]["ips"]) < 0.02 and abs(r["bias"]["dr"]) < 0.01 and r["logging_value"] < r["truth"]


def test_campaign_lift_va_roi():
    y = np.r_[np.ones(10), np.zeros(990)].astype(int)
    p = np.r_[np.full(10, 0.9), np.full(990, 0.01)]
    top = EV.campaign(y, p, value=100, cost=1, fracs=(0.01,))[0]
    assert top["conversions"] == 10 and top["lift"] == pytest.approx(100) and top["roi"] > top["roi_random"]


# ------------------------------------------------------------------------------------- thống kê online
def test_assign_on_dinh_va_can_bang():
    g = [ON.assign(i) for i in range(20000)]
    assert g == [ON.assign(i) for i in range(20000)]
    assert abs(np.mean([x == "treatment" for x in g]) - 0.5) < 0.02
    assert ON.assign(5, "exp1") == ON.assign(5, "exp1")


def test_kiem_dinh_hai_ti_le_srm_co_mau():
    t = ON.two_proportion(100, 10000, 150, 10000)
    assert t["p_value"] < 0.01 and t["ci"][0] > 0
    assert ON.two_proportion(100, 10000, 101, 10000)["p_value"] > 0.5
    assert ON.srm(5000, 5050)["ok"] and not ON.srm(5000, 5600)["ok"]
    n = ON.sample_size(0.05, 0.1)
    assert 25000 < n < 35000                          # tham chiếu: ~31.200 / nhóm cho 5% -> 5,5%


def test_team_draft_cong_bang_va_khong_trung():
    rng = np.random.default_rng(0)
    out, team = ON.team_draft([0, 1, 2], [2, 1, 0], 3, rng)
    assert sorted(out) == [0, 1, 2] and abs(team.count("a") - team.count("b")) <= 1


def test_event_log_va_online_metrics(tmp_path):
    log = ON.EventLog(tmp_path / "e.jsonl")
    for t in ("impression", "click", "accept"):
        log.write(ON.RecoEvent("2", "loan", t, "treatment", "v2-x"))
    log.write(ON.RecoEvent("3", "loan", "impression", "control", "v1"))
    m = ON.online_metrics(log.read())
    assert m["per_user"]["treatment"]["user_accept_rate"] == 1.0 and m["per_user"]["control"]["users_accept"] == 0
    with pytest.raises(ValueError):
        ON.RecoEvent("2", "loan", "buy", "t", "v")


# ------------------------------------------------------------------------------------- phục vụ
def test_phuc_vu_chan_san_pham_vua_bo_qua_va_xep_theo_gia_tri(toy):
    ds, m = toy
    d = ds.split("test").head(50).reset_index(drop=True)
    cid = str(d.client_id.iat[0])
    ev = [{"client_id": cid, "product": "a", "event_type": "dismiss", "ts": dt.date.today().isoformat()}]
    out = SV.recommend(ds, d, m, k=2, events=ev)
    assert not ((out.client_id == cid) & (out["product"] == "a")).any()
    assert out.attrs["filtered"]["suppressed"] >= 1
    assert (out.groupby("client_id")["expected_value"].apply(lambda s: s.is_monotonic_decreasing)).all()
    old = [{**ev[0], "ts": (dt.date.today() - dt.timedelta(days=90)).isoformat()}]
    assert SV.suppression(old, TOY) == {}


def test_luat_nghiep_vu_berka():
    d = pd.DataFrame({"avg_balance_3m": [10000, 90000], "overdraft_3m": [0, 2], "age": [30, 70]})
    ds = Dataset(BERKA, d, [], {c: {} for c in BERKA.codes})
    M = SV.rule_mask(ds, d)
    j = {c: i for i, c in enumerate(BERKA.codes)}
    assert not M[0, j["card_gold"]] and M[1, j["card_gold"]]
    assert M[0, j["loan"]] and not M[1, j["loan"]]
    assert M[0, j["card_junior"]] and not M[1, j["card_junior"]]
