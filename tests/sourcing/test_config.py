"""Nạp và kiểm tra entry YAML (sourcing/config.py)."""

from __future__ import annotations

import os
import pathlib

import pytest

from sourcing.config import (
    CdcMode,
    ConfigError,
    Engine,
    SourceType,
    from_dict,
    load_all,
    load_config,
)

from .conftest import entry, make_cfg

REPO = pathlib.Path(__file__).resolve().parents[2]
SOURCES_DIR = REPO / "sourcing_etl" / "sources"


# ---------- mặc định ----------


def test_entry_toi_thieu_nhan_mac_dinh():
    cfg = make_cfg()
    assert cfg.source_type is SourceType.API
    assert cfg.cdc.mode is CdcMode.CDC5
    assert cfg.runtime.engine is Engine.PYTHON_SHELL
    assert cfg.enabled is True
    assert cfg.schedule.enabled is False
    assert cfg.target.file_format == "ndjson.gz"
    assert cfg.api.base_url == "https://api.test"
    assert cfg.jdbc is None and cfg.kafka is None and cfg.s3file is None


def test_dataset_mac_dinh_theo_ten_job():
    assert make_cfg().raw_path == "landing/t_job"


def test_dataset_va_prefix_tuy_chinh():
    cfg = make_cfg(target={"prefix": "raw/", "dataset": "binance/depth"})
    assert cfg.raw_path == "raw/binance/depth"


def test_ten_lay_tu_default_name_khi_thieu_khoa_name():
    raw = entry()
    del raw["name"]
    assert from_dict(raw, default_name="tu_ten_file").name == "tu_ten_file"


# ---------- lỗi cấu hình: phải chặn lúc nạp, kèm tên job ----------


def test_thieu_source_type():
    raw = entry()
    del raw["source_type"]
    with pytest.raises(ConfigError, match=r"\[t_job\].*source_type"):
        from_dict(raw)


@pytest.mark.parametrize(
    "over, field",
    [
        ({"source_type": "ftp"}, "source_type"),
        ({"cdc": {"mode": "cdc3"}}, "cdc.mode"),
        ({"runtime": {"engine": "emr"}}, "runtime.engine"),
    ],
)
def test_enum_sai_liet_ke_gia_tri_hop_le(over, field):
    with pytest.raises(ConfigError, match=field) as ei:
        make_cfg(**over)
    assert "Cho phep" in str(ei.value)


def test_khoi_nguon_co_khoa_la():
    with pytest.raises(ConfigError, match=r"\[t_job\] khoi 'api' sai"):
        make_cfg(api={"base_urll": "typo"})


def test_khoi_nguon_thieu_khoa_bat_buoc():
    raw = entry(source_type="jdbc")
    raw["jdbc"] = {"schema": "public"}  # thieu secret_name, table
    with pytest.raises(ConfigError, match="khoi 'jdbc' sai"):
        from_dict(raw)


def test_cdc1_bat_buoc_watermark_column():
    with pytest.raises(ConfigError, match="thieu cdc.watermark_column"):
        make_cfg(cdc={"mode": "cdc1"})


def test_cdc1_kafka_khong_can_watermark_column():
    raw = {
        "name": "k",
        "source_type": "kafka",
        "kafka": {"bootstrap_servers": "b:9092", "topic": "t", "group_id": "g"},
        "cdc": {"mode": "cdc1"},
    }
    assert from_dict(raw).cdc.watermark_column is None


def test_cdc5_cam_watermark_column():
    with pytest.raises(ConfigError, match="cdc5.*khong dung watermark_column"):
        make_cfg(cdc={"mode": "cdc5", "watermark_column": "updated_at"})


def test_jdbc_num_partitions_chi_cho_spark():
    raw = {
        "name": "j",
        "source_type": "jdbc",
        "jdbc": {"secret_name": "fingraph/src/s", "schema": "public", "table": "t", "num_partitions": 4},
        "cdc": {"mode": "cdc5"},
    }
    with pytest.raises(ConfigError, match="num_partitions"):
        from_dict(raw)
    raw["runtime"] = {"engine": "spark"}
    assert from_dict(raw).jdbc.num_partitions == 4


@pytest.mark.parametrize("fmt, ok", [("ndjson.gz", True), ("ndjson", True), ("parquet", False)])
def test_raw_chi_ghi_ndjson(fmt, ok):
    if ok:
        assert make_cfg(target={"file_format": fmt}).target.file_format == fmt
    else:
        with pytest.raises(ConfigError, match="chua ho tro"):
            make_cfg(target={"file_format": fmt})


# ---------- ${BIEN} ----------


def test_expand_env_de_quy(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "10.0.1.5:9092")
    raw = {
        "name": "k",
        "source_type": "kafka",
        "kafka": {"bootstrap_servers": "${KAFKA_BOOTSTRAP}", "topic": "t", "group_id": "g"},
        "cdc": {"mode": "cdc1"},
        "runtime": {"additional_python_modules": ["x==${KAFKA_BOOTSTRAP}"]},
    }
    cfg = from_dict(raw)
    assert cfg.kafka.bootstrap_servers == "10.0.1.5:9092"
    assert cfg.runtime.additional_python_modules == ["x==10.0.1.5:9092"]


def test_bien_chua_dat_giu_nguyen_chuoi(monkeypatch):
    # Chuoi ${...} lot vao thong bao loi thi nhin la biet thieu bien nao.
    monkeypatch.delenv("CHUA_DAT_BAO_GIO", raising=False)
    cfg = make_cfg(api={"base_url": "${CHUA_DAT_BAO_GIO}"})
    assert cfg.api.base_url == "${CHUA_DAT_BAO_GIO}"


# ---------- load_config / load_all ----------


def _write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


YAML_OK = """\
name: {name}
source_type: api
api:
  base_url: https://api.test
cdc:
  mode: cdc5
"""


def test_load_config_lay_ten_tu_file_khi_thieu_name(tmp_path):
    p = tmp_path / "tu_file.yml"
    _write(p, YAML_OK.replace("name: {name}\n", ""))
    assert load_config(str(p)).name == "tu_file"


def test_load_config_file_rong_bao_thieu_source_type(tmp_path):
    p = tmp_path / "rong.yml"
    _write(p, "")
    with pytest.raises(ConfigError, match=r"\[rong\] thieu 'source_type'"):
        load_config(str(p))


def test_load_all_quet_thu_muc_con_va_bo_file_khac(tmp_path):
    _write(tmp_path / "a" / "job_a.yml", YAML_OK.format(name="job_a"))
    _write(tmp_path / "b" / "c" / "job_b.yaml", YAML_OK.format(name="job_b"))
    _write(tmp_path / "README.md", "khong phai entry")
    assert sorted(load_all(str(tmp_path))) == ["job_a", "job_b"]


def test_load_all_chan_name_lech_ten_file(tmp_path):
    _write(tmp_path / "job_a.yml", YAML_OK.format(name="job_khac"))
    with pytest.raises(ConfigError, match="khong trung ten file"):
        load_all(str(tmp_path))


def test_load_all_chan_trung_ten_file_o_hai_thu_muc(tmp_path):
    _write(tmp_path / "x" / "dup.yml", YAML_OK.format(name="dup"))
    _write(tmp_path / "y" / "dup.yml", YAML_OK.format(name="dup"))
    with pytest.raises(ConfigError, match="Trung ten job: dup"):
        load_all(str(tmp_path))


# ---------- entry THẬT trong repo ----------
# Chạy trong CI: một entry sai sẽ làm đỏ PR trước khi Terraform kịp seed nó
# vào DynamoDB và Glue job chết lúc 2 giờ sáng.


@pytest.fixture(scope="module")
def real_entries():
    return load_all(str(SOURCES_DIR))


def test_moi_entry_that_deu_hop_le(real_entries):
    assert len(real_entries) > 0


def test_entry_that_khong_chua_khoa_bi_mat(real_entries):
    """Khoá API phải đi qua secret_name/secret_inject, không nằm trong YAML."""
    suspicious = ("key", "token", "secret", "password", "apikey")
    for name, cfg in real_entries.items():
        if cfg.api is None:
            continue
        for k in list(cfg.api.query) + list(cfg.api.headers):
            assert not any(s in k.lower() for s in suspicious), (
                f"{name}: '{k}' trong query/headers — dung secret_inject"
            )


def test_entry_that_secret_inject_dung_dinh_dang(real_entries):
    for name, cfg in real_entries.items():
        if cfg.api is None:
            continue
        for dest in cfg.api.secret_inject.values():
            where, _, target = dest.partition(".")
            assert where in ("header", "query") and target, f"{name}: '{dest}'"
        if cfg.api.secret_inject:
            assert cfg.api.secret_name, f"{name}: co secret_inject ma thieu secret_name"


def test_entry_that_pagination_hop_le(real_entries):
    for name, cfg in real_entries.items():
        if cfg.api is not None:
            assert cfg.api.pagination in ("none", "page", "cursor", "offset"), name
            if cfg.api.pagination == "cursor":
                assert cfg.api.cursor_path, f"{name}: cursor ma thieu cursor_path"


def test_entry_that_kafka_la_cdc1(real_entries):
    # Kafka khong co "snapshot toan bo" — reader luon chay nhu CDC1.
    for name, cfg in real_entries.items():
        if cfg.source_type is SourceType.KAFKA:
            assert cfg.cdc.mode is CdcMode.CDC1, name


def test_entry_that_lich_dung_cu_phap_eventbridge(real_entries):
    """Lambda schedule-sync dua thang bieu thuc vao EventBridge Scheduler: sai cu phap
    thi tao lich that bai. Kiem dang rate(N don_vi) / cron(6 truong) ngay o day."""
    import re

    rate = re.compile(r"^rate\(\d+ (minute|minutes|hour|hours|day|days)\)$")
    for name, cfg in real_entries.items():
        expr = cfg.schedule.expression.strip()
        if expr.startswith("cron("):
            assert expr.endswith(")") and len(expr[5:-1].split()) == 6, f"{name}: {expr}"
        else:
            assert rate.match(expr), f"{name}: {expr}"


def test_thu_muc_sources_ton_tai():
    assert os.path.isdir(SOURCES_DIR)


# ---------- profile + tien to secret (Glue job dung chung) ----------


def test_profile_mac_dinh_la_src_shell():
    assert make_cfg().runtime.profile == "src-shell"


def test_profile_la_bi_chan():
    with pytest.raises(ConfigError, match="runtime.profile"):
        make_cfg(runtime={"profile": "src-khong-co"})


@pytest.mark.parametrize("block, secret", [
    ("api", "fingraph/alphavantage"),          # thieu src/
    ("api", "rds!db-1234"),                    # secret master RDS tu sinh
    ("jdbc", "fingraph/rds/core_banking"),
])
def test_secret_ngoai_tien_to_bi_chan(block, secret):
    over = ({"api": {"base_url": "https://x", "secret_name": secret}} if block == "api" else
            {"source_type": "jdbc", "jdbc": {"secret_name": secret, "schema": "s", "table": "t"},
             "cdc": {"mode": "cdc5"}})
    with pytest.raises(ConfigError, match="fingraph/src/"):
        make_cfg(**over)


def test_moi_entry_that_dung_tien_to_secret():
    from sourcing.config import SECRET_PREFIX, load_all
    for name, cfg in load_all(str(SOURCES_DIR)).items():
        s = (cfg.api and cfg.api.secret_name) or (cfg.jdbc and cfg.jdbc.secret_name)
        assert not s or s.startswith(SECRET_PREFIX), name
