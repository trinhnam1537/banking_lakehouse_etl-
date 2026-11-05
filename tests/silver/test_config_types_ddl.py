"""Phần không cần Spark: nạp entry, sinh biểu thức ép kiểu, DDL."""

from __future__ import annotations

import pytest

from silver.config import ColumnSpec, ConfigError, from_dict, load_all
from silver.ddl import generate_ddl, split_statements
from silver.types import cast_sql, json_path, path_tree, split_conflicts, tree_to_ddl


def _entry(**over):
    e = {"name": "x", "source": {"dataset": "d"}, "target": {"table": "t"},
         "columns": [{"name": "id"}, {"name": "v", "type": "int"}],
         "technical": {"keys": ["id"]}}
    e.update(over)
    return e


# ---------- entry that ----------


def test_moi_entry_that_nap_duoc():
    """Moi entry silver trong repo phai nap duoc; them entry sai la do o day."""
    entries = load_all("silver_etl/entries")
    assert entries, "silver_etl/entries rong"
    for name, cfg in entries.items():
        assert cfg.name == name


def test_entry_silver_doc_dung_dataset_cua_mot_entry_sourcing():
    """Orchestrator noi sourcing -> silver theo dataset. Go sai mot ky tu la entry
    silver khong bao gio duoc kich, va khong co loi nao bao ra."""
    from sourcing.config import load_all as load_sources

    datasets = {c.dataset for c in load_sources("sourcing_etl/sources").values()}
    for name, cfg in load_all("silver_etl/entries").items():
        assert cfg.source.dataset in datasets, f"{name}: khong entry sourcing nao ghi '{cfg.source.dataset}'"


def test_entry_co_target_ddl_thi_file_ton_tai_va_du_cot():
    """DDL viet tay phai tao DUNG bang cua entry va co MOI cot entry se ghi."""
    import os

    from silver.ddl import expected_columns

    for name, cfg in load_all("silver_etl/entries").items():
        if not cfg.target.ddl:
            continue
        path = os.path.join("silver_etl/ddl", cfg.target.ddl)
        text = open(path, encoding="utf-8").read()
        assert cfg.target.fqn in text, f"{name}: {path} khong tao {cfg.target.fqn}"
        for col, _, _ in expected_columns(cfg):
            assert f"`{col}`" in text, f"{name}: {path} thieu cot {col}"


def test_moi_ddl_trong_thu_muc_tach_duoc_cau_lenh():
    import glob
    for f in glob.glob("silver_etl/ddl/**/*.sql", recursive=True):
        stmts = split_statements(open(f, encoding="utf-8").read())
        assert stmts and stmts[0].upper().startswith("CREATE TABLE"), f


# ---------- kiem tra entry ----------


@pytest.mark.parametrize("over, msg", [
    ({"columns": [{"name": "id", "type": "decmal(2,1)"}]}, "kieu"),
    ({"columns": [{"name": "id"}, {"name": "id"}]}, "trung ten cot"),
    ({"columns": [{"name": "ds_key"}], "technical": {}}, "cot ky thuat"),
    ({"technical": {"keys": ["khong_co"]}}, "technical.keys"),
    ({"writer": {"type": "scd2"}, "technical": {}}, "can technical.keys"),
    ({"writer": {"type": "simple", "mode": "overwrite_partition"}}, "partition_by"),
    ({"writer": {"type": "scd9"}}, "writer.type"),
    ({"columns": [{"name": "id", "transform": "upper(x)"}]}, "{col}"),
    ({"columns": [{"name": "id", "from": "header"}]}, "columns.from"),
    ({"columns": [{"name": "id", "colour": "red"}]}, "khoa la"),
    ({"quality": {"not_null": ["zz"]}}, "not_null"),
])
def test_entry_sai_bi_chan_luc_nap(over, msg):
    with pytest.raises(ConfigError, match=msg):
        from_dict(_entry(**over))


def test_cot_trung_gian_khong_vao_bang_khong_vao_hash():
    cfg = from_dict(_entry(columns=[{"name": "id"}, {"name": "_tmp", "keep": False},
                                    {"name": "ts", "type": "timestamp", "hash": False}]))
    assert [c.name for c in cfg.business_columns] == ["id", "ts"]
    assert cfg.hash_columns == ["id"]


# ---------- ep kieu ----------


@pytest.mark.parametrize("col, expected", [
    (ColumnSpec("x"), "`x`"),
    (ColumnSpec("x", type="int"), "try_cast(trim(`x`) AS int)"),
    (ColumnSpec("x", type="long"), "try_cast(trim(`x`) AS bigint)"),
    (ColumnSpec("x", type="decimal(38,18)"), "try_cast(trim(`x`) AS decimal(38,18))"),
    (ColumnSpec("x", type="timestamp"), "try_cast(`x` AS timestamp)"),
    (ColumnSpec("x", type="timestamp", format="epoch_ms"), "timestamp_millis(try_cast(`x` AS bigint))"),
    (ColumnSpec("x", type="date", format="MM/dd/yyyy"), "to_date(try_to_timestamp(`x`, 'MM/dd/yyyy'))"),
    (ColumnSpec("x", type="array<string>"), "from_json(`x`, 'array<string>')"),
])
def test_cast_sql(col, expected):
    assert cast_sql("`x`", col) == expected


def test_boolean_hieu_nhieu_cach_viet():
    s = cast_sql("`x`", ColumnSpec("x", type="boolean"))
    assert "'y'" in s and "'1'" in s and "'n'" in s and "'0'" in s


def test_json_path_ten_co_gach_ngang():
    assert json_path("relationships.direct-parent.data", "$") == "$['relationships']['direct-parent']['data']"


def test_path_tree_va_xung_dot():
    ok, conflict = split_conflicts(["a.b", "a.b.c", "a.d", "e"])
    assert ok == ["e", "a.b", "a.d"] and conflict == ["a.b.c"]
    assert tree_to_ddl(path_tree(ok)) == "struct<`e`:string,`a`:struct<`b`:string,`d`:string>>"


# ---------- DDL ----------


def test_ddl_scd2_co_cot_hieu_luc_scd4a_partition_ngay():
    scd2 = generate_ddl(from_dict(_entry(writer={"type": "scd2"})))
    assert "`ds_valid_from` date" in scd2 and "`ds_is_current` boolean" in scd2
    scd4a = generate_ddl(from_dict(_entry(writer={"type": "scd4a"})))
    assert "PARTITIONED BY (ds_partition_date)" in scd4a and "ds_valid_from" not in scd4a


def test_split_statements_khong_cat_trong_chuoi():
    """Bug that 09-28: COMMENT chua ';' bi cat doi."""
    sql = "CREATE TABLE a (x int COMMENT 'mot; hai -- ba'); -- ghi chu; khong phai lenh\nDROP TABLE b;"
    assert split_statements(sql) == ["CREATE TABLE a (x int COMMENT 'mot; hai -- ba')", "DROP TABLE b"]


def test_path_dau_cham_that_trong_ten_khoa():
    # Bank Marketing additional: cot `emp.var.rate` — `\.` la dau cham THAT, khong phai long nhau
    from silver.types import json_path, path_tree, split_path, tree_to_ddl

    assert split_path(r"emp\.var\.rate") == ["emp.var.rate"]
    assert split_path("a.b-c.d") == ["a", "b-c", "d"]
    assert json_path(r"emp\.var\.rate") == "$['emp.var.rate']"
    assert tree_to_ddl(path_tree([r"emp\.var\.rate", "x.y"])) == "struct<`x`:struct<`y`:string>,`emp.var.rate`:string>"
