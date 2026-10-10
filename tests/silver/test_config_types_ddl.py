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
    cfgs = load_all("silver_etl/entries")
    assert len(cfgs) >= 5
    assert {c.writer.type.value for c in cfgs.values()} == {"scd2", "scd4a", "simple"}


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
