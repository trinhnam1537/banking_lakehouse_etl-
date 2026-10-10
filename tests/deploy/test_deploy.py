"""Đóng gói + deploy code ETL không qua Terraform (deploy/)."""

from __future__ import annotations

import json
import sys
import types
import zipfile

import boto3
import pytest
from moto import mock_aws

from deploy import config_sync, glue_bootstrap
from deploy.build import REPO, build, package_files
from deploy.deploy import deploy, platform_params

REGION = "ap-southeast-1"
SHA = "0123456789abcdef0123456789abcdef01234567"


@pytest.fixture(autouse=True)
def _fake_aws_env(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                 "AWS_SESSION_TOKEN": "t", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


# ---------- build ----------


def test_zip_chi_co_package_va_tat_dinh(tmp_path):
    a, b = build(tmp_path / "a.zip"), build(tmp_path / "b.zip")
    assert a.read_bytes() == b.read_bytes()  # cung code -> cung byte
    names = zipfile.ZipFile(a).namelist()
    assert {"sourcing/main.py", "silver/main.py", "orchestrator/trigger.py",
            "orchestrator/schedule_sync.py"} <= set(names)
    assert not [n for n in names if n.startswith("tests/") or "__pycache__" in n
                or not n.endswith(".py")]


def test_zip_import_duoc_handler_lambda(tmp_path):
    """Lambda khai thang orchestrator.trigger.handler: phai import duoc tu zip, khong pyspark."""
    z = build(tmp_path / "etl.zip")
    import subprocess
    code = ("import sys; sys.path.insert(0, %r); "
            "import orchestrator.trigger, orchestrator.schedule_sync, silver.manifest; "
            "print('pyspark' in sys.modules)" % str(z))
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True,
                         cwd=str(tmp_path))
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False"


# ---------- bootstrap ----------


def test_bootstrap_doc_tham_so_glue():
    argv = ["x.py", "--JOB_NAME", "j", "--ETL_RELEASE", "abc1234", "--job-bookmark-option", "y"]
    assert glue_bootstrap.arg("ETL_RELEASE", argv) == "abc1234"
    assert glue_bootstrap.arg("KHONG_CO", argv) is None
    assert glue_bootstrap.arg("ETL_RELEASE", ["x", "--ETL_RELEASE", "-"]) is None


@pytest.mark.parametrize("bad", ["../../etc", "main", "ABCDEF1", "abc"])
def test_bootstrap_chan_release_khong_phai_sha(bad):
    with pytest.raises(SystemExit, match="khong hop le"):
        glue_bootstrap.resolve_release(["x", "--ETL_RELEASE", bad])


def test_bootstrap_lay_release_tu_ssm_khi_thieu():
    with mock_aws():
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/fingraph/dev/etl/release", Value=SHA, Type="String")
        argv = ["x", "--ETL_RELEASE_PARAM", "/fingraph/dev/etl/release"]
        assert glue_bootstrap.resolve_release(argv, ssm) == SHA


@pytest.fixture
def isolated_sourcing(monkeypatch):
    """Test duoi import mot package 'sourcing' GIA tu zip. Phai don no khi xong,
    ke ca khi package that chua tung duoc import (monkeypatch khong co gi de
    khoi phuc -> ban gia nam lai trong sys.modules va lam hong test sau)."""
    def own():
        return [m for m in sys.modules if m == "sourcing" or m.startswith("sourcing.")]

    saved = {m: sys.modules[m] for m in own()}
    for m in saved:
        del sys.modules[m]
    monkeypatch.setattr(sys, "path", list(sys.path))
    yield
    for m in own():
        del sys.modules[m]
    sys.modules.update(saved)


def test_bootstrap_tai_zip_va_goi_module(tmp_path, isolated_sourcing):
    # zip gia chua mot package 'sourcing' voi main() tra 7
    src = tmp_path / "pkg" / "sourcing"
    src.mkdir(parents=True)
    (src / "__init__.py").write_text("")
    (src / "main.py").write_text("def main():\n    return 7\n")
    z = tmp_path / "etl.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.write(src / "__init__.py", "sourcing/__init__.py")
        f.write(src / "main.py", "sourcing/main.py")

    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket="bkt", CreateBucketConfiguration={"LocationConstraint": REGION})
        s3.upload_file(str(z), "bkt", f"_ops/etl/releases/{SHA}/etl.zip")
        argv = ["x", "--ARTIFACT_BUCKET", "bkt", "--ETL_RELEASE", SHA]
        assert glue_bootstrap.main(argv, s3_client=s3, tmp_dir=str(tmp_path)) == 7
    assert str(tmp_path / f"etl-{SHA}.zip") in sys.path


def test_bootstrap_chan_module_la():
    with pytest.raises(SystemExit, match="ETL_MODULE"):
        glue_bootstrap.main(["x", "--ETL_RELEASE", SHA, "--ARTIFACT_BUCKET", "b",
                             "--ETL_MODULE", "os.system"],
                            s3_client=types.SimpleNamespace(download_file=lambda *a: None))


# ---------- config_sync ----------


def _table(ddb, name):
    ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "job_name", "KeyType": "HASH"}],
                     AttributeDefinitions=[{"AttributeName": "job_name", "AttributeType": "S"}])
    return ddb.Table(name)


def test_config_sync_tao_giu_sua_tat(tmp_path):
    d = tmp_path / "sources"
    (d / "x").mkdir(parents=True)
    base = "source_type: api\napi: {base_url: 'https://a'}\ncdc: {mode: cdc5}\n"
    (d / "x" / "a.yml").write_text("name: a\n" + base)
    (d / "x" / "b.yml").write_text("name: b\nowner: nam\n" + base)

    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=REGION)
        t = _table(ddb, "cfg")
        # item cu: c khong con trong repo; b co thuoc tinh cua dispatcher cu
        t.put_item(Item={"job_name": "c", "enabled": True, "config": "{}"})
        t.put_item(Item={"job_name": "b", "enabled": True, "last_dispatched_at": "x",
                         "config": json.dumps({"name": "b"})})

        r1 = config_sync.sync("cfg", config_sync.load_sourcing(str(d)), dynamodb=ddb, log=lambda m: None)
        assert (r1.created, r1.updated, r1.disabled) == (["a"], ["b"], ["c"])
        b = t.get_item(Key={"job_name": "b"})["Item"]
        assert b["owner"] == "nam" and b["last_dispatched_at"] == "x"  # giu thuoc tinh khac
        assert json.loads(b["config"])["api"]["base_url"] == "https://a"
        assert t.get_item(Key={"job_name": "c"})["Item"]["enabled"] is False

        r2 = config_sync.sync("cfg", config_sync.load_sourcing(str(d)), dynamodb=ddb, log=lambda m: None)
        assert r2.unchanged == ["a", "b"] and not (r2.created or r2.updated or r2.disabled)


def test_config_sync_so_theo_ngu_nghia_khong_theo_chuoi():
    """Terraform jsonencode khac thu tu khoa voi Python -> van coi la KHONG doi."""
    desired = {"enabled": True, "config": json.dumps({"a": 1, "b": 2}, sort_keys=True)}
    assert config_sync._same({"enabled": True, "config": '{"b":2,"a":1}'}, desired)
    assert not config_sync._same({"enabled": True, "config": '{"b":3,"a":1}'}, desired)


def test_config_sync_entry_sai_khong_ghi_gi(tmp_path):
    d = tmp_path / "sources"
    d.mkdir()
    (d / "a.yml").write_text("name: a\nsource_type: api\napi: {base_url: x}\ncdc: {mode: cdc9}\n")
    from sourcing.config import ConfigError
    with pytest.raises(ConfigError):
        config_sync.load_sourcing(str(d))


def test_config_sync_entry_that_cua_repo():
    src = config_sync.load_sourcing(str(REPO / "sourcing_etl" / "sources"))
    slv = config_sync.load_silver(str(REPO / "silver_etl" / "entries"))
    assert sorted(src) == ["berka_account", "berka_card", "berka_client", "berka_disp",
                           "berka_district", "berka_loan", "berka_order", "berka_trans"]
    assert src["berka_trans"]["source_type"] == "jdbc" and src["berka_trans"]["cdc_mode"] == "cdc1"
    assert slv == {}  # chua co entry silver nao
    assert all(set(v) >= {"enabled", "config", "config_sha"} for v in src.values())


# ---------- deploy (dau-cuoi, AWS gia lap) ----------


class _FakeLambda:
    def __init__(self):
        self.updated = []

    def update_function_code(self, **kw):
        self.updated.append(kw)

    def get_waiter(self, name):
        return types.SimpleNamespace(wait=lambda **kw: None)


class _Session:
    def __init__(self, lam):
        self.lam = lam

    def client(self, name):
        return self.lam if name == "lambda" else boto3.client(name, region_name=REGION)

    def resource(self, name):
        return boto3.resource(name, region_name=REGION)


def _platform(ssm, **over):
    params = {"artifact_bucket": "lake", "src_config_table": "src-cfg",
              "slv_config_table": "slv-cfg", "lambda_functions": "fn-trigger, fn-sync",
              "release_param": "/fingraph/dev/etl/release", **over}
    for k, v in params.items():
        ssm.put_parameter(Name=f"/fingraph/dev/platform/{k}", Value=v, Type="String")


def test_thieu_tham_so_nen_tang_bao_ro():
    with mock_aws():
        ssm = boto3.client("ssm", region_name=REGION)
        ssm.put_parameter(Name="/fingraph/dev/platform/artifact_bucket", Value="b", Type="String")
        with pytest.raises(SystemExit, match="src_config_table"):
            platform_params("dev", ssm)


@pytest.mark.parametrize("dry_run", [True, False])
def test_deploy_dau_cuoi(dry_run):
    with mock_aws():
        ssm = boto3.client("ssm", region_name=REGION)
        _platform(ssm)
        boto3.client("s3", region_name=REGION).create_bucket(
            Bucket="lake", CreateBucketConfiguration={"LocationConstraint": REGION})
        ddb = boto3.resource("dynamodb", region_name=REGION)
        _table(ddb, "src-cfg")
        _table(ddb, "slv-cfg")
        lam = _FakeLambda()

        out = deploy("dev", SHA, dry_run=dry_run, session=_Session(lam), log=lambda m: None)

        keys = [o["Key"] for o in boto3.client("s3", region_name=REGION)
                .list_objects_v2(Bucket="lake").get("Contents", [])]
        n_src = ddb.Table("src-cfg").scan()["Count"]
        if dry_run:
            assert keys == [] and lam.updated == [] and n_src == 0
            assert len(out["reports"][0].created) == 8  # bao cao van du: 8 entry Berka
            return
        assert f"_ops/etl/releases/{SHA}/etl.zip" in keys
        assert "_ops/etl/bootstrap/glue_bootstrap.py" in keys
        assert {"_ops/glue-scripts/bronze_ingest.py", "_ops/glue-scripts/onto_closure.py"} <= set(keys)
        assert [u["FunctionName"] for u in lam.updated] == ["fn-trigger", "fn-sync"]
        assert lam.updated[0]["S3Key"] == f"_ops/etl/releases/{SHA}/etl.zip"
        assert ssm.get_parameter(Name="/fingraph/dev/etl/release")["Parameter"]["Value"] == SHA
        assert n_src == 8 and ddb.Table("slv-cfg").scan()["Count"] == 0


def test_deploy_chan_sha_la():
    with pytest.raises(SystemExit, match="sha"):
        deploy("dev", "main")
