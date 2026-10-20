"""Script mà MỌI Glue job profile thực sự chạy (script_location).

Glue job profile (src-shell, slv-spark, ...) do repo hạ tầng tạo là một cái
khuôn: không chứa code ETL nào. Code nằm trên S3 theo phiên bản:

    s3://<ARTIFACT_BUCKET>/_ops/etl/releases/<sha>/etl.zip

File này làm đúng ba việc, rồi nhường cho code thật:

  1. xác định phiên bản: --ETL_RELEASE (Lambda trigger truyền theo từng lần
     chạy), nếu thiếu thì đọc SSM --ETL_RELEASE_PARAM (bản đang deploy)
  2. tải etl.zip về /tmp, chèn vào sys.path (Python import thẳng từ zip)
  3. gọi --ETL_MODULE (mặc định sourcing.main) .main()

Nhờ vậy một lần chạy luôn dùng ĐÚNG một phiên bản code, kể cả khi CI deploy
bản mới ngay giữa lúc job đang chạy; và đổi code không cần sửa Glue job.

File được CI upload lên khoá CỐ ĐỊNH _ops/etl/bootstrap/glue_bootstrap.py.
Giữ nó nhỏ và ổn định: đổi file này là đổi hợp đồng với repo hạ tầng. Chỉ
dùng thư viện chuẩn + boto3 (có sẵn trên Glue); chạy được trên Python 3.9.

Spark (slv-spark): executor cũng cần code -> Lambda truyền thêm
--extra-py-files trỏ đúng etl.zip của lần chạy; ở đây chỉ lo phía driver.
"""

import importlib
import os
import re
import sys

RELEASE_RE = re.compile(r"^[0-9a-f]{7,40}$")
DEFAULT_PREFIX = "_ops/etl/releases"


def arg(name, argv=None):
    """Đọc `--NAME value` từ tham số Glue (không dùng argparse: phải bỏ qua
    mọi tham số khác của Glue và của code ETL)."""
    argv = sys.argv if argv is None else argv
    flag = "--" + name
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            value = argv[i + 1]
            return None if value in ("", "-") else value
    return None


def resolve_release(argv=None, ssm_client=None):
    release = arg("ETL_RELEASE", argv)
    if not release:
        param = arg("ETL_RELEASE_PARAM", argv)
        if not param:
            raise SystemExit("Thieu --ETL_RELEASE va --ETL_RELEASE_PARAM: khong biet chay ban code nao")
        if ssm_client is None:
            import boto3
            ssm_client = boto3.client("ssm")
        release = ssm_client.get_parameter(Name=param)["Parameter"]["Value"].strip()
        print("ETL_RELEASE tu SSM %s = %s" % (param, release))
    # Phien ban la mot phan cua khoa S3: chi nhan SHA hex, khong cho '../'
    if not RELEASE_RE.match(release):
        raise SystemExit("ETL_RELEASE khong hop le: %r (can commit SHA hex)" % release)
    return release


def fetch(release, argv=None, s3_client=None, tmp_dir="/tmp"):
    bucket = arg("ARTIFACT_BUCKET", argv)
    if not bucket:
        raise SystemExit("Thieu --ARTIFACT_BUCKET")
    prefix = (arg("ETL_ARTIFACT_PREFIX", argv) or DEFAULT_PREFIX).strip("/")
    key = "%s/%s/etl.zip" % (prefix, release)
    local = os.path.join(tmp_dir, "etl-%s.zip" % release)
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    s3_client.download_file(bucket, key, local)
    print("Code ETL: s3://%s/%s" % (bucket, key))
    return local


def main(argv=None, ssm_client=None, s3_client=None, tmp_dir="/tmp"):
    release = resolve_release(argv, ssm_client)
    local = fetch(release, argv, s3_client, tmp_dir)
    if local not in sys.path:
        sys.path.insert(0, local)
    # Code ETL biết mình đang chạy bản nào (ghi vào log / RUN#)
    os.environ["ETL_RELEASE"] = release

    module_name = arg("ETL_MODULE", argv) or "sourcing.main"
    if not re.match(r"^(sourcing|silver)\.[a-z_]+$", module_name):
        raise SystemExit("ETL_MODULE khong duoc phep: %r" % module_name)
    module = importlib.import_module(module_name)
    return module.main()


def finish(rc):
    """Kết thúc theo mã trả về của code ETL.

    Glue Spark (slv-spark) chạy script này qua runpy.run_path và coi MỌI SystemExit,
    kể cả SystemExit(0), là lỗi: job bị đánh FAILED dù ETL đã SUCCESS (lỗi thật trên
    dev 10-15 → 10-20). Vì vậy thành công thì trả về bình thường, chỉ thoát khi rc khác 0.
    Python Shell (src-shell) chạy được cả hai cách.
    """
    if rc:
        sys.exit(rc)


if __name__ == "__main__":
    finish(main())
