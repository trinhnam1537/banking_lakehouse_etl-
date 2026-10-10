"""Đóng gói code ETL thành MỘT etl.zip dùng cho cả Glue lẫn Lambda.

Package nằm ở gốc zip (sourcing/, silver/, orchestrator/) nên:
  - Glue: bootstrap chèn zip vào sys.path rồi `import sourcing.main`
  - Lambda: handler khai thẳng `orchestrator.trigger.handler`,
    `orchestrator.schedule_sync.handler`, `silver.lambda_router.handler`

Zip TẤT ĐỊNH: thứ tự file cố định, mtime cố định -> cùng code cho cùng byte,
so hash được giữa hai lần build. Chỉ đóng *.py: không test, không __pycache__.
"""

from __future__ import annotations

import argparse
import pathlib
import zipfile
from typing import Iterable, List

REPO = pathlib.Path(__file__).resolve().parent.parent
PACKAGES = ("sourcing", "silver", "orchestrator")
_FIXED_TIME = (1980, 1, 1, 0, 0, 0)


def package_files(root: pathlib.Path = REPO, packages: Iterable[str] = PACKAGES) -> List[pathlib.Path]:
    files: List[pathlib.Path] = []
    for pkg in packages:
        base = root / pkg
        if not (base / "__init__.py").is_file():
            raise SystemExit(f"Thieu package {pkg}/ trong {root}")
        files.extend(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)
    return sorted(files, key=lambda p: p.relative_to(root).as_posix())


def build(out: pathlib.Path, root: pathlib.Path = REPO) -> pathlib.Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in package_files(root):
            info = zipfile.ZipInfo(f.relative_to(root).as_posix(), date_time=_FIXED_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, f.read_bytes())
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Dong goi etl.zip")
    ap.add_argument("--out", type=pathlib.Path, default=REPO / "dist" / "etl.zip")
    path = build(ap.parse_args().out)
    print(path, path.stat().st_size, "bytes")
