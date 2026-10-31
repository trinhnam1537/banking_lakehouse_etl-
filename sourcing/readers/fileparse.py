"""Đọc file theo LUỒNG: CSV / NDJSON / JSON, có hoặc không gzip. XLSX là ngoại lệ (xem `_iter_xlsx`).

Dùng chung cho reader `s3file` (file trong S3) và `httpfile` (file tải qua HTTP).

Trước đây reader `s3file` gọi `Body.read()` rồi parse thành list: file 470 MB
(PaySim) cần vài GB RAM trong khi Glue Python Shell có 1 GB. Ở đây luồng byte đi
thẳng qua gzip -> giải mã -> parser, mỗi lần chỉ giữ một dòng; reader gom thành
lô `batch_size` bản ghi.

Mỗi bản ghi đi kèm số thứ tự trong file (`_src_row`, đếm từ 1, không tính header).
Nhiều file nguồn không có khoá nghiệp vụ (PaySim không có mã giao dịch): cặp
(`_src_file`, `_src_row`) là khoá tự nhiên để silver khử trùng khi một file bị
đọc lại (at-least-once).
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from typing import Any, Dict, Iterator, Tuple

# CSV có ô rất dài (OpenSanctions: chuỗi lý do cấm vận hàng chục KB) vượt mức
# 128 KB mặc định của module csv. sys.maxsize tràn C long trên Windows.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# File JSON nguyên khối (một mảng) không đọc theo luồng được bằng thư viện chuẩn.
# Chặn ở mức an toàn với 1 GB RAM; file lớn hơn thì dùng NDJSON.
MAX_JSON_BYTES = 128 * 1024 * 1024

# XLSX là file zip: openpyxl cần đọc ngẫu nhiên (seek) nên phải nạp cả file vào
# RAM. Excel tối đa ~1 triệu dòng; file > 64 MB thì xuất CSV ở nguồn.
MAX_XLSX_BYTES = 64 * 1024 * 1024

# zip cần đọc ngẫu nhiên (danh mục nằm CUỐI file) -> nạp cả archive vào RAM.
# tar.gz đọc tuần tự, không giới hạn. Parquet (footer cuối file) nạp từng member.
MAX_ZIP_BYTES = 256 * 1024 * 1024
MAX_PARQUET_BYTES = 256 * 1024 * 1024
ARCHIVE_KINDS = ("zip", "tar.gz")


class _Raw(io.RawIOBase):
    """Bọc mọi đối tượng có `.read(n)` (botocore StreamingBody, HTTPResponse)
    thành luồng byte chuẩn để xếp chồng gzip/TextIOWrapper lên trên."""

    def __init__(self, src):
        self._src = src

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        chunk = self._src.read(len(b))
        if not chunk:
            return 0
        n = len(chunk)
        b[:n] = chunk
        return n


def is_gzip(name: str, compression: str) -> bool:
    if compression == "gzip":
        return True
    if compression == "none":
        return False
    return name.endswith(".gz")  # auto


def iter_records(
    stream,
    file_format: str,
    *,
    name: str = "",
    gzip_on: bool = False,
    encoding: str = "utf-8-sig",
    csv_delimiter: str = ",",
    repeated_header: bool = False,
    sheet: str = "",
    header_row: int = 1,
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    """Sinh (số thứ tự, bản ghi). `stream` là luồng byte bất kỳ có `.read(n)`.

    `sheet`, `header_row` chỉ dùng cho xlsx.
    """
    binary = io.BufferedReader(_Raw(stream), buffer_size=1024 * 1024)
    if gzip_on:
        binary = gzip.GzipFile(fileobj=binary, mode="rb")

    if file_format == "xlsx":
        yield from _iter_xlsx(binary, name, sheet, header_row)
        return

    if file_format == "parquet":
        yield from _iter_parquet(binary, name)
        return

    if file_format == "json":
        body = binary.read(MAX_JSON_BYTES + 1)
        if len(body) > MAX_JSON_BYTES:
            raise ValueError(
                f"{name}: file JSON nguyen khoi > {MAX_JSON_BYTES // 2**20} MB, khong doc "
                f"theo luong duoc. Doi nguon sang ndjson (moi dong mot ban ghi)."
            )
        doc = json.loads(body.decode(encoding))
        rows = doc if isinstance(doc, list) else [doc]
        for i, rec in enumerate(rows, 1):
            yield i, rec if isinstance(rec, dict) else {"value": rec}
        return

    text = io.TextIOWrapper(binary, encoding=encoding, newline="")
    if file_format == "ndjson":
        n = 0
        for line_no, line in enumerate(text, 1):
            if not line.strip():
                continue
            n += 1
            try:
                rec = json.loads(line)
            except ValueError as e:
                raise ValueError(f"{name}: dong {line_no} khong phai JSON: {e}") from None
            yield n, rec if isinstance(rec, dict) else {"value": rec}
        return

    if file_format == "csv":
        yield from _iter_csv(text, csv_delimiter, name, repeated_header)
        return

    raise ValueError(f"{name}: file_format khong ho tro: {file_format}")


def _iter_parquet(binary, name: str):
    """Parquet -> dict từng dòng, đọc theo row group (pyarrow, profile src-shell cài sẵn).

    Kiểu: số/chuỗi/bool/list giữ nguyên; ngày giờ -> ISO; decimal -> chuỗi (như
    json.dumps(default=str) của writer). Mảng embedding (MBD dialog) giữ là list số.
    """
    import pyarrow.parquet as pq  # import tai cho: chi entry parquet can

    body = binary.read(MAX_PARQUET_BYTES + 1)
    if len(body) > MAX_PARQUET_BYTES:
        raise ValueError(f"{name}: file parquet > {MAX_PARQUET_BYTES // 2**20} MB — dung profile src-spark.")
    pf = pq.ParquetFile(io.BytesIO(body))
    n = 0
    for batch in pf.iter_batches(batch_size=10_000):
        for rec in batch.to_pylist():
            n += 1
            yield n, {k: _cell(v) for k, v in rec.items()}


def iter_archive(stream, kind: str, patterns, *, name: str = "") -> Iterator[Tuple[str, Any]]:
    """Sinh (đường dẫn member, luồng byte) cho mỗi member khớp `patterns` (fnmatch).

    kind = "zip" | "tar.gz". Zip LỒNG zip khai bằng dấu `!`:
        "bank-additional.zip!bank-additional/bank-additional-full.csv"
    (UCI Bank Marketing: archive ngoài chứa hai zip con). Caller phải đọc HẾT member
    trước khi lấy member sau — tar.gz đọc tuần tự, không quay lại được.
    """
    import fnmatch
    import tarfile
    import zipfile

    pats = list(patterns)
    if kind == "tar.gz":
        tf = tarfile.open(fileobj=io.BufferedReader(_Raw(stream), buffer_size=1024 * 1024), mode="r|gz")
        for m in tf:
            if m.isfile() and any(fnmatch.fnmatch(m.name, p) for p in pats):
                yield m.name, tf.extractfile(m)
        return
    if kind != "zip":
        raise ValueError(f"{name}: archive khong ho tro: {kind}")

    body = io.BufferedReader(_Raw(stream)).read(MAX_ZIP_BYTES + 1)
    if len(body) > MAX_ZIP_BYTES:
        raise ValueError(f"{name}: zip > {MAX_ZIP_BYTES // 2**20} MB — tai ve S3 roi dung s3file.")

    def walk(zf, prefix, pats_here):
        names = sorted(n for n in zf.namelist() if not n.endswith("/") and not n.startswith("__MACOSX/"))
        for n in names:
            direct = [p for p in pats_here if "!" not in p and fnmatch.fnmatch(n, p)]
            nested = [p.split("!", 1)[1] for p in pats_here
                      if "!" in p and fnmatch.fnmatch(n, p.split("!", 1)[0])]
            if direct:
                with zf.open(n) as f:
                    yield prefix + n, f
            if nested:
                with zipfile.ZipFile(io.BytesIO(zf.read(n))) as inner:
                    yield from walk(inner, prefix + n + "!", nested)

    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        yield from walk(zf, "", pats)


def hive_parts(path: str) -> Dict[str, str]:
    """'targets/fold=0/part-0.parquet' -> {'fold': '0'} (cột partition Hive ở tên thư mục)."""
    out = {}
    for seg in path.split("/")[:-1]:
        if "=" in seg:
            k, v = seg.split("=", 1)
            out[k] = v
    return out


def _cell(v: Any) -> Any:
    """Ô Excel -> giá trị JSON được. Ngày giờ -> ISO; số/chuỗi/bool giữ nguyên
    kiểu (tầng raw không ép schema, silver tự cast)."""
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if type(v).__name__ == "Decimal":
        return str(v)
    return v


def _iter_xlsx(binary, name: str, sheet: str, header_row: int):
    """Một sheet XLSX -> dict theo dòng header.

    `data_only=True`: lấy GIÁ TRỊ đã tính của ô công thức, không lấy chuỗi
    "=SUM(...)". File chưa từng được Excel mở lại sau khi sửa công thức thì giá
    trị cache có thể rỗng — giới hạn của định dạng, không phải của reader.
    Dòng trống hoàn toàn bị bỏ qua.
    """
    # Import tai cho: chi entry xlsx can openpyxl (profile src-shell cai san)
    from openpyxl import load_workbook

    body = binary.read(MAX_XLSX_BYTES + 1)
    if len(body) > MAX_XLSX_BYTES:
        raise ValueError(f"{name}: file XLSX > {MAX_XLSX_BYTES // 2**20} MB — xuat CSV o nguon.")
    wb = load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    try:
        if sheet:
            if sheet not in wb.sheetnames:
                raise ValueError(f"{name}: khong co sheet {sheet!r}. Co: {wb.sheetnames}")
            ws = wb[sheet]
        else:
            ws = wb.worksheets[0]

        rows = ws.iter_rows(min_row=header_row, values_only=True)
        raw_header = next(rows, None)
        if not raw_header:
            return
        # Cot header rong -> _col<N> (Excel hay co cot thua ben phai)
        header = [str(h).strip() if h not in (None, "") else f"_col{i}"
                  for i, h in enumerate(raw_header, 1)]
        _check_header(header, name)
        n = 0
        for row in rows:
            if row is None or all(v in (None, "") for v in row):
                continue
            n += 1
            rec = {h: _cell(v) for h, v in zip(header, row)}
            if len(row) > len(header):
                extra = [_cell(v) for v in row[len(header):] if v not in (None, "")]
                if extra:
                    rec["_extra_values"] = extra
            yield n, rec
    finally:
        wb.close()


def _check_header(header, name: str) -> None:
    dup = sorted({c for c in header if header.count(c) > 1})
    if dup:
        # Map theo ten cot se ghi de cot trung ten -> mat du lieu im lang (IBM AML
        # co HAI cot "Account"). Bat buoc nguon/producer doi ten truoc.
        raise ValueError(f"{name}: header CSV co cot trung ten {dup} — "
                         f"se bi ghi de, mat du lieu. Doi ten cot o nguon.")


def _iter_csv(text, delimiter: str, name: str, repeated_header: bool):
    """CSV -> dict theo header. `repeated_header`: dòng nào có ô đầu trùng tên cột
    đầu của header là một HEADER MỚI, áp cho các dòng sau nó.

    File tỷ giá năm của CNB đổi danh sách tiền tệ giữa năm bằng cách chèn một
    dòng `Date|...` mới: đọc như CSV thường thì mọi dòng sau đó lệch cột mà không
    báo lỗi gì.
    """
    rows = csv.reader(text, delimiter=delimiter)
    header = next(rows, None)
    if not header:
        return
    _check_header(header, name)
    n = 0
    for row in rows:
        if not row:
            continue
        if repeated_header and row[0] == header[0]:
            header = row
            _check_header(header, name)
            continue
        n += 1
        rec = dict(zip(header, row))
        if len(row) > len(header):
            rec["_extra_values"] = row[len(header):]  # giu, khong vut (giong restkey)
        yield n, rec
