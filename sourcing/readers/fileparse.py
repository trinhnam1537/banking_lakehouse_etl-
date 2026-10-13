"""Đọc file theo LUỒNG: CSV / NDJSON / JSON, có hoặc không gzip.

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
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    """Sinh (số thứ tự, bản ghi). `stream` là luồng byte bất kỳ có `.read(n)`."""
    binary = io.BufferedReader(_Raw(stream), buffer_size=1024 * 1024)
    if gzip_on:
        binary = gzip.GzipFile(fileobj=binary, mode="rb")

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
