"""Kiểm grounding SAU KHI SINH (bước O11): không dùng LLM, chỉ đối chiếu id.

Hợp đồng với model (ghi trong system prompt): mọi sự kiện về dữ liệu phải trích id dạng
[uid:<32 hex>] hoặc [triple:<32 hex>]. Ở đây kiểm:
  1. Mọi id được trích có xuất hiện trong kết quả tool CỦA PHIÊN NÀY -> không bịa id.
  2. Câu trả lời có nói về dữ liệu (có số / có tên thực thể) thì phải có ít nhất một trích dẫn.
Vi phạm -> không trả câu trả lời đó cho người dùng; vòng lặp yêu cầu model sửa (tối đa 1 lần),
sau đó trả lời "không đủ căn cứ".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Set

CITE = re.compile(r"\[(uid|triple):([0-9a-f]{32})\]")
LOOKS_FACTUAL = re.compile(r"\d|Khách |Tài khoản |Doanh nghiệp ")


@dataclass
class Report:
    ok: bool
    cited: List[str] = field(default_factory=list)
    unknown: List[str] = field(default_factory=list)
    reason: str = ""


def check(answer: str, seen_ids: Set[str]) -> Report:
    cited = [m.group(2) for m in CITE.finditer(answer)]
    unknown = sorted({c for c in cited if c not in seen_ids})
    if unknown:
        return Report(False, cited, unknown, f"{len(unknown)} id khong co trong ket qua tool cua phien")
    if not cited and seen_ids and LOOKS_FACTUAL.search(answer):
        return Report(False, cited, [], "cau tra loi neu du lieu nhung khong trich dan id nao")
    return Report(True, cited)
