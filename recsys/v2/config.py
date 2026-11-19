"""Cấu hình nghiệp vụ của hệ gợi ý v2: sản phẩm, giá trị, chi phí liên hệ, luật đủ điều kiện, chính sách liên hệ.

Giá trị và chi phí là GIẢ ĐỊNH để tính chỉ số kinh doanh (ROI chiến dịch, giá trị kỳ vọng khi xếp hạng). Đổi ở đây là
đổi cả báo cáo lẫn thứ tự gợi ý; ghi rõ trong docs/RECSYS-V2.md mục 5 rằng đây là tham số, không phải số đo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class Product:
    code: str           # mã trong dữ liệu (cột nhãn)
    label_vi: str
    iri: str            # lớp ontology (fin:...)
    value: float        # giá trị kỳ vọng nếu khách nhận (đơn vị tiền tệ của miền, giả định)
    pitch: str = ""     # gợi ý cách nói chuyện cho RM


@dataclass(frozen=True)
class Domain:
    name: str
    products: List[Product]
    label_prefix: str                   # cột nhãn = prefix + code
    currency: str
    contact_cost: float                 # chi phí một lần liên hệ (gọi điện / tin nhắn), giả định
    min_p_show: float = 0.002           # dưới ngưỡng này không gợi ý (gợi ý tồi làm mất niềm tin)
    cooldown_days: Dict[str, int] = field(default_factory=lambda: {"dismiss": 60, "accept": 180, "impression": 0})
    max_contacts_per_month: int = 2     # chính sách liên hệ: tối đa số lần chào một khách / tháng

    @property
    def codes(self) -> List[str]:
        return [p.code for p in self.products]

    @property
    def labels(self) -> List[str]:
        return [self.label_prefix + p.code for p in self.products]

    def product(self, code: str) -> Product:
        return next(p for p in self.products if p.code == code)


MBD = Domain(
    name="mbd",
    products=[Product("1", "Sản phẩm ẩn danh 1", "fin:AnonymousProduct_1", 120.0),
              Product("2", "Sản phẩm ẩn danh 2", "fin:AnonymousProduct_2", 300.0),
              Product("3", "Sản phẩm ẩn danh 3", "fin:AnonymousProduct_3", 150.0),
              Product("4", "Sản phẩm ẩn danh 4", "fin:AnonymousProduct_4", 100.0)],
    label_prefix="t", currency="USD (giả định)", contact_cost=2.0, min_p_show=0.001)

# Berka: chỉ gợi ý vay và thẻ. household (SIPO) và insurance có nhãn lệch do giai đoạn mở tài khoản (docs/RECSYS.md
# mục 4.3): 70% "mua" household trong 3 tháng đầu là thao tác mở tài khoản, không phải quyết định mua.
BERKA = Domain(
    name="berka",
    products=[Product("loan", "Khoản vay", "fin:Loan", 6000.0,
                      "Khách có dòng tiền vào ổn định; giới thiệu hạn mức vay tiêu dùng, kỳ hạn linh hoạt."),
              Product("card_classic", "Thẻ classic", "fin:ClassicCard", 1500.0,
                      "Khách rút tiền mặt thường xuyên; thẻ giúp thanh toán không tiền mặt, miễn phí năm đầu."),
              Product("card_junior", "Thẻ junior", "fin:JuniorCard", 800.0,
                      "Thẻ phụ cho con: kiểm soát chi tiêu, gắn với tài khoản của khách."),
              Product("card_gold", "Thẻ vàng", "fin:GoldCard", 4000.0,
                      "Số dư cao và ổn định; quyền lợi hạng vàng, hạn mức lớn.")],
    label_prefix="y_", currency="CZK", contact_cost=60.0, min_p_show=0.003)

DOMAINS = {"mbd": MBD, "berka": BERKA}

# Luật đủ điều kiện Berka (áp SAU mô hình, trước khi xếp hạng): (mã sản phẩm, mô tả, hàm trên dòng đặc trưng)
GOLD_MIN_AVG_BALANCE_3M = 50000.0
BERKA_RULES = [
    ("card_gold", f"số dư bình quân 3 tháng >= {GOLD_MIN_AVG_BALANCE_3M:,.0f} CZK",
     lambda r: (r.get("avg_balance_3m") or 0) >= GOLD_MIN_AVG_BALANCE_3M),
    ("loan", "không thấu chi trong 3 tháng gần nhất",
     lambda r: (r.get("overdraft_3m") or 0) == 0),
    ("card_junior", "khách từ 25 đến 60 tuổi (giả định có con nhỏ)",
     lambda r: 25 <= (r.get("age") or 0) <= 60),
]
