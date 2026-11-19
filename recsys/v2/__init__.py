"""Hệ gợi ý sản phẩm v2 (docs/RECSYS-V2.md): mô hình champion + hiệu chỉnh xác suất, đánh giá offline/online đầy đủ,
tầng phục vụ có luật nghiệp vụ, giải thích, nhật ký sự kiện và xuất ra lake cho portal / agent.

    python -m recsys.v2.run train     --domain mbd|berka     # huấn luyện + đăng ký mô hình (registry/)
    python -m recsys.v2.run evaluate  --domain mbd|berka     # đánh giá offline đầy đủ -> reports/
    python -m recsys.v2.run online    --domain mbd           # mô phỏng A/B, interleaving, bandit -> reports/
    python -m recsys.v2.run score     --domain berka         # chấm điểm tháng mới nhất + luật nghiệp vụ -> parquet
    python -m recsys.v2.run publish   --domain berka         # đưa bảng gợi ý lên track_a_gold (persona analyst)
"""

import lightgbm  # noqa: F401  (phải trước pandas/pyarrow trên Windows, xem recsys/__init__.py)
