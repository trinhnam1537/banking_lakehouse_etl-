"""Hệ gợi ý sản phẩm (UC2): đặc trưng qua Athena dưới persona, LightGBM theo sản phẩm, đánh giá theo khách và sản phẩm.

lightgbm PHẢI được import trước pyarrow/pandas: trên Windows, nạp lightgbm SAU pyarrow làm Dataset.construct() crash
"access violation reading 0x0" (xung đột DLL OpenMP, đo 11-18 với lightgbm 4.7 + pyarrow 17). Import ở đây để mọi
module trong package đều theo đúng thứ tự.
"""

try:
    import lightgbm  # noqa: F401  (thu tu import co chu dich, xem tren)
except ImportError:  # portal: venv rieng khong co lightgbm, chi dung recsys.v2.online / config
    pass
