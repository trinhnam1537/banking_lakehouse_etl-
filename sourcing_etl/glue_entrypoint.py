"""Script mà Glue thực sự chạy.

Glue Python Shell chỉ nhận MỘT file .py làm `script_location`. Toàn bộ
framework được đóng gói thành `sourcing.zip` và nạp qua `--extra-py-files`,
nên file này chỉ cần gọi vào đó.

Giữ nó mỏng có chủ ý: mọi logic nằm trong package đã qua kiểm thử, còn đây
chỉ là chỗ bắt tay giữa Glue và framework.

VÌ SAO PHẢI TỰ THÊM ZIP VÀO sys.path: Python Shell chỉ *cài* `.whl`/`.egg`
trong `--extra-py-files`. File `.zip` được tải về `/tmp/glue-python-libs-*/`
("Considering file without prefix as a python extra file") nhưng KHÔNG nằm
trên sys.path, nên `import sourcing` chết với ModuleNotFoundError. Python
import thẳng từ zip được (zipimport), chỉ cần chỉ đường cho nó.
"""

import glob
import os
import sys


def _add_framework_zip() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = glob.glob("/tmp/glue-python-libs-*/sourcing.zip") + [
        os.path.join(here, "sourcing.zip"),
        os.path.join(os.getcwd(), "sourcing.zip"),
    ]
    for path in candidates:
        if os.path.isfile(path) and path not in sys.path:
            sys.path.insert(0, path)
            print(f"Framework: {path}")
            return


_add_framework_zip()

from sourcing.main import main  # noqa: E402  (phai sau khi them zip)

if __name__ == "__main__":
    sys.exit(main())
