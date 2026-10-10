"""Script Glue (Spark 5.0) của mọi job silver.

Framework đóng gói thành `silver.zip` (gồm cả `sourcing/` vì silver dùng lại
state store) qua `--extra-py-files`. Glue ETL KHÁC Python Shell ở chỗ nó tự
thêm .zip vào sys.path — nhưng giữ đoạn tự tìm dưới đây để hai loại job cư xử
giống nhau, khỏi phải nhớ loại nào cần loại nào không.
"""

import glob
import os
import sys


def _add_framework_zip() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    for path in glob.glob("/tmp/glue-python-libs-*/silver.zip") + glob.glob("/tmp/*/silver.zip") + [
        os.path.join(here, "silver.zip"), os.path.join(os.getcwd(), "silver.zip")
    ]:
        if os.path.isfile(path) and path not in sys.path:
            sys.path.insert(0, path)
            print(f"Framework: {path}")
            return


_add_framework_zip()

from silver.main import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
