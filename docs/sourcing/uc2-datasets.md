# Nguồn cho UC2 — gợi ý sản phẩm ngân hàng: Bank Marketing, MBD, Santander

UC2 = dự đoán sản phẩm khách **sẽ mua tiếp** (next-best-product / propensity). Ba bộ dữ liệu công khai bổ sung cho
Berka (nguồn ví dụ chính của tầng gold/pre-train, xem `docs/TRANSFORMS.md`):

| Bộ | Quy mô | Nhãn UC2 | Đường vào | Entry | Trạng thái |
|---|---|---|---|---|---|
| **UCI Bank Marketing** | 45.211 + 41.188 dòng | `y`: khách đăng ký tiền gửi có kỳ hạn sau cuộc gọi telemarketing | `httpfile`, zip **lồng** zip | `bank_marketing_full`, `bank_marketing_additional` | Chạy được |
| **MBD-mini** (Sber AI Lab, arXiv 2409.17587) | `targets` 1.202.688 dòng, `client_split` 100.224 | `target_1..4`: mua 4 sản phẩm trong tháng sau ngày báo cáo | `httpfile`, tar.gz chứa parquet | `mbd_targets`, `mbd_client_split` | Chạy được |
| MBD-mini `detail` (trx/geo/dialog) | archive 1,97 GB | Đặc trưng hành vi | tar.gz 2 GB | `mbd_detail_*` | **Tắt**, xử lý sau |
| **Santander Product Recommendation** (Kaggle 2016) | ~13,6 triệu dòng × 48 cột | 24 cờ `ind_*_ult1`, sản phẩm thêm vào tháng sau | `s3file` trên **Spark**, từ `_inbox` | `santander_train` | **Tắt**, cần tải tay |

## Bank Marketing (UCI #222)

- URL: `https://archive.ics.uci.edu/static/public/222/bank+marketing.zip`. Đây là **một zip chứa hai zip con**,
  nên reader `httpfile` mới hỗ trợ `compression: zip` + `archive_members`, và zip lồng zip khai bằng dấu `!`:
  ```yaml
  compression: zip
  archive_members: ["bank-additional.zip!bank-additional/bank-additional-full.csv"]
  csv_delimiter: ";"
  ```
- Bỏ qua thư mục rác `__MACOSX/` của archive. CDC5: nguồn tĩnh (phát hành 2014), mỗi lần chụp lại toàn bộ.
- Tên cột có dấu chấm (`emp.var.rate`): tầng raw giữ nguyên, silver đổi sang snake_case.

## MBD-mini (Hugging Face `ai-lab/MBD-mini`)

- Không có API dòng (dataset viewer bị tắt), chỉ có file `*.tar.gz`. Mỗi archive là một thư mục parquet chia
  theo `fold=0..4` kiểu Hive. Reader đọc tar.gz **tuần tự theo luồng** (`tarfile` mode `r|gz`), từng member
  parquet đọc bằng `pyarrow` (profile `src-shell` cài sẵn `pyarrow==17.0.0`). Cột `fold` lấy từ tên thư mục.
- **Vì sao `mbd_targets` > 1 triệu dòng mà không chạy Spark:** gzip của archive phải giải nén tuần tự từ đầu tới
  cuối, nên Spark không chia song song được. Phần parse đo trên máy chỉ mất 16 giây cho 1,2 triệu dòng.
- `detail.tar.gz` (1,97 GB, khoảng 10 GB NDJSON khi ra raw) vượt sức Python Shell. Hướng xử lý sau: một job tải
  archive về `landing/_inbox/mbd/` và giải nén thành parquet, rồi entry `s3file` đọc bằng `src-spark`
  (`spark_main` đọc được parquet từ 11-2026, xem [spark-sourcing.md](spark-sourcing.md)). Còn thiếu job giải nén.

## Santander Product Recommendation (Kaggle)

Không tải tự động được: competition bắt đăng nhập và bấm *I Understand and Accept* luật thi trên web. Làm một lần:

```powershell
pip install kaggle                       # token: kaggle.com > Settings > API > Create New Token -> ~/.kaggle/kaggle.json
kaggle competitions download -c santander-product-recommendation -f train_ver2.csv.zip
Expand-Archive train_ver2.csv.zip .
aws s3 cp train_ver2.csv s3://fingraph-lakehouse-dev-439024108643/landing/_inbox/santander/train/ --profile fingraph
```

Rồi đặt `enabled: true` trong `santander_train.yml`, deploy và kích `src.santander_train`. Entry chạy trên
**`src-spark`**: `spark.read.csv` chia file 2,3 GB thành nhiều split đọc song song, mọi cột giữ dạng chuỗi
(raw không ép kiểu). File gốc mã hoá `latin-1`.

## Sourcing bằng Spark (`src-spark`) cho nguồn > 1 triệu dòng

| | Python Shell (`src-shell`) | Spark (`src-spark`) |
|---|---|---|
| Điểm vào | `sourcing.main` | `sourcing.spark_main` |
| Đọc | reader sinh từng lô | MỘT DataFrame, đọc song song |
| JDBC | chia khúc keyset tuần tự (`chunk_size`) | `jdbc.parallel`: N kết nối, mỗi kết nối một khoảng của `column` |
| Ghi raw | `RawWriter` gom lô → `ndjson.gz` | `df.write.json(gzip)`, mỗi partition một file |
| Mốc CDC1 | bản ghi cuối mỗi lô (+ PARTIAL) | `MAX(cột mốc)` của cả DataFrame (không có PARTIAL) |
| Giữ nguyên | RUN#/CKPT#, layout `landing/<dataset>/ingest_date=...`, `_src_*`, manifest → silver chain | |

```yaml
jdbc:
  parallel:
    column: trans_id        # cột số/ngày, nên có index
    num_partitions: 8       # số kết nối song song, lịch sự với DB nguồn
    # lower_bound / upper_bound: bỏ trống -> SELECT MIN/MAX trên đúng query đã lọc :watermark
  query: |
    SELECT ... FROM financial.trans WHERE trans_id > :watermark     # không LIMIT, không ORDER BY
runtime:
  engine: spark
  profile: src-spark
```

`berka_trans` (1.056.320 dòng) đã chuyển sang `src-spark`. Entry `berka_trans_spark_bench` có checkpoint và dataset
riêng (`berka/trans_spark_bench`, không có silver), dùng để đo Spark song song so với Python Shell (115 s).
