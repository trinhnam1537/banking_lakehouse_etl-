# Tiến độ ingest các nguồn (nguồn → raw → silver)

Bảng theo dõi chung cho mọi nguồn. Mỗi lần một nguồn đổi trạng thái (chạy được raw, có silver,
bật lịch), sửa bảng ở mục 2 và ghi một dòng vào **nhật ký** ở mục 7. Chi tiết kỹ thuật từng
nguồn nằm trong tài liệu riêng (xem [README.md](README.md)).

Cập nhật lần cuối: **02-11-2026**.

---

## 1. Quy tắc dung lượng: dùng bản lớn nhưng không quá 5 GB

Đồ án cần dữ liệu **đủ lớn** để việc chia song song, CDC, file size có ý nghĩa, nhưng phải chạy
được trên ngân sách dev. Quy tắc:

| Quy tắc | Nội dung |
|---|---|
| **R1. Ngân sách 5 GB / nguồn** | Tổng dung lượng **file thật ghi trên S3** (`landing/<dataset>/`, `ndjson.gz` sau nén) của một nguồn, cho một lần nạp đầy đủ, không quá **5 GB** |
| **R2. Nguồn tăng trưởng tính theo cửa sổ giữ** | Nguồn chạy liên tục (Kafka, file hằng ngày, CDC5 chụp lại) tính dung lượng **trong thời gian giữ** (retention) của raw, không tính vô hạn. Phải có lifecycle S3 để không vượt R1 |
| **R3. Ưu tiên bản lớn nhất vừa ngân sách** | Bộ có nhiều bản (Small/Medium/Large, mini/full) thì chọn bản lớn nhất mà ước tính raw vẫn ≤ 5 GB |
| **R4. Vượt ngân sách thì cắt có chủ đích** | Không lấy mẫu ngẫu nhiên từng dòng. Cắt theo **thực thể** (giữ trọn khách/tài khoản) hoặc theo **cửa sổ thời gian**, để quan hệ và chuỗi giao dịch còn nguyên. Cách làm bằng Spark ở mục 5 |
| **R5. > 1 triệu dòng thì chạy `src-spark`** | Python Shell (1 GB RAM, đọc tuần tự) chỉ dành cho nguồn nhỏ hoặc file nén không chia được |

**Quy đổi ước tính** (đo lại sau lần chạy đầu, ghi số thật vào mục 2):
NDJSON lặp lại tên cột ở mỗi dòng nên **to gấp 2–4 lần CSV** khi chưa nén. Sau gzip, raw thường
còn **khoảng 1/3 đến 1/2 dung lượng CSV gốc**. Bảng nhiều cột tên dài (Santander, Home Credit)
nén tốt hơn.

---

## 2. Bảng tiến độ

Ký hiệu: ✅ xong · 🟡 đang làm / chạy được một phần · ⏸ đã viết entry nhưng đang tắt ·
❌ chưa làm · — không áp dụng.

### 2.1 Hiện trạng

| # | Nguồn | Hành vi nguồn | Cách kéo (reader · CDC · lịch) | Quy mô hiện tại | Tăng trưởng | Raw | Silver |
|---|---|---|---|---|---|---|---|
| 1 | **Berka** (8 bảng) | Bộ tĩnh lịch sử (ngân hàng Séc 1993–98) trên Postgres | `jdbc` · CDC1 chia khúc 50 nghìn dòng · `berka_trans` chạy Spark 8 partition | 1,08 triệu dòng, ~80 MB (trans 1,06 triệu) | 0 (tĩnh) | ✅ | ✅ 8 bảng |
| 2 | **Core banking** (8 bảng) | OLTP mô phỏng trên Postgres (EC2), simulator chạy mỗi ngày một ngày nghiệp vụ | `jdbc` (chỉ SSL) · CDC1 (district CDC5) · 18:00 UTC hằng ngày | 5,4 nghìn khách · 4,5 nghìn tài khoản · ~1,06 triệu giao dịch | +60–70 nghìn giao dịch/tháng | ✅ | ✅ 8 bảng |
| 3 | **Bank Marketing** (2) | File tĩnh UCI (zip lồng zip) | `httpfile` · CDC5 · chạy một lần | 45.211 + 41.188 dòng, < 10 MB | 0 | ✅ | ✅ `silver_bank_marketing` 2 bảng |
| 4 | **MBD-mini** (`client_split`, `targets`) | File tĩnh HuggingFace, tar.gz chứa Parquet | `httpfile` · CDC5 · chạy một lần | targets 1,2 triệu · client_split 100 nghìn | 0 | ✅ | ✅ `silver_mbd` 2 bảng |
| 5 | **OneDrive** (`uploads`) | Người dùng thả file thủ công, không đều | `onedrive` (Graph API) · CDC1 theo `lastModified\|path` · 01:00 UTC hằng ngày | Vài file CSV, KB–MB. Tối đa 64 MB/file | Tuỳ người upload (~100 MB/tháng) | ✅ | ❌ |
| 6 | **Thẻ, Kafka** (`cards_authorizations`) | Luồng sự kiện liên tục, producer `--rate 2` | `kafka` (profile edge) · CDC1 theo offset từng partition · micro-batch 5 phút | Vài chục nghìn tin | ~2 tin/s ≈ 170 nghìn/ngày ≈ 5 triệu/tháng. Topic giữ 7 ngày | ✅ | ✅ `silver_cards.card_authorization` (149.874 dòng tới 01-11) |
| 7 | **Nhãn chargeback thẻ** | File nhãn đến trễ 20 ngày, producer tự ghi vào `_inbox` | `s3file` · CDC1 theo khoá S3 · 01:00 UTC hằng ngày | 0 (file đầu tiên dự kiến ~20-11-2026) | ~0,15% số giao dịch | ❌ | ❌ |
| 8 | **FX** (CNB, Vietcombank) | Tỷ giá công bố mỗi ngày làm việc | `httpfile` (CNB) · `api` (VCB) · 15:00 / 02:00 UTC | CNB 3.990 dòng. VCB 400 ngày × 20 = 8.000 dòng (đang kéo dần từ 06/2020) | Vài chục dòng/ngày | ✅ | ✅ `silver_fx` 2 bảng |
| 9 | **OpenSanctions** (3) | File bulk, cập nhật nhiều lần mỗi ngày | `httpfile` · CDC5 hằng tuần (Chủ nhật) + delta 6 giờ | Bộ `sanctions`: 72 nghìn đối tượng, targets 74 MB, entities ~370 MB | Chụp lại toàn bộ mỗi tuần, đo được ~75 MB/tuần (targets + entities) | ✅ | ✅ `silver_opensanctions` 3 bảng |
| 10 | **IBM AML** (5) | Lịch sử bằng file lô + luồng chuyển tiền + file tài khoản/pattern + nhãn trễ 14 ngày | `aml_transfers_history` Spark M · cdc1 khoá S3 · `aml_labels`, `aml_accounts` Spark S · `aml_transfers` qua Kafka (đang tắt) | **HI-Medium** 16 ngày: 31.891.251 giao dịch, 2.087.786 tài khoản, 2.756 vụ (22.743 dòng). Bỏ 12 ngày đuôi (7 nghìn dòng, ~100% rửa tiền) | Replay ~6 giao dịch/s | ✅ | ✅ `silver_aml` 4 bảng |
| 11 | **PaySim** (2) | File EOD mỗi ngày + nhãn trễ | `s3file` CSV gzip · CDC1 theo khoá S3 · 00:30 UTC | 6.362.620 giao dịch / 31 ngày, 0,13% gian lận | ~15 MB nén/ngày khi phát lại | ✅ | ✅ `silver_paysim` 2 bảng |
| 12 | **Lending / Home Credit** (8) | OLTP mô phỏng trên Postgres + file nhãn vỡ nợ | `jdbc` (`src-vpc`, đang tắt) + `s3file` cho nhãn | Seed 5.000 hồ sơ ≈ 600 nghìn dòng. **Chặn:** Kaggle đòi chấp nhận luật thi | Trả góp và POS theo kỳ 30 ngày | ❌ | ❌ |
| 13 | **Santander** | File tĩnh Kaggle, phải tải tay | `s3file` trên Spark · chạy một lần | 13,6 triệu dòng × 48 cột, 2,3 GB CSV. **Chặn:** Kaggle đòi chấp nhận luật thi | 0 | ⏸ | ❌ |
| 14 | **MBD-mini chi tiết** (trx, geo, dialog) | File tĩnh, archive 1,97 GB | `httpfile` · đang tắt | ~10 GB NDJSON chưa nén khi ra raw | 0 | ⏸ | ❌ |

### 2.2 Bản dữ liệu chọn dùng và ngân sách raw

Cột "Raw ước tính" là dung lượng `ndjson.gz` trên S3 cho **một lần nạp đầy đủ** (hoặc cho cửa sổ
giữ, với nguồn tăng trưởng). "Raw đo được" điền sau lần chạy thật.

| # | Nguồn | Bản đang dùng | **Bản mục tiêu** | Raw ước tính | Raw đo được | Trong 5 GB? | Ghi chú |
|---|---|---|---|---|---|---|---|
| 1 | Berka | Toàn bộ | Toàn bộ (không có bản lớn hơn) | ~30 MB | | ✅ | |
| 2 | Core banking | Seed Berka + simulator | Như cũ | ~50 MB + ~5 MB/tháng | | ✅ | |
| 3 | Bank Marketing | Toàn bộ | Toàn bộ | < 5 MB | 1,3 MB | ✅ | Nhỏ, giữ cho UC2 |
| 4 | MBD-mini (targets, split) | Toàn bộ | Toàn bộ | ~40 MB | 19,7 MB | ✅ | |
| 5 | OneDrive | — | — | ~30 MB/tháng | | ✅ | Giữ 12 tháng |
| 6 | Thẻ Kafka | `--rate 2` | `--rate 2`, giữ raw **6 tháng** | ~400 MB/tháng → ~2,4 GB | 37 MB / 327 file (2 ngày) | ✅ | Cần lifecycle S3 + gộp file nhỏ (mục 4) |
| 7 | Chargeback | — | — | < 10 MB/tháng | | ✅ | |
| 8 | FX | Toàn bộ lịch sử | Toàn bộ | < 5 MB | 0,5 MB | ✅ | |
| 9 | OpenSanctions | Bộ `sanctions` | Bộ `sanctions`, giữ **4 bản chụp** tuần | ~150 MB/tuần gz → ~600 MB | 74 MB/bản chụp + 22 MB delta | ✅ nếu có lifecycle | Không có lifecycle thì vượt 5 GB sau ~30 tuần. Bộ `default` (2,6 GB/bản chụp) **không dùng** |
| 10 | IBM AML | **HI-Medium** 16 ngày | **HI-Medium** (~32 triệu giao dịch, ~2,9 GB CSV) | ~1,2–1,5 GB | **2,67 GB** (transfers 1.977 MB, labels 626 MB, accounts 69 MB) | ✅ | Bản Large (~180 triệu, ~16 GB CSV) vượt ngân sách: cắt theo mục 5.1 nếu cần |
| 11 | PaySim | **Toàn bộ 6,36 triệu** | **Toàn bộ 6,36 triệu** | ~200–250 MB | 324 MB (eod 288, labels 37) | ✅ | |
| 12 | Home Credit | Seed 5.000 hồ sơ | **Toàn bộ 7 bảng** (~58 triệu dòng, 2,6 GB CSV) | ~1–1,5 GB | | ✅ | Lịch sử nạp qua `s3file`/Spark, **không** đổ 58 triệu dòng vào Postgres (mục 3) |
| 13 | Santander | — | **Toàn bộ** 13,6 triệu dòng | ~1–1,5 GB | | ✅ | |
| 14 | MBD-mini chi tiết | — | **trx đầy đủ**, geo và dialog lấy theo `fold` | ~2,5–3,5 GB | | 🟡 sát ngưỡng | Cắt theo `fold` (mục 5.1). MBD đầy đủ (~1 tỷ giao dịch) **không dùng** |

Tổng mục tiêu: khoảng **9–12 GB** raw cho cả lakehouse, không nguồn nào quá 5 GB.

---

## 3. Việc tiếp theo, theo thứ tự ưu tiên

| Ưu tiên | Việc | Nguồn | Điều kiện xong |
|---|---|---|---|
| P0 | Thêm lifecycle S3 cho `landing/cards/`, `landing/opensanctions/` (mục 4) | 6, 9 | Rule trong repo hạ tầng, ghi vào `docs/SETUP-LOG.md` |
| P1 | **Chấp nhận luật thi Kaggle** (Home Credit, Santander) — việc của chủ tài khoản Kaggle | 12, 13 | `kaggle competitions download` chạy được |
| P1 | Santander: tải, đặt CSV **không nén** vào `_inbox`, bật `santander_train`, viết silver | 13 | Raw ✅, silver ✅ |
| P2 | Home Credit đầy đủ: 7 file CSV lên `_inbox/lending/history/`, entry `s3file` Spark cho lịch sử. JDBC `lending_*` chỉ kéo phần simulator sinh thêm | 12 | Raw ✅ 8 bảng |
| P2 | MBD chi tiết: job giải nén `detail.tar.gz` thành parquet trong `_inbox/mbd/`, rồi entry `s3file` Spark | 14 | Raw ✅, ≤ 5 GB |
| P2 | Merge nhánh `feat/aml-paysim-silver` vào `main` (đang deploy tay; CI từ `main` sẽ đưa con trỏ về code cũ) | | PR merge |
| P3 | Silver cho OneDrive (lược đồ phụ thuộc file), chargeback thẻ (chờ file đầu ~20-11) | 5, 7 | Entry trong `silver_etl/entries/<nguồn>/` |
| P3 | Silver cho Home Credit, Santander | 12, 13 | Như trên |

---

## 4. Giữ dung lượng nguồn tăng trưởng trong ngân sách

| Nguồn | Cơ chế | Đề xuất |
|---|---|---|
| Thẻ Kafka | Mỗi micro-batch 5 phút một file nhỏ → rất nhiều file | Tăng ngưỡng cuộn file của writer cho entry này, hoặc chạy compaction hằng ngày (Spark đọc `ingest_date=D`, ghi lại 1–4 file). Lifecycle: chuyển `landing/cards/authorizations/` sang Infrequent Access sau 30 ngày, **xoá sau 180 ngày** (silver đã giữ bản sạch) |
| OpenSanctions | CDC5 chụp lại toàn bộ mỗi tuần | Lifecycle **xoá sau 35 ngày** cho `landing/opensanctions/entities/` và `targets/` (giữ ~4 bản chụp). Lịch sử thay đổi lấy từ `opensanctions_delta`, không cần giữ mọi bản chụp |
| Core banking, PaySim, chargeback, FX | CDC1, mỗi lần chỉ thêm phần mới | Tăng chậm, không cần lifecycle trong thời gian đồ án |
| OneDrive | Phụ thuộc người dùng | Giới hạn 64 MB/file đã có. Theo dõi tổng mỗi tháng ở mục 2.2 |

---

## 5. Khi dữ liệu lớn hơn 5 GB: cách xử lý tối ưu bằng Spark

Áp dụng cho IBM AML Large, MBD đầy đủ, OpenSanctions `default`, hay bất kỳ nguồn nào ước tính
raw vượt 5 GB. Thứ tự: **cắt trước** (5.1), rồi **đọc song song đúng cách** (5.2), rồi **ghi gọn**
(5.3). Chỉ khi cắt không được mới tính đến đổi định dạng raw (5.4).

### 5.1 Cắt dữ liệu ngay khi đọc, giữ nguyên quan hệ

Cắt trong cùng job Spark, trước bước ghi, nên phần bị bỏ **không bao giờ xuống S3**.

| Cách cắt | Khi nào dùng | Ví dụ |
|---|---|---|
| **Theo thực thể (hash)** | Dữ liệu dạng đồ thị/chuỗi: phải giữ trọn mọi giao dịch của một tài khoản | IBM AML Large: giữ giao dịch có **cả hai đầu** thuộc tập tài khoản đã chọn |
| **Theo cửa sổ thời gian** | Dữ liệu có trục thời gian, muốn giữ đủ mật độ thật | IBM AML Large: lấy 10 ngày đầu thay vì toàn bộ |
| **Theo phân vùng có sẵn** | Bộ đã chia sẵn (fold, năm, tháng) | MBD: lấy `fold IN (0, 1)` |
| **Bỏ cột** | Cột không dùng ở silver/gold | Cột trùng, cột mô tả dài |

Lấy mẫu theo hash là **tất định** (chạy lại ra đúng tập cũ), khác `sample()` ngẫu nhiên:

```python
from pyspark.sql import functions as F

KEEP = 20   # giữ 20% tài khoản -> ~20% dữ liệu (ước tính trước rồi chỉnh)
acc = F.col("from_account")
df = df.where((F.xxhash64(acc) % 100 + 100) % 100 < KEEP)

# Với giao dịch: giữ khi CẢ HAI đầu thuộc tập đã chọn, để đồ thị không bị cụt cạnh
keep = lambda c: (F.xxhash64(F.col(c)) % 100 + 100) % 100 < KEEP
df = df.where(keep("from_account") & keep("to_account"))
```

Bảng phụ (accounts, labels, patterns) lọc bằng **cùng hàm hash** để khớp với bảng giao dịch.
Ghi tham số cắt (`KEEP`, cửa sổ ngày, fold) vào entry YAML và vào manifest, để biết raw là tập con.

### 5.2 Đọc song song đúng cách

| Vấn đề | Cách làm |
|---|---|
| **File gzip không chia được**: một file `.csv.gz` 3 GB chỉ được **một** task đọc | Đặt CSV **không nén** vào `_inbox` (S3 rẻ, file nguồn chỉ giữ tạm), hoặc chia trước thành nhiều file `.gz` ~128–256 MB. Spark chia file không nén theo `spark.sql.files.maxPartitionBytes` (mặc định 128 MB) |
| **Archive tar.gz** (MBD) | Spark không đọc trực tiếp được. Một bước giải nén riêng ra parquet trong `_inbox`, sau đó Spark đọc parquet song song |
| **`inferSchema` đọc file hai lần** | Raw giữ mọi cột dạng chuỗi: đọc với schema khai sẵn hoặc `inferSchema=false` |
| **JDBC bảng lớn** | `jdbc.parallel`: cột chia có index, `num_partitions` 8–16, `fetchsize` 10.000. Không vượt quá số kết nối DB nguồn chịu được |
| **Nạp lịch sử lớn qua Postgres** (Home Credit đầy đủ) | Không đổ vào DB nguồn rồi đọc lại. Lịch sử đi `s3file` Spark trực tiếp từ CSV. JDBC chỉ kéo phần tăng thêm |

### 5.3 Ghi gọn: số file và kích thước file

| Vấn đề | Cách làm |
|---|---|
| Quá nhiều file nhỏ (mỗi partition một file) | `repartition(n)` trước khi ghi, với `n ≈ dung lượng ước tính sau nén / 128 MB`. Không dùng `coalesce(1)`: dồn mọi thứ về một task |
| File quá to | `option("maxRecordsPerFile", ...)` để chặn trên |
| Dữ liệu chuỗi thời gian lớn | `partitionBy` theo ngày sự kiện **bên dưới** `ingest_date=`, để silver và replay đọc từng ngày |
| Lỗi giữa chừng để lại file rác | Giữ hợp đồng hiện có: dữ liệu lên S3 → tiến mốc → manifest. Silver chỉ đọc file có trong manifest |
| Tối ưu chung | Bật AQE (`spark.sql.adaptive.enabled=true`, mặc định trên Glue 4), Glue auto scaling, worker G.1X. Tăng worker trước khi tăng loại máy |

### 5.4 Đổi định dạng raw (chỉ khi đã cắt mà vẫn vượt)

Với bảng **rộng** (nhiều cột), Parquet + zstd thường nhỏ hơn `ndjson.gz` nhiều lần vì không lặp
tên cột và nén theo cột. Đổi định dạng là thay đổi hợp đồng raw: chỉ áp dụng cho entry
`src-spark`, khai trong entry (ví dụ `raw_format: parquet`), ghi vào manifest, và silver phải đọc
theo định dạng trong manifest. Việc này chưa có trong code. Làm khi có nguồn thực sự cần.

### 5.5 Áp dụng cho từng bộ vượt ngân sách

| Bộ | Kích thước gốc | Cách làm đề xuất | Raw dự kiến |
|---|---|---|---|
| IBM AML **Large** (~180 triệu, ~16 GB CSV) | Vượt | Dùng **HI-Medium** thay thế. Nếu bắt buộc Large: hash theo tài khoản giữ ~25%, cả hai đầu giao dịch | ~3–4 GB |
| **MBD chi tiết** (~10 GB NDJSON chưa nén) | Sát | Giải nén ra parquet, giữ `trx` đầy đủ, `geo` và `dialog` lấy `fold IN (0,1)` | ~2,5–3,5 GB |
| **MBD đầy đủ** (~1 tỷ giao dịch) | Vượt xa | Không dùng. Nếu cần: hash theo `client_id` giữ ~5% | ~4–5 GB |
| **OpenSanctions `default`** (2,6 GB/bản chụp) | Vượt khi tích luỹ | Không dùng. Bộ `sanctions` đủ cho sàng lọc AML | — |
| **Home Credit đầy đủ** (58 triệu dòng) | Trong ngân sách | Nạp lịch sử thẳng từ CSV bằng Spark, `bureau_balance` và `installments_payments` chia nhiều file | ~1–1,5 GB |

---

## 6. Checklist khi đưa một nguồn mới lên

Một nguồn được tính **Raw ✅** khi qua đủ các bước sau:

1. Entry YAML có trong `sourcing_etl/sources/<nguồn>/`, pytest xanh.
2. Chạy thật trên stack local (`python -m local.run_entry <entry>`).
3. Có ước tính raw ở mục 2.2, không quá 5 GB. Vượt thì có phương án mục 5.
4. Chạy trên AWS, có manifest trong `landing/_manifests/`. Ghi số dòng, số file, MB thật vào 2.2.
5. Chạy lại lần hai: CDC1 chỉ kéo phần mới, CDC5 thay bản chụp đúng cách.
6. Hạ tầng mới (secret, lifecycle, profile) ghi vào `docs/SETUP-LOG.md` của repo hạ tầng.
7. Bật `schedule.enabled` sau khi bước 4–5 thành công.

**Silver ✅** khi có entry trong `silver_etl/entries/<nguồn>/` và chạy được
`python -m local.run_flow <nguồn>`.

---

## 7. Nhật ký

| Ngày | Nguồn | Thay đổi |
|---|---|---|
| 13-10-2026 | Mọi entry | Chạy end-to-end trên stack local |
| 14-10-2026 | Core banking | Hạ tầng AWS: Postgres public chỉ SSL, lịch 18:00 UTC |
| 31-10-2026 | Thẻ | Kafka giai đoạn 1: producer + consumer edge trên máy nguồn, raw ✅ |
| 01-11-2026 | Tất cả | Tạo tài liệu này, chốt ngân sách 5 GB/nguồn và bản mục tiêu cho từng bộ |
| 01-11-2026 | Framework Spark | Cỡ máy S/M/L/XL theo entry, `jdbc.parallel` (`rows_per_partition`, `predicates`), s3file cdc1 + parquet trên Spark, lấy mẫu tất định. Xem [spark-sourcing.md](spark-sourcing.md) |
| 01-11-2026 | IBM AML | Entry `aml_transfers_history` (Spark M), `replay history`; `aml_labels`, `aml_accounts` chuyển sang Spark S |
| 02-11-2026 | IBM AML, PaySim, FX, OpenSanctions | Raw ✅ trên AWS. AML HI-Medium 16 ngày: sourcing transfers ~4 phút (5 × G.1X), silver transfer 31,9 triệu dòng 7,6 phút |
| 02-11-2026 | 7 nguồn | Silver ✅ 16 bảng: `silver_aml` (4), `silver_paysim` (2), `silver_bank_marketing` (2), `silver_mbd` (2), `silver_cards` (1), `silver_fx` (2), `silver_opensanctions` (3). Mọi bảng rows_in = rows_out, 0 trùng khoá, 0 lỗi ép kiểu. Grant Lake Formation đã apply |
