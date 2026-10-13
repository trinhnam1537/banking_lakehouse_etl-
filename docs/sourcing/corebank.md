# Core banking (Postgres `corebank`)

Hệ thống core banking giả lập của ngân hàng FinGraph. Lịch sử là dữ liệu **thật đã ẩn danh** của
Berka (ngân hàng Séc 1993–1998, dời +24 năm). Từ `SIM_EPOCH` (2023-01-01), simulator sinh giao
dịch mới mỗi ngày nghiệp vụ. Đây là nguồn trung tâm: khách hàng ở đây là chủ thẻ của luồng thẻ
và là người vay của hệ thống cho vay.

| | |
|---|---|
| Loại | OLTP quan hệ, nguồn CDC |
| Hệ quản trị | PostgreSQL 16 |
| Database / schema | `corebank` / `core` |
| Quy mô (bản đủ) | 77 quận · 5.369 khách · 4.500 tài khoản · 892 thẻ · 682 khoản vay · 6.471 lệnh định kỳ · ~1,06 triệu giao dịch lịch sử, cộng ~60–70 nghìn giao dịch mỗi tháng simulator |
| Entry | 8 entry `corebank_*` trong `sourcing_etl/sources/corebank/` |
| Dataset raw | `landing/corebank/<bảng>/` |
| Profile | `src-shell` (Glue ngoài VPC, có `psycopg2`). Bật, lịch 18:00 UTC. Hạ tầng: [README mục 7.1](README.md#71-core-banking-trên-aws-đã-viết-chờ-apply-14-10-2026) |
| Code | `simulators/corebank/{schema.sql, seed.py, simulate.py}` |

Khác với nguồn [berka.md](berka.md): entry `berka_*` đọc **thẳng server CTU**, là dữ liệu tĩnh
từ 1999. Còn `corebank_*` đọc hệ thống **của mình**: có cột audit, có dữ liệu mới mỗi ngày, có PII
giả lập, nên CDC1 có ý nghĩa thật.

---

## 1. Kết nối và credential

| Môi trường | Host:port | User ETL | Secret |
|---|---|---|---|
| Local (`local/docker-compose.yml`) | `localhost:5433` | `etl_reader` / `etl_reader` | `fingraph/src/corebank_pg` trong moto (tạo bởi `local.bootstrap`) |
| AWS | **IP public** của EC2 "hệ thống nguồn", cổng 5432, **chỉ SSL** | `etl_reader`, mật khẩu 48 hex do máy tự sinh | `fingraph/src/corebank_pg` (Terraform tạo vỏ, máy nguồn ghi giá trị) |

```json
{"host": "<IP public>", "port": 5432, "username": "etl_reader", "password": "<48 hex>",
 "dbname": "corebank", "sslmode": "require"}
```

- `etl_reader` chỉ có quyền `USAGE` trên schema `core` và `SELECT` trên mọi bảng, kể cả bảng tạo
  sau này (`ALTER DEFAULT PRIVILEGES`). Role được tạo **trước** `schema.sql`: local bằng
  `local/initdb/corebank-roles.sql`, AWS bằng `corebank-init.sh` của repo hạ tầng.
  `schema.sql` không chứa mật khẩu nào.
- Reader còn mở phiên `READ ONLY`. Có hai lớp chặn ghi.
- `sslmode` trong secret được reader truyền cho psycopg2. Trên AWS là `require`, và Postgres từ chối
  mọi kết nối TCP không SSL (đã thử: "no pg_hba.conf entry"). Đã kiểm chứng TLSv1.3.
- Chủ DB chỉ dành cho simulator (local `corebank`/`corebank`, AWS secret `fingraph/sim/corebank_owner`,
  Glue không đọc được). ETL không bao giờ dùng user này.

---

## 2. Schema

Sơ đồ quan hệ (giống Berka, đổi tên cột cho đọc được):

```
district (77) ◀── client ──┐
     ▲                     │ disp: OWNER | DISPONENT      (mỗi tài khoản đúng 1 OWNER)
     └──────── account ◀───┘
                 ▲  ▲  ▲
        trans ───┘  │  └─── standing_order
                   loan (≤ 1 / tài khoản)
        card ──▶ disp          (thẻ gắn với QUYỀN trên tài khoản)
```

Quy ước chung, và lý do tầng sourcing dựa vào chúng:

| Quy ước | Bảng | Dùng cho |
|---|---|---|
| `created_at`, `updated_at timestamptz`, trigger `touch_updated_at` khi UPDATE, có index | mọi bảng trừ `trans` | Mốc CDC1 |
| Chỉ INSERT, khoá tăng dần (`core.trans_id_seq` bắt đầu từ 4.000.000, sau id lớn nhất của Berka là 3.682.987) | `trans` | CDC1 chia khúc theo `trans_id` |
| Xoá mềm (`status`), không DELETE | `client`, `account`, `card` | CDC1 thấy được bản ghi "bị xoá" |
| Tiền là `numeric`, raw ghi dạng **chuỗi** | mọi cột tiền | Không mất chính xác ở raw |

### `core.district` — 77 dòng, bảng tham chiếu

| Cột | Kiểu | Ý nghĩa (cột Berka gốc) |
|---|---|---|
| `district_id` | integer PK | |
| `district_name` | text | A2 |
| `region` | text | A3 |
| `inhabitants` | integer | A4 dân số |
| `munic_lt_500`, `munic_500_1999`, `munic_2000_9999`, `munic_ge_10000` | integer | A5–A8 số xã theo quy mô |
| `cities` | integer | A9 |
| `urban_ratio` | numeric(5,1) | A10 tỷ lệ dân thành thị (%) |
| `avg_salary` | integer | A11 lương trung bình (CZK). Simulator dùng để sinh lương |
| `unemployment_95`, `unemployment_96` | numeric(5,2) | A12, A13. **A12 có NULL** |
| `entrepreneurs_per_1000` | integer | A14 |
| `crimes_95`, `crimes_96` | integer | A15, A16. **A15 có NULL** |

### `core.client` — khách hàng (PII)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `client_id` | integer PK | |
| `full_name` | text | **Giả lập** (Faker `cs_CZ` theo giới tính, tất định theo `client_id`). N khách có thể mang tên trong danh sách cấm vận (`--sanction-hits`) |
| `national_id` | text UNIQUE | **Giả lập**, dạng rodné číslo `YYMMDD/XXXX`, tháng +50 với nữ (đúng quy ước Séc) |
| `gender` | char(1) | `F` / `M` (CTU đã giải mã từ `birth_number`) |
| `birth_date` | date | Dời +24 năm cùng dữ liệu để tuổi khách không đổi |
| `district_id` | integer FK | |
| `email`, `phone`, `address` | text | Giả lập. Simulator đổi `phone`/`address` vài khách mỗi ngày |
| `status` | text | `ACTIVE` / `CLOSED` |

### `core.account`

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `account_id` | integer PK | |
| `district_id` | integer FK | Chi nhánh |
| `frequency` | text | `POPLATEK MESICNE` hằng tháng / `POPLATEK TYDNE` hằng tuần / `POPLATEK PO OBRATU` sau mỗi giao dịch (kỳ sao kê) |
| `opened_date` | date | |
| `currency` | char(3) | `CZK` |
| `balance` | numeric(18,2) | Số dư hiện tại. Seed lấy từ giao dịch cuối, simulator cập nhật mỗi ngày |
| `status` | text | |

### `core.disp` — quyền trên tài khoản (cạnh đồ thị sở hữu)

`disp_id` PK · `client_id` FK · `account_id` FK · `disp_type` (`OWNER` / `DISPONENT`)

### `core.card`

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `card_id` | integer PK | |
| `disp_id` | integer FK | |
| `card_type` | text | `classic` / `junior` / `gold` |
| `issued_date` | date | |
| `card_number` | text UNIQUE | **Giả lập**, 16 số hợp lệ Luhn. **Entry không đọc cột này nguyên văn** |
| `status` | text | `ACTIVE` / `BLOCKED` (simulator thỉnh thoảng khoá thẻ) |

### `core.loan`

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `loan_id` | integer PK | |
| `account_id` | integer FK | |
| `granted_date` | date | |
| `amount` | numeric(18,2) | |
| `duration` | integer | tháng |
| `payments` | numeric(18,2) | trả hằng tháng |
| `status` | char(1) | `A` xong, trả đủ · `B` xong, **không trả đủ** · `C` đang chạy, đúng hạn · `D` đang chạy, **quá hạn** |

### `core.standing_order` — lệnh chuyển tiền định kỳ (bảng `order` của Berka)

`order_id` PK · `account_id` FK · `bank_to` · `account_to` bigint · `amount` numeric(12,2) ·
`k_symbol` (`SIPO` hộ gia đình, `POJISTNE` bảo hiểm, `UVER` trả vay, `LEASING`, NULL) · `active` bool

### `core.trans` — sổ giao dịch (chỉ INSERT)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `trans_id` | bigint PK | |
| `account_id` | integer FK | |
| `trans_date` | date | Ngày nghiệp vụ |
| `booked_at` | timestamptz | Berka chỉ có ngày: giờ được sinh tất định trong khoảng 07:00–20:00 UTC |
| `trans_type` | text | `PRIJEM` ghi có · `VYDAJ` ghi nợ · `VYBER` **lỗi gốc của Berka** (rút tiền ghi vào cột type) |
| `operation` | text | `VYBER` rút tiền mặt · `VKLAD` nộp tiền mặt · `PREVOD NA UCET` chuyển đi · `PREVOD Z UCTU` nhận về · `VYBER KARTOU` rút bằng thẻ · NULL (ghi lãi) |
| `amount`, `balance` | numeric(18,2) | `balance` là số dư sau giao dịch, **có thể âm** (thấu chi) |
| `k_symbol` | text | `UROK` lãi · `SLUZBY` phí · `SANKC. UROK` lãi phạt · `SIPO` · `DUCHOD` lương hưu · `POJISTNE` · `UVER` · NULL / `' '` |
| `bank`, `counter_account` | text, bigint | Ngân hàng và tài khoản đối ứng (chỉ có ở chuyển khoản) |
| `created_at` | timestamptz | |

---

## 3. Luồng giả lập

### 3.1 Seed: `python -m simulators.corebank.seed`

Đọc MariaDB công khai của CTU (`relational.fel.cvut.cz`, database `financial`), biến đổi, rồi
`COPY` vào Postgres.

| Tuỳ chọn | Tác dụng |
|---|---|
| `--limit-accounts N` | Chỉ lấy N tài khoản đầu và mọi thứ liên quan (thử nhanh: 300 tài khoản ≈ 68 nghìn giao dịch, 2 phút) |
| `--sanction-hits N` | Thay tên N khách bằng tên **người** trong `targets.simple.csv` của OpenSanctions. Đáp án ghi ra `data/ground_truth/sanction_hits.csv`, không ghi vào DB |
| `--truncate` | Xoá `core.*` rồi nạp lại |

Biến đổi: dời mọi ngày **+24 năm** (bội của 4 nên ngày 29/02 vẫn hợp lệ, ngày trong tháng giữ
nguyên). Sinh tên, số định danh, email, điện thoại, địa chỉ và số thẻ, tất cả **tất định** theo id
nên chạy lại cho ra y hệt. `account.balance` lấy từ giao dịch cuối.

### 3.2 Simulator: `python -m simulators.corebank.simulate --days N | --until D | --live`

Bắt đầu từ ngày sau giao dịch cuối, nhưng không sớm hơn `SIM_EPOCH`. Mỗi ngày nghiệp vụ:

| Khi nào | Hành vi | Bảng bị đổi |
|---|---|---|
| Mỗi ngày | Giao dịch thường ~ Poisson theo **nhịp lịch sử của từng tài khoản**. Tỷ lệ: rút tiền mặt 42%, chuyển đi 20%, nộp tiền 18%, nhận về 10%, rút thẻ 10% (chỉ tài khoản có thẻ). Không cho chi quá -5.000 CZK | `trans`, `account.balance` |
| Ngày 10 | Lương (70% tài khoản), mức theo lương trung bình của quận. Khách ≥ 60 tuổi nhận lương hưu (`DUCHOD`) | `trans` |
| Ngày 15 | Chạy mọi lệnh định kỳ đang bật | `trans` |
| Ngày 20 | Trả nợ vay (`UVER`). Thiếu tiền thì loan C→D, đủ lại thì D→C. Hết kỳ hạn: A nếu đang C, B nếu đang D | `trans`, `loan` |
| Cuối tháng | Lãi `UROK` 0,1% trên dư dương, lãi phạt `SANKC. UROK` 2% trên dư âm, phí `SLUZBY` 14,6 CZK | `trans` |
| Rải rác | Khách mới (client + account + disp OWNER, 30% có thẻ). Đổi điện thoại hoặc địa chỉ 0–8 khách/ngày. Thỉnh thoảng khoá một thẻ | `client`, `account`, `disp`, `card` |

Mọi thay đổi đi qua `INSERT`/`UPDATE` thường, nên trigger `updated_at` chạy đúng như ở hệ thống
thật.

---

## 4. Entry sourcing

| Entry | CDC | Mốc | Câu SQL (tóm tắt) | Ghi chú |
|---|---|---|---|---|
| `corebank_district` | CDC5 | — | `SELECT ... FROM core.district` | 77 dòng, chụp toàn bộ |
| `corebank_client` | CDC1 | `updated_at`, lookback 300 s | `WHERE updated_at > :watermark ORDER BY updated_at` | **PII** |
| `corebank_account` | CDC1 | `updated_at` | như trên | `balance` đổi mỗi ngày nên phần lớn tài khoản được kéo lại |
| `corebank_disp` | CDC1 | `updated_at` | như trên | |
| `corebank_card` | CDC1 | `updated_at` | `left(card_number,6)\|\|'******'\|\|right(card_number,4) AS card_number_masked, md5(card_number) AS card_token` | **Che số thẻ ngay ở nguồn** |
| `corebank_loan` | CDC1 | `updated_at` | | `status` đổi theo thời gian |
| `corebank_standing_order` | CDC1 | `updated_at` | | |
| `corebank_trans` | CDC1, **chia khúc** 50.000 | `trans_id` (int) | `WHERE trans_id > :watermark ORDER BY trans_id LIMIT :chunk_size` | Append-only |

Vì sao **lookback 300 giây** cho các bảng theo `updated_at`: transaction A bắt đầu trước
transaction B nhưng commit sau B, nên mang `updated_at` nhỏ hơn mốc mà lần chạy trước đã lưu theo
B. Không kéo chồng thì dòng của A mất vĩnh viễn. Cái giá là trùng lặp, silver khử trùng theo khoá.

Vì sao `trans` chia khúc: mỗi khúc là một truy vấn ngắn, không giữ snapshot hàng giờ, và đứt mạng
chỉ mất một khúc. Mốc `trans_id` là duy nhất và tăng dần, điều kiện bắt buộc của keyset pagination.

### Raw mẫu (`landing/corebank/trans/`)

```json
{"trans_id": 4003261, "account_id": 58, "trans_date": "2023-02-05", "booked_at": "2023-02-05T08:16:27+00:00",
 "trans_type": "PRIJEM", "operation": "VKLAD", "amount": "4462.00", "balance": "3643.64",
 "k_symbol": null, "bank": null, "counter_account": null, "created_at": "2026-10-13T15:55:03.069475+00:00",
 "_src_job": "corebank_trans", "_src_type": "jdbc", "_src_cdc": "cdc1", "_src_ingest_ts": "2026-10-13T15:55:04+00:00"}
```

Manifest có thêm danh sách `columns` lấy từ `cursor.description`, để silver phát hiện schema drift.

---

## 5. Chất lượng dữ liệu: việc của silver

| Vấn đề | Nguồn gốc | Xử lý |
|---|---|---|
| `trans_type = 'VYBER'` (16.666 dòng lịch sử) | Lỗi gốc Berka | Gộp vào `VYDAJ` |
| `k_symbol` rỗng theo 3 kiểu: NULL, `''`, `' '` | Berka | Trim, chuỗi rỗng thành NULL |
| `balance` âm | Thấu chi thật | Giữ nguyên, gắn cờ |
| Giao dịch lịch sử chỉ có ngày, giờ là sinh ra | Berka | Không dùng `booked_at` lịch sử cho phân tích theo giờ |
| `operation` NULL ở dòng lãi | Berka | Hợp lệ |
| Trùng lặp do lookback hoặc chạy lại | At-least-once | Khử trùng theo khoá chính và `updated_at` lớn nhất |
| `created_at`/`updated_at` là giờ **thật** (2026), `trans_date` là giờ **nghiệp vụ** (2023) | Thiết kế simulator | Phân tích nghiệp vụ dùng `trans_date`/`booked_at`, không dùng cột audit |

## 6. Silver (`silver_corebank`)

Mỗi entry sourcing có đúng một entry silver đọc cùng dataset, ở
`silver_etl/entries/corebank/`. Trên AWS, sourcing ghi manifest, SFN bắn "Job Run Succeeded",
rồi Lambda trigger tìm entry silver có `source.dataset` khớp và chạy Glue `slv-spark`. Không cần
cấu hình nối nào thêm.

| Bảng silver | Writer | Khoá | Ngày dữ liệu | Vì sao chọn writer này |
|---|---|---|---|---|
| `district` | simple `overwrite_table` | `district_id` | ingest | Bảng tham chiếu 77 dòng, nguồn CDC5. Lô rỗng bị từ chối. `min_rows: 70` |
| `client` | **scd2** delta | `client_id` | ingest | Cần lịch sử đổi điện thoại, địa chỉ, trạng thái |
| `account` | scd2 delta | `account_id` | ingest | Mỗi lần `balance` đổi là một phiên bản, trả lời được "số dư cuối ngày D" |
| `disp` | scd2 delta | `disp_id` | ingest | Cạnh sở hữu client → account |
| `card` | scd2 delta | `card_id` | ingest | Thẻ bị khoá theo thời gian. Chỉ có số đã che và `card_token` |
| `loan` | scd2 delta | `loan_id` | ingest | Thời điểm C → D (quá hạn) là nhãn rủi ro tín dụng |
| `standing_order` | scd2 delta | `order_id` | ingest | |
| `trans` | simple `append` | `trans_id` | **`trans_date`** (nghiệp vụ) | Bảng sự kiện, chỉ INSERT. Idempotent theo `ds_source`. **Partition theo tháng** (DDL viết tay `silver_etl/ddl/silver_corebank/trans.sql`) |

`scd2` với `snapshot: false` (delta): manifest CDC1 chỉ chứa dòng **đổi**, nên key vắng mặt không có
nghĩa là bị xoá. Nhiều manifest trong một ngày **chồng lên nhau**. Chạy lại một manifest chỉ hoàn tác
phiên bản của **chính manifest đó** (sửa writer ngày 13-10; trước đó lô thứ hai trong ngày xoá mất thay
đổi của lô thứ nhất, có test canh). Hai lần đổi trong cùng một ngày cho ra một phiên bản trung gian dài
0 ngày (`ds_valid_from = ds_valid_to`).

### Chuẩn hoá ở silver

| Bảng | Cột mới | Từ |
|---|---|---|
| `trans` | `direction` (CREDIT/DEBIT), `signed_amount` | `trans_type_code`. `VYBER` (lỗi gốc) tính là DEBIT, gắn cờ `is_berka_type_error` |
| `trans` | `operation_type` (CASH_WITHDRAWAL, TRANSFER_OUT, BANK_POSTING…), `purpose` (INTEREST, PENSION, LOAN_PAYMENT…) | mã tiếng Séc. Cột mã gốc vẫn giữ |
| `trans`, `standing_order` | `k_symbol` | `NULL`, `''`, `' '` đều thành NULL |
| `trans` | `is_overdraft` | `balance_after < 0` |
| `account` | `statement_frequency` (MONTHLY/WEEKLY/AFTER_TRANSACTION), `is_overdrawn` | |
| `loan` | `loan_status` (FINISHED_PAID…), `is_problem_loan` (B, D), `maturity_date` | |
| `client`, `disp` | `is_active`, `is_owner` | |
| mọi bảng scd2 | `source_created_at`, `source_updated_at` (**`hash: false`**) | Cột audit không làm sinh phiên bản. `source_updated_at` là `cdc_index` để chọn bản mới nhất khi lookback kéo trùng |

Kiểm tra chất lượng: `not_null` theo khoá và cột bắt buộc, không chấp nhận lỗi ép kiểu
(`max_cast_error_ratio: 0`). Trùng khoá trong lô (do lookback) chỉ cảnh báo, và giữ bản có
`source_updated_at` lớn nhất.

### Chạy và kiểm chứng trên máy

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"     # Spark 3.5 + Java 17/21; Windows cần winutils (HADOOP_HOME)
.\.venv\Scripts\python.exe -m pip install pyspark==3.5.4
python -m local.run_flow corebank                       # 8 sourcing -> 8 silver
python -m simulators.corebank.simulate --days 15
python -m local.run_flow corebank --sql "SELECT * FROM glue_catalog.silver_corebank.loan WHERE loan_id = 5015"
python -m local.run_flow corebank --silver-only         # chạy lại silver trên manifest mới nhất hôm nay
```

Bảng Iceberg nằm ở `data/local_warehouse/` (catalog hadoop, vẫn mang tên `glue_catalog`, nên SQL y hệt
trên Glue). File raw được tải từ moto về `data/local_lake/`.

Kết quả ngày 13-10-2026 (bản seed 300 tài khoản):

| Lần chạy | Kết quả |
|---|---|
| Lần 1 | 8/8 bảng SUCCESS: `trans` 71.775, `client` 398, `disp` 398, `standing_order` 434, `account` 332, `district` 77, `card` 66, `loan` 19 |
| Chạy lại silver trên cùng manifest | `account`: hoàn tác 332, ghi lại 332. Trạng thái bảng không đổi |
| Lần 2 ngay sau đó | `account` nhận 103 dòng do lookback, **N=103**, không sinh phiên bản rác |
| Sau 15 ngày simulator | `trans` +1.727. `client` có phiên bản U khi đổi điện thoại. Loan 5015 C → D thành 2 phiên bản |
| Đối soát `trans` | 73.502 dòng = 73.502 `trans_id` khác nhau. 961 dòng `VYBER` có cờ. 0 `k_symbol` rỗng. Partition theo tháng |

**Trên AWS:** entry sourcing và silver đều đã bật. Khi PR hạ tầng được apply, máy nguồn tự seed Berka
đầy đủ (4.500 tài khoản, ~1,06 triệu giao dịch) và mỗi ngày sinh thêm một ngày nghiệp vụ. Lịch 18:00 UTC
kéo về `landing/corebank/*`, silver tự chạy theo. Thứ tự apply nằm trong SETUP-LOG của repo hạ tầng
(mục 10-14).

## 7. Vận hành

```powershell
python -m local.run_entry corebank_trans                                # chạy một lần
python -m local.run_entry corebank_trans --override jdbc.chunk_size=1000
python -m local.run_entry corebank_client --reset                       # kéo lại từ đầu
docker exec -it fingraph-sources-corebank-db-1 psql -U corebank -d corebank   # xem DB
```

Đặt `COREBANK_DSN` để simulator trỏ sang DB khác (mặc định
`postgresql://corebank:corebank@localhost:5433/corebank`).
