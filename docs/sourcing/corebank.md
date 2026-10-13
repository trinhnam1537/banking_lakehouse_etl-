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
| Profile | `src-vpc`: **tắt** cho tới khi repo hạ tầng tạo profile (xem [README](README.md#7-hạ-tầng-còn-thiếu)) |
| Code | `simulators/corebank/{schema.sql, seed.py, simulate.py}` |

Khác với nguồn [berka.md](berka.md): entry `berka_*` đọc **thẳng server CTU**, là dữ liệu tĩnh
từ 1999. Còn `corebank_*` đọc hệ thống **của mình**: có cột audit, có dữ liệu mới mỗi ngày, có PII
giả lập, nên CDC1 có ý nghĩa thật.

---

## 1. Kết nối và credential

| Môi trường | Host:port | User ETL | Secret |
|---|---|---|---|
| Local (`local/docker-compose.yml`) | `localhost:5433` | `etl_reader` / `etl_reader` | `fingraph/src/corebank_pg` trong moto (tạo bởi `local.bootstrap`) |
| AWS (đề xuất) | IP riêng của EC2 "hệ thống nguồn", cổng 5432 | `etl_reader`, mật khẩu đặt tay | `fingraph/src/corebank_pg`, **chưa tạo** |

```json
{"host": "localhost", "port": 5433, "username": "etl_reader", "password": "etl_reader", "dbname": "corebank"}
```

- `etl_reader` được tạo trong `schema.sql`, chỉ có quyền `USAGE` trên schema `core` và `SELECT`
  trên mọi bảng, kể cả bảng tạo sau này (`ALTER DEFAULT PRIVILEGES`).
- Reader còn mở phiên `READ ONLY`. Có hai lớp chặn ghi.
- Chủ DB (`corebank`/`corebank`) chỉ dành cho simulator, ETL không bao giờ dùng.

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

## 6. Vận hành

```powershell
python -m local.run_entry corebank_trans                                # chạy một lần
python -m local.run_entry corebank_trans --override jdbc.chunk_size=1000
python -m local.run_entry corebank_client --reset                       # kéo lại từ đầu
docker exec -it fingraph-sources-corebank-db-1 psql -U corebank -d corebank   # xem DB
```

Đặt `COREBANK_DSN` để simulator trỏ sang DB khác (mặc định
`postgresql://corebank:corebank@localhost:5433/corebank`).
