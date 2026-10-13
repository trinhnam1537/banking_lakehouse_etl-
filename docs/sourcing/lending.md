# Hệ thống cho vay (Postgres `lending`) — Home Credit Default Risk

Hệ thống cho vay tiêu dùng của ngân hàng giả lập, theo bộ **Home Credit Default Risk** (Kaggle
2018): hồ sơ vay, lịch sử tín dụng ở tổ chức khác (tương đương **CIC** ở Việt Nam), các khoản vay
trước và lịch sử trả góp. Nhãn vỡ nợ (`TARGET`) được tách thành luồng nhãn đến trễ.

| | |
|---|---|
| Loại | OLTP quan hệ, nguồn CDC + file nhãn |
| Hệ quản trị | PostgreSQL 16 |
| Database / schema | `lending` / `lending` |
| Quy mô gốc (Kaggle) | 307.511 hồ sơ (train) · 1,7 triệu bureau · **27,3 triệu** bureau_balance · 1,67 triệu previous · 10 triệu POS · 3,8 triệu thẻ · 13,6 triệu trả góp (~2,6 GB CSV) |
| Quy mô mặc định khi seed | 5.000 hồ sơ, lấy mẫu theo `SK_ID_CURR` (2.000 hồ sơ ≈ 240 nghìn dòng tổng) |
| Entry | 7 entry JDBC `lending_*` cộng `lending_default_labels` (s3file) |
| Dataset raw | `landing/lending/<bảng>/`, `landing/lending/default_labels/` |
| Profile | JDBC: `src-vpc` (**tắt**, chờ hạ tầng). Nhãn: `src-shell` (bật) |
| Code | `simulators/lending/{schema.sql, generator.py, seed.py, simulate.py}` |

---

## 1. Lấy dữ liệu gốc

Bộ Kaggle cần tài khoản và **đồng ý luật cuộc thi**. Luật cấm phân phối lại dữ liệu, nên không
commit và không để bucket public. Thư mục `data/` đã có trong `.gitignore`.

```powershell
pip install kaggle      # cần %USERPROFILE%\.kaggle\kaggle.json
kaggle competitions download -c home-credit-default-risk -p data/home-credit
Expand-Archive data/home-credit/home-credit-default-risk.zip data/home-credit
```

Nếu không có file Kaggle, `seed.py` tự chuyển sang **chế độ tổng hợp** (`generator.py`). Chế độ
này sinh cùng schema và phân phối gần đúng: hợp đồng tiền mặt ~90%, nữ ~66%,
`DAYS_EMPLOYED = 365243` ~18%, `EXT_SOURCE_1` thiếu ~55%, tỷ lệ vỡ nợ ~8%.

## 2. Kết nối và credential

| Môi trường | Host:port | User ETL | Secret |
|---|---|---|---|
| Local | `localhost:5434` | `etl_reader` / `etl_reader` | `fingraph/src/lending_pg` (moto) |
| AWS (đề xuất) | EC2 "hệ thống nguồn", cổng 5432 (container thứ hai) | `etl_reader` | `fingraph/src/lending_pg`, **chưa tạo** |

Secret có dạng `{host, port, username, password, dbname}`, quyền giống [corebank](corebank.md#1-kết-nối-và-credential).

---

## 3. Schema

```
application (sk_id_curr) ──┬── bureau (sk_id_bureau) ── bureau_balance (id)
   customer_ref ─▶ core.client
                           └── previous_application (sk_id_prev) ──┬── pos_cash_balance (id)
                                                                   ├── credit_card_balance (id)
                                                                   └── installments_payments (id)
```

Khác bản Kaggle, và lý do:

| Thay đổi | Vì sao |
|---|---|
| Thêm `id bigserial` cho 4 bảng con | Bản gốc không có khoá đơn (`bureau_balance` khoá ghép, `installments_payments` có **dòng trùng hoàn toàn**). Reader JDBC chỉ nhận **một** cột mốc |
| `created_at`/`updated_at` + trigger cho `application`, `bureau`, `previous_application` | Ba bảng này bị sửa (status, quá hạn), cần mốc CDC1 theo thời gian |
| `application.customer_ref` | Nối hồ sơ vay về `core.client.client_id`, khoảng 70% hồ sơ. **Ánh xạ giả lập** |
| `application_date`, `status` | Bản gốc không có ngày tuyệt đối hay trạng thái. Các cột `DAYS_*` vẫn giữ nghĩa "số ngày tính tới ngày nộp" |
| `TARGET` **không** nằm trong DB | Nhãn đến sau, nằm ở file nhãn riêng (mục 4.3) |
| Cột ít dùng (122 cột gốc) dồn vào `extra jsonb` | Không mất dữ liệu mà schema vẫn gọn |
| Tên cột viết thường | Postgres không phải đặt ngoặc kép |

### `lending.application` — hồ sơ vay hiện tại

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `sk_id_curr` | bigint PK | |
| `customer_ref` | integer | `core.client.client_id`, NULL nếu khách chưa có tài khoản |
| `application_date` | date | Ngày nộp. Lịch sử nằm trong năm trước `SIM_EPOCH` |
| `status` | text | `SUBMITTED` → `APPROVED` / `REJECTED` → `DISBURSED`. Lịch sử đều là `DISBURSED` |
| `name_contract_type` | text | `Cash loans` / `Revolving loans` |
| `code_gender`, `flag_own_car`, `flag_own_realty` | text | |
| `cnt_children` | integer | |
| `amt_income_total`, `amt_credit`, `amt_annuity`, `amt_goods_price` | numeric(14,2) | Tiền tệ **không được công bố** ở bản gốc |
| `name_income_type`, `name_education_type`, `name_family_status`, `name_housing_type` | text | |
| `days_birth` | integer | Âm, số ngày trước ngày nộp |
| `days_employed` | integer | Âm. **365243 là MÃ** "không đi làm/hưu trí", không phải số ngày |
| `occupation_type`, `organization_type` | text | `organization_type = 'XNA'` nghĩa là không áp dụng |
| `ext_source_1..3` | numeric(8,6) | Điểm tín dụng ngoài (0–1), nhiều NULL |
| `extra` | jsonb | Các cột gốc còn lại (chế độ Kaggle) |

### `lending.bureau` — khoản vay ở tổ chức khác (≈ CIC)

`sk_id_bureau` PK · `sk_id_curr` FK · `credit_active` (`Active`/`Closed`/`Sold`/`Bad debt`) ·
`credit_currency` · `days_credit` · `credit_day_overdue` · `days_credit_enddate` ·
`amt_credit_sum`, `amt_credit_sum_debt`, `amt_credit_sum_overdue` · `credit_type`
(`Consumer credit`, `Credit card`, `Car loan`, `Mortgage`...) · `days_credit_update` · `amt_annuity`

### `lending.bureau_balance` — tình trạng tháng của khoản vay CIC

`id` PK · `sk_id_bureau` FK · `months_balance` (0 = tháng nộp hồ sơ, -1, -2...) · `status`
(`C` đã đóng, `X` không rõ, `0` đúng hạn, `1..5` số kỳ quá hạn)

### `lending.previous_application` — hồ sơ hoặc hợp đồng trước đó tại ngân hàng

`sk_id_prev` PK · `sk_id_curr` FK · `name_contract_type` · `amt_annuity`, `amt_application`,
`amt_credit`, `amt_goods_price` · `name_contract_status` (`Approved`/`Refused`/`Canceled`/`Unused offer`) ·
`days_decision` · `name_payment_type` · `code_reject_reason` (`XAP` = không áp dụng) ·
`name_client_type` · `cnt_payment` · `name_yield_group`

### `lending.pos_cash_balance` · `lending.credit_card_balance` · `lending.installments_payments`

| Bảng | Cột chính |
|---|---|
| `pos_cash_balance` | `id`, `sk_id_prev`, `sk_id_curr`, `months_balance`, `cnt_instalment`, `cnt_instalment_future`, `name_contract_status`, `sk_dpd`, `sk_dpd_def` |
| `credit_card_balance` | `id`, `sk_id_prev`, `sk_id_curr`, `months_balance`, `amt_balance`, `amt_credit_limit_actual`, `amt_drawings_current`, `amt_payment_current`, `amt_total_receivable`, `cnt_drawings_current`, `name_contract_status`, `sk_dpd` |
| `installments_payments` | `id`, `sk_id_prev`, `sk_id_curr`, `num_instalment_version`, `num_instalment_number`, `days_instalment` (hạn), `days_entry_payment` (ngày trả thật), `amt_instalment`, `amt_payment` |

`sk_dpd` là số ngày quá hạn. Trả trễ bằng `days_entry_payment - days_instalment > 0`.

---

## 4. Luồng giả lập

### 4.1 Seed: `python -m simulators.lending.seed [--applicants N] [--synthetic] [--truncate]`

- **Chế độ Kaggle** (có `data/home-credit/application_train.csv`): lấy mẫu N hồ sơ bằng
  reservoir sampling, rồi **đọc theo luồng** 6 file con và chỉ giữ dòng thuộc các hồ sơ đã chọn.
  Toàn vẹn tham chiếu được giữ, khối lượng giảm hàng chục lần.
- **Chế độ tổng hợp**: `generator.py` sinh hồ sơ, 0–15 khoản CIC (mỗi khoản tối đa 24 tháng
  balance), 0–8 hồ sơ trước, kèm POS, thẻ và trả góp.
- Cả hai chế độ: `application_date` ngẫu nhiên trong năm trước `SIM_EPOCH`, `status = DISBURSED`,
  ~70% gán `customer_ref` (nếu corebank đã seed). Nhãn ghi một file
  `landing/_inbox/lending/labels/dt=2022-12-31/` (`--no-labels` để bỏ).

### 4.2 Simulator: `python -m simulators.lending.simulate --days N | --live`

| Mỗi ngày | Bảng |
|---|---|
| Hồ sơ mới ~0,2% số hiện có, `status = SUBMITTED` | `application` (INSERT) |
| Hồ sơ SUBMITTED ≥ 2 ngày: `ext_source_2 < 0,25` thì REJECTED, còn lại APPROVED | `application` (UPDATE) |
| APPROVED → DISBURSED, sinh hợp đồng `previous_application` (`days_decision = 0`) | `application`, `previous_application` |
| Đúng kỳ hạn (mỗi 30 ngày kể từ giải ngân = ngày nộp + 3): một dòng trả góp và một dòng POS, ~8% trả trễ | `installments_payments`, `pos_cash_balance` |
| 0–5 khoản CIC đổi `credit_day_overdue` | `bureau` (UPDATE) |
| Khoản vay đủ 90 ngày: quyết định vỡ nợ theo `ext_source`, đưa vào nhãn trễ (`--label-delay-days`, mặc định 7) | file nhãn |

### 4.3 File nhãn

`landing/_inbox/lending/labels/dt=<ngày>/labels_<YYYYMMDD>.csv.gz`, header
`sk_id_curr, target, label_source` (`label_source` là `kaggle` / `synthetic` / `simulated`).

---

## 5. Entry sourcing

| Entry | CDC | Mốc | Ghi chú |
|---|---|---|---|
| `lending_application` | CDC1 | `updated_at`, lookback 300 s | Trạng thái hồ sơ đổi theo thời gian |
| `lending_bureau` | CDC1 | `updated_at` | |
| `lending_previous_application` | CDC1 | `updated_at` | |
| `lending_bureau_balance` | CDC1, chia khúc 100.000 | `id` | Bảng lớn nhất |
| `lending_pos_cash_balance` | CDC1, chia khúc 100.000 | `id` | Append-only |
| `lending_credit_card_balance` | CDC1, chia khúc 100.000 | `id` | Append-only |
| `lending_installments_payments` | CDC1, chia khúc 100.000 | `id` | Append-only |
| `lending_default_labels` | CDC1 (`s3file`) | khoá S3 | `landing/_inbox/lending/labels/` → `landing/lending/default_labels/` |

Raw mẫu (`lending/application`, cắt bớt):

```json
{"sk_id_curr": 100002, "customer_ref": 107, "application_date": "2022-05-20", "status": "DISBURSED",
 "name_contract_type": "Cash loans", "code_gender": "M", "amt_income_total": "165900.00",
 "amt_credit": "1156000.00", "days_birth": -9037, "days_employed": -9785, "ext_source_1": null,
 "ext_source_2": "0.926840", "extra": null, "_src_job": "lending_application", "...": "..."}
```

## 6. Chất lượng dữ liệu: việc của silver

| Vấn đề | Xử lý |
|---|---|
| `days_employed = 365243` | Thành NULL kèm cờ `is_unemployed_or_pensioner` |
| `XNA`, `XAP` (giá trị "không áp dụng") | Thành NULL |
| `DAYS_*` tương đối | Ngày tuyệt đối = `application_date + days_*` |
| `code_gender = 'XNA'` (4 dòng trong bản Kaggle) | NULL |
| Trùng hoàn toàn trong `installments_payments` (bản Kaggle) | Giữ nguyên ở raw, `id` phân biệt. Silver quyết định gộp hay giữ |
| `amt_*` không rõ tiền tệ | Ghi rõ trong mô hình là "đơn vị tiền gốc", không quy đổi VND |
| `customer_ref` là liên kết giả lập | Đánh dấu ở lineage. Không dùng để kết luận nghiệp vụ |
