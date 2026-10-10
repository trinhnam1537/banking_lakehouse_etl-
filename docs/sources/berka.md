# Nguồn Berka (PKDD'99): phân tích và cách sourcing

Dữ liệu **thật, đã ẩn danh** của một ngân hàng Séc trong giai đoạn 1993–1998. Đây là
nguồn "core banking" đầu tiên của hệ thống. Mọi số liệu dưới đây được đo trực tiếp
trên server ngày 10-10-2026.

| | |
|---|---|
| Server | MariaDB 10.6 công khai của CTU, `relational.fel.cvut.cz:3306`, user `guest` (chỉ đọc) |
| Database | **`financial`** (bản gốc). **Không dùng** `Financial_std`: bản đó còn 137k giao dịch, mất ngày, tài khoản và số dư |
| Kích thước | 8 bảng, ~1,08 triệu dòng, ~80 MB |
| Secret | `fingraph/berka_ctu` = `{host, port, username, password, dbname}` (tài khoản guest công khai) |
| Entry | `sourcing_etl/sources/berka/berka_<bảng>.yml` → `landing/berka/<bảng>/` |

## Quan hệ giữa các bảng

```
district (77) ◀── client (5.369) ──┐
     ▲                              │ disp (5.369): OWNER | DISPONENT
     └──────── account (4.500) ◀────┘
                 ▲   ▲   ▲
     trans ──────┘   │   └────── order (6.471, lệnh định kỳ)
  (1.056.320)        │
                   loan (682, ≤ 1 / tài khoản)

     card (892) ──▶ disp     (thẻ gắn với QUYỀN trên tài khoản, không gắn với tài khoản)
```

Mỗi tài khoản có **đúng một OWNER**, đã kiểm tra: 0 ngoại lệ. Bảng `disp` chính là các
cạnh của đồ thị sở hữu `client —OWNER/DISPONENT→ account` cho tầng ontology.

## Từng bảng

| Bảng | Dòng | Khoá | CDC | Ghi chú |
|---|---|---|---|---|
| `account` | 4.500 | `account_id` | CDC5 | `frequency`: sao kê hằng tháng / hằng tuần / sau mỗi giao dịch. Mở tài khoản từ 1993-01 đến 1997-12 |
| `client` | 5.369 | `client_id` | CDC5 | Bản CTU đã giải mã `birth_number` gốc thành `gender` + `birth_date`. **PII** |
| `disp` | 5.369 | `disp_id` | CDC5 | OWNER 4.500 · DISPONENT 869 |
| `card` | 892 | `card_id` | CDC5 | classic 659 · junior 145 · gold 88 |
| `loan` | 682 | `loan_id` | CDC5 | `status`: A xong-trả đủ · B xong-**không trả đủ** · C đang chạy-đúng hạn · D đang chạy-**quá hạn**. `payments = amount / duration` |
| `order` | 6.471 | `order_id` | CDC5 | `order` là **từ khoá SQL**. `amount` là `decimal(6,1)` |
| `district` | 77 | `district_id` | CDC5 | Cột tên `A2..A16`, nghĩa ghi trong entry. `A10/A12/A13` là `decimal`; `A12`, `A15` có NULL |
| `trans` | 1.056.320 | `trans_id` | **CDC1 theo `trans_id`** | Xem bên dưới |

### Vì sao `trans` dùng CDC1 dù nguồn tĩnh

- Nguồn không đổi từ 1999, nên lần chạy đầu kéo hết, các lần sau ra `NO_DATA`.
- Lý do thật sự là **khả năng chạy tiếp**: cứ 20 lô × 5.000 dòng thì ghi một ảnh chụp
  PARTIAL. Nếu mạng Singapore–Praha đứt giữa chừng, lần sau chạy tiếp từ `trans_id` đã ghi,
  không kéo lại cả triệu dòng.
- **Không dùng `date` làm mốc**: `trans_id` không tăng theo ngày (105.300 cặp ngược thứ tự),
  và nhiều giao dịch trùng ngày.
- Đã thử reader thật trên server: 10.000 dòng mất ~4 giây, RAM đỉnh 8,4 MB (cursor đọc dần).

## Từ điển mã (tiếng Séc → nghĩa)

| Cột | Mã | Nghĩa | Số dòng |
|---|---|---|---|
| `trans.type` | `PRIJEM` | Ghi có (tiền vào) | 405.083 |
| | `VYDAJ` | Ghi nợ (tiền ra) | 634.571 |
| | `VYBER` | **Lỗi đã biết**: rút tiền nhưng ghi vào cột `type`. Silver gộp vào `VYDAJ` | 16.666 |
| `trans.operation` | `VYBER` | Rút tiền mặt | 434.918 |
| | `PREVOD NA UCET` | Chuyển đi ngân hàng khác | 208.283 |
| | `VKLAD` | Nộp tiền mặt | 156.743 |
| | `PREVOD Z UCTU` | Nhận từ ngân hàng khác | 65.226 |
| | `VYBER KARTOU` | Rút bằng thẻ | 8.036 |
| | NULL | Ghi lãi (khớp đúng số dòng `UROK`) | 183.114 |
| `trans.k_symbol`, `order.k_symbol` | `UROK` | Lãi tiền gửi | |
| | `SANKC. UROK` | Lãi phạt khi số dư âm | |
| | `SLUZBY` | Phí sao kê | |
| | `SIPO` | Thanh toán hộ gia đình | |
| | `DUCHOD` | Lương hưu | |
| | `POJISTNE` | Bảo hiểm | |
| | `UVER` | Trả nợ vay | |
| | `LEASING` | Thuê tài chính (chỉ có ở `order`) | |
| `account.frequency` | `POPLATEK MESICNE` / `TYDNE` / `PO OBRATU` | Sao kê hằng tháng / hằng tuần / sau mỗi giao dịch | |

## Chất lượng dữ liệu: việc của silver

| Vấn đề | Quy mô | Xử lý ở silver |
|---|---|---|
| `k_symbol` trống theo ba kiểu: NULL, `''`, `' '` (một dấu cách) | trans: 481.881 NULL + 53.433 `' '`; order: 1.379 `''` | Trim, chuỗi rỗng thành NULL |
| `type = VYBER` | 16.666 | Gộp vào `VYDAJ` |
| Số dư âm | 2.999 giao dịch | Giữ nguyên (thấu chi thật), gắn cờ |
| Ngày 1993–1998 | mọi bảng có ngày | Giữ nguyên ở raw. Dời ngày (nếu cần) làm khi dựng "core banking" riêng |
| Số tiền | CZK, số nguyên ở `trans`/`loan`; `decimal` ở `order`/`loan.payments` | Raw giữ **chuỗi** cho `decimal` (reader không đổi sang float). Silver ép `decimal(18,2)` |

## Đường đi

```
MariaDB CTU (public) ──internet──▶ Glue Python Shell (NGOÀI VPC, PyMySQL qua PyPI)
                                      ├─ GetSecretValue fingraph/berka_ctu
                                      └─▶ landing/berka/<bảng>/ingest_date=…/  + manifest
```

Glue sourcing hiện chạy ngoài VPC và có internet, nên đọc **thẳng** được server CTU. Chưa
cần RDS, Glue Connection hay VPC endpoint. Những thứ đó chỉ cần khi chuyển sang "core banking"
riêng trong private subnet (bước sau: copy Berka về Postgres của mình, rồi cho simulator
sinh giao dịch mới).

**Chạy:** lịch đang tắt vì nguồn tĩnh. Kích tay bằng Lambda trigger:

```json
{"job_ids": ["src.berka_district", "src.berka_account", "src.berka_client", "src.berka_disp",
             "src.berka_card", "src.berka_loan", "src.berka_order", "src.berka_trans"],
 "chain": false}
```

## Ánh xạ sang ontology (định hướng)

| Berka | FIBO |
|---|---|
| `client` | `fibo-fnd-pty-pty:Party` / người tự nhiên |
| `account` | `fibo-fbc-pas-caa:Account` (tài khoản tiền gửi) |
| `disp` OWNER / DISPONENT | `isAccountHolder` / bên được uỷ quyền |
| `loan` | `fibo-loan-ln-ln:Loan`, `status` là trạng thái khoản vay |
| `card` | thẻ gắn với một vai trò trên tài khoản |
| `district` | vị trí địa lý (quận/huyện, vùng) |
