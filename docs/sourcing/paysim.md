# PaySim — đối tác ví điện tử, file đối soát cuối ngày

PaySim (Lopez-Rojas và cộng sự, 2016) mô phỏng giao dịch mobile money trong 30 ngày, có nhãn gian
lận. Trong ngân hàng giả lập, PaySim đóng vai **đối tác ví điện tử** gửi một **file đối soát mỗi
cuối ngày** (EOD) vào S3. Đây là kênh nạp theo lô bằng file, khác với Kafka và JDBC.

| | |
|---|---|
| Loại | File EOD (CSV gzip) theo ngày + nhãn trễ |
| Giấy phép | CC BY-SA 4.0 (Kaggle `ntnu-testimon/paysim1`) |
| Quy mô gốc | 6.362.620 giao dịch, `step` 1–743 (giờ), ~470 MB CSV, 8.213 gian lận (0,13%) |
| Entry | `paysim_eod`, `paysim_labels` (s3file, `src-shell`, bật) |
| Dataset raw | `landing/paysim/eod/`, `landing/paysim/labels/` |
| Code | `simulators/paysim/eod.py` (`synth`, `publish`) |

```powershell
kaggle datasets download -d ntnu-testimon/paysim1 -p data/paysim --unzip
# -> data/paysim/PS_20174392719_1491204439457_log.csv
```

## 1. Schema gốc

| Cột | Ý nghĩa |
|---|---|
| `step` | Giờ thứ mấy (1 = giờ đầu tiên). Không có ngày tuyệt đối |
| `type` | `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER` |
| `amount` | Số tiền (đơn vị tiền tệ không công bố) |
| `nameOrig`, `oldbalanceOrg`, `newbalanceOrig` | Bên gửi và số dư trước/sau. **Chú ý: `Org`, không phải `Orig`** |
| `nameDest`, `oldbalanceDest`, `newbalanceDest` | Bên nhận. `M...` là merchant, **không có số dư** |
| `isFraud` | Nhãn |
| `isFlaggedFraud` | Quy tắc của hệ thống cũ: TRANSFER > 200.000. Chỉ 16 dòng |

Đặc điểm cần biết trước khi dùng:

- **`nameOrig` gần như không lặp lại**: mỗi "khách" chỉ có một giao dịch, nên không dựng được lịch
  sử hành vi phía người gửi. Phía người nhận (`nameDest`) thì có lặp.
- Gian lận **chỉ** ở `TRANSFER` và `CASH_OUT`. Mẫu điển hình: TRANSFER rút sạch số dư sang tài
  khoản trung gian, rồi CASH_OUT ngay.
- **Số dư không nhất quán**: rất nhiều dòng có số dư cũ = mới = 0 dù có giao dịch. Không dùng để
  đối soát.
- Các step sau ~400 rất thưa, nhưng gian lận vẫn rải đều, nên tỷ lệ gian lận ở giờ thưa cao bất
  thường.

## 2. Luồng giả lập (`simulators/paysim/eod.py`)

| Lệnh | Việc |
|---|---|
| `synth --rows N` | Sinh file **đúng định dạng Kaggle** ở `data/paysim-synthetic/`, giữ các đặc điểm trên |
| `publish [--days N] [--pace S] [--label-delay-days 30]` | Chia theo ngày, đẩy file EOD và file nhãn vào inbox. Ưu tiên bộ Kaggle nếu có |

Biến đổi của "đối tác":

| Việc | Lý do |
|---|---|
| Ngày = `SIM_EPOCH + (step-1) // 24`. `txn_ts` = giờ `(step-1) % 24`, phút và giây sinh tất định | Trục thời gian chung |
| **`txn_ref = "PS" + số dòng gốc`** (9 chữ số) | PaySim không có mã giao dịch, mà đối tác thật luôn có mã đối soát. Silver khử trùng theo mã này |
| Đổi tên cột sang snake_case (`oldbalanceOrg` → `old_balance_orig`) | Bỏ lỗi chính tả gốc |
| `isFraud`/`isFlaggedFraud` → file nhãn trễ 30 ngày | Khiếu nại và hoàn tiền đến muộn |
| Mỗi ngày một file ~15 MB nén | Thay cho một file 470 MB |

## 3. File đích

| Prefix inbox | Header |
|---|---|
| `paysim/eod/dt=YYYY-MM-DD/paysim_eod_YYYYMMDD.csv.gz` | `txn_ref, txn_ts, step, type, amount, name_orig, old_balance_orig, new_balance_orig, name_dest, old_balance_dest, new_balance_dest` |
| `paysim/labels/dt=YYYY-MM-DD/labels_YYYYMMDD.csv.gz` | `txn_ref, is_fraud, is_flagged_fraud` |

## 4. Entry sourcing

| Entry | Định dạng | Mốc |
|---|---|---|
| `paysim_eod` | csv gzip, lô 20.000 dòng | CDC1, khoá S3 của file cuối (thứ tự từ điển `dt=…` = thứ tự ngày) |
| `paysim_labels` | csv gzip | CDC1, khoá S3 |

Reader **đọc file theo luồng**. Chỉ lô cuối của mỗi file mang mốc, nên file đọc dở không bao giờ
được đánh dấu là đã xong. Raw có thêm `_src_file` và `_src_row`:

```json
{"txn_ref": "PS000000001", "txn_ts": "2023-01-01T00:08:36Z", "step": "1", "type": "TRANSFER",
 "amount": "401606.80", "name_orig": "C8178851181", "old_balance_orig": "0.00", "new_balance_orig": "0.00",
 "name_dest": "C9542301413", "old_balance_dest": "71639.33", "new_balance_dest": "473246.13",
 "_src_file": "landing/_inbox/paysim/eod/dt=2023-01-01/paysim_eod_20230101.csv.gz", "_src_row": 1, "...": "..."}
```

**Rủi ro của mốc theo khoá S3:** file có khoá đứng trước mốc mà đến muộn (gửi bù ngày cũ) sẽ bị
bỏ qua. Đối tác phải gửi file theo thứ tự ngày. Nếu cần gửi bù thì đặt vào ngày hiện tại, hoặc
`reset_checkpoint` rồi nạp lại (silver khử trùng theo `txn_ref`).

## 5. Chất lượng dữ liệu: việc của silver

| Vấn đề | Xử lý |
|---|---|
| Mọi giá trị là chuỗi (CSV) | Ép `decimal(18,2)`, `int`, `timestamp` |
| Số dư không nhất quán, merchant không có số dư | Không dùng để đối soát. Gắn cờ `balance_inconsistent` |
| `nameOrig` không lặp | Không xây đặc trưng hành vi phía người gửi |
| Tiền tệ không rõ | Giữ đơn vị gốc |
