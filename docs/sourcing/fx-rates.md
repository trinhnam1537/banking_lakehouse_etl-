# Tỷ giá — Vietcombank (VND) và Ngân hàng Quốc gia Séc (CZK)

Dữ liệu tham chiếu để quy đổi giao dịch về một đồng tiền. Hai nguồn **thật** bổ sung cho nhau:

| | Vietcombank | CNB (Česká národní banka) |
|---|---|---|
| Đồng gốc | VND | CZK |
| Dùng cho | Quy đổi mọi thứ về VND (IBM AML, thẻ...) | Core banking và Berka tính bằng **CZK**, mà VCB không niêm yết CZK |
| Lịch sử | từ ~06/2020 | từ 1991 |
| Entry | `vcb_exchange_rates` (`api`, CDC1 theo cửa sổ ngày) | `cnb_czk_rates` (`httpfile`, CDC5) |
| Dataset raw | `landing/fx/vcb_rates/` | `landing/fx/cnb_rates/` |
| Credential | Không | Không |
| Profile | `src-shell`, bật | `src-shell`, bật |

---

## 1. Vietcombank

### Hai endpoint (kiểm tra ngày 11-10-2026)

| Endpoint | Kết quả | Dùng? |
|---|---|---|
| `https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx` | XML, **chỉ tỷ giá hiện tại**. Trong response ghi rõ *"Only one request every 5 minutes!"*. Số có dấu phẩy ngăn hàng nghìn (`"17,609.30"`), tên tiền đệm khoảng trắng, `DateTime` dạng Mỹ `10/11/2026 12:13:57 AM` | **Không** |
| `https://www.vietcombank.com.vn/api/exchangerates?date=YYYY-MM-DD` | JSON, **có lịch sử theo ngày** | **Có**. Reader `api` dùng được ngay, không cần parser XML |

Độ sâu lịch sử: `2020-01-02` trả `Count: 0`, `2020-06-01` có dữ liệu. Entry bắt đầu từ
**2020-06-01**.

### Schema response

```json
{"Count": 20, "Date": "2020-06-01T00:00:00", "UpdatedDate": "2020-06-01T23:00:00+07:00",
 "Data": [{"currencyName": "US DOLLAR", "currencyCode": "USD", "cash": "23140.00",
           "transfer": "23170.00", "sell": "23350.00", "icon": "/-/media/..."}]}
```

| Trường | Ý nghĩa |
|---|---|
| `currencyCode` | 20 mã: USD, EUR, GBP, JPY, AUD, SGD, THB, CAD, CHF, HKD, CNY, DKK, INR, KRW, KWD, MYR, NOK, RUB, SAR, SEK |
| `cash` | Giá **mua tiền mặt** (VND cho 1 đơn vị) |
| `transfer` | Giá **mua chuyển khoản** |
| `sell` | Giá **bán** |
| `UpdatedDate` | Thời điểm chốt tỷ giá trong ngày (+07:00) |

Không có: **CZK, MXN, BRL, ILS, BTC**. Giá trị là **chuỗi**, có thể là `"-"` hoặc rỗng khi không
niêm yết (thường gặp ở `cash` của DKK, INR, KWD...).

### Entry `vcb_exchange_rates`

```yaml
api:
  base_url: https://www.vietcombank.com.vn/api/exchangerates
  records_path: Data
  raw_mode: response            # mỗi ngày một dòng raw = nguyên response
  rate_limit_per_sec: 0.5
  date_window:                  # tính năng mới của reader api
    param: date
    end_offset_days: 1          # chỉ kéo ngày đã CHỐT
    utc_offset_hours: 7         # "hôm nay" theo giờ VN
    max_windows: 400            # backfill ~2.300 ngày = 6 lần chạy
cdc:
  mode: cdc1
  watermark_column: _window     # mốc = ngày đã gọi xong
  initial_watermark: "2020-06-01"
```

Reader gọi một request cho mỗi ngày từ (mốc + 1) tới (hôm nay theo giờ VN − 1). Ngày nguồn trả
rỗng (nghỉ lễ) vẫn tiến mốc. Cách làm này giống `DatetimeBasedCursor` của Airbyte.

**Rủi ro:** đây là endpoint của website, không phải API chính thức. VCB có thể đổi hoặc chặn IP
AWS (như SSI từng chặn, xem SETUP-LOG). Phải chạy thử từ Glue trước khi bật lịch.

## 2. CNB — tỷ giá CZK

### URL

```
https://www.cnb.cz/en/financial-markets/foreign-exchange-market/central-bank-exchange-rate-fixing/central-bank-exchange-rate-fixing/year.txt?year=YYYY
```

### Định dạng và bẫy

```
Date|1 AUD|1 BGN|1 BRL|1 CAD|...|100 JPY|...|1 USD|1 XDR
03.01.2022|15.818|12.690|3.906|17.210|...|19.016|...|21.951|30.666
```

| Đặc điểm | Hệ quả |
|---|---|
| Phân cách `\|`, ngày `dd.mm.yyyy` | `csv_delimiter: "\|"`. Silver parse ngày theo định dạng Séc |
| **Đơn vị nằm trong tên cột** (`100 JPY`, `1000 ITL`) | Tỷ giá 1 đơn vị = giá trị / số đầu tên cột |
| **Header lặp giữa năm** khi danh sách tiền đổi: 1993 có 3 header, 1995 có 3, 1997 và 1998 mỗi năm 2, 2022 có 2 | `csv_repeated_header: true` (tính năng mới của reader). Thiếu nó thì mọi dòng sau header mới **lệch cột mà không báo lỗi** |
| Tiền cũ trước euro (ATS, DEM, FRF, XEU...) | Dữ liệu thật thời Berka |

### Entry `cnb_czk_rates`

CDC5 với `urls` tĩnh cho năm 1993–1998 (tỷ giá thật thời Berka) và 2017–2026 (trục thời gian sau
khi dời +24 năm). 16 file, khoảng 4.000 dòng. File năm hiện tại đổi mỗi ngày làm việc, nên lịch
đề xuất là hằng ngày lúc 22:00 giờ VN (CNB công bố 14:30 giờ Praha).

Raw mẫu:

```json
{"Date": "04.01.1993", "1 ATS": "2.549", "1 AUD": "19.894", "100 BEF": "87.120", "1 USD": "28.911",
 "_src_file": "1993", "_src_url": "https://www.cnb.cz/...year.txt?year=1993", "_src_row": 1, "...": "..."}
```

Cột không đổi qua các năm, nên mỗi dòng raw là một **map tiền → giá trị** với tập khoá khác nhau.
Silver unpivot thành `(date, currency, units, rate_czk)`.

## 3. Quy đổi ở silver

| Từ | Sang VND |
|---|---|
| 20 đồng của VCB | `transfer` của ngày giao dịch (hoặc ngày làm việc gần nhất trước đó) |
| CZK | CZK → EUR (CNB, cùng ngày) → VND (VCB) |
| MXN, BRL, ILS, BTC (IBM AML) | Tỷ giá ngầm trong chính giao dịch, hoặc giữ đơn vị gốc |
| Ngày trước 06/2020 | Không có VCB: giữ đơn vị gốc |
