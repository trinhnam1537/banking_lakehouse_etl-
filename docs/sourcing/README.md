# Nguồn dữ liệu của FinGraph — tổng quan

Thư mục này mô tả **mọi nguồn dữ liệu** mà tầng sourcing kéo về `landing/`. Tài liệu này là
bản đồ chung. Mỗi nguồn có một tài liệu riêng ghi đầy đủ bảng, schema, credential, entry, luồng
giả lập và chất lượng dữ liệu.

| Muốn biết | Đọc |
|---|---|
| Kiến trúc tầng sourcing (orchestrator, Glue, checkpoint) | [../SOURCING-ARCHITECTURE.md](../SOURCING-ARCHITECTURE.md) |
| Rà soát framework đọc/ghi: đã sửa gì, vì sao, còn gì | [framework-review.md](framework-review.md) |
| Cách viết một entry YAML | [../../sourcing_etl/README.md](../../sourcing_etl/README.md) |
| Chi tiết một nguồn | bảng ngay dưới đây |

---

## 1. Danh mục nguồn

| # | Nguồn | Vai trò trong ngân hàng giả lập | Đường vào | Reader | Entry | Tài liệu |
|---|---|---|---|---|---|---|
| 0 | **Berka** (CTU, dữ liệu thật đã ẩn danh) | Bản gốc của core banking: đọc thẳng server công khai | MariaDB công khai | `jdbc` | 8 `berka_*` | [berka.md](berka.md) |
| 1 | **Core banking** (Berka + simulator) | Khách hàng, tài khoản, thẻ, khoản vay, sổ giao dịch | Postgres `corebank` | `jdbc` | 8 `corebank_*` | [corebank.md](corebank.md) |
| 2 | **IBM AML** (NeurIPS 2023) | Chuyển tiền liên ngân hàng, có nhãn rửa tiền | Kafka + file S3 | `kafka`, `s3file` | 4 `aml_*` | [ibm-aml.md](ibm-aml.md) |
| 3 | **PaySim** | Đối tác ví điện tử, file đối soát cuối ngày | File EOD lên S3 | `s3file` | 2 `paysim_*` | [paysim.md](paysim.md) |
| 4 | **Thẻ kiểu Sparkov** | Giao dịch thẻ của khách core banking, chạy liên tục | Kafka + file S3 | `kafka`, `s3file` | 2 `cards_*` | [cards.md](cards.md) |
| 5 | **Home Credit** | Hệ thống cho vay, lịch sử tín dụng (≈ CIC) | Postgres `lending` | `jdbc`, `s3file` | 8 `lending_*` | [lending.md](lending.md) |
| 6 | **OpenSanctions** | Danh sách cấm vận cho sàng lọc AML | Tải file HTTP | `httpfile` | 3 `opensanctions_*` | [opensanctions.md](opensanctions.md) |
| 7 | **Tỷ giá VCB + CNB** | Quy đổi về VND (VCB) và CZK (CNB) | REST JSON, file HTTP | `api`, `httpfile` | 2 | [fx-rates.md](fx-rates.md) |
| 8 | **OneDrive cá nhân** | Người dùng (BA, đối tác) kéo thả file CSV/Excel, kéo về hằng ngày | Microsoft Graph (refresh token OAuth) | `onedrive` | 1 `onedrive_uploads` (tắt) | [onedrive.md](onedrive.md) |

Tổng cộng **38 entry** trong `sourcing_etl/sources/`. GLEIF và FDIC (nguồn cũ) đã được gỡ ở
commit `4d70a79`. Muốn dùng lại thì lấy entry từ lịch sử git.

### Trạng thái từng entry

| Trạng thái | Entry | Vì sao |
|---|---|---|
| **Bật, chạy được ngay trên AWS** (profile `src-shell`) | `berka_*`, `opensanctions_*`, `vcb_exchange_rates`, `cnb_czk_rates`, mọi entry `s3file` (`aml_accounts`, `aml_patterns`, `aml_labels`, `paysim_*`, `cards_chargebacks`, `lending_default_labels`) | Nguồn công khai trên internet hoặc file trong S3 của mình |
| **Bật, có lịch hằng ngày** (profile `src-shell`) | `corebank_*` | Postgres core banking trên máy EC2 nguồn, mở 5432 **chỉ SSL**. Chạy được sau khi apply PR hạ tầng (mục 7) |
| **Tắt, chờ hạ tầng** (profile `src-vpc`) | `lending_*` (JDBC), `aml_transfers`, `cards_authorizations` | Chưa dựng trên AWS, xem [mục 7](#7-hạ-tầng) |

Mọi entry đều đã **chạy thật end-to-end trên stack local** (mục 6), ngày 13-10-2026.

**Silver:** core banking là nguồn đầu tiên thông luồng **nguồn → landing → silver**: 8 entry silver
trong `silver_etl/entries/corebank/`, chạy bằng `python -m local.run_flow corebank`. Chi tiết ở
[corebank.md mục 6](corebank.md#6-silver-silver_corebank). Các nguồn khác chưa có entry silver. Lịch
(`schedule.enabled`) của mọi entry đang tắt theo quy ước của repo: chạy tay thành công rồi mới bật.

---

## 2. Bức tranh chung

```
 HỆ THỐNG NGUỒN (giả lập)                  ĐƯỜNG VÀO                     ENTRY → landing/<dataset>/
 ─────────────────────────                  ─────────                     ──────────────────────────
 Postgres corebank ◀── corebank.simulate    JDBC (SQL trong entry)        corebank_*      corebank/*
   (Berka dời +24 năm)
 Postgres lending  ◀── lending.simulate     JDBC                          lending_*       lending/*
   (Home Credit / tổng hợp)
 Kafka aml.interbank.transfers ◀── aml.replay stream                      aml_transfers   aml/transfers
 Kafka card.authorizations     ◀── cards.stream                           cards_authorizations
 S3 landing/_inbox/  ◀── aml.replay static, paysim.eod, nhãn trễ          aml_*, paysim_*, *_labels
 data.opensanctions.org (thật)              HTTP file (CSV/NDJSON)        opensanctions_*
 vietcombank.com.vn, cnb.cz (thật)          REST JSON / HTTP file         vcb_exchange_rates, cnb_czk_rates
```

Ba quyết định thiết kế xuyên suốt mọi nguồn:

### 2.1 Một trục thời gian chung

Các bộ dữ liệu đến từ những thời kỳ khác nhau: Berka 1993–1998, IBM AML 09/2022, Sparkov
2019–2020, còn PaySim và Home Credit không có ngày tuyệt đối. Simulator dời tất cả về cùng một
trục:

| Mốc | Ý nghĩa |
|---|---|
| ≤ 2022-12-31 | **Lịch sử**: Berka dời +24 năm (2017–2022), hồ sơ vay Home Credit trong năm 2022 |
| `SIM_EPOCH` = **2023-01-01** | Ngày nghiệp vụ đầu tiên do simulator sinh. IBM AML, PaySim và thẻ bắt đầu phát từ đây |
| từ ~06/2020 | Tỷ giá VCB có lịch sử, quy đổi được mọi thứ trên trục này sang VND |

Raw giữ **nguyên** những gì hệ thống nguồn gửi. Việc dời ngày nằm ở simulator, tức là "trong hệ
thống nguồn", không nằm ở tầng sourcing.

### 2.2 Danh tính: nối gì được với gì

Các bộ dữ liệu gốc **không có khoá chung**. Simulator tạo một số liên kết có chủ đích, và tài
liệu ghi rõ đây là **liên kết giả lập**:

| Liên kết | Khoá | Ghi chú |
|---|---|---|
| Thẻ ↔ core banking | `card_token` = md5(số thẻ) | Có ở `corebank_card` và `cards_authorizations`. Số thẻ đầy đủ không bao giờ xuống raw |
| Hồ sơ vay ↔ khách core banking | `lending.application.customer_ref` = `core.client.client_id` | Khoảng 70% hồ sơ có khách tương ứng, phần còn lại là khách chưa mở tài khoản |
| Khách ↔ danh sách cấm vận | so tên (`core.client.full_name` với `opensanctions`) | `corebank.seed --sanction-hits N` gieo N ca trúng. Đáp án nằm ở `data/ground_truth/` |
| OpenSanctions ↔ GLEIF | `leiCode` trong entity FtM | Khoá thật duy nhất giữa các nguồn công khai |
| IBM AML, PaySim | — | Hai "vũ trụ" riêng: đối tác liên ngân hàng và ví điện tử |

### 2.3 Nhãn đến trễ, không đi cùng giao dịch

Bốn nguồn có nhãn: rửa tiền (IBM AML), gian lận (PaySim, thẻ) và vỡ nợ (Home Credit). Ở ngân
hàng thật, nhãn chỉ có **sau** điều tra, chargeback hay quá hạn. Vì vậy simulator:

- **bỏ nhãn khỏi luồng giao dịch** (Kafka, file EOD, DB nguồn), và
- ghi nhãn thành file riêng `landing/_inbox/<nguồn>/labels/dt=<ngày>/`, **trễ N ngày nghiệp vụ**
  (AML 14 ngày, thẻ 20 ngày, PaySim 30 ngày, vay 90 ngày sau giải ngân).

Có tách như vậy thì demo "phát hiện thời gian thực" mới không bị rò nhãn.

---

## 3. Credential và kết nối

| Secret (Secrets Manager) | Dạng | Dùng bởi | Ghi chú |
|---|---|---|---|
| `fingraph/src/berka_ctu` | `{host, port, username, password, dbname}` | `berka_*` | Tài khoản **guest công khai** của CTU, Terraform tạo ở repo hạ tầng |
| `fingraph/src/corebank_pg` | như trên | `corebank_*` | User **`etl_reader` chỉ có quyền SELECT**, tạo trong `schema.sql`. **Chưa có trên AWS** |
| `fingraph/src/lending_pg` | như trên | `lending_*` | Như trên. **Chưa có trên AWS** |
| (không cần) | — | `opensanctions_*`, `vcb_*`, `cnb_*`, `s3file` | Nguồn công khai, hoặc S3 qua IAM role của Glue |

Quy tắc chung, đã có test canh:

- Entry chỉ khai **tên** secret. Giá trị nằm trong Secrets Manager, dưới tiền tố `fingraph/src/`
  (Glue chỉ đọc được tiền tố này).
- Reader JDBC mở phiên **READ ONLY** (mặc định `read_only: true`). Câu SQL do entry tự viết,
  nên hàng rào này nằm ở phía database.
- Mật khẩu `etl_reader`/`etl_reader` ở trên **chỉ dành cho stack local**. Trên AWS phải tạo
  mật khẩu thật bằng tay, không đưa vào Terraform (state lưu giá trị dạng rõ).

---

## 4. Dữ liệu cá nhân và nhạy cảm

| Nguồn | Trường nhạy cảm | Xử lý |
|---|---|---|
| `corebank_client` | `full_name`, `national_id`, `birth_date`, `email`, `phone`, `address` | Giả lập nhưng có hình dạng như thật. Coi là PII: silver che và phân quyền |
| `corebank_card` | số thẻ | **Che ngay trong SQL** (`card_number_masked`, `card_token`). PAN không xuống raw |
| `cards_authorizations` | `cc_num`, họ tên, địa chỉ, ngày sinh | `cc_num` đã che từ producer, các trường còn lại là PII |
| `opensanctions_*` | tên và ngày sinh của người thật | Dữ liệu công khai, giấy phép CC BY-NC 4.0 |
| `lending_application` | thu nhập, nghề nghiệp | PII tài chính |

## 5. Giấy phép

| Nguồn | Giấy phép | Hệ quả |
|---|---|---|
| Berka | PKDD'99 Discovery Challenge, dùng cho nghiên cứu | OK |
| IBM AML | CDLA-Sharing 1.0 | OK, ghi nguồn |
| PaySim | CC BY-SA 4.0 | OK |
| Sparkov (generator) | MIT. Bộ Kaggle của kartik2112: xem trang dataset | OK |
| Home Credit | **Luật cuộc thi Kaggle**: không phân phối lại | Không commit, không để bucket public. `data/` đã có trong `.gitignore` |
| OpenSanctions | **CC BY-NC 4.0** | Phi thương mại (đồ án) thì OK. Ghi nguồn |
| VCB, CNB | Dữ liệu công bố trên website | Endpoint web, không phải API chính thức, có thể đổi |

---

## 6. Chạy toàn bộ trên máy (stack local)

Stack local mô phỏng đúng hạ tầng AWS: Postgres ×2, Redpanda, và **moto** giả lập S3, DynamoDB
và Secrets Manager. Framework sourcing chạy **nguyên bản** trên đó.

```powershell
# 0. Thư viện (một lần)
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt -r simulators\requirements.txt

# 1. Hệ thống nguồn + "AWS" giả lập
docker compose -f local/docker-compose.yml up -d
.\.venv\Scripts\python.exe -m local.bootstrap          # bucket, bảng job_log, secret, topic

# 2. Nạp lịch sử và sinh dữ liệu mới
python -m simulators.corebank.seed --limit-accounts 300 --sanction-hits 3   # bỏ --limit để nạp đủ
python -m simulators.corebank.simulate --days 30
python -m simulators.lending.seed --applicants 2000
python -m simulators.lending.simulate --days 100
python -m simulators.aml.replay synth --transactions 50000                  # hoặc tải bộ Kaggle
python -m simulators.aml.replay static --dataset SYN
python -m simulators.aml.replay stream --dataset SYN
python -m simulators.paysim.eod synth --rows 60000                          # hoặc tải bộ Kaggle
python -m simulators.paysim.eod publish --days 5
python -m simulators.cards.stream generate --days 3

# 3. Kéo về landing bằng framework thật
python -m local.run_entry corebank_trans
python -m local.run_entry aml_transfers --show 1
python -m local.run_entry vcb_exchange_rates --override api.date_window.max_windows=3
```

| Địa chỉ | Dịch vụ |
|---|---|
| `localhost:5433` | Postgres `corebank` (chủ DB `corebank`/`corebank`, ETL `etl_reader`/`etl_reader`) |
| `localhost:5434` | Postgres `lending` (`lending`/`lending`, ETL `etl_reader`/`etl_reader`) |
| `localhost:19092` | Kafka (Redpanda) |
| `http://localhost:8088` | Redpanda Console để xem topic |
| `http://localhost:5050` | moto (S3, DynamoDB, Secrets Manager). Dữ liệu nằm trong RAM: restart là mất |

`local.run_entry` khác Glue đúng hai chỗ: ép `enabled: true` và profile `src-shell`. Phần còn lại
(checkpoint, writer, manifest, CDC) là code thật.

### Kết quả lần chạy kiểm chứng (13-10-2026)

| Entry | Bản ghi | Ghi chú |
|---|---|---|
| `berka_trans` (server CTU thật) | 1.056.320 | **22 file** (writer cuộn 50.000 dòng/file; cách cũ là ~212 file), chia khúc 50.000, phiên READ ONLY |
| `corebank_trans` | 71.642, rồi 133 | Lần 2 chỉ kéo 2 ngày simulator vừa sinh: CDC1 chia khúc hoạt động đúng |
| `opensanctions_entities` | 305.356 | NDJSON ~370 MB đọc theo luồng, 4 file raw |
| `lending_bureau_balance` | 164.280 | 4 file, chia khúc 100.000 dòng |
| `aml_transfers` | 50.051 | 6 partition, mỗi partition một mốc offset |
| `opensanctions_targets` | 73.595 | File 74 MB, **RAM đỉnh 211 MB** (đọc theo luồng), 75 giây |
| `opensanctions_delta` | 283 | 2 phiên bản delta |
| `cnb_czk_rates` | 3.978 | 16 file năm, header lặp giữa năm đã xử lý |
| `vcb_exchange_rates` | 3 | 3 ngày từ 2020-06-01 |

---

## 7. Hạ tầng

### 7.1 Core banking trên AWS (đã viết, chờ apply, 14-10-2026)

Phương án đã chọn: **Postgres public chỉ SSL**. Glue `src-shell` chạy ngoài VPC (không có IP cố
định), nên đọc Postgres qua IP public của máy nguồn.

```
EC2 nguồn (t3.medium spot, public subnet; module kafka của repo hạ tầng)
  ├─ Redpanda (như cũ)
  ├─ Postgres corebank :5432   pg_hba: CHỈ hostssl + scram-sha-256; chứng chỉ tự ký
  ├─ corebank-init.sh  (mỗi lần boot) sinh/đọc lại mật khẩu -> Secrets Manager, ghi IP public mới
  └─ timer 17:00 UTC   tải etl.zip của bản đang deploy -> seed Berka nếu DB rỗng -> simulate 1 ngày
Secrets Manager
  fingraph/src/corebank_pg      etl_reader (CHỈ SELECT) + sslmode=require   <- Glue src-shell
  fingraph/sim/corebank_owner   chủ DB                                       <- simulator trên EC2
EventBridge Schedule 18:00 UTC -> Lambda trigger -> Glue src-shell (corebank_*) -> manifest
  -> "Job Run Succeeded" -> Lambda trigger -> Glue slv-spark (8 bảng silver_corebank)
```

| Rủi ro | Giảm thiểu |
|---|---|
| Cổng 5432 mở ra internet, bị quét và dò mật khẩu | Chỉ nhận SSL. scram-sha-256. Mật khẩu 48 ký tự hex ngẫu nhiên. Glue dùng user chỉ SELECT, phiên READ ONLY. `log_connections=on` |
| Mật khẩu lọt qua Terraform state | Terraform **chỉ tạo vỏ secret**. Mật khẩu do máy tự sinh |
| Chứng chỉ tự ký, không xác thực máy chủ (sslmode=require) | Chấp nhận ở dev: đường truyền vẫn được mã hoá. Lên prd thì dùng RDS + `verify-full` |
| IP public đổi khi spot bị thay máy | Mỗi lần boot, máy ghi lại `host` vào cả hai secret |

Trước đó đã cân nhắc và bỏ hai phương án: Glue trong VPC với NAT (~32 USD/tháng, hoặc dùng NAT
instance) và VPC endpoint (~15 USD/tháng, phải đặt wheel lên S3). Lý do: tốn chi phí và công sức hơn
cho môi trường dev.

### 7.2 Còn thiếu cho các nguồn khác

Các entry `lending_*` (JDBC) và Kafka (`aml_transfers`, `cards_authorizations`) vẫn dùng profile
**`src-vpc`**, chưa có ở repo hạ tầng. Hai cách: làm như core banking (thêm container Postgres
`lending`, hoặc Redpanda listener SASL/SSL public), hoặc dựng `src-vpc` theo đề xuất dưới đây:

| Hạng mục | Đề xuất | Chi phí ước tính |
|---|---|---|
| Máy "hệ thống nguồn" | Dùng lại EC2 Redpanda (t3.medium spot) đang có: thêm 2 container Postgres vào `modules/kafka/docker-compose.yml`, dữ liệu trên EBS rời | ~0 (máy đã có) |
| Glue profile `src-vpc` | Thêm vào `var.profiles`: `python_modules = [psycopg2-binary, confluent-kafka, PyMySQL]` cùng **Glue NETWORK connection** (private subnet + SG được vào 5432/19092) | 0 |
| Endpoint cho Glue trong VPC | S3 Gateway (đã có), **DynamoDB Gateway** (miễn phí), **Secrets Manager Interface** (bắt buộc, vì không có NAT) | ~7–8 USD/tháng (1 AZ) |
| Secret | `fingraph/src/corebank_pg`, `fingraph/src/lending_pg`, tạo bằng tay với mật khẩu thật | ~0,8 USD/tháng |
| Simulator | Chạy trên chính EC2 đó (cron/systemd), đặt `SIM_TARGET=aws`, `RAW_BUCKET=<bucket>` | 0 |

Phương án thay thế rẻ hơn nhưng yếu hơn: RDS public cộng SSL. Glue không có IP cố định nên phải
mở cổng 5432 ra internet. Không khuyến nghị.

**Sau khi apply:** xoá `"src-vpc"` khỏi `PENDING_PROFILES` trong `sourcing/config.py`, đặt
`enabled: true` cho các entry tương ứng. pytest sẽ chặn nếu làm ngược thứ tự.
