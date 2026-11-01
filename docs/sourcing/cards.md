# Giao dịch thẻ (kiểu Sparkov) — luồng chạy liên tục

Giao dịch thẻ của **khách core banking**, sinh liên tục theo cách của Sparkov (Brandon Harris,
MIT), bộ sinh đứng sau bộ Kaggle *Credit Card Transactions Fraud Detection* (kartik2112, 1,85 triệu
giao dịch 2019–2020). Đây là nguồn duy nhất sinh được dữ liệu mới **vô hạn**, nên đóng vai luồng
thời gian thực.

| | |
|---|---|
| Loại | Luồng sự kiện (Kafka) + chargeback trễ (file) |
| Topic | `card.authorizations` (6 partition, key = `card_token`) |
| Entry | `cards_authorizations` (kafka, `src-vpc`, **tắt**), `cards_chargebacks` (s3file, `src-shell`, bật) |
| Dataset raw | `landing/cards/authorizations/`, `landing/cards/chargebacks/` |
| Code | `simulators/cards/stream.py` (`generate`, `replay`) |

```powershell
# (tuỳ chọn) bộ Kaggle để phát lại thay vì sinh mới
kaggle datasets download -d kartik2112/fraud-detection -p data/sparkov --unzip
```

## 1. Hai chế độ

| Chế độ | Lệnh | Chủ thẻ |
|---|---|---|
| **generate** (mặc định) | `generate --days 7 --speed 0` (thời gian nghiệp vụ từ `SIM_EPOCH`), hoặc `generate --live --rate 5` (giờ **thật**) | **Khách core banking**: `core.card` JOIN `disp` JOIN `client`, thẻ đang ACTIVE. Không đọc được corebank thì sinh 1.000 chủ thẻ bằng Faker |
| **replay** | `replay data/sparkov/fraudTrain.csv [--limit N]` | 1.000 khách Mỹ của bộ Kaggle, thời gian dời về `SIM_EPOCH` |

Cách sinh (theo tinh thần Sparkov):

- 14 nhóm merchant của Sparkov (`grocery_pos`, `gas_transport`, `shopping_net`, `misc_net`,
  `travel`...), mỗi nhóm có tỷ trọng, trung vị số tiền và tỷ lệ giao dịch đêm riêng. 800 merchant
  tên Séc, toạ độ trong lãnh thổ Séc.
- Mỗi chủ thẻ có nhịp riêng (0,5–3 giao dịch/ngày). 20% "cú đêm" giao dịch 0–5 giờ sáng.
- Số tiền theo phân phối **gamma**. Giờ giao dịch dồn quanh 15h.
- **Gian lận theo đợt**: mỗi ngày ~0,15% thẻ bị lộ, sinh 3–8 giao dịch liên tiếp lúc 0–4 giờ sáng,
  nhóm `shopping_net`/`misc_net`/`grocery_pos`, 250–1.300 mỗi giao dịch.

## 2. Schema sự kiện (giữ tên cột của Sparkov)

| Trường | Ví dụ | Ghi chú |
|---|---|---|
| `trans_date_trans_time` | `2023-01-01 04:38:38` | Thời gian nghiệp vụ |
| `cc_num` | `495364******8527` | **Đã che** (PCI DSS). Sparkov gốc để số thẻ đầy đủ |
| `card_token` | `4c76dce750ae7520ef7107b1942a6e69` | md5(số thẻ). **Khoá nối về `corebank_card.card_token`**. Cũng là key Kafka |
| `merchant`, `category` | `Horáčková Soukupová s.r.o.`, `grocery_pos` | Bỏ tiền tố `fraud_` vô nghĩa của bộ Kaggle |
| `amt` | `122.18` | Số thực |
| `first`, `last`, `gender`, `dob`, `job` | `Matyáš`, `Vávra`, `M`, `1966-02-14` | **PII** của chủ thẻ (lấy từ `core.client`) |
| `street`, `city`, `state`, `zip`, `lat`, `long`, `city_pop` | `Mlýnská 70`, `Ledvice`, `CZ` | Địa chỉ chủ thẻ |
| `trans_num` | `ad0934ec87c9f6239b3cd4e0b5b4276c` | Mã giao dịch tất định |
| `unix_time` | `1672547918` | |
| `merch_lat`, `merch_long` | `50.434664`, `16.201696` | Khoảng cách chủ thẻ–merchant là đặc trưng gian lận kinh điển |

**Không có `is_fraud`** trong sự kiện. Nhãn nằm ở file chargeback
`landing/_inbox/cards/labels/dt=<ngày>/labels_<YYYYMMDD>.csv.gz`, header `trans_num, is_fraud`,
đến trễ 20 ngày (`--label-delay-days`).

## 3. Entry sourcing

| Entry | Reader | Mốc | Ghi chú |
|---|---|---|---|
| `cards_authorizations` | `kafka`, profile **`edge`** | offset từng partition | Micro-batch 5 phút trên **máy nguồn** (systemd), không qua Glue |
| `cards_chargebacks` | `s3file` csv | khoá S3 | |

Raw của Kafka giữ nguyên bản tin, `value` là chuỗi JSON (như [ibm-aml.md](ibm-aml.md#5-entry-sourcing)).

### 3.1 Triển khai trên AWS: Kafka build dần (10-31)

```
máy nguồn EC2 (spot t3.medium, đã chạy sẵn Redpanda + Postgres core banking)
  fingraph-cards-stream.service   simulators.cards.stream generate --live --rate 2  ─► topic card.authorizations (3 partition, giữ 7 ngày)
        │ chủ thẻ = khách core banking (Postgres cùng máy); nhãn gian lận -> landing/_inbox/cards/labels/ sau 20 ngày
        ▼
  Redpanda :19092
        ▼
  fingraph-kafka-sink.timer (5 phút sau lần trước XONG)  ─► sourcing.main --JOB_NAME cards_authorizations
        │ cấu hình từ job_config, mốc = offset từng partition trong job_log (CKPT#)
        ▼
  landing/cards/authorizations/ingest_date=.../*.ndjson.gz  +  landing/_manifests/cards/authorizations/...
```

**Vì sao consumer chạy trên máy nguồn (profile `edge`) mà không qua Glue:** broker chỉ có IP riêng trong VPC.
Glue Python Shell chạy ngoài VPC nên không tới được. Đưa Glue vào VPC (`src-vpc`) thì cần NAT (~32 USD/tháng)
hoặc VPC endpoint cho S3/DynamoDB/Secrets/SSM/Logs (~7 USD/tháng mỗi endpoint interface). Máy nguồn đã chạy sát
broker nên **không tốn thêm đồng nào**. Framework vẫn y nguyên (`sourcing.main`, cùng job_log, manifest, layout
raw), chỉ khác chỗ chạy.

Ràng buộc của profile `edge` (`sourcing/config.py`): chỉ cho `source_type: kafka`, bắt buộc `schedule.enabled:
false` (lịch nằm ở systemd), và Lambda trigger từ chối entry `edge`.

Vận hành (qua SSM, không cần SSH):

```bash
aws ssm start-session --target <kafka_instance_id> --profile fingraph
sudo journalctl -u fingraph-cards-stream -f                        # producer
sudo systemctl start fingraph-kafka-sink && journalctl -u fingraph-kafka-sink -n 50   # consumer chạy ngay
cd /opt/fingraph && sudo docker compose exec redpanda rpk topic describe card.authorizations
sudo docker compose exec redpanda rpk group describe fingraph-src-cards   # lag consumer
```

Hoặc xem trên Redpanda Console: `.\scripts\console.ps1` (repo hạ tầng).

Bật/tắt: `cards_stream_enabled`, `kafka_sink_jobs` trong `envs/dev/terraform.tfvars` (repo hạ tầng). Đổi
user_data thì **EC2 được dựng lại** (spot one-time): EBS dữ liệu (Redpanda, Postgres) giữ nguyên, IP riêng đổi,
và Terraform tự cập nhật `--KAFKA_BOOTSTRAP` của các Glue job.

**Giai đoạn tiếp theo:**
1. Entry silver `cards_authorizations`: tách `card_transaction` / `merchant` / `card_holder`, khử trùng theo `trans_num`.
2. Streaming thật (Spark Structured Streaming / Glue Streaming A2, `enable_streaming`) để đo độ trễ so với micro-batch. Chỉ bật trong tuần benchmark (~20 USD/ngày).
3. Thêm topic thứ hai: `aml.transfers` (IBM AML replay) dùng cùng consumer `edge`.

## 4. Chất lượng dữ liệu: việc của silver

| Vấn đề | Xử lý |
|---|---|
| Sự kiện dàn phẳng: mỗi dòng lặp lại hồ sơ chủ thẻ | Tách `card_holder` (SCD theo `card_token`), `merchant`, `card_transaction` |
| PII trong sự kiện | Che hoặc phân quyền. Hồ sơ chủ thẻ chuẩn lấy từ `corebank_client` |
| Trùng do at-least-once | Khử theo `trans_num` |
| Chủ thẻ lấy theo thẻ ACTIVE **lúc khởi động** generator | Thẻ bị khoá sau đó vẫn có thể còn giao dịch trong phiên đang chạy. Có thể dùng làm ca kiểm thử "giao dịch trên thẻ đã khoá" |
