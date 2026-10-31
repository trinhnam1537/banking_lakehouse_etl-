# Tầng gold và pre-train — PySpark tự viết, UC2 trên Berka

```
landing ──(YAML: sourcing)──> silver_* ──gold-spark──> track_a_gold ──pretrain-spark──> track_a_pretrain
                                          transforms/gold/            transforms/pretrain/
```

| | sourcing / silver | gold / pre-train |
|---|---|---|
| Logic | entry **YAML** + framework | **PySpark tự viết**: `transforms/<tầng>/<logic>.py`, hàm `run(spark, args)` |
| Glue job | profile dùng chung | `fingraph-dev-gold-spark`, `fingraph-dev-pretrain-spark` (repo hạ tầng `modules/transforms`) |
| Chọn logic | `--JOB_NAME` | `--TRANSFORM` (mặc định theo job), `--LAYER` khoá job vào đúng tầng |
| Ghi | append / merge theo entry | `createOrReplace` Iceberg: tính lại toàn bộ, idempotent, snapshot cũ còn để time travel |

Vì sao không YAML: logic gold/pre-train gồm lưới khách × tháng, cửa sổ trượt, nhãn nhìn về tương lai, chia tập theo
thời gian. Khai báo hoá những thứ này thành YAML là tự viết một ngôn ngữ lập trình nửa vời.

Tầng tiếp theo, ontology + knowledge graph (đặc trưng đồ thị cho UC2, phục vụ AI agent), thiết kế ở
[`docs/ONTOLOGY.md`](ONTOLOGY.md).

Thêm logic mới: tạo module `transforms/<tầng>/<ten>.py` có `run(spark, args) -> dict`, thêm một dòng vào
`transforms/main.py: REGISTRY`, rồi deploy. Không phải sửa hạ tầng.

## UC2 — gợi ý sản phẩm, Berka

### Gold (`transforms/gold/berka_uc2.py`)

**`track_a_gold.berka_product_event`** `(client_id, product, acquired_month, source)`: tháng đầu tiên khách sở hữu
mỗi sản phẩm.

| Sản phẩm | Suy ra từ | Gắn với khách qua |
|---|---|---|
| `loan` | `loan.granted_date` | tài khoản → khách OWNER |
| `card_classic` / `card_junior` / `card_gold` | `card.issued_date`, `card_type` | disp → khách (OWNER hoặc DISPONENT) |
| `insurance` | giao dịch **đầu tiên** `k_symbol = POJISTNE` | tài khoản → OWNER |
| `household` | giao dịch đầu tiên `k_symbol = SIPO` | tài khoản → OWNER |

Bảng `standing_order` của Berka là ảnh chụp cuối kỳ, **không có ngày tạo**. Vì vậy thời điểm bắt đầu bảo hiểm /
thanh toán hộ gia đình lấy từ khoản trừ đầu tiên trên sổ giao dịch, đó là mốc sớm nhất quan sát được.

**`track_a_gold.berka_client_month`** `(client_id, month)`: ảnh chụp cuối tháng, từ tháng mở tài khoản đầu tiên
tới tháng cuối của dữ liệu.

| Nhóm | Cột |
|---|---|
| Hồ sơ | `gender`, `age`, `district_id`, `region`, `district_avg_salary`, `district_unemployment_96`, `district_urban_ratio`, `district_inhabitants`, `district_entrepreneurs_per_1000`, `n_accounts`, `tenure_months` |
| Hành vi 1 tháng | `txn_cnt`, `inflow`, `outflow`, `avg_balance`, `min_balance`, `end_balance` (forward-fill khi tháng không giao dịch), `cash_wd_cnt`, `card_wd_cnt`, `inflow_trend` |
| Hành vi 3 tháng | `txn_cnt_3m`, `inflow_3m`, `outflow_3m`, `net_flow_3m`, `avg_balance_3m`, `min_balance_3m`, `cash_wd_cnt_3m`, `card_wd_cnt_3m`, `pension_3m`, `overdraft_3m` |
| Sản phẩm | `has_<sản phẩm>` (đang giữ tại cuối tháng), `n_products` |

**Bất biến point-in-time:** mọi cột của tháng `m` chỉ dùng giao dịch có `trans_date` ≤ cuối tháng `m`.

### Pre-train (`transforms/pretrain/uc2_next_product.py`)

**`track_a_pretrain.uc2_next_product`**: mỗi dòng là `(khách, tháng chụp m, sản phẩm p)` với `p` **chưa giữ** tại `m`.

- `label = 1` nếu khách mua `p` trong `(m, m + H]`, mặc định `H = 3` tháng (`--HORIZON_MONTHS`).
- Đặc trưng: mọi cột của `berka_client_month` tại `m`, kể cả `has_*` của sản phẩm khác (sản phẩm đã giữ là tín
  hiệu mạnh cho sản phẩm tiếp theo).
- Dạng **dài** (một dòng một cặp khách × sản phẩm): đọc thẳng được cho mô hình propensity theo từng sản phẩm, cho
  LightGBM `lambdarank` (nhóm theo `client_id, month`), hoặc two-tower.
- Partition theo `split`.

Chống rò rỉ tương lai (lý do tầng này tách khỏi gold):

1. Đặc trưng ở `m` chỉ dùng dữ liệu ≤ `m` (bất biến của gold).
2. Bỏ cặp mà khách đã giữ sản phẩm tại `m`.
3. Chia tập **theo thời gian**: `train` < `VALID_START` (1997-07) ≤ `valid` < `TEST_START` (1998-01) ≤ `test`.
4. Bỏ tháng chụp có `m + H` vượt tháng cuối của dữ liệu. Nhãn 0 ở đó nghĩa là "chưa biết", không phải "không mua".
5. Bỏ tháng có `tenure_months < MIN_TENURE` (3), vì chưa đủ lịch sử cho đặc trưng 3 tháng.

**`track_a_pretrain.uc2_next_product_stats`** `(split, product)`: số dòng, số dương, tỉ lệ dương, số khách. Xem bảng
này trước khi huấn luyện để biết độ mất cân bằng và tập nào rỗng.

## Chạy

```powershell
aws glue start-job-run --job-name fingraph-dev-gold-spark --profile fingraph
aws glue start-job-run --job-name fingraph-dev-pretrain-spark --profile fingraph
# tham so khac:
aws glue start-job-run --job-name fingraph-dev-pretrain-spark --profile fingraph `
  --arguments '{\"--HORIZON_MONTHS\":\"6\",\"--FEATURE_VERSION\":\"v1_h6\"}'
```

```sql
-- Athena, workgroup fingraph-dev-data-engineer hoac fingraph-dev-analyst
SELECT split, product, rows, positives, positive_rate FROM track_a_pretrain.uc2_next_product_stats ORDER BY 1, 2;
SELECT * FROM track_a_pretrain.uc2_next_product WHERE split = 'train' AND label = 1 LIMIT 20;
```

Quyền (Lake Formation, repo hạ tầng `modules/governance`): role `fingraph-dev-xfm-glue` được **SELECT** trên
`silver_*` và `track_a_gold`, **ALL** trên `track_a_gold` và `track_a_pretrain`. DE có ALL. DA/BA được SELECT
`track_a_pretrain`.
