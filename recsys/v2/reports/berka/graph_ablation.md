# O10: đặc trưng đồ thị cho hệ gợi ý Berka (2026-11-22)

Tập test 40,310 dòng (khách × tháng), tháng 1998-01-01 .. 1998-09-01. Cùng mô hình champion, cùng tham số, chỉ khác đặc trưng. Sinh bởi `python -m recsys.v2.ablation`; 3 seed (trung bình ± độ lệch chuẩn).

## 1. Xếp hạng theo khách (test)

| Chỉ số | Không đồ thị | Đồ thị (tỉ lệ thô) | Đồ thị (chuẩn hoá theo tháng) |
|---|---:|---:|---:|
| hit@1 | 0.8130 ± 0.0119 | 0.7996 ± 0.0089 | 0.8135 ± 0.0149 |
| mrr | 0.9009 ± 0.0070 | 0.8877 ± 0.0057 | 0.9001 ± 0.0099 |
| map@3 | 0.8993 ± 0.0076 | 0.8828 ± 0.0066 | 0.8985 ± 0.0111 |
| ndcg@3 | 0.9238 ± 0.0062 | 0.9080 ± 0.0059 | 0.9232 ± 0.0094 |
| recall@3 | 0.9937 ± 0.0024 | 0.9805 ± 0.0048 | 0.9937 ± 0.0044 |

Chênh lệch so với không đồ thị (seed 0, bootstrap ghép cặp 1000 lần, CI 95%):

- Đồ thị (tỉ lệ thô): Δ hit@1 -0.0192 [-0.0351; -0.0022]; Δ map@3 -0.0165 [-0.0253; -0.0078]; Δ ndcg@3 -0.0143 [-0.0213; -0.0074]
- Đồ thị (chuẩn hoá theo tháng): Δ hit@1 -0.0262 [-0.0416; -0.0102]; Δ map@3 -0.0197 [-0.0280; -0.0111]; Δ ndcg@3 -0.0164 [-0.0229; -0.0096]

## 2. Nhắm khách theo sản phẩm (PR-AUC, test)

| Sản phẩm | Ca dương | Không đồ thị | Đồ thị (tỉ lệ thô) | Đồ thị (chuẩn hoá theo tháng) |
|---|---:|---:|---:|---:|
| loan | 338 | 0.1706 ± 0.0074 | 0.1740 ± 0.0032 | 0.1722 ± 0.0050 |
| card_classic | 770 | 0.0896 ± 0.0009 | 0.0844 ± 0.0034 | 0.0971 ± 0.0009 |
| card_junior | 151 | 0.1344 ± 0.0124 | 0.1437 ± 0.0052 | 0.1492 ± 0.0128 |
| card_gold | 129 | 0.0420 ± 0.0021 | 0.0284 ± 0.0018 | 0.0387 ± 0.0019 |

## 3. Đóng góp từng nhóm (bản chuẩn hoá, seed 0)

Bỏ một nhóm (các nhóm khác giữ nguyên):

| dropped | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|
| g_district | 0.8013 | 0.8890 | 0.9152 |
| g_region | 0.8159 | 0.9016 | 0.9259 |
| g_cohold | 0.7896 | 0.8822 | 0.9093 |
| g_partner | 0.7962 | 0.8899 | 0.9163 |
| g_trans | 0.8152 | 0.9005 | 0.9249 |
| g_cohold_deg | 0.8137 | 0.9000 | 0.9245 |
| g_partner_deg | 0.8196 | 0.9030 | 0.9267 |

Chỉ giữ một nhóm (cộng vào đặc trưng gốc):

| only | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|
| g_district | 0.8123 | 0.8985 | 0.9231 |
| g_region | 0.8042 | 0.8934 | 0.9186 |
| g_cohold | 0.8093 | 0.8959 | 0.9207 |
| g_partner | 0.8137 | 0.8992 | 0.9238 |
| g_trans | 0.7940 | 0.8835 | 0.9101 |

Tỉ trọng gain của đặc trưng đồ thị (bản chuẩn hoá): 19.2%. Mười đặc trưng đứng đầu: `product` 0.213, `inflow_3m` 0.137, `tenure_months` 0.085, `cash_wd_cnt_3m` 0.070, `avg_balance_3m` 0.063, `age` 0.059, `outflow_3m` 0.055, `own_g_trans` 0.046, `avg_balance` 0.038, `min_balance_3m` 0.030.

## 4. Trôi theo thời gian, độ phủ, rò rỉ

| Đặc trưng | Có giá trị (test) | PSI train→test, thô | PSI train→test, chuẩn hoá |
|---|---:|---:|---:|
| `g_district_loan` | 100.0% | 1.346 | 0.729 |
| `g_district_card_classic` | 100.0% | 3.083 | 1.901 |
| `g_district_card_junior` | 100.0% | 0.990 | 0.448 |
| `g_district_card_gold` | 100.0% | 1.115 | 0.184 |
| `g_region_loan` | 100.0% | 11.262 | 1.542 |
| `g_region_card_classic` | 100.0% | 12.380 | 3.355 |
| `g_region_card_junior` | 100.0% | 8.137 | 6.247 |
| `g_region_card_gold` | 100.0% | 9.090 | 1.689 |
| `g_cohold_loan` | 19.3% | 0.000 | 0.000 |
| `g_cohold_card_classic` | 19.3% | 0.000 | 0.000 |
| `g_cohold_card_junior` | 19.3% | 0.000 | 0.000 |
| `g_cohold_card_gold` | 19.3% | 0.000 | 0.000 |
| `g_partner_loan` | 0.5% | 0.000 | 8.912 |
| `g_partner_card_classic` | 0.5% | 0.000 | 0.000 |
| `g_partner_card_junior` | 0.5% | 0.000 | 0.000 |
| `g_partner_card_gold` | 0.5% | 0.000 | 0.000 |
| `g_trans_loan` | 76.4% | 0.034 | 0.034 |
| `g_trans_card_classic` | 76.3% | 0.002 | 0.002 |
| `g_trans_card_junior` | 72.1% | 0.000 | 0.000 |
| `g_trans_card_gold` | 72.1% | 0.000 | 0.000 |
| `g_cohold_deg` | 100.0% | 0.000 | 0.000 |
| `g_partner_deg` | 100.0% | 0.000 | 0.000 |

PSI > 0,25 là trôi đáng kể.
Tương quan lớn nhất giữa đặc trưng đồ thị và nhãn của chính sản phẩm: `g_partner_card_classic` +0.312 (ngưỡng nghi rò rỉ 0,5).
Đồ thị chuyển tiền: 25 / 6,064 tài khoản đối tác có từ 2 khách trở lên (Berka ẩn danh tài khoản ngoài ngân hàng, nên gần như không có hàng xóm hai bước).
