# Đánh giá offline hệ gợi ý v2: berka (2026-11-19)

Mô hình `v2-20261119-2256`; tập test 40,310 dòng (khách × tháng), tháng 1998-01-01, 1998-02-01, 1998-03-01, 1998-04-01, 1998-05-01, 1998-06-01, 1998-07-01, 1998-08-01, 1998-09-01.
Sinh bởi `python -m recsys.v2.run evaluate`. Giải thích từng nhóm chỉ số: docs/RECSYS-V2.md mục 4.

## 1. Độ chính xác xếp hạng (từng khách, dòng có ít nhất 1 sản phẩm được mua)

| model | rows | hit@1 | mrr | map@3 | ndcg@3 | recall@3 |
|---|---|---|---|---|---|---|
| v2 (xep theo calibrated, so sanh) | 1369 | 0.8313 | 0.9108 | 0.9098 | 0.9323 | 0.9963 |
| v2 (xep theo raw, champion) | 1369 | 0.7860 | 0.8863 | 0.8848 | 0.9132 | 0.9942 |
| popularity | 1369 | 0.3514 | 0.6376 | 0.6180 | 0.6972 | 0.9233 |
| cooccurrence | 1369 | 0.3514 | 0.6376 | 0.6180 | 0.6972 | 0.9233 |

**Bootstrap ghép cặp 1000 lần, chênh lệch so với `popularity` (CI 95%):**

| Mô hình | Δ Hit@1 | Δ MAP@3 [CI 95%] | Δ NDCG@3 |
|---|---:|---|---:|
| v2 (xep theo raw, champion) | +0.4350 | +0.2669 [+0.2503; +0.2835] | +0.2160 |
| v2 (xep theo calibrated, so sanh) | +0.4801 | +0.2918 [+0.2746; +0.3076] | +0.2350 |
| cooccurrence | +0.0000 | +0.0000 [+0.0000; +0.0000] | +0.0000 |

## 2. Nhắm khách theo sản phẩm (v2)

| product | pos | rate | roc_auc | pr_auc | lift@1% | recall@5% |
|---|---|---|---|---|---|---|
| loan | 338 | 0.0097 | 0.9411 | 0.1829 | 27.2 | 0.6627 |
| card_classic | 770 | 0.0212 | 0.8699 | 0.0897 | 6.0960 | 0.2506 |
| card_junior | 151 | 0.0038 | 0.9720 | 0.1248 | 35.1 | 0.8940 |
| card_gold | 129 | 0.0032 | 0.9419 | 0.0452 | 19.4 | 0.6589 |

## 3. Hiệu chỉnh xác suất (trước / sau isotonic)

| Sản phẩm | Tỉ lệ thật | p TB trước | p TB sau | Brier trước | Brier sau | Brier tham chiếu | ECE trước | ECE sau |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| loan | 0.9691% | 0.8088% | 1.1316% | 0.00866 | 0.00855 | 0.00960 | 0.00429 | 0.00163 |
| card_classic | 2.1181% | 0.8210% | 1.4580% | 0.02033 | 0.01983 | 0.02073 | 0.01420 | 0.00660 |
| card_junior | 0.3835% | 0.2561% | 0.2163% | 0.00366 | 0.00352 | 0.00382 | 0.00335 | 0.00167 |
| card_gold | 0.3237% | 0.1975% | 0.2343% | 0.00318 | 0.00319 | 0.00323 | 0.00297 | 0.00098 |

Brier tham chiếu = dự đoán hằng số bằng tỉ lệ nền. ECE tính theo 10 bin phân vị.

## 4. Ngoài độ chính xác (gợi ý top-1)

| Mô hình | Độ phủ | Gini hiển thị | Độ mới | Cá nhân hoá | Tỉ trọng hiển thị theo sản phẩm |
|---|---:|---:|---:|---:|---|
| v2 (xep theo raw, champion) | 1.00 | 0.456 | - | 0.572 | 55%, 36%, 6%, 4% |
| v2 (xep theo calibrated, so sanh) | 1.00 | 0.262 | - | 0.678 | 37%, 31%, 3%, 29% |
| popularity | 0.75 | 0.674 | - | 0.235 | 87%, 12%, 2%, 0% |
| cooccurrence | 0.75 | 0.674 | - | 0.235 | 87%, 12%, 2%, 0% |

## 5. Phân khúc (v2)

**tham nien** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.898):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| 13-24 | 524 | 0.7977 | 0.8899 | 0.9170 |
| 7-12 | 326 | 0.7546 | 0.8640 | 0.8953 |
| <=6 thang | 82 | 0.6463 | 0.8150 | 0.8631 |
| >24 | 437 | 0.8215 | 0.9073 | 0.9314 |

**gioi tinh** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.989):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| nam | 690 | 0.7812 | 0.8800 | 0.9092 |
| nu | 679 | 0.7909 | 0.8898 | 0.9173 |

**vung** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.880):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| Prague | 222 | 0.7793 | 0.8829 | 0.9132 |
| central Bohemia | 181 | 0.7790 | 0.8831 | 0.9134 |
| east Bohemia | 159 | 0.8176 | 0.9046 | 0.9294 |
| north Bohemia | 152 | 0.6776 | 0.8180 | 0.8615 |
| north Moravia | 191 | 0.7277 | 0.8438 | 0.8763 |
| south Bohemia | 130 | 0.8538 | 0.9231 | 0.9430 |
| south Moravia | 200 | 0.8200 | 0.9083 | 0.9323 |
| west Bohemia | 134 | 0.8582 | 0.9291 | 0.9477 |

**so du** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.959):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| cao | 1078 | 0.7690 | 0.8771 | 0.9080 |
| thap | 59 | 0.8644 | 0.9096 | 0.9241 |
| vua | 232 | 0.8448 | 0.9145 | 0.9345 |

## 6. Ổn định theo thời gian

| month | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| 1998-01-01 | 124 | 0.8065 | 0.8952 | 0.9203 |
| 1998-02-01 | 131 | 0.7099 | 0.8435 | 0.8803 |
| 1998-03-01 | 136 | 0.7426 | 0.8640 | 0.8975 |
| 1998-04-01 | 154 | 0.8247 | 0.9026 | 0.9245 |
| 1998-05-01 | 155 | 0.8129 | 0.8978 | 0.9227 |
| 1998-06-01 | 169 | 0.7811 | 0.8807 | 0.9100 |
| 1998-07-01 | 161 | 0.7702 | 0.8758 | 0.9079 |
| 1998-08-01 | 173 | 0.7977 | 0.8931 | 0.9208 |
| 1998-09-01 | 166 | 0.8133 | 0.9026 | 0.9279 |

PSI phân bố điểm train -> test: loan 0.081, card_classic 0.013, card_junior 0.003, card_gold 0.023 (< 0,1 ổn định; 0,1-0,25 theo dõi; > 0,25 trôi).

## 7. Giá trị kinh doanh (chiến dịch một sản phẩm; giá trị và chi phí liên hệ là GIẢ ĐỊNH, CZK)

**Khoản vay** (giá trị 6,000, chi phí liên hệ 60):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 349 | 92.0 | 3.3821 | 27.2 | 25.4 | -0.0309 | 84.5 |
| 5% | 1744 | 224.0 | 16.9 | 13.3 | 11.8 | -0.0309 | 240.3 |
| 10% | 3488 | 273.0 | 33.8 | 8.0765 | 6.8268 | -0.0309 | 327.6 |

**Thẻ classic** (giá trị 1,500, chi phí liên hệ 60):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 364 | 47.0 | 7.7100 | 6.0960 | 2.2280 | -0.4705 | 45.1 |
| 5% | 1818 | 193.0 | 38.5 | 5.0120 | 1.6540 | -0.4705 | 166.6 |
| 10% | 3635 | 350.0 | 77.0 | 4.5458 | 1.4072 | -0.4705 | 291.5 |

**Thẻ junior** (giá trị 800, chi phí liên hệ 60):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 394 | 53.0 | 1.5110 | 35.1 | 0.7936 | -0.9489 | 33.5 |
| 5% | 1969 | 135.0 | 7.5510 | 17.9 | -0.0858 | -0.9489 | 73.5 |
| 10% | 3938 | 143.0 | 15.1 | 9.4690 | -0.5158 | -0.9489 | 80.3 |

**Thẻ vàng** (giá trị 4,000, chi phí liên hệ 60):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 399 | 25.0 | 1.2915 | 19.4 | 3.1771 | -0.7842 | 24.6 |
| 5% | 1993 | 85.0 | 6.4508 | 13.2 | 1.8433 | -0.7842 | 65.0 |
| 10% | 3986 | 116.0 | 12.9 | 8.9911 | 0.9401 | -0.7842 | 79.4 |

## 8. Đánh giá ngoại tuyến chính sách (off-policy)

Log mô phỏng: chính sách cũ = sản phẩm phổ biến nhất, thăm dò ε = 0.3; 40,310 khách; giá trị chính sách cũ 0.0165. Giá trị THẬT của chính sách mới (top-1 v2) = 0.0267.

| Bộ ước lượng | Ước lượng | Độ chệch |
|---|---:|---:|
| IPS | 0.0260 | -0.0007 |
| SNIPS | 0.0263 | -0.0004 |
| DM | 0.0212 | -0.0055 |
| DR | 0.0262 | -0.0005 |

## 9. Đặc trưng quan trọng (gain)

`product` 0.221, `outflow_3m` 0.097, `tenure_months` 0.087, `inflow_3m` 0.079, `avg_balance_3m` 0.071, `age` 0.064, `cash_wd_cnt_3m` 0.057, `avg_balance` 0.040, `end_balance` 0.035, `min_balance_3m` 0.033, `inflow` 0.030, `cash_wd_cnt` 0.024, `moy` 0.021, `txn_cnt_3m` 0.019, `inflow_trend` 0.015
