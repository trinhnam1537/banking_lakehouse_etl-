# Đánh giá offline hệ gợi ý v2: mbd (2026-11-19)

Mô hình `v2-20261119-2300`; tập test 200,448 dòng (khách × tháng), tháng 2022-12-31, 2023-01-31.
Sinh bởi `python -m recsys.v2.run evaluate`. Giải thích từng nhóm chỉ số: docs/RECSYS-V2.md mục 4.

## 1. Độ chính xác xếp hạng (từng khách, dòng có ít nhất 1 sản phẩm được mua)

| model | rows | hit@1 | mrr | map@3 | ndcg@3 | recall@3 |
|---|---|---|---|---|---|---|
| v2 (xep theo raw, champion) | 1434 | 0.5084 | 0.7107 | 0.6970 | 0.7646 | 0.9569 |
| v2 (xep theo calibrated, so sanh) | 1434 | 0.4819 | 0.6973 | 0.6816 | 0.7515 | 0.9496 |
| v1_lgb_per_product | 1434 | 0.4596 | 0.6863 | 0.6736 | 0.7469 | 0.9562 |
| repeat | 1434 | 0.4205 | 0.6633 | 0.6500 | 0.7296 | 0.9569 |
| popularity | 1434 | 0.3145 | 0.5911 | 0.5746 | 0.6707 | 0.9468 |

**Bootstrap ghép cặp 1000 lần, chênh lệch so với `v1_lgb_per_product` (CI 95%):**

| Mô hình | Δ Hit@1 | Δ MAP@3 [CI 95%] | Δ NDCG@3 |
|---|---:|---|---:|
| v2 (xep theo raw, champion) | +0.0492 | +0.0236 [+0.0119; +0.0356] | +0.0178 |
| v2 (xep theo calibrated, so sanh) | +0.0220 | +0.0079 [-0.0052; +0.0197] | +0.0046 |
| popularity | -0.1441 | -0.0984 [-0.1211; -0.0783] | -0.0757 |
| repeat | -0.0388 | -0.0235 [-0.0420; -0.0058] | -0.0172 |

## 2. Nhắm khách theo sản phẩm (v2)

| product | pos | rate | roc_auc | pr_auc | lift@1% | recall@5% |
|---|---|---|---|---|---|---|
| 1 | 451 | 0.0022 | 0.7119 | 0.0100 | 12.9 | 0.3038 |
| 2 | 81 | 0.0004 | 0.7668 | 0.0110 | 22.2 | 0.3951 |
| 3 | 460 | 0.0023 | 0.7644 | 0.0102 | 10.2 | 0.2478 |
| 4 | 492 | 0.0025 | 0.7943 | 0.0276 | 22.4 | 0.4146 |

## 3. Hiệu chỉnh xác suất (trước / sau isotonic)

| Sản phẩm | Tỉ lệ thật | p TB trước | p TB sau | Brier trước | Brier sau | Brier tham chiếu | ECE trước | ECE sau |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.2250% | 0.2978% | 0.2525% | 0.00224 | 0.00223 | 0.00224 | 0.00089 | 0.00045 |
| 2 | 0.0404% | 0.1126% | 0.0612% | 0.00040 | 0.00041 | 0.00040 | 0.00072 | 0.00023 |
| 3 | 0.2295% | 0.2968% | 0.2901% | 0.00228 | 0.00228 | 0.00229 | 0.00068 | 0.00061 |
| 4 | 0.2455% | 0.2522% | 0.2656% | 0.00241 | 0.00243 | 0.00245 | 0.00068 | 0.00049 |

Brier tham chiếu = dự đoán hằng số bằng tỉ lệ nền. ECE tính theo 10 bin phân vị.

## 4. Ngoài độ chính xác (gợi ý top-1)

| Mô hình | Độ phủ | Gini hiển thị | Độ mới | Cá nhân hoá | Tỉ trọng hiển thị theo sản phẩm |
|---|---:|---:|---:|---:|---|
| v2 (xep theo raw, champion) | 1.00 | 0.507 | 0.912 | 0.534 | 59%, 0%, 35%, 6% |
| v2 (xep theo calibrated, so sanh) | 1.00 | 0.470 | 0.918 | 0.570 | 34%, 0%, 55%, 11% |
| popularity | 0.25 | 0.750 | 0.954 | 0.000 | 100%, 0%, 0%, 0% |
| repeat | 1.00 | 0.711 | 0.903 | 0.098 | 95%, 0%, 3%, 2% |
| v1_lgb_per_product | 1.00 | 0.682 | 0.918 | 0.183 | 7%, 0%, 90%, 3% |

## 5. Phân khúc (v2)

**hoat dong** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.871):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| co giao dich thang nay | 1276 | 0.4898 | 0.6858 | 0.7558 |
| khong giao dich | 158 | 0.6582 | 0.7874 | 0.8351 |

**lich su mua** (tỉ số MAP@3 xấu nhất / tốt nhất = 0.905):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| chua tung mua | 941 | 0.4750 | 0.6728 | 0.7460 |
| da tung mua | 493 | 0.5720 | 0.7432 | 0.7999 |

**tham nien** (tỉ số MAP@3 xấu nhất / tốt nhất = 1.000):

| segment | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| 9+ | 1434 | 0.5084 | 0.6970 | 0.7646 |

## 6. Ổn định theo thời gian

| month | rows | hit@1 | map@3 | ndcg@3 |
|---|---|---|---|---|
| 2022-12-31 | 759 | 0.4783 | 0.6724 | 0.7421 |
| 2023-01-31 | 675 | 0.5422 | 0.7247 | 0.7899 |

PSI phân bố điểm train -> test: 1 0.474, 2 0.238, 3 0.247, 4 0.152 (< 0,1 ổn định; 0,1-0,25 theo dõi; > 0,25 trôi).

## 7. Giá trị kinh doanh (chiến dịch một sản phẩm; giá trị và chi phí liên hệ là GIẢ ĐỊNH, USD (giả định))

**Sản phẩm ẩn danh 1** (giá trị 120, chi phí liên hệ 2):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 2004 | 58.0 | 4.5089 | 12.9 | 0.7365 | -0.8650 | 51.1 |
| 5% | 10022 | 137.0 | 22.5 | 6.0756 | -0.1798 | -0.8650 | 123.1 |
| 10% | 20045 | 180.0 | 45.1 | 3.9911 | -0.4612 | -0.8650 | 187.1 |

**Sản phẩm ẩn danh 2** (giá trị 300, chi phí liên hệ 2):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 2004 | 18.0 | 0.8098 | 22.2 | 0.3473 | -0.9394 | 42.3 |
| 5% | 10022 | 32.0 | 4.0498 | 7.9015 | -0.5211 | -0.9394 | 55.3 |
| 10% | 20045 | 37.0 | 8.1001 | 4.5679 | -0.7231 | -0.9394 | 70.1 |

**Sản phẩm ẩn danh 3** (giá trị 150, chi phí liên hệ 2):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 2004 | 47.0 | 4.5989 | 10.2 | 0.7590 | -0.8279 | 44.7 |
| 5% | 10022 | 114.0 | 23.0 | 4.9567 | -0.1469 | -0.8279 | 133.4 |
| 10% | 20045 | 164.0 | 46.0 | 3.5652 | -0.3864 | -0.8279 | 201.9 |

**Sản phẩm ẩn danh 4** (giá trị 100, chi phí liên hệ 2):

| top | contacts | conversions | random_conversions | lift | roi | roi_random | expected_conversions_model |
|---|---|---|---|---|---|---|---|
| 1% | 2004 | 110.0 | 4.9188 | 22.4 | 1.7445 | -0.8773 | 96.0 |
| 5% | 10022 | 204.0 | 24.6 | 8.2930 | 0.0178 | -0.8773 | 193.8 |
| 10% | 20045 | 239.0 | 49.2 | 4.8577 | -0.4038 | -0.8773 | 259.2 |

## 8. Đánh giá ngoại tuyến chính sách (off-policy)

Log mô phỏng: chính sách cũ = sản phẩm phổ biến nhất, thăm dò ε = 0.3; 200,448 khách; giá trị chính sách cũ 0.0022. Giá trị THẬT của chính sách mới (top-1 v2) = 0.0036.

| Bộ ước lượng | Ước lượng | Độ chệch |
|---|---:|---:|
| IPS | 0.0034 | -0.0003 |
| SNIPS | 0.0034 | -0.0002 |
| DM | 0.0039 | +0.0002 |
| DR | 0.0034 | -0.0002 |

## 9. Đặc trưng quan trọng (gain)

`product` 0.277, `trx_m` 0.105, `own_since` 0.061, `trx_mean3` 0.052, `trx_share_m` 0.049, `any_since` 0.041, `trx_per_month` 0.037, `own_cnt` 0.033, `trx_m3` 0.029, `dtd` 0.025, `trx_std3` 0.025, `midx` 0.021, `trx_ratio_mean` 0.020, `any_rate` 0.019, `trx_m2` 0.018
