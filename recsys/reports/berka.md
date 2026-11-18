### Berka UC2: thăm dò

- 158,615 dòng (khách × tháng chụp), 4,500 khách, tháng 1993-04-01 .. 1998-09-01.
- train: 93,744 dòng; tỉ lệ mua trong 3 tháng (trên khách chưa giữ): household 17.96%, loan 1.49%, card_classic 0.83%, insurance 1.03%, card_junior 0.22%, card_gold 0.07%.
- valid: 24,561 dòng; tỉ lệ mua trong 3 tháng (trên khách chưa giữ): household 9.53%, loan 1.42%, card_classic 1.51%, insurance 0.49%, card_junior 0.24%, card_gold 0.20%.
- test: 40,310 dòng; tỉ lệ mua trong 3 tháng (trên khách chưa giữ): household 3.93%, loan 0.97%, card_classic 2.12%, insurance 0.18%, card_junior 0.38%, card_gold 0.32%.
- Tỉ lệ khách-tháng đang giữ: household 68.5%, loan 10.6%, card_classic 5.4%, insurance 10.7%, card_junior 1.4%, card_gold 0.5%.
- Số ca dương trên tập train ít ở thẻ gold (67) và thẻ junior (206): mô hình cho hai sản phẩm này có phương sai cao. Nhãn 3 tháng của cùng một lần mua lặp ở 3 tháng chụp liên tiếp.

**Tỉ lệ mua trong 3 tháng theo tenure (tháng), tập test:**

| product | 1-3 | 4-6 | 7-12 | 13-24 | 25-60 | >60 |
|---|---|---|---|---|---|---|
| household | 70.85% | 45.63% | 1.80% | 0.70% | 0.00% | 0.00% |
| loan | 1.47% | 2.67% | 3.30% | 1.96% | 0.00% | 0.00% |
| card_classic | 0.49% | 3.80% | 4.50% | 2.66% | 1.44% | 1.05% |
| insurance | 8.91% | 3.33% | 0.26% | 0.03% | 0.00% | 0.00% |
| card_junior | 0.00% | 0.54% | 0.85% | 0.43% | 0.29% | 0.18% |
| card_gold | 0.00% | 0.36% | 0.46% | 0.27% | 0.34% | 0.27% |

### Berka UC2, moi khach (6 san pham, mua trong 3 thang)

Train 93,744 / valid 24,561 / test 40,310 dong.

**Xep hang san pham cho tung khach** (dong test co >= 1 san pham duoc mua):

| model | rows | hit@1 | mrr | map@3 |
|---|---|---|---|---|
| popularity | 1794 | 0.3802 | 0.5857 | 0.5248 |
| lightgbm | 1794 | 0.8389 | 0.9105 | 0.9020 |
| cooccurrence | 1794 | 0.3802 | 0.5857 | 0.5248 |


**Nham khach theo tung san pham** (tap test):

| model | product | pos | rate | roc_auc | pr_auc | lift@1% | recall@5% |
|---|---|---|---|---|---|---|---|
| lightgbm | household | 442 | 0.0393 | 0.9944 | 0.8727 | 24.8 | 0.9050 |
| lightgbm | loan | 338 | 0.0097 | 0.9587 | 0.1939 | 28.4 | 0.6746 |
| lightgbm | card_classic | 770 | 0.0212 | 0.8754 | 0.0988 | 6.7445 | 0.2429 |
| lightgbm | insurance | 64 | 0.0018 | 0.9925 | 0.1620 | 68.8 | 1.0000 |
| lightgbm | card_junior | 151 | 0.0038 | 0.9830 | 0.1423 | 43.7 | 0.9603 |
| lightgbm | card_gold | 129 | 0.0032 | 0.9403 | 0.0495 | 17.8 | 0.6434 |
| cooccurrence | household | 442 | 0.0393 | 0.5034 | 0.0458 | 2.4996 | 0.0837 |
| cooccurrence | loan | 338 | 0.0097 | 0.5352 | 0.0107 | 0.8870 | 0.0680 |
| cooccurrence | card_classic | 770 | 0.0212 | 0.5286 | 0.0229 | 2.0752 | 0.1000 |
| cooccurrence | insurance | 64 | 0.0018 | 0.5000 | 0.0018 | 0.0000 | 0.0156 |
| cooccurrence | card_junior | 151 | 0.0038 | 0.4856 | 0.0037 | 0.0000 | 0.0132 |
| cooccurrence | card_gold | 129 | 0.0032 | 0.5000 | 0.0032 | 1.5486 | 0.0388 |


**Dac trung quan trong nhat (ty le gain, LightGBM)**:

- household: tenure_months 0.54, txn_cnt_3m 0.18, inflow 0.06, net_flow_3m 0.03, inflow_trend 0.03, cash_wd_cnt_3m 0.02
- loan: tenure_months 0.19, cash_wd_cnt_3m 0.12, inflow_3m 0.11, cash_wd_cnt 0.06, outflow_3m 0.04, min_balance_3m 0.04
- card_classic: avg_balance_3m 0.39, age 0.10, end_balance 0.07, avg_balance 0.06, tenure_months 0.05, district_urban_ratio 0.03
- insurance: tenure_months 0.26, txn_cnt_3m 0.16, inflow_3m 0.08, age 0.05, inflow 0.04, district_unemployment_96 0.03
- card_junior: age 0.45, avg_balance_3m 0.23, tenure_months 0.07, district_inhabitants 0.04, end_balance 0.03, district_unemployment_96 0.03
- card_gold: avg_balance_3m 0.48, age 0.08, inflow_3m 0.07, district_unemployment_96 0.07, min_balance_3m 0.05, end_balance 0.05

### Berka UC2, khach on dinh (tenure > 6 thang)

Train 78,915 / valid 22,700 / test 39,000 dong.

**Xep hang san pham cho tung khach** (dong test co >= 1 san pham duoc mua):

| model | rows | hit@1 | mrr | map@3 |
|---|---|---|---|---|
| popularity | 1320 | 0.3402 | 0.6241 | 0.5951 |
| lightgbm | 1320 | 0.7841 | 0.8786 | 0.8699 |
| cooccurrence | 1320 | 0.3402 | 0.6214 | 0.5833 |


**Nham khach theo tung san pham** (tap test):

| model | product | pos | rate | roc_auc | pr_auc | lift@1% | recall@5% |
|---|---|---|---|---|---|---|---|
| lightgbm | household | 40 | 0.0038 | 0.9822 | 0.2419 | 49.9 | 0.9250 |
| lightgbm | loan | 306 | 0.0091 | 0.9632 | 0.2022 | 30.4 | 0.6961 |
| lightgbm | card_classic | 727 | 0.0207 | 0.8790 | 0.0991 | 6.8862 | 0.2737 |
| lightgbm | insurance | 12 | 0.0003 | 0.9962 | 0.1103 | 83.3 | 1.0000 |
| lightgbm | card_junior | 145 | 0.0038 | 0.9786 | 0.1494 | 42.0 | 0.9103 |
| lightgbm | card_gold | 125 | 0.0032 | 0.9509 | 0.0502 | 20.8 | 0.6640 |
| cooccurrence | household | 40 | 0.0038 | 0.5000 | 0.0038 | 0.0000 | 0.0500 |
| cooccurrence | loan | 306 | 0.0091 | 0.5374 | 0.0102 | 0.9800 | 0.0686 |
| cooccurrence | card_classic | 727 | 0.0207 | 0.5320 | 0.0227 | 2.0659 | 0.1004 |
| cooccurrence | insurance | 12 | 0.0003 | 0.4348 | 0.0003 | 0.0000 | 0.0000 |
| cooccurrence | card_junior | 145 | 0.0038 | 0.4857 | 0.0037 | 0.0000 | 0.0138 |
| cooccurrence | card_gold | 125 | 0.0032 | 0.5000 | 0.0032 | 1.6019 | 0.0400 |


**Dac trung quan trong nhat (ty le gain, LightGBM)**:

- household: txn_cnt_3m 0.30, outflow_3m 0.21, tenure_months 0.13, cash_wd_cnt_3m 0.13, inflow_3m 0.07, inflow 0.03
- loan: tenure_months 0.19, cash_wd_cnt_3m 0.14, outflow_3m 0.07, cash_wd_cnt 0.06, inflow_3m 0.06, txn_cnt_3m 0.05
- card_classic: avg_balance_3m 0.42, age 0.09, end_balance 0.08, avg_balance 0.05, tenure_months 0.03, district_inhabitants 0.03
- insurance: txn_cnt_3m 0.35, age 0.09, min_balance_3m 0.07, tenure_months 0.07, avg_balance_3m 0.04, net_flow_3m 0.04
- card_junior: age 0.44, avg_balance_3m 0.36, district_inhabitants 0.03, end_balance 0.03, district_unemployment_96 0.02, outflow_3m 0.02
- card_gold: avg_balance_3m 0.33, min_balance_3m 0.11, age 0.08, end_balance 0.07, district_urban_ratio 0.05, district_unemployment_96 0.05
