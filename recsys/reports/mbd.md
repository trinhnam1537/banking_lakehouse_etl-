### MBD-mini: thăm dò

- 1,202,688 dòng (khách × tháng), 100,224 khách, 12 tháng báo cáo (2022-02-28 .. 2023-01-31).
- Tỉ lệ mua theo sản phẩm (mỗi khách-tháng): sp1 0.446%, sp2 0.048%, sp3 0.377%, sp4 0.254%; có ít nhất 1 sản phẩm: 1.07%.
- 10.4% khách mua ít nhất một lần trong 12 tháng; trong số đó 22.3% mua từ 2 lần.
- Tính lặp lại: sp1: 3.46% nếu tháng trước có mua, 0.422% nếu không (gấp 8 lần); sp2: 6.34% nếu tháng trước có mua, 0.046% nếu không (gấp 137 lần); sp3: 2.67% nếu tháng trước có mua, 0.377% nếu không (gấp 7 lần); sp4: 6.14% nếu tháng trước có mua, 0.241% nếu không (gấp 25 lần).
- Tương quan giữa nhãn các sản phẩm thấp (|r| lớn nhất 0.049): ít tín hiệu bán chéo trực tiếp.
- 22.6% khách-tháng không có giao dịch mới; tỉ lệ mua của nhóm này 0.77% so với 1.16% ở nhóm có giao dịch.

### MBD-mini (4 san pham, du doan thang sau)

Train 902,016 / valid 100,224 / test 200,448 dong.

**Xep hang san pham cho tung khach** (dong test co >= 1 san pham duoc mua):

| model | rows | hit@1 | mrr | map@3 |
|---|---|---|---|---|
| popularity | 1434 | 0.3145 | 0.5911 | 0.5746 |
| lightgbm | 1434 | 0.4805 | 0.6962 | 0.6837 |
| repeat | 1434 | 0.4205 | 0.6633 | 0.6500 |


**Nham khach theo tung san pham** (tap test):

| model | product | pos | rate | roc_auc | pr_auc | lift@1% | recall@5% |
|---|---|---|---|---|---|---|---|
| lightgbm | sp1 | 451 | 0.0022 | 0.7129 | 0.0112 | 13.3 | 0.2927 |
| lightgbm | sp2 | 81 | 0.0004 | 0.7627 | 0.0076 | 22.2 | 0.3210 |
| lightgbm | sp3 | 460 | 0.0023 | 0.7579 | 0.0125 | 9.3501 | 0.2304 |
| lightgbm | sp4 | 492 | 0.0025 | 0.7989 | 0.0396 | 25.2 | 0.4004 |
| repeat | sp1 | 451 | 0.0022 | 0.5416 | 0.0033 | 3.7703 | 0.1353 |
| repeat | sp2 | 81 | 0.0004 | 0.6091 | 0.0089 | 22.2 | 0.2963 |
| repeat | sp3 | 460 | 0.0023 | 0.5468 | 0.0043 | 7.6105 | 0.1435 |
| repeat | sp4 | 492 | 0.0025 | 0.6581 | 0.0206 | 24.0 | 0.3435 |


**Dac trung quan trong nhat (ty le gain, LightGBM)**:

- sp1: dtd 0.18, trx_share_m 0.14, trx_per_month 0.12, midx 0.10, trx_m 0.09, t1_since 0.09
- sp2: trx_m 0.24, trx_3m 0.13, t2_cnt 0.10, t2_since 0.10, trx_per_month 0.08, trx_share_m 0.08
- sp3: trx_m 0.27, trx_share_m 0.13, trx_per_month 0.13, trx_3m 0.11, tc 0.06, trx_m1 0.06
- sp4: t4_cnt 0.28, trx_m 0.17, any_cnt 0.11, trx_per_month 0.08, trx_share_m 0.08, t4_since 0.05
