# Hệ gợi ý sản phẩm ngân hàng (UC2): phân tích dữ liệu, chọn bộ dữ liệu, mô hình, đánh giá

Tài liệu cho chương "Gợi ý sản phẩm" của luận văn. Mã ở `recsys/`. Kết quả đầy đủ ở `recsys/reports/{mbd,berka}.md`.
Mọi dữ liệu đọc qua Athena dưới persona `fingraph-dev-data-engineer` (Lake Formation), không đọc thẳng S3.

```powershell
. ..\Hai_nam_graduation_project\docs\iam\assume-all.ps1        # profile fg-de-mcp (1 lan MFA)
.\.venv\Scripts\python.exe -m pip install -r recsys\requirements.txt
.\.venv\Scripts\python.exe -m recsys.mbd                         # EDA + train + danh gia MBD-mini
.\.venv\Scripts\python.exe -m recsys.berka                       # Berka UC2 (2 bien the)
.\.venv\Scripts\python.exe -m recsys.recommend --k 2             # goi y top-2 + ly do, thang moi nhat
```

## 1. Bài toán

Dự đoán sản phẩm khách hàng **sẽ mua tiếp** trong một khoảng thời gian tới, rồi dùng kết quả theo hai cách.

| Cách dùng | Câu hỏi | Chỉ số |
|---|---|---|
| Gợi ý cho từng khách (RM, app) | Khách này nên được gợi ý sản phẩm nào trước? | Hit@1, MRR, MAP@3, chỉ tính các (khách, tháng) có ít nhất một sản phẩm được mua |
| Chiến dịch một sản phẩm | Nên gọi cho top k% khách nào? | ROC-AUC, PR-AUC, lift@1%, recall@5% |

Nhãn rất hiếm, khoảng 0,2–4% tuỳ sản phẩm. Vì vậy PR-AUC và lift quan trọng hơn ROC-AUC, vốn trông đẹp ngay cả khi
mô hình vô dụng với nhãn hiếm.

## 2. Kiểm kê bộ dữ liệu có sẵn trong lake

| Bộ | Quy mô | Nhãn | Thời gian | Đánh giá cho gợi ý |
|---|---|---|---|---|
| **MBD-mini** (Sber AI Lab, `silver_mbd`) | 100.224 khách × 12 tháng = 1,2 triệu dòng | 4 sản phẩm, mua trong tháng sau | 2022-02..2023-01 | **Chọn (chính).** Ngân hàng thật, đủ lớn, nhãn theo tháng, nhiều khách mua lặp lại |
| **Berka UC2** (`track_a_pretrain.uc2_next_product`) | 4.500 khách, 158.615 khách-tháng | 6 sản phẩm, mua trong 3 tháng | 1993..1998 | **Chọn (phụ).** Nối được với knowledge graph và agent; nhưng có artefact, xem mục 3.2 |
| Bank Marketing (`silver_bank_marketing`) | 45.211 + 41.188 cuộc gọi | 1 sản phẩm (tiền gửi có kỳ hạn), tỉ lệ 11–12% | 2008–2010 | Không chọn. Một sản phẩm thì là bài toán propensity, không phải gợi ý; cột `duration_sec` chỉ biết SAU cuộc gọi (rò rỉ nếu dùng) |
| Santander Product Recommendation | 13,6 triệu dòng, 24 sản phẩm | Sản phẩm thêm vào tháng sau | 2015–2016 | Bộ chuẩn nhất cho bài toán này nhưng **chưa nạp** (Kaggle bắt đăng nhập, xem `docs/sourcing/uc2-datasets.md`). Đề xuất bổ sung |
| corebank (`silver_corebank`) | Bản sao Berka + simulator | | | Trùng Berka, không thêm thông tin |

## 3. Phân tích thăm dò (EDA)

### 3.1 MBD-mini

- Có 1.202.688 dòng khách-tháng, 100.224 khách, đủ 12 tháng cho mọi khách. Tập chia sẵn 5 fold theo khách.
- Tỉ lệ mua mỗi khách-tháng là sp1 0,446%, sp2 0,048%, sp3 0,377% và sp4 0,254%. Chỉ **1,07%** khách-tháng có ít nhất
  một sản phẩm. Trong 12 tháng, 10,4% khách mua ít nhất một lần, và 22,3% trong số đó mua từ hai lần trở lên.
- **Hành vi lặp lại là tín hiệu mạnh nhất.** Nếu tháng trước đã mua, xác suất mua lại của sp2 là 6,34%, so với 0,046%
  nếu không mua (gấp 137 lần). Với sp4 là gấp 25 lần, sp1 gấp 8 lần, sp3 gấp 7 lần.
- **Ít tín hiệu bán chéo.** Hệ số tương quan lớn nhất giữa nhãn các sản phẩm chỉ là 0,049.
- Cột `trans_count` trong silver là số **luỹ kế**, trung bình tăng từ 11 lên 188 qua 12 tháng. Đặc trưng hoạt động phải lấy
  chênh lệch giữa các tháng. Nhóm khách-tháng không có giao dịch mới chiếm 22,6% và mua ít hơn: 0,77% so với 1,16%.
- Tỉ lệ mua sp1 giảm dần theo thời gian, từ 0,70% vào tháng 3/2022 xuống 0,16% vào tháng 1/2023. Đây là trôi phân phối,
  nên phải chia tập theo thời gian.

### 3.2 Berka UC2: phát hiện artefact

Tỉ lệ mua trong 3 tháng theo thâm niên tài khoản, trên tập test và chỉ tính khách chưa giữ sản phẩm:

| Sản phẩm | 1–3 tháng | 4–6 | 7–12 | 13–24 | 25–60 | > 60 |
|---|---:|---:|---:|---:|---:|---:|
| household | 70,85% | 45,63% | 1,80% | 0,70% | 0,00% | 0,00% |
| insurance | 8,91% | 3,33% | 0,26% | 0,03% | 0,00% | 0,00% |
| loan | 1,47% | 2,67% | 3,30% | 1,96% | 0,00% | 0,00% |
| card_classic | 0,49% | 3,80% | 4,50% | 2,66% | 1,44% | 1,05% |

Household (lệnh chi SIPO) và insurance (POJISTNE) không phải sản phẩm được bán. Chúng được **suy ra từ lần đầu khoản
chi định kỳ xuất hiện trong giao dịch**, nên gần như luôn xảy ra ngay sau khi mở tài khoản. Mô hình học được quy luật
"tài khoản mới thì sắp có khoản chi định kỳ" (ROC-AUC 0,99), không học được nhu cầu thật. Loan bằng 0 sau 24 tháng cũng
là đặc điểm của cách bộ Berka được dựng.

Có thêm hai nguồn làm thổi phồng số liệu. Thẻ junior gắn với tuổi, nên đặc trưng `age` chiếm 45% gain. Nhãn có cửa sổ
3 tháng, nên mỗi lần mua lặp lại ở ba tháng chụp liên tiếp. Vì vậy Berka được báo cáo thêm một biến thể **chỉ gồm khách
ổn định** (tenure trên 6 tháng), và **card_classic** là chỉ số đáng tin nhất của Berka.

## 4. Phương pháp

- **Đặc trưng MBD.** File `recsys/mbd.py` dùng câu SQL window function chạy trên Athena. Với mỗi (khách, tháng m), mỗi
  sản phẩm có lịch sử mua lag 1, lag 2, lag 3, số lần đã mua và số tháng kể từ lần mua cuối. Thêm hoạt động giao dịch
  theo tháng, 3 tháng và tỉ trọng, thâm niên quan sát và tháng trong năm. **Chống rò rỉ:** dòng m chỉ dùng nhãn của các
  dòng trước m.
- **Đặc trưng Berka.** Lấy từ `track_a_pretrain.uc2_next_product`, đã chống rò rỉ ở tầng pre-train. Gồm số dư, dòng tiền,
  rút tiền mặt và thẻ, lương hưu, thấu chi, nhân khẩu học, thông tin quận và các sản phẩm đang giữ.
- **Chia tập theo thời gian.** Với MBD, train là các tháng báo cáo đến hết 2022-10, valid là 2022-11, test là 2022-12 và
  2023-01. Với Berka, train là trước 1997-07, valid đến hết 1997-12, test từ 1998-01 trở đi.
- **Mô hình.** Mỗi sản phẩm có một LightGBM nhị phân, dừng sớm theo PR-AUC trên tập valid. Mô hình chỉ học trên các dòng
  mà sản phẩm còn được gợi ý. MBD lấy mẫu 25% ca âm trên tập train, cùng tỉ lệ cho mọi sản phẩm, nên thứ tự xếp hạng
  được giữ nguyên. Để xếp hạng cho từng khách, điểm của các sản phẩm được so trực tiếp.
- **Đối chứng.** Mô hình phải thắng các đối chứng này mới có giá trị.
  - *popularity*: mọi khách nhận cùng một thứ tự sản phẩm theo tỉ lệ mua.
  - *repeat* (MBD): luật ưu tiên sản phẩm đã mua tháng trước, rồi theo số lần đã mua.
  - *cooccurrence* (Berka): luật kết hợp bán chéo P(mua p | đang giữ q).

## 5. Kết quả (tập test)

**Gợi ý cho từng khách**

| Bộ dữ liệu | Số dòng | popularity MAP@3 | Luật MAP@3 | **LightGBM MAP@3** | LightGBM Hit@1 |
|---|---:|---:|---:|---:|---:|
| MBD-mini | 1.434 | 0,575 | 0,650 (repeat) | **0,684** | 0,481 |
| Berka, mọi khách | 1.794 | 0,525 | 0,525 (cooccurrence) | 0,902 | 0,839 |
| Berka, khách ổn định | 1.320 | 0,595 | 0,583 (cooccurrence) | **0,870** | 0,784 |

**Nhắm khách cho chiến dịch (LightGBM)**

| Bộ | Sản phẩm | Ca dương | Tỉ lệ | ROC-AUC | PR-AUC | Lift@1% | Recall@5% |
|---|---|---:|---:|---:|---:|---:|---:|
| MBD | sp1 | 451 | 0,22% | 0,713 | 0,011 | 13,3 | 0,29 |
| MBD | sp2 | 81 | 0,04% | 0,763 | 0,008 | 22,2 | 0,32 |
| MBD | sp3 | 460 | 0,23% | 0,758 | 0,013 | 9,4 | 0,23 |
| MBD | sp4 | 492 | 0,25% | 0,799 | 0,040 | 25,2 | 0,40 |
| Berka ổn định | card_classic | 727 | 2,07% | 0,879 | 0,099 | 6,9 | 0,27 |
| Berka ổn định | loan | 306 | 0,91% | 0,963 | 0,202 | 30,4 | 0,70 |

Cách đọc kết quả:

- **MBD.** LightGBM hơn popularity 0,11 MAP@3 và hơn luật repeat 0,034. Khi nhắm 1% khách, mô hình tìm được số người mua
  nhiều hơn chọn ngẫu nhiên từ 9 đến 25 lần. Với sp1, sp3 và sp4, luật repeat chỉ đạt ROC-AUC 0,54–0,66, vì đa số người
  mua chưa từng mua trước đó. Mô hình bổ sung tín hiệu từ hoạt động giao dịch, nên `trx_m` và `trx_3m` nằm trong top gain
  của mọi sản phẩm.
- **Berka.** Con số cao nhưng phần lớn đến từ artefact ở mục 3.2. Không nên dùng làm con số chính. Card_classic là chỉ số
  đáng tin nhất: lift 6,9 với số dư trung bình 3 tháng là đặc trưng chính.
- **Bán chéo** gần như không có trên cả hai bộ. Ở Berka, luật cooccurrence không hơn popularity. Ở MBD, tương quan giữa
  nhãn các sản phẩm dưới 0,05. Hành vi giao dịch của chính khách hàng mới mang tín hiệu.

## 6. Phục vụ (serving)

`recsys/recommend.py` chấm điểm tháng báo cáo mới nhất cho 100.224 khách. Kết quả là top-k sản phẩm, kèm `p_calibrated`
(xác suất đã hiệu chỉnh lại sau lấy mẫu âm) và **lý do bằng tiếng Việt**. Lý do là ba đặc trưng có đóng góp SHAP dương
lớn nhất, ví dụ "[sp4] số lần đã mua; tổng số lần mua mọi sản phẩm; số giao dịch tháng này". Hàm
`recommend_mbd(client_ids, k)` dùng được trong portal và có thể làm tool cho agent.

## 7. Giới hạn và việc tiếp theo

1. **MBD mới dùng bảng nhãn.** Phần `detail` của MBD (giao dịch, địa lý, hội thoại, 1,97 GB) chưa nạp, xem
   `docs/sourcing/uc2-datasets.md`. Đây là nguồn đặc trưng hành vi giàu nhất và là bước cải thiện lớn nhất. Paper gốc
   (arXiv 2409.17587) dùng chính phần này.
2. **Nạp Santander** (24 sản phẩm, 13,6 triệu dòng) để có bộ chuẩn so sánh với các công trình khác (MAP@7 Kaggle).
3. Chưa tinh chỉnh siêu tham số. LightGBM đang dùng tham số mặc định hợp lý và dừng sớm.
4. Mô hình của sp2 (81 ca dương trên test) và card_gold (125 ca) có phương sai cao. Nên báo khoảng tin cậy bằng bootstrap.
5. Có thể đưa vào lake như một Glue job (`transforms/pretrain` → bảng `track_a_gold.reco_topk`) để agent và portal đọc qua
   Lake Formation thay vì file cục bộ.
