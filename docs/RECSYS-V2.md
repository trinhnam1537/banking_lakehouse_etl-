# Hệ gợi ý sản phẩm v2: kiến trúc, đánh giá offline và online, phục vụ người dùng cuối

Tài liệu này mô tả hệ gợi ý sản phẩm v2 của FinGraph. Nó trả lời bốn câu hỏi:

1. Hệ thống được xây như thế nào, từ dữ liệu tới màn hình người dùng (mục 1-3).
2. Đánh giá một **mô hình AI** gợi ý cần đo những gì (mục 4).
3. Đánh giá **online** làm thế nào khi chưa có người dùng thật ở quy mô lớn (mục 5).
4. Đánh giá một **sản phẩm AI** khác đánh giá mô hình ở đâu, và người dùng cuối dùng hệ thống ra sao (mục 6-7).

Kết quả đo nằm ở mục 8. Bản v1 (một LightGBM cho mỗi sản phẩm, chỉ đo độ chính xác) mô tả ở [RECSYS.md](RECSYS.md).
So sánh tám họ mô hình nằm ở [RECSYS-BENCH.md](RECSYS-BENCH.md).

| Thành phần | Mã nguồn |
|---|---|
| Cấu hình nghiệp vụ (sản phẩm, giá trị, chi phí, luật, chính sách liên hệ) | `recsys/v2/config.py` |
| Nạp dữ liệu qua persona (hợp đồng chung cho mọi miền) | `recsys/v2/data.py` |
| Mô hình champion + hiệu chỉnh + giải thích + registry | `recsys/v2/model.py` |
| Đánh giá offline (7 nhóm chỉ số + off-policy + bootstrap) | `recsys/v2/evaluate.py` |
| Đánh giá online (nhật ký sự kiện, A/B, interleaving, bandit) | `recsys/v2/online.py` |
| Tầng phục vụ (luật, cool-down, giá trị kỳ vọng, lý do, xuất lake) | `recsys/v2/serve.py` |
| Điều phối | `recsys/v2/run.py` |
| Báo cáo máy sinh | `recsys/v2/reports/<miền>/offline.md`, `online_sim.md` |
| Portal (RM, chiến dịch, giám sát, khách) | `apps/portal/app.py` |
| Agent (`recommend`, `my_recommendations`) | `agent/tools.py`, `agent/customer.py` |
| Quyền khách đọc gợi ý của mình (Lake Formation) | repo hạ tầng, `modules/governance` (`customer_gold_tables`) |

---

## 1. Hai miền dữ liệu, hai vai trò

| | MBD-mini (Sber) | Berka (ngân hàng Séc) |
|---|---|---|
| Quy mô | 100.224 khách doanh nghiệp × 12 tháng, 1,2 triệu dòng | 4.500 khách × 66 tháng, 158.615 dòng |
| Sản phẩm | 4 sản phẩm ẩn danh, mua LẶP được | vay, thẻ classic, thẻ junior, thẻ vàng; mỗi sản phẩm mua một lần |
| Nhãn | mua sản phẩm k trong tháng sau | mua sản phẩm CHƯA giữ trong 3 tháng sau |
| Đủ điều kiện | mọi sản phẩm | sản phẩm khách chưa giữ + luật nghiệp vụ |
| Vai trò | **benchmark offline** (lớn, nhãn hiếm 0,04-0,25%) | **phục vụ người dùng cuối** (portal, agent; khách có mặt trong ontology và persona khách) |

Berka không gợi ý `household` (thanh toán định kỳ) và `insurance`. Nhãn của hai sản phẩm này lệch do giai đoạn mở tài
khoản: 70% "mua" household trong 3 tháng đầu là thao tác mở tài khoản, không phải quyết định mua ([RECSYS.md](RECSYS.md)
mục 4.3).

## 2. Kiến trúc

```
silver / gold (Iceberg, Lake Formation)
   │ Athena dưới persona data engineer (recsys.data.athena_df, cache theo băm câu SQL)
   ▼
đặc trưng (khách × tháng, chống rò rỉ thời gian; chia train / valid / test theo THỜI GIAN)
   ▼
mô hình champion: LightGBM multi-task dạng dài  ──►  hiệu chỉnh isotonic theo sản phẩm (học trên valid)
   │                                                │
   │  registry/<miền>/<phiên bản>/model_card.json   │  chọn điểm xếp hạng (thô / đã hiệu chỉnh) trên valid, cross-fit
   ▼                                                ▼
tầng phục vụ: đủ điều kiện → luật nghiệp vụ → cool-down (từ nhật ký) → ngưỡng p tối thiểu → giá trị kỳ vọng → lý do SHAP
   │  gán nhóm A/B ổn định theo băm client_id (control = độ phổ biến, treatment = v2)
   ▼
track_a_gold.reco_topk_berka (gợi ý đang phục vụ) + reco_score_berka (điểm mọi sản phẩm)   ← persona analyst ghi
   │  Lake Formation: analyst đọc hết; khách chỉ đọc dòng của mình (data cells filter client_id)
   ├──► Portal: RM (gợi ý + lý do + phản hồi), Chiến dịch (chọn khách), Giám sát, Khách ("Ưu đãi cho bạn")
   └──► Agent: tool recommend (RM), my_recommendations (khách)
            │
            ▼
nhật ký sự kiện (impression / click / accept / dismiss) ──► chỉ số online, kiểm định A/B, cool-down, dữ liệu huấn luyện lại
```

Ba quyết định thiết kế:

1. **Mô hình chỉ cho xác suất, tầng phục vụ mới quyết định hiển thị gì.** Luật nghiệp vụ, chính sách liên hệ và giá
   trị sản phẩm đổi thường xuyên hơn mô hình. Tách ra thì đổi luật không cần huấn luyện lại, và đánh giá mô hình
   (mục 4) không lẫn với đánh giá chính sách (mục 5).
2. **Bảng gợi ý là dữ liệu trong lake, không phải API riêng.** Nhờ vậy dùng lại toàn bộ quản trị sẵn có: Lake Formation
   lọc dòng cho khách, time travel Iceberg theo phiên bản mô hình (mỗi lần chấm là một snapshot, cập nhật bằng
   `DELETE` + `INSERT`, không `DROP` vì drop làm mất filter và quyền), audit qua CloudTrail.
3. **Mọi tương tác đều ghi sự kiện.** Không có nhật ký thì không đánh giá online được, không thực hiện được "không chào
   lại sản phẩm khách vừa từ chối", và không có dữ liệu để huấn luyện lại.

## 3. Mô hình

### 3.1 Champion: LightGBM multi-task dạng dài

Mỗi dòng (khách, tháng) được nhân K lần, một lần cho mỗi sản phẩm. Thêm cột `product` (biến phân loại) và các đặc trưng
`own_*` là lịch sử của CHÍNH sản phẩm đó (đã mua tháng trước, số lần mua, số tháng từ lần mua cuối...). Chỉ dòng
(khách, sản phẩm) đủ điều kiện mới vào huấn luyện.

Chọn mô hình này vì bench ([RECSYS-BENCH.md](RECSYS-BENCH.md) mục 5) cho thấy nó tốt nhất trên MBD (MAP@3 0,697, CI 95%
dương so với LightGBM theo sản phẩm), huấn luyện nhanh nhất (26 giây), và **một mô hình cho một thang điểm chung** giữa
các sản phẩm. Sản phẩm hiếm (sp2 MBD có 430 ca dương, thẻ vàng Berka có 67) mượn được quy luật từ sản phẩm khác.

Huấn luyện:

- Lấy mẫu âm 25% trên train, giữ mọi mẫu dương.
- Dừng sớm theo average precision trên valid, patience 100.
- Tham số: `num_leaves` 63, `learning_rate` 0,05, `min_data_in_leaf` 200.

### 3.2 Hiệu chỉnh xác suất

Con số "khả năng quan tâm 7,3%" hiển thị cho RM và khách phải đúng nghĩa xác suất. Hiệu chỉnh gồm hai bước:

1. **Bù lấy mẫu âm.** Giữ tỉ lệ r mẫu âm làm odds bị nhân 1/r, nên lấy odds_gốc = odds × r.
2. **Isotonic regression theo từng sản phẩm, học trên valid.** Đây là hàm đơn điệu không giả định dạng sigmoid. Đo bằng
   Brier score và ECE (mục 4, nhóm 3).

**Hiệu chỉnh có thể làm hỏng xếp hạng (phát hiện 19-11).** Isotonic là hàm bậc thang:

- Nó tạo ra nhiều điểm bằng nhau (13% số dòng MBD có ít nhất hai sản phẩm cùng điểm).
- Nó đổi thứ tự GIỮA các sản phẩm.

Trên test MBD, xếp theo điểm đã hiệu chỉnh cho MAP@3 0,682, thấp hơn mức 0,697 khi xếp theo điểm thô. Với Berka thì ngược
lại. Vì vậy hệ thống:

- chọn điểm dùng để XẾP HẠNG (thô hoặc đã hiệu chỉnh) trên valid;
- luôn dùng xác suất đã hiệu chỉnh để HIỂN THỊ và tính giá trị kỳ vọng;
- phá hoà bằng điểm thô.

**Rò rỉ trong bước chọn (phát hiện 19-11).** Lần đầu, isotonic được học và chấm trên CÙNG tập valid. Bản hiệu chỉnh vì
thế trông tốt giả: valid MAP@3 0,722, nhưng test chỉ 0,682. Đã sửa bằng **cross-fit**:

1. Chia valid theo khách thành 2 nửa.
2. Học isotonic trên nửa này, chấm xếp hạng trên nửa kia.
3. Đổi vai hai nửa.

Đây là ví dụ điển hình: mọi bước chọn (siêu tham số, ngưỡng, cách xếp hạng) đều là "huấn luyện", nên phải dùng dữ liệu
khác với dữ liệu dùng để chấm.

### 3.3 Từ xác suất đến danh sách gợi ý (tầng phục vụ)

1. **Đủ điều kiện:** bỏ sản phẩm khách đang giữ.
2. **Luật nghiệp vụ** (`config.BERKA_RULES`):
   - Thẻ vàng yêu cầu số dư bình quân 3 tháng ≥ 50.000 CZK.
   - Vay yêu cầu không thấu chi trong 3 tháng.
   - Thẻ junior chỉ chào khách từ 25 đến 60 tuổi.
   - Luật đặt SAU mô hình, nên đổi luật không cần huấn luyện lại.
3. **Chính sách liên hệ:** không chào lại sản phẩm khách vừa bỏ qua (60 ngày) hoặc vừa nhận (180 ngày). Thông tin đọc
   từ nhật ký sự kiện.
4. **Ngưỡng tối thiểu** `min_p_show` (Berka 0,3%): dưới ngưỡng thì không gợi ý. Gợi ý tồi làm mất niềm tin nhanh hơn
   là không có gợi ý.
5. **Xếp theo giá trị kỳ vọng** = p_cal × giá trị sản phẩm. Thẻ vàng hiếm nhưng giá trị cao, nên có thể đứng trước thẻ
   classic dù xác suất thấp hơn.
6. **Lý do:** lấy 4 đặc trưng có đóng góp SHAP dương lớn nhất cho đúng sản phẩm được gợi ý, chuyển thành câu tiếng
   Việt, bỏ đặc trưng `product` (tỉ lệ nền giống nhau với mọi khách, không phải lý do cá nhân). Kèm gợi ý cách nói
   chuyện cho RM.

Số dòng bị lọc ở mỗi bước ghi ở `out.attrs["filtered"]`, giúp trả lời câu hỏi "vì sao khách X không có gợi ý".

---

## 4. Đánh giá offline một mô hình AI gợi ý: đo những gì

Một con số độ chính xác không đủ. Mô hình có thể đứng đầu bảng MAP@3 nhưng hiển thị xác suất sai, chỉ gợi ý một sản
phẩm, kém hẳn với một nhóm khách, trôi theo thời gian, hoặc không mang lại giá trị kinh doanh. Bộ đánh giá
(`recsys/v2/evaluate.py`) đo bảy nhóm, cộng đánh giá ngoại tuyến chính sách. Mọi số đều tính trên tập **test** (các
tháng sau cùng). Tập test chỉ dùng một lần để báo cáo.

| Nhóm | Câu hỏi | Chỉ số | Vì sao cần |
|---|---|---|---|
| 1. Xếp hạng | Sản phẩm khách thật sự mua có đứng đầu danh sách không? | Hit@1, MRR, MAP@3, NDCG@3, Recall@3 (dòng có ≥ 1 sản phẩm được mua) | Đúng cách dùng: RM / app hiển thị 1-3 gợi ý cho một khách |
| 2. Nhắm khách | Với một sản phẩm, top x% khách có chứa nhiều người mua không? | ROC-AUC, PR-AUC, lift@1%, recall@5% | Cách dùng thứ hai: chiến dịch một sản phẩm. Nhãn hiếm nên PR-AUC và lift quan trọng hơn ROC-AUC |
| 3. Hiệu chỉnh | "7,3%" có đúng là 7,3% không? | Brier (so với Brier của dự đoán hằng số), ECE theo bin phân vị, bảng độ tin cậy | Con số hiển thị cho người dùng và dùng để tính giá trị kỳ vọng, ROI |
| 4. Ngoài độ chính xác | Gợi ý có đa dạng, cá nhân hoá, có giới thiệu sản phẩm mới không? | Độ phủ danh mục, Gini phân bố hiển thị, độ mới (top-1 là sản phẩm chưa từng mua), cá nhân hoá (1 − độ trùng top-k giữa hai khách ngẫu nhiên) | Mô hình "luôn gợi ý sản phẩm phổ biến nhất" có thể đạt Hit@1 khá mà vô dụng |
| 5. Phân khúc | Có nhóm khách nào bị phục vụ kém hẳn không? | Chỉ số nhóm 1 theo thâm niên, giới tính, vùng, mức số dư; tỉ số MAP@3 xấu nhất / tốt nhất | Công bằng và độ bền: trung bình tốt có thể che một nhóm rất kém |
| 6. Ổn định | Mô hình có giữ chất lượng qua các tháng không? Dữ liệu có trôi không? | Chỉ số theo từng tháng test; PSI phân bố điểm train → test | Quyết định khi nào huấn luyện lại; PSI > 0,25 là trôi đáng kể |
| 7. Kinh doanh | Dùng mô hình thì lời bao nhiêu so với chọn ngẫu nhiên? | Chiến dịch top 1/5/10%: chuyển đổi, doanh thu, chi phí liên hệ, ROI, so với ngẫu nhiên | Ngôn ngữ của người ra quyết định; tham số giá trị / chi phí ghi rõ là giả định |
| + Off-policy | Đưa chính sách mới lên online thì tỉ lệ nhận sẽ là bao nhiêu? | IPS, SNIPS, DM, DR trên log mô phỏng; so với giá trị thật | Cầu nối offline → online (mục 5.4) |
| + Ý nghĩa thống kê | Chênh lệch với mô hình cũ có thật không? | Bootstrap ghép cặp 1000 lần, CI 95% | Tập test nhỏ (sp2 MBD chỉ 81 ca dương); không kết luận từ chênh lệch nằm trong nhiễu |

**Đối chứng bắt buộc phải thắng:**

- **popularity:** tỉ lệ mua của sản phẩm trên train, giống nhau cho mọi khách.
- **Luật hành vi:**
  - MBD dùng `repeat`: mua tháng trước > số lần đã mua > độ phổ biến.
  - Berka dùng `cooccurrence`: P(mua p | đang giữ q).
- **Mô hình v1** (MBD): LightGBM riêng cho mỗi sản phẩm, đặc trưng gốc.

Mô hình không thắng luật đơn giản thì không đáng đưa vào vận hành.

**Chống rò rỉ thời gian:**

- Đặc trưng tại tháng t chỉ dùng dữ liệu đến hết t.
- Nhãn là hành vi SAU t.
- Chia train / valid / test theo tháng, không chia ngẫu nhiên.

Bench có thêm phép kiểm tra tự động: `lag1` của dòng m phải đúng bằng nhãn của dòng m−1, và không đặc trưng nào tương
quan quá 0,5 với nhãn.

---

## 5. Đánh giá online

### 5.1 Vì sao offline chưa đủ

Đánh giá offline đo **dự đoán** trên lịch sử. Online đo **tác động** khi hệ thống thay đổi hành vi. Bốn điều chỉ online
mới thấy:

- Khách có nhận lời khi được chào không? Nhãn offline là "mua", không phải "nhận lời vì được gợi ý".
- Vị trí hiển thị và cách diễn đạt ảnh hưởng thế nào.
- Mệt mỏi khi bị chào lặp lại.
- Vòng phản hồi: mô hình học từ dữ liệu do chính nó tạo ra.

Chỉ số online:

- **Chính:** tỉ lệ khách nhận lời chào (đơn vị là KHÁCH, không phải lượt hiển thị, vì nhiều lượt hiển thị cho cùng một
  khách không độc lập).
- **Phụ:** CTR.
- **Guardrail:** tỉ lệ bỏ qua / ẩn gợi ý, độ phủ sản phẩm, và về sau là số khiếu nại.

### 5.2 Hạ tầng online thật (đã chạy trên portal)

| Thành phần | Làm gì |
|---|---|
| `RecoEvent` | Một sự kiện = (khách, sản phẩm, loại sự kiện, nhóm thử nghiệm, phiên bản mô hình, vị trí, xác suất hiển thị, bề mặt, người thao tác, thời điểm). Không chứa PII |
| `EventLog` | Ghi JSONL chỉ thêm. Portal ghi `impression` khi hiển thị; `accept` / `dismiss` / `click` khi RM hoặc khách bấm |
| `assign(client_id)` | Gán nhóm bằng băm SHA-256 của (tên thử nghiệm, client_id): cùng khách luôn cùng nhóm, không cần lưu trạng thái, độc lập giữa các thử nghiệm |
| `online_metrics` | CTR, tỉ lệ nhận, tỉ lệ bỏ qua theo nhóm và sản phẩm; tỉ lệ nhận theo khách |
| Bảng `reco_topk_berka` | Mỗi khách chỉ có danh sách của nhóm mình: treatment = v2, control = độ phổ biến. Hai nhóm dùng chung luật, cool-down và ngưỡng, chỉ khác thứ tự |
| Trang "Giám sát gợi ý" | Model card, chỉ số online theo nhóm, z-test hai tỉ lệ, kiểm SRM |

### 5.3 Phương pháp thử nghiệm

**Quy trình đưa mô hình lên:**

1. **Shadow:** mô hình mới chấm điểm song song, không hiển thị. So phân bố điểm và độ trùng danh sách với bản đang chạy.
2. **A/A:** hai nhóm cùng chính sách. Kiểm tra hạ tầng gán nhóm và ghi sự kiện; kỳ vọng không có khác biệt.
3. **A/B 50/50:** chạy đủ cỡ mẫu tính TRƯỚC, không dừng sớm khi thấy p < 0,05.
4. **Tăng dần** 10% → 50% → 100% khi guardrail ổn.
5. **Rollback** bằng cách đổi file `CHAMPION` trong registry, rồi chấm lại.

**Bốn kiểm tra thống kê** (`online.py`):

- **Cỡ mẫu / power** (`sample_size`): số khách mỗi nhóm để phát hiện mức tăng tương đối cho trước, với α = 0,05 và
  power = 0,8.
- **SRM** (`srm`): chi-bình phương trên số khách mỗi nhóm. p < 0,001 nghĩa là gán nhóm hỏng, không đọc kết quả.
- **Kiểm định** (`two_proportion`): z-test hai tỉ lệ, CI của chênh lệch tuyệt đối, lift tương đối.
- **Nhìn trộm** (`aa_peeking`): đo trực tiếp mức tăng dương tính giả khi xem p-value mỗi ngày và dừng ngay khi có ý
  nghĩa.

**Interleaving team-draft:** mỗi khách thấy MỘT danh sách trộn từ hai chính sách, click được tính cho đội đã đưa món đó
vào. Cách này nhạy hơn A/B nhiều lần vì mỗi khách tự so sánh hai chính sách, nhưng chỉ đo **sở thích tương đối**, không
đo được tác động kinh doanh tuyệt đối.

**Bandit (Thompson sampling):** thay vì chia cố định 50/50, phân bổ dần lưu lượng sang chính sách đang tốt hơn
(Beta-Bernoulli). Cách này giảm chi phí cơ hội (regret), đổi lại suy luận thống kê khó hơn.

### 5.4 Mô phỏng để kiểm chứng phương pháp

Đồ án chưa có đủ người dùng thật, nên các phương pháp trên được kiểm chứng trên **khách ảo**:

- Rút khách từ tháng test.
- Khách trả lời theo **nhãn thật** của tháng sau: khách thật sự mua sản phẩm k thì dễ nhận lời chào k (xác suất chú ý
  0,5, nhận 0,7), khách không mua thì hiếm khi nhận (0,03 và 0,05).
- Có thiên lệch vị trí (1,0 / 0,6 / 0,4) và mệt mỏi (×0,7 cho mỗi lần đã bị chào cùng sản phẩm).

Các tham số này là GIẢ ĐỊNH. Mô phỏng cho biết phương pháp thử nghiệm phản ứng thế nào (cần bao nhiêu khách, nhìn trộm
sai bao nhiêu, interleaving nhạy hơn bao nhiêu), **không** cho biết doanh thu thật.

**Đánh giá ngoại tuyến chính sách (OPE)** là cầu nối giữa hai thế giới. Từ log của chính sách cũ (có thăm dò ε), ước
lượng tỉ lệ nhận của chính sách mới mà không cần triển khai:

- **IPS:** không chệch, phương sai lớn.
- **SNIPS:** tự chuẩn hoá.
- **DM:** dùng mô hình phần thưởng, chệch nếu mô hình sai.
- **DR:** kết hợp hai cách; đúng nếu MỘT trong hai đúng.

Vì MBD và Berka có nhãn của MỌI sản phẩm, giá trị thật của chính sách mới tính được. Nhờ đó đo được độ chệch của từng
bộ ước lượng, điều không làm được trên log thật.

---

## 6. Đánh giá một SẢN PHẨM AI: rộng hơn đánh giá mô hình

Mô hình tốt là điều kiện cần. Sản phẩm AI còn phải đạt ở các tầng sau, mỗi tầng có cách đo riêng:

| Tầng | Câu hỏi | Cách đo / bằng chứng trong hệ thống |
|---|---|---|
| Mô hình | Dự đoán đúng, hiệu chỉnh, ổn định, công bằng | Mục 4 (bảy nhóm chỉ số), báo cáo `offline.md` |
| Giá trị người dùng | Người dùng có nhận gợi ý, có thấy hữu ích, có bị làm phiền | Mục 5: tỉ lệ nhận theo khách, tỉ lệ bỏ qua; A/B có guardrail |
| Giá trị kinh doanh | Lời bao nhiêu sau chi phí | ROI chiến dịch (offline, nhóm 7) → doanh thu thật theo nhóm A/B (online) |
| Tin cậy và giải thích | Người dùng hiểu vì sao được gợi ý | Lý do SHAP tiếng Việt trên mỗi gợi ý; gợi ý cách nói chuyện cho RM; nút "Ẩn gợi ý này" |
| An toàn và chính sách | Không gợi ý sản phẩm không phù hợp, không làm phiền | Luật nghiệp vụ, ngưỡng p tối thiểu, cool-down sau từ chối; đếm bằng `attrs["filtered"]` |
| Quyền riêng tư và phân quyền | Ai thấy gợi ý của ai | Lake Formation: analyst đọc hết; khách chỉ đọc dòng của mình. Portal không tự lọc. Sự kiện không chứa PII |
| Vận hành | Có tái lập, có rollback, có giám sát | Registry có model card (dữ liệu, tháng train/valid, tham số, chỉ số); `CHAMPION` để rollback; PSI và chỉ số theo tháng; trang Giám sát |
| Hiệu năng và chi phí | Nhanh và rẻ đủ chưa | Thời gian huấn luyện, chấm điểm, publish (mục 8.4); chi phí Athena khi publish |
| Tuân thủ | Truy vết được quyết định | Mỗi dòng gợi ý có `model_version`, `scored_month`, `variant`; snapshot Iceberg theo lần chấm; nhật ký sự kiện |

---

## 7. Người dùng cuối dùng hệ thống như thế nào

| Người dùng | Bề mặt | Họ làm gì | Dữ liệu và quyền |
|---|---|---|---|
| **Cán bộ quan hệ khách hàng (RM)** | Portal, trang "Gợi ý cho khách (RM)" | Nhập mã khách và xem tối đa 3 gợi ý: sản phẩm, khả năng quan tâm (%), lý do, gợi ý cách nói chuyện, nhóm thử nghiệm. Sau cuộc gọi bấm "Khách đồng ý" / "Khách không quan tâm" / "Đã giới thiệu" | Persona analyst, đọc `reco_topk_berka`; mỗi lần bấm ghi một sự kiện |
| **RM qua trợ lý AI** | Portal, "Trợ lý AI" hoặc CLI | Hỏi "Nên chào sản phẩm gì cho Khách 2, vì sao?". Agent gọi tool `recommend`, trả lời kèm trích dẫn `[metric:id]`, phiên bản mô hình, lý do | Như trên; câu đánh giá A22 |
| **Marketing** | Portal, "Chiến dịch gợi ý" | Chọn sản phẩm, số khách liên hệ và chi phí một lần liên hệ. Portal trả danh sách khách xếp theo xác suất, số khách kỳ vọng nhận, so với chọn ngẫu nhiên, doanh thu và ROI kỳ vọng, rồi tải CSV | Persona analyst, đọc `reco_score_berka` |
| **Khách hàng** | Portal, "Ưu đãi cho bạn" (persona khách) | Thấy tối đa 2 ưu đãi kèm "Vì sao bạn thấy gợi ý này". Bấm "Tôi quan tâm" hoặc "Ẩn gợi ý này" (không chào lại trong 60 ngày) | Persona `customer-client-2`; Lake Formation chỉ trả dòng của chính khách |
| **Khách qua trợ lý** | `agent/customer.py` | Hỏi "Có gói nào hợp với tôi?". Tool `my_recommendations` đọc dưới quyền của khách | Như trên |
| **Data scientist / vận hành** | `python -m recsys.v2.run`, trang "Giám sát gợi ý" | Huấn luyện, đánh giá, chấm điểm, publish; theo dõi model card, PSI, chỉ số online; đổi `CHAMPION` để rollback | Persona data engineer (đọc dữ liệu), analyst (ghi gold) |

Một vòng đời:

1. Cuối tháng, chạy `run all --domain berka` để huấn luyện lại (hoặc chỉ `score`), sau đó `publish`.
2. RM và khách thấy gợi ý mới, và phản hồi của họ được ghi vào nhật ký.
3. Tháng sau, `score` đọc nhật ký để không chào lại sản phẩm khách vừa từ chối.
4. Trang Giám sát cho biết nhóm treatment có hơn control không, khi đã đủ cỡ mẫu.

---

## 8. Kết quả (19-11-2026)

Báo cáo đầy đủ: `recsys/v2/reports/{mbd,berka}/offline.md` và `online_sim.md`. Model card:
`recsys/v2/registry/<miền>/<phiên bản>/model_card.json`.

### 8.1 Offline: xếp hạng (test)

| Miền | Mô hình | Hit@1 | MAP@3 | NDCG@3 | Δ MAP@3 so với đối chứng [CI 95%] |
|---|---|---:|---:|---:|---|
| MBD | **v2 champion** (xếp theo điểm thô, chọn trên valid) | **0,508** | **0,697** | **0,765** | +0,024 [+0,012; +0,036] so với v1 |
| MBD | v2 xếp theo p_cal (so sánh) | 0,482 | 0,682 | 0,752 | +0,008 [−0,005; +0,020] |
| MBD | v1 LightGBM mỗi sản phẩm | 0,460 | 0,674 | 0,747 | — |
| MBD | repeat (luật hành vi) | 0,421 | 0,650 | 0,730 | −0,024 |
| MBD | popularity | 0,315 | 0,575 | 0,671 | −0,098 |
| Berka | **v2 champion** (xếp theo điểm thô, chọn trên valid) | **0,786** | **0,885** | **0,913** | +0,267 [+0,250; +0,284] so với popularity |
| Berka | v2 xếp theo p_cal (so sánh) | 0,831 | 0,910 | 0,932 | +0,292 [+0,275; +0,308] |
| Berka | popularity (= cooccurrence) | 0,351 | 0,618 | 0,697 | — |

Với Berka, cách xếp được chọn trên valid (điểm thô, valid 0,896 so với 0,888) KHÔNG phải cách tốt nhất trên test (p_cal,
0,910). Hệ thống vẫn giữ lựa chọn theo valid, vì chọn lại theo test là dùng test để huấn luyện. Chênh lệch trên valid
nhỏ (0,009). Đây là tín hiệu để lần sau dùng nhiều tháng valid hơn khi chọn.

Ở Berka, `cooccurrence` trùng `popularity`: sau khi bỏ household và bảo hiểm, xác suất có điều kiện P(mua p | giữ q)
không đổi thứ tự so với tỉ lệ nền.

### 8.2 Offline: các nhóm còn lại

| Nhóm | MBD | Berka |
|---|---|---|
| Nhắm khách (ROC-AUC theo sản phẩm) | 0,71-0,79; lift@1% 10-22 | 0,87-0,97; lift@1% 6-35 |
| Hiệu chỉnh: ECE trước → sau isotonic | sp1 0,00089 → 0,00045; sp2 0,00072 → 0,00023 | vay 0,0043 → 0,0016; classic 0,0142 → 0,0066; vàng 0,0030 → 0,0010 |
| Hiệu chỉnh: p trung bình so với tỉ lệ thật | sp2: 0,113% → 0,061% (thật 0,040%) | classic: 0,82% → 1,46% (thật 2,12%) |
| Độ phủ (top-1) | v2 100%; popularity 25% | v2 100%; popularity 75% |
| Gini hiển thị (thấp = đa dạng) | v2 0,51; repeat 0,71; popularity 0,75 | v2 0,46; popularity 0,67 |
| Cá nhân hoá | v2 0,53; repeat 0,10; popularity 0 | v2 0,57; popularity 0,24 |
| Độ mới (top-1 là sản phẩm chưa từng mua) | v2 0,91 | không áp dụng (Berka chỉ gợi ý sản phẩm chưa giữ) |
| Phân khúc: MAP@3 xấu nhất / tốt nhất | hoạt động 0,87; lịch sử mua 0,91; thâm niên 1,00 | thâm niên 0,90; giới tính 0,99; vùng 0,88; số dư 0,96 |
| Ổn định: PSI điểm train → test | sp1 **0,47**; sp2 0,24; sp3 0,25; sp4 0,15 | 0,003-0,081 |
| Kinh doanh (ví dụ) | (giá trị giả định) | Vay, top 1% (349 khách): 92 người mua so với 3,4 nếu chọn ngẫu nhiên (lift 27), ROI 25,4 so với −0,03 |
| Off-policy: thật / IPS / SNIPS / DM / DR | 0,0036 / 0,0034 / 0,0034 / 0,0039 / 0,0034 | 0,0267 / 0,0260 / 0,0263 / 0,0212 / 0,0262 |

Đọc kết quả:

- **Hiệu chỉnh giảm ECE 2-4 lần** ở cả hai miền. Brier sau hiệu chỉnh không cao hơn Brier của dự đoán hằng số (sp2 MBD
  ngang nhau vì nhãn quá hiếm: 81 ca dương ở test).
- **v2 phân bố gợi ý đều hơn hẳn đối chứng** (Gini 0,46-0,51 so với 0,67-0,75). Đối chứng gần như chỉ gợi ý một sản
  phẩm. Tuy vậy sp2 MBD không bao giờ lên top-1 vì xác suất luôn thấp nhất; muốn chào sp2 phải dùng giá trị kỳ vọng
  hoặc hạn ngạch.
- **MBD trôi mạnh** (PSI sp1 = 0,47 > 0,25): phân bố điểm của 2 tháng test khác train. Khi vận hành, mức này là tín hiệu
  huấn luyện lại. Berka ổn định (PSI < 0,1).
- **Off-policy:** IPS, SNIPS và DR lệch dưới 0,0007 so với giá trị thật. DM (chỉ dựa vào mô hình phần thưởng) lệch nhiều
  nhất ở Berka (−0,0055), vì mô hình ước xác suất mua trong 3 tháng, không đúng với phần thưởng của log. Điều này khớp
  lý thuyết: DR an toàn khi mô hình phần thưởng sai.

### 8.3 Online (mô phỏng)

| Thí nghiệm | MBD (tỉ lệ nền 0,3%) | Berka (tỉ lệ nền 1,8%) |
|---|---|---|
| A/B 14 ngày, tỉ lệ khách nhận: control → v2 | 0,30% → 0,40% (+32%), **p = 0,26: không kết luận được** | 1,80% → 2,74% (+52%), CI chênh lệch [+0,07; +1,81] điểm %, **p = 0,036** |
| SRM | p = 0,026 (đạt ngưỡng 0,001) | p = 0,50 |
| Cỡ mẫu cần cho mức tăng 20% / 50% | 143.336 / 26.049 khách mỗi nhóm | ít hơn nhiều (tỉ lệ nền cao hơn) |
| A/A: dương tính giả khi nhìn trộm mỗi ngày / chỉ xem cuối kỳ | 8% / 3% (100 lần) | **16% / 5%** (300 lần) |
| Bandit Thompson | không học được (chênh lệch thật 0,0027 so với 0,0024) | **93% lưu lượng cho v2**; regret 14,5 so với 73,2 khi chia đều |
| Interleaving (8.000 khách), tỉ lệ thắng của v2 | 0,48 (p = 0,46) | 0,54 (p = 0,07) |

Kết luận về phương pháp:

1. **Với sản phẩm mua hiếm (MBD), A/B gần như không dùng được ở quy mô đồ án**: cần hơn 140 nghìn khách mỗi nhóm để
   thấy mức tăng 20%. Khi đó, đánh giá offline có khoảng tin cậy và đánh giá off-policy là công cụ chính.
2. **Nhìn trộm làm sai kết luận thật sự**: dương tính giả tăng từ 5% lên 16%. Phải cố định cỡ mẫu trước, hoặc dùng
   kiểm định tuần tự có hiệu chỉnh alpha.
3. **Bandit hữu ích khi chênh lệch đủ lớn** (Berka): giảm chi phí cơ hội 5 lần so với chia đều. Khi chênh lệch nhỏ
   hơn nhiễu (MBD), bandit không hơn chia đều.
4. **Interleaving trong mô phỏng này không nhạy hơn A/B** như lý thuyết dự đoán. Click ngẫu nhiên của khách không mua
   (xác suất 3%) gây nhiễu, và hai danh sách trùng nhau nhiều (chỉ có 4 sản phẩm) nên phần lớn phiên hoà. Interleaving
   phát huy khi danh mục lớn và các danh sách khác nhau nhiều.

### 8.4 Hiệu năng hệ thống

| Bước | Berka | MBD |
|---|---|---|
| Nạp đặc trưng (cache Athena) | 1 s | 188 s (gồm tính đặc trưng v2 bằng pandas) |
| Huấn luyện + hiệu chỉnh + chọn cách xếp | khoảng 5 s | khoảng 25 s |
| Đánh giá offline đầy đủ (bootstrap 1000 lần) | khoảng 15 s | khoảng 40 s |
| Chấm điểm + luật + SHAP cho 4.500 khách | 3-6 s | — |
| Publish 2 bảng Iceberg (persona analyst) | khoảng 25 s mỗi bảng | — |

Chi phí Athena khi publish dưới 0,01 USD (bảng vài trăm KB). Không cần hạ tầng mới.

### 8.5 Kiểm thử đầu-cuối trên portal (Streamlit AppTest, 19-11)

- **Trang RM, khách 2:** "1. Thẻ classic", khả năng quan tâm 7,3%. Lý do: số dư bình quân 3 tháng, số dư bình quân
  tháng, tiền ra 3 tháng; nhóm treatment. Bấm "Khách đồng ý" ghi `impression` và `accept` vào nhật ký.
- **Trang Chiến dịch** (thẻ classic, 200 khách): 833 khách đủ điều kiện, kỳ vọng 16,6 người nhận, ROI kỳ vọng 7,3x.
- **Trang Giám sát:** model card và báo cáo offline hiển thị đúng.
- **Trang "Ưu đãi cho bạn"** (persona khách): Lake Formation từ chối vì quyền lọc dòng ở repo hạ tầng (PR
  `feat/reco-customer-access`) chưa apply. Portal báo rõ lý do, không có đường vòng.

---

## 9. Phát hiện và bài học

| Phát hiện | Bằng chứng | Xử lý |
|---|---|---|
| Hiệu chỉnh isotonic theo sản phẩm có thể làm giảm xếp hạng | MBD test: p_cal 0,682 so với điểm thô 0,697; 13% dòng có điểm bằng nhau | Tách điểm xếp hạng khỏi xác suất hiển thị; chọn cách xếp trên valid |
| Chọn và chấm trên cùng một tập làm kết quả đẹp giả | MBD: chọn không cross-fit cho valid 0,722 nhưng test 0,682 | Cross-fit trên valid |
| AUC từng sản phẩm không đủ để chọn mô hình gợi ý | CatBoost: AUC 0,70-0,80 nhưng MAP@3 0,47, thua popularity (RECSYS-BENCH) | Bộ đánh giá luôn đo cả xếp hạng lẫn hiệu chỉnh |
| Drop bảng làm mất quyền Lake Formation | Thiết kế publish | `DELETE` + `INSERT` trên bảng Iceberg có sẵn |
| A/B không đủ cỡ mẫu cho sản phẩm hiếm | MBD: +32%, p = 0,26 | Dùng offline có CI và off-policy làm công cụ chính |
| Nhìn trộm p-value làm dương tính giả tăng gấp ba | A/A Berka: 16% so với 5% | Cố định cỡ mẫu; trang Giám sát có cảnh báo |
| Nhãn "mua" khác với "nhận lời vì được gợi ý" | Mọi chỉ số offline chỉ đo dự đoán hành vi tự nhiên | Nhật ký sự kiện + A/B để đo tác động |

## 10. Hạn chế và bước tiếp theo

1. **Chưa có người dùng thật.** Kết quả online là mô phỏng với tham số giả định. Hạ tầng (sự kiện, gán nhóm, kiểm
   định) đã chạy trên portal.
2. **Persona khách chưa đọc được gợi ý.** Cần apply Terraform `customer_gold_tables` bằng lệnh `-target` (SETUP-LOG,
   mục 11-19).
3. **Nhật ký sự kiện là file cục bộ.** Bước tiếp: `track_a_gold.reco_event` (Iceberg) hoặc một topic Kafka.
4. **Gợi ý chưa vào knowledge graph** (checklist O10). Cần mapping `fin:Recommendation` (`fin:forCustomer`,
   `fin:recommends`, `fin:score`, `fin:modelVersion`) đọc từ `reco_topk_berka`, tham chiếu node khách của `berka.yml`,
   rồi dựng lại `track_a_onto` (`onto.build_graph`, Glue, khoảng 6 phút).
5. **Giá trị sản phẩm và chi phí liên hệ là giả định.** ROI tuyệt đối chưa có nghĩa; chỉ so sánh tương đối có nghĩa.
6. **MBD trôi mạnh (PSI 0,47) và sp2 không bao giờ lên top-1.** Cần lịch huấn luyện lại theo PSI, và hạn ngạch hoặc
   giá trị riêng cho sản phẩm hiếm.
7. **Cách xếp chỉ được chọn trên một khoảng valid.** Nên dùng nhiều tháng (rolling) để giảm phương sai.

## 11. Chạy lại

```powershell
# Credential persona (repo hạ tầng): . .\docs\iam\assume-all.ps1
.\.venv\Scripts\python.exe -m recsys.v2.run all --domain mbd       # huấn luyện + offline + online mô phỏng (~5 phút)
.\.venv\Scripts\python.exe -m recsys.v2.run all --domain berka     # huấn luyện + offline + chấm điểm (~30 s)
.\.venv\Scripts\python.exe -m recsys.v2.run online --domain berka  # online mô phỏng Berka (~15 s)
.\.venv\Scripts\python.exe -m recsys.v2.run publish --domain berka # ghi track_a_gold.reco_topk_berka, reco_score_berka
.\.venv\Scripts\python.exe -m pytest tests/recsys tests/portal -q  # không cần AWS
..\portal-venv\Scripts\streamlit.exe run apps\portal\app.py         # portal
```
