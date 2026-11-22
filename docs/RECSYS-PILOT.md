# Pilot người dùng thật cho hệ gợi ý v2

Tài liệu này mô tả cách đánh giá hệ gợi ý với **người thật** ở quy mô đồ án (6–10 người, khoảng 30 phút mỗi người).
Nó bổ sung cho [RECSYS-V2.md](RECSYS-V2.md):

- Mục 4 (offline) đo **dự đoán** trên lịch sử.
- Mục 5 (online mô phỏng) kiểm chứng **phương pháp** thử nghiệm.
- Pilot đo điều hai mục trên không đo được: **người dùng thấy gợi ý thế nào**.

| Thành phần | Mã nguồn |
|---|---|
| Sinh bài, phân công, phân tích, chấm SUS | `recsys/v2/pilot.py` |
| Trang làm bài | Portal, trang "Đánh giá mù (pilot)" (`apps/portal/app.py`, `page_reco_pilot`) |
| Câu trả lời | `recsys/v2/events/pilot.jsonl` (chỉ thêm, không chứa PII, không commit) |
| Báo cáo | `recsys/v2/reports/berka/pilot.md`, `pilot.json` |

## 1. Vì sao cần pilot, và nó KHÔNG đo được gì

Online mô phỏng cho khách ảo trả lời theo nhãn thật, đúng loại nhãn v2 được huấn luyện để đoán. Vì vậy mức tăng
+52% trong A/B mô phỏng của Berka thực chất là kết quả offline nói lại. Nó chứng minh phương pháp thử nghiệm chạy
đúng, không chứng minh người dùng thích v2.

Pilot trả lời ba câu hỏi:

1. **Sở thích:** người đóng vai cán bộ quan hệ khách hàng (RM) có chọn danh sách của v2 hơn danh sách theo độ phổ
   biến không, khi KHÔNG biết danh sách nào do cách nào tạo ra?
2. **Giải thích:** lý do SHAP tiếng Việt có giúp RM nói chuyện với khách không, có khớp với hồ sơ không?
3. **Khả dụng:** portal có dễ dùng không (SUS)?

Pilot **không** đo doanh thu, cũng không đo khách thật có nhận lời hay không. Người tham gia đóng vai RM, không phải
khách. Muốn đo hai điều đó cần A/B trên khách thật đủ cỡ mẫu (RECSYS-V2.md mục 5.3).

## 2. Thiết kế

### 2.1 Bài đánh giá (`python -m recsys.v2.pilot build`)

- **Nguồn:** khách ở **tháng test đầu tiên** (1998-01). Đã biết khách mua gì trong 3 tháng sau, nhưng người tham gia
  không thấy thông tin này.
- **Chỉ chọn khách mà top-1 của hai chính sách khác nhau.** Nếu giống nhau thì so sánh vô nghĩa.
- **Một nửa số bài là khách thật sự mua.** Nhờ vậy so được người, mô hình và hành vi thật (mục 3.2).
- **Mỗi bài gồm:** hồ sơ (tuổi, giới tính, thâm niên, số dư, dòng tiền, rút tiền mặt, thấu chi, lương hưu, vùng, sản
  phẩm đang giữ), danh sách A và B (mỗi danh sách 2 sản phẩm), và lý do SHAP của gợi ý top-1 v2.
- **Phân công** (`assignment`): mỗi người làm 20 bài, gồm 5 bài chung cho mọi người (đo đồng thuận giữa người chấm)
  và 15 bài riêng chọn ổn định theo băm (người, bài). Thứ tự xáo theo người để tránh hiệu ứng thứ tự.
- **Phía nào là v2** (`v2_side`): băm (người, bài), cân bằng A/B. Giá trị được ghi vào câu trả lời, nhưng không bao
  giờ hiện cho người tham gia.

Với 8 người, cần 125 bài. Tháng 1998-01 chỉ có 124 bài đủ điều kiện, nên pilot tối đa khoảng 8 người. Muốn nhiều
người hơn, lấy thêm tháng test (sửa `build`).

### 2.2 Một bài trên portal

1. Người tham gia xem hồ sơ khách và hai danh sách A, B.
2. Chọn "A", "B" hoặc "Như nhau", và chọn MỘT sản phẩm sẽ chào nếu chỉ được chào một.
3. **Chỉ sau khi đã chọn**, portal mới hiện lý do của mô hình cho gợi ý top-1 v2. Chỉ v2 có lý do; hiện trước thì lộ
   danh sách nào là v2.
4. Chấm lý do: mức có ích (1–5), có khớp hồ sơ không (có / một phần / không), góp ý tự do.
5. Lưu và sang bài tiếp. Portal ghi thời gian làm mỗi bài. Làm dở có thể quay lại, bài đã làm được bỏ qua.

## 3. Phân tích (`python -m recsys.v2.pilot analyze --sus sus.csv`)

### 3.1 Sở thích mù

- **Tỉ lệ thắng của v2** = số lần chọn v2 / số câu trả lời không hoà. Báo hai khoảng tin cậy:
  - **Wilson**: coi mọi câu trả lời là độc lập.
  - **Bootstrap theo người**: lấy mẫu lại NGƯỜI, không lấy mẫu lại câu trả lời, vì 20 câu của cùng một người không
    độc lập. Đây là khoảng tin cậy nên dùng để kết luận.
- **Kiểm định dấu** (nhị thức hai phía, H0: tỉ lệ thắng = 0,5).
- **Cỡ mẫu:** để phát hiện tỉ lệ thắng 0,65 so với 0,5 (alpha 0,05, power 0,8) cần khoảng 85 câu trả lời không hoà,
  tức 6–8 người × 15–20 bài. Tỉ lệ thắng thật thấp hơn 0,65 thì pilot không đủ lực để phát hiện.
- Câu bấm lại: chỉ giữ lần trả lời CUỐI cho mỗi (người, bài).

### 3.2 Khớp hành vi thật

Chỉ tính trên các bài mà khách thật sự mua. So top-1 của bốn danh sách với sản phẩm khách đã mua:

| Danh sách | Ý nghĩa |
|---|---|
| v2 | Mô hình |
| Độ phổ biến | Đối chứng |
| Danh sách người tham gia chọn | Người + mô hình |
| Sản phẩm người tham gia sẽ chào | Phán đoán của người |

Nếu người tham gia chọn v2 nhiều nhưng v2 trúng ít hơn độ phổ biến, nghĩa là gợi ý "trông hợp lý" chưa chắc đúng.
Đây là lý do cần cả đánh giá offline lẫn đánh giá bằng người.

### 3.3 Giải thích và khả dụng

- **Lý do:** điểm trung bình và phân bố của mức có ích; tỉ lệ có / một phần / không khớp hồ sơ. Đọc kỹ các góp ý
  tự do: chúng thường chỉ ra lý do khó hiểu (vd "số dư thấp nhất 3 tháng" làm lý do cho thẻ vàng).
- **SUS:** 10 câu, thang 1–5. Câu lẻ tính điểm − 1, câu chẵn tính 5 − điểm, cộng lại rồi nhân 2,5 để ra thang 0–100.
  Mốc tham chiếu: 68 là trung bình của các hệ thống được khảo sát, trên 80 là tốt.

## 4. Quy trình một buổi (khoảng 30 phút mỗi người)

| Bước | Thời gian | Nội dung |
|---|---|---|
| 0. Chuẩn bị | trước buổi | `python -m recsys.v2.pilot build --participants 8`; mở portal (`streamlit run apps/portal/app.py`), vai trò "Chuyên viên phân tích / RM"; cấp mã P01, P02... |
| 1. Giới thiệu | 3 phút | Đọc kịch bản mục 4.1. Không nói có danh sách "của mô hình mới" |
| 2. Thử | 2 phút | Làm 1 bài mẫu cùng người điều phối (dùng mã PTEST, xoá dòng PTEST khỏi `pilot.jsonl` sau đó) |
| 3. Làm bài | 15–20 phút | 20 bài ở trang "Đánh giá mù (pilot)". Người điều phối không gợi ý |
| 4. Dùng trang RM | 5 phút | Trang "Gợi ý cho khách (RM)": tra 3 khách bất kỳ, bấm phản hồi. Sự kiện vào nhật ký online thật (`events/berka.jsonl`), gán nhóm A/B thật |
| 5. Bảng hỏi | 5 phút | SUS (phụ lục A) + 3 câu mở (phụ lục B). Ghi vào `sus.csv`: `participant,q1,...,q10` |

### 4.1 Kịch bản giới thiệu

> "Bạn đóng vai cán bộ quan hệ khách hàng của một ngân hàng. Mỗi bài, bạn sẽ thấy hồ sơ một khách và hai danh sách
> sản phẩm có thể chào. Hai danh sách do hai cách khác nhau tạo ra, thứ tự ngẫu nhiên. Hãy chọn danh sách bạn thấy
> hợp với khách hơn, theo kinh nghiệm hoặc cảm nhận của bạn. Không có đáp án đúng sai: chúng tôi đánh giá hệ thống,
> không đánh giá bạn. Dữ liệu là bộ dữ liệu công khai đã ẩn danh của một ngân hàng Séc năm 1998, số tiền tính bằng
> CZK. Câu trả lời chỉ gắn với mã người tham gia, không gắn với tên bạn."

### 4.2 Đạo đức và dữ liệu

- Người tham gia đồng ý bằng lời sau khi nghe kịch bản. Có thể dừng bất cứ lúc nào.
- Không thu tên, email hay thông tin cá nhân của người tham gia. Mã P01… không lưu cùng danh sách tên.
- Dữ liệu khách là Berka (công khai, ẩn danh). `tasks.parquet` và `pilot.jsonl` không được commit (`.gitignore`).

## 5. Mối đe doạ tính hợp lệ (ghi vào luận văn)

| Đe doạ | Ảnh hưởng | Cách giảm |
|---|---|---|
| Người tham gia là sinh viên, không phải RM thật | Sở thích có thể khác chuyên gia | Nói rõ trong hạn chế; nếu được, mời 1–2 người làm ngân hàng |
| Ít người (6–10) | Khoảng tin cậy rộng | Bootstrap theo người; báo kết quả từng người |
| Hồ sơ là dữ liệu năm 1998 | Bối cảnh lạ | Kịch bản nói rõ; số tiền giữ nguyên CZK |
| Lý do chỉ có ở v2 | Lộ nhóm nếu hiện trước | Lý do chỉ hiện sau khi đã chọn |
| Thứ tự A/B, mệt mỏi | Thiên lệch vị trí | v2 ở A hoặc B theo băm; thứ tự bài xáo theo người |
| Người điều phối gợi ý | Thiên lệch người thử | Kịch bản cố định; không trả lời câu hỏi về nội dung |
| Chỉ 4 sản phẩm, danh sách 2 món | Hai danh sách hay trùng | Chỉ chọn bài có top-1 khác nhau |

## 6. Chạy

```powershell
.\.venv\Scripts\python.exe -m recsys.v2.pilot build --participants 8     # cần cache dữ liệu Berka (hoặc persona DE)
..\portal-venv\Scripts\streamlit.exe run apps\portal\app.py              # trang "Đánh giá mù (pilot)"
.\.venv\Scripts\python.exe -m recsys.v2.pilot analyze --sus sus.csv     # -> recsys/v2/reports/berka/pilot.md
```

---

## Phụ lục A. Bảng hỏi SUS (tiếng Việt)

Thang 1–5: 1 = hoàn toàn không đồng ý, 5 = hoàn toàn đồng ý. Giữ nguyên thứ tự câu, kể cả câu phát biểu tiêu cực
(câu chẵn).

1. Tôi nghĩ tôi sẽ muốn dùng hệ thống này thường xuyên.
2. Tôi thấy hệ thống phức tạp một cách không cần thiết.
3. Tôi thấy hệ thống dễ sử dụng.
4. Tôi nghĩ tôi cần người hỗ trợ kỹ thuật mới dùng được hệ thống này.
5. Tôi thấy các chức năng của hệ thống được kết hợp tốt với nhau.
6. Tôi thấy hệ thống có quá nhiều chỗ không nhất quán.
7. Tôi nghĩ hầu hết mọi người sẽ học dùng hệ thống này rất nhanh.
8. Tôi thấy hệ thống rất rườm rà, khó dùng.
9. Tôi cảm thấy tự tin khi dùng hệ thống.
10. Tôi cần học nhiều thứ trước khi có thể dùng được hệ thống này.

## Phụ lục B. Câu hỏi mở

1. Khi chọn giữa hai danh sách, bạn dựa vào thông tin nào trong hồ sơ nhiều nhất?
2. Lý do của mô hình có chỗ nào khó hiểu hoặc không thuyết phục không? Cho một ví dụ.
3. Nếu là RM thật, bạn muốn trang gợi ý cho thêm thông tin gì trước khi gọi cho khách?
