# Benchmark mô hình gợi ý sản phẩm (MBD-mini): lý thuyết, xử lý dữ liệu, huấn luyện, kết quả

Tài liệu này so sánh các họ mô hình mạnh hơn LightGBM gốc của hệ gợi ý (docs/RECSYS.md) trên **cùng dữ liệu, cùng
cách chia, cùng chỉ số**, để trả lời: mô hình nào nên dùng cho gợi ý sản phẩm, và cải thiện đến từ đâu.

Code: `recsys/bench/` (`features.py`: đặc trưng v2, `models.py`: các họ mô hình, `run.py`: điều phối + báo cáo).
Kết quả máy sinh: `recsys/bench/reports/bench.md`, `bench.json`, `bench.log`.

```
python -m recsys.bench.run            # đầy đủ (~1 giờ trên 8 lõi CPU)
python -m recsys.bench.run --quick    # thử nhanh
```

---

## 1. Bài toán

MBD-mini (Sber AI Lab): 100.224 khách × 12 tháng báo cáo (2022-02 … 2023-01). Với mỗi khách $u$ và tháng báo cáo
$m$, nhãn $y_{u,m,k} \in \{0,1\}$ cho biết khách có mua sản phẩm $k \in \{1,2,3,4\}$ trong **tháng $m+1$** hay không.

Hai cách dùng, hai họ chỉ số (giữ nguyên như `recsys/metrics.py`):

| Cách dùng | Câu hỏi | Chỉ số |
|---|---|---|
| Gợi ý cho từng khách (RM, app) | Với khách $u$, xếp 4 sản phẩm theo khả năng mua | Hit@1, MRR, MAP@3 (chỉ trên dòng có ≥ 1 sản phẩm được mua) |
| Chiến dịch một sản phẩm | Với sản phẩm $k$, chọn top $x\%$ khách | ROC-AUC, PR-AUC, lift@1%, recall@5% |

Đặc điểm khó: **nhãn cực hiếm** (tỉ lệ dương train: sp1 0,51%, sp2 **0,048%**, sp3 0,42%, sp4 0,26%), tín hiệu
thô chỉ có **hai đại lượng giao dịch** mỗi tháng (`trans_count` luỹ kế, `diff_trans_date`) cùng lịch sử mua.

## 2. Xử lý dữ liệu

### 2.1 Nguồn và đường đi

```
silver_mbd.target_month (Iceberg, Lake Formation)
   │  Athena, persona data engineer (recsys.data.athena_df, cache parquet theo băm câu SQL)
   ▼
recsys.mbd.FEATURE_SQL: 30 đặc trưng gốc (cửa sổ SQL theo khách)   ── dùng chung với LightGBM gốc
   │  pandas (recsys.bench.features.add_v2)
   ▼
58 đặc trưng v2 = 30 gốc + 28 mới
```

### 2.2 Chống rò rỉ thời gian (quan trọng nhất)

- `trans_count` trong silver là **luỹ kế** (tăng dần theo tháng), nên hoạt động trong tháng là **chênh lệch**:
  $\text{trx}_m = tc_m - tc_{m-1}$.
- Nhãn của dòng $m-1$ là việc mua trong tháng $m$, **đã biết** vào ngày báo cáo $m$ ⇒ được dùng làm đặc trưng
  (`t{k}_lag1`). Nhãn của chính dòng $m$ thì **không bao giờ** vào đặc trưng.
- Mọi phép trượt (`rolling`) và trễ (`shift`) đều theo từng khách, theo thứ tự `report_month`, chỉ nhìn về quá khứ.
- **Kiểm tra tự động** (`features.check_no_leak`, chạy mỗi lần benchmark): `t{k}_lag1` của dòng $m$ phải đúng bằng
  nhãn dòng $m-1$ cùng khách; không đặc trưng nào có $|r| \ge 0{,}5$ với nhãn của chính dòng.

### 2.3 Chia tập theo thời gian

Không chia ngẫu nhiên (sẽ rò rỉ tương lai qua cùng một khách): train = tháng báo cáo < 2022-11-30 (902.016 dòng),
valid = 2022-11-30 (100.224), test = 2022-12-31 và 2023-01-31 (200.448). Valid dùng để dừng sớm, chọn siêu tham số,
chọn thành viên ensemble; **test chỉ dùng một lần** để báo cáo.

### 2.4 Đặc trưng v2 (28 đặc trưng mới)

| Nhóm | Đặc trưng | Ý nghĩa |
|---|---|---|
| Động thái giao dịch | `trx_m2`, `trx_m3`, `trx_mean3`, `trx_std3`, `trx_mean6`, `trx_max6` | Mức và độ biến động hoạt động 3–6 tháng |
| Xu hướng | `trx_trend` = $\text{trx}_m - \overline{\text{trx}}_{m-3..m-1}$, `trx_ratio_mean` | Khách đang tăng hay giảm hoạt động |
| Ngủ đông | `zero_streak` (số tháng liên tiếp không giao dịch), `dtd_delta`, `log_tc` | Dấu hiệu rời bỏ |
| Lịch sử mua từng sản phẩm | `t{k}_rate` = số lần mua / số tháng quan sát, `t{k}_ever`, `t{k}_recent3` | Tần suất và độ gần |
| Lịch sử mua chung | `any_lag1`, `any_recent3`, `any_since`, `n_products_ever`, `any_rate` | Khách "hay mua" nói chung |

## 3. Lý thuyết và toán học các mô hình

### 3.1 Gradient boosting cây quyết định (GBDT): nền chung của LightGBM, XGBoost, CatBoost

Mô hình cộng $F_T(x) = \sum_{t=1}^{T} \eta\, f_t(x)$, mỗi $f_t$ là một cây hồi quy. Đầu ra xác suất
$\hat p = \sigma(F(x)) = 1/(1+e^{-F(x)})$, hàm mất mát log-loss (cross-entropy nhị phân):

$$\ell(y,F) = -\,y\log\sigma(F) - (1-y)\log(1-\sigma(F)).$$

Tại vòng $t$, khai triển Taylor bậc hai quanh $F_{t-1}$ với $g_i = \partial_F \ell = \hat p_i - y_i$ và
$h_i = \partial_F^2 \ell = \hat p_i(1-\hat p_i)$:

$$\mathcal{L}^{(t)} \approx \sum_i \Big[g_i f_t(x_i) + \tfrac12 h_i f_t(x_i)^2\Big] + \gamma J + \tfrac12\lambda\sum_{j=1}^{J} w_j^2,$$

với $J$ lá, $w_j$ giá trị lá. Với tập mẫu $I_j$ rơi vào lá $j$, đặt $G_j=\sum_{i\in I_j} g_i$, $H_j=\sum_{i\in I_j} h_i$:

$$w_j^\* = -\frac{G_j}{H_j + \lambda}, \qquad \text{Gain}(\text{tách}) = \tfrac12\Big[\frac{G_L^2}{H_L+\lambda} + \frac{G_R^2}{H_R+\lambda} - \frac{(G_L+G_R)^2}{H_L+H_R+\lambda}\Big] - \gamma.$$

$\lambda$ (`lambda_l2`) co giá trị lá về 0; $\gamma$ (`min_gain_to_split`) cấm tách lợi ít. Với nhãn hiếm, $H$ của
lá chứa ít dương rất nhỏ nên lá dễ "học thuộc", vì vậy cần `min_data_in_leaf` lớn và $\lambda$ lớn.

**Khác nhau giữa ba thư viện:**

| | LightGBM | XGBoost (`hist`) | CatBoost |
|---|---|---|---|
| Cách mọc cây | theo lá (leaf-wise): tách lá có gain lớn nhất toàn cây → sâu, mạnh, dễ quá khớp | theo tầng (depth-wise) tới `max_depth` | cây **đối xứng** (oblivious): mọi nút cùng tầng dùng cùng một phép tách → biến thiên thấp, ít quá khớp |
| Tìm điểm tách | histogram `max_bin` + GOSS/EFB | histogram | histogram + **ordered boosting**: gradient của mẫu $i$ tính từ mô hình chỉ học các mẫu đứng trước $i$ trong một hoán vị ngẫu nhiên, khử thiên lệch gradient (prediction shift) |
| Điểm mạnh với bài này | nhanh, tốt với nhiều dòng | ổn định | tốt khi tín hiệu yếu, nhiễu cao |

### 3.2 Lấy mẫu âm và hiệu chỉnh xác suất

Train giữ mọi mẫu dương và chỉ $r = 0{,}25$ mẫu âm (nhanh gấp ~4 lần, cân bằng hơn). Theo Bayes, odds mô hình học
được bị nhân $1/r$:

$$\frac{\hat p_s}{1-\hat p_s} = \frac{1}{r}\cdot\frac{p}{1-p} \;\Rightarrow\; p = \frac{r\,o_s}{1 + r\,o_s},\quad o_s=\frac{\hat p_s}{1-\hat p_s}.$$

Vì cùng $r$ cho mọi sản phẩm nên **thứ tự** xếp hạng không đổi, nhưng để **trộn** mô hình (ensemble) và so với GRU
(huấn luyện không lấy mẫu) thì mọi điểm đều được đưa về thang gốc bằng công thức trên (`models.calibrate`).

### 3.3 Mô hình gộp đa sản phẩm (multi-task, dạng "dài")

Thay 4 mô hình riêng, nhân mỗi dòng thành 4 dòng $(u, m, k)$ với mã sản phẩm $k$ (biến phân loại) và đặc trưng
"của chính sản phẩm đó" (`own_lag1..3`, `own_cnt`, `own_since`, `own_rate`…). Một cây chung học quy luật dùng chung
("mua tháng trước thì dễ mua lại") từ **tổng** số mẫu dương của cả 4 sản phẩm, lợi cho sp2 (chỉ 432 dương ở train).
Đầu ra cùng một thang nên so trực tiếp giữa các sản phẩm.

### 3.4 Mạng hồi quy GRU theo chuỗi tháng

Đầu vào bước $m$: $x_m$ = (hoạt động giao dịch chuẩn hoá, tháng trong năm, nhãn trễ 1 tháng của 4 sản phẩm). GRU:

$$z_m=\sigma(W_z x_m+U_z h_{m-1}),\quad r_m=\sigma(W_r x_m+U_r h_{m-1}),$$
$$\tilde h_m=\tanh\!\big(W_h x_m+U_h(r_m\odot h_{m-1})\big),\quad h_m=(1-z_m)\odot h_{m-1}+z_m\odot\tilde h_m,$$

đầu ra $\hat y_m = \sigma(\text{MLP}(h_m)) \in [0,1]^4$ (đa nhãn). Mạng **nhân quả**: $h_m$ chỉ phụ thuộc
$x_{\le m}$, và $x_m$ chỉ chứa nhãn của bước $m-1$ ⇒ không rò rỉ. Mất mát BCE có trọng số dương theo sản phẩm
$w_k=\min(50, (1-\pi_k)/\pi_k)$ (thay cho lấy mẫu âm), chỉ tính trên bước thuộc train (mặt nạ); hiệu chỉnh ngược
$o = o_w / w_k$ giống 3.2. Hai tầng GRU 64 chiều, dropout 0,1, AdamW, cắt gradient 1,0, dừng sớm theo PR-AUC valid.

### 3.5 Ensemble

Trung bình xác suất đã hiệu chỉnh của tập con mô hình tốt nhất. Thành viên chọn **tham lam trên valid MAP@3**
(thêm dần theo thứ hạng valid). Trung bình giảm phương sai khi các mô hình sai khác nhau (cây theo lá, cây đối
xứng, mạng hồi quy).

### 3.6 Chỉ số

- $\text{Hit@1}$: tỉ lệ dòng mà sản phẩm xếp đầu được mua. $\text{MRR}=\frac1N\sum 1/\text{rank}_{\text{dương đầu tiên}}$.
- $\text{MAP@3} = \frac1N\sum_u \frac{1}{\min(|Y_u|,3)}\sum_{i=1}^{3} P@i\cdot \mathbb 1[\text{sp hạng } i \in Y_u]$.
- PR-AUC (average precision) $=\sum_n (R_n - R_{n-1}) P_n$: với nhãn hiếm, đáng tin hơn ROC-AUC (ROC-AUC
  "đẹp giả" vì số âm khổng lồ). lift@1% = tỉ lệ dương trong top 1% / tỉ lệ dương chung.
- **Khoảng tin cậy**: bootstrap ghép cặp 1000 lần trên các dòng test có dương; chênh lệch có ý nghĩa khi CI 95%
  không chứa 0. Cần thiết vì test chỉ có ~1.400 dòng có dương (sp2 chỉ 81).

## 4. Vấn đề khi huấn luyện và cách xử lý

| Vấn đề | Biểu hiện | Xử lý |
|---|---|---|
| Nhãn cực hiếm (0,05–0,5%) | Mô hình đoán toàn 0 vẫn đạt accuracy 99,5%; ROC-AUC cao nhưng vô dụng | Đánh giá bằng PR-AUC, lift, MAP; lấy mẫu âm (cây) hoặc trọng số dương (GRU); `min_data_in_leaf`, `lambda_l2` lớn |
| Dừng sớm rất sớm (vài chục vòng) | Valid PR-AUC nhiễu vì chỉ vài chục mẫu dương / sản phẩm | Patience 100; learning rate nhỏ khi tinh chỉnh; ghi `best_iter` để thấy |
| Lệch mục tiêu khi tinh chỉnh | Tối ưu PR-AUC trung bình từng sản phẩm làm **MAP@3 giảm** (lần thử nhanh: 0,6906 → 0,6759) | Chọn siêu tham số theo **valid MAP@3**, đúng mục tiêu dùng thật |
| So điểm giữa sản phẩm | Mô hình riêng từng sản phẩm, xác suất sau lấy mẫu lệch thang | Hiệu chỉnh odds (3.2) trước khi xếp hạng và trộn |
| Rò rỉ thời gian | `trans_count` luỹ kế; nhãn tháng sau | Đặc trưng là chênh lệch; chỉ dùng nhãn trễ; kiểm tra tự động; chia theo thời gian |
| Phương sai đánh giá | sp2 có 81 dương ở test | Bootstrap CI; không kết luận từ chênh lệch nhỏ |
| Windows: crash `access violation` | `lgb.Dataset` khi import lightgbm sau pyarrow, hoặc mảng Fortran-order | Import lightgbm trước pandas; `np.ascontiguousarray` |
| `max_bin` | LightGBM báo Fatal khi đổi `max_bin` sau khi dựng Dataset | Truyền `max_bin` vào `Dataset(params=...)` |
| Biến phân loại ở tập valid | "unknown name in categorical_feature" | Khai `feature_name` và `categorical_feature` cho cả Dataset valid |
| GRU: chuỗi độ dài khác nhau | Khách vào giữa kỳ | Đệm 0 + mặt nạ theo split cho mất mát và đánh giá |

## 5. Kết quả

Chạy đầy đủ ngày 19-11-2026 trên 8 lõi CPU, tổng khoảng 15 phút. Số liệu máy sinh nằm ở
`recsys/bench/reports/bench.md` và `bench.json`. Tập test gồm 200.448 dòng (2 tháng). Chỉ số xếp hạng tính trên 1.434
dòng có ít nhất một sản phẩm được mua.

**Xếp hạng sản phẩm cho từng khách (test):**

| Mô hình | Hit@1 | MRR | MAP@3 | Δ MAP@3 so với LightGBM gốc (CI 95%) | Thời gian train |
|---|---:|---:|---:|---|---:|
| **LightGBM multi-task** | **0,508** | **0,711** | **0,697** | **+0,024 [+0,012; +0,036]** | 26 s |
| LightGBM tinh chỉnh (16 thử) | 0,497 | 0,704 | 0,691 | +0,018 [+0,005; +0,031] | 412 s |
| Ensemble (lgb_tuned + GRU) | 0,493 | 0,702 | 0,689 | +0,015 [+0,003; +0,027] | — |
| LightGBM đặc trưng v2 | 0,476 | 0,696 | 0,682 | +0,009 [−0,005; +0,023] | 27 s |
| LightGBM gốc (đặc trưng v1) | 0,460 | 0,686 | 0,674 | 0 | 10 s |
| GRU theo chuỗi | 0,461 | 0,682 | 0,668 | −0,005 [−0,018; +0,008] | 224 s |
| XGBoost | 0,453 | 0,680 | 0,667 | −0,007 [−0,016; +0,002] | 34 s |
| Luật lặp lại (repeat) | 0,421 | 0,663 | 0,650 | −0,024 [−0,042; −0,006] | 0 |
| Phổ biến (popularity) | 0,315 | 0,591 | 0,575 | — | 0 |
| CatBoost | 0,240 | 0,526 | 0,472 | −0,202 [−0,219; −0,184] | 80 s |

**Nhắm khách theo sản phẩm (trung bình 4 sản phẩm, test):** ROC-AUC của mọi họ cây nằm trong khoảng 0,745–0,770, lift@1%
từ 17 đến 19 lần. Ensemble có ROC-AUC cao nhất (0,770). LightGBM tinh chỉnh có lift@1% cao nhất (19,0). Chênh lệch giữa
các mô hình ở góc nhìn này nhỏ hơn nhiều so với góc nhìn xếp hạng theo khách.

**Đọc kết quả:**
- **Multi-task thắng vì chia sẻ thống kê giữa sản phẩm.** sp2 chỉ có khoảng 430 ca dương ở train. Mô hình riêng của sp2
  dừng sớm và cho thang điểm nhiễu. Mô hình chung học quy luật "mua lại" từ cả 4 sản phẩm (đặc trưng `own_*`), nên sp2
  đạt PR-AUC 0,0137 so với 0,0082 của mô hình gốc. Mô hình chung cũng cho **một thang điểm duy nhất**, đúng thứ mà xếp
  hạng giữa các sản phẩm cần.
- **Đặc trưng v2** giúp ít (+0,009, CI chứa 0). **Tinh chỉnh** giúp có ý nghĩa nhưng tốn gấp 16 lần thời gian.
- **GRU** tốt ở valid (0,694) nhưng không giữ được trên test: chuỗi chỉ có 12 tháng với 2 đại lượng giao dịch, quá ít
  tín hiệu cho mô hình chuỗi.
- **CatBoost hỏng ở xếp hạng, không hỏng ở từng sản phẩm.** AUC từng sản phẩm vẫn từ 0,70 đến 0,80. Nguyên nhân: với
  Logloss, CatBoost khởi đầu từ logit 0 (p = 0,5) thay vì tỉ lệ nền. sp2 dừng sớm ở vòng 13, nên điểm vẫn gần 0,5 và
  luôn đứng đầu danh sách của mọi khách. Cách sửa (chưa chạy lại): `boost_from_average=True`, hoặc dừng sớm theo Logloss
  thay vì PRAUC. Đây là ví dụ cho thấy **AUC theo sản phẩm không đủ để chọn mô hình gợi ý**: phải đo thêm chỉ số xếp
  hạng và độ hiệu chỉnh.

## 6. Kết luận và khuyến nghị

1. **Chọn LightGBM multi-task làm mô hình chính (champion)** cho hệ gợi ý v2: tốt nhất trên test với cải thiện có ý
   nghĩa thống kê, train nhanh nhất, và một mô hình thay cho bốn mô hình.
2. Giữ **luật repeat làm đối chứng (control)** khi thử nghiệm online (A/B), vì đó là cách ngân hàng làm khi chưa có mô
   hình.
3. Chưa nên dùng ensemble: chỉ hơn ở valid, trên test không hơn multi-task, mà vận hành phức tạp hơn.
4. Trước khi hiển thị xác suất cho người dùng, cần **hiệu chỉnh** trên tập valid (isotonic hoặc Platt) và đo Brier / ECE.
   Hiệu chỉnh odds theo tỉ lệ lấy mẫu chỉ sửa lệch do lấy mẫu, không sửa lệch do mô hình.
5. Muốn tăng tiếp thì phải thêm dữ liệu, không phải thêm mô hình: nạp phần `detail` của MBD (giao dịch, địa lý, hội thoại).
