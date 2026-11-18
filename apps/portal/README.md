# FinGraph Portal: ứng dụng cho người dùng cuối

Lớp downstream của lakehouse: một ứng dụng web (Streamlit) để người dùng nghiệp vụ dùng dữ liệu mà không
cần biết Athena, Glue hay Neptune. Portal **không có quyền riêng**: mỗi vai trò chạy bằng credential của
role persona tương ứng. Lake Formation che cột PII và lọc dòng khách hàng ở tầng lưu trữ, nên portal không
tự che hay lọc gì thêm, và không có đường vòng để đọc dữ liệu ngoài quyền của vai trò.

```
Người dùng ──▶ Portal (Streamlit, máy người dùng / máy nội bộ)
                 │  boto3.Session(profile = role persona)
                 ├──▶ Athena (workgroup của persona) ──▶ Lake Formation ──▶ S3 Iceberg (silver/gold/pretrain/onto)
                 ├──▶ Lambda neptune-ops ──▶ Neptune (chỉ Data engineer)
                 └──▶ agent/ (9 tool + grounding) ──▶ Claude trên Bedrock
```

## Vai trò và trang

| Vai trò | Profile mặc định | Trang |
|---|---|---|
| Chuyên viên phân tích / RM / AML (DA-BA) | `fg-analyst-mcp` | Tổng quan, Khách hàng 360, Điều tra AML (hàng đợi cấm vận), Gian lận thẻ, Truy vấn SQL, Trợ lý AI |
| Data engineer | `fg-de-mcp` | Như trên, cộng lần chạy Glue gần nhất và tìm đường liên hệ trên Neptune |
| Khách hàng (client 2) | `fg-client2-mcp` | Tài khoản của tôi: tài khoản, số dư theo tháng, 50 giao dịch gần nhất, khoản vay |

| Trang | Nguồn | Ghi chú |
|---|---|---|
| Tổng quan | Glue Data Catalog (metadata đã lọc theo LF) | Mỗi vai trò thấy số bảng khác nhau |
| Khách hàng 360 | `track_a_gold.berka_client_month`, `track_a_onto` (tool `customer_360`) | Hồ sơ, lịch sử số dư, sản phẩm đang giữ, quan hệ trong đồ thị tri thức |
| Gợi ý sản phẩm | `track_a_pretrain.uc2_next_product` (tập train) | **Baseline thống kê**: tỉ lệ mua của khách cùng vùng và nhóm tuổi, kèm lift. Mốc so sánh cho model gợi ý; sẽ thay bằng bảng điểm của pipeline pretrain |
| Điều tra AML | `track_a_onto.same_as_review`, Neptune | Hàng đợi khớp tên với OpenSanctions (nhãn không PII); tìm đường liên hệ giữa hai uid |
| Gian lận thẻ | `track_a_gold.card_rule_eval_summary` / `_daily` | Precision, recall, F1 của rule realtime; cảnh báo vài giây nằm ở Grafana |
| Truy vấn SQL | Athena | Chỉ SELECT/WITH (`agent.tools.check_readonly_sql`), tải CSV |
| Trợ lý AI | `agent.loop.ask` | Trả lời kèm trích dẫn `[uid:…]`, bị chặn nếu trích dẫn không có trong kết quả tool |

## Chạy

```powershell
# 1. Credential persona (một lần MFA, repo hạ tầng)
..\Hai_nam_graduation_project\scripts\assume-all.ps1

# 2. Môi trường riêng cho portal (không lẫn venv của framework, vốn phải tương thích Python 3.9)
python -m venv ..\portal-venv
..\portal-venv\Scripts\python.exe -m pip install -r apps\portal\requirements.txt

# 3. Chạy từ thư mục gốc repo ETL
..\portal-venv\Scripts\streamlit.exe run apps\portal\app.py
```

Biến môi trường tuỳ chọn: `FINGRAPH_PROFILE_ANALYST`, `FINGRAPH_PROFILE_DE`, `FINGRAPH_PROFILE_CUSTOMER` (đổi
profile), `FINGRAPH_PROVIDER` (`bedrock-runtime` mặc định, hoặc `anthropic` với `ANTHROPIC_API_KEY`).

## Chi phí

Không có hạ tầng mới. Mỗi trang là vài truy vấn Athena (5 USD/TB quét, bảng gold vài MB), kết quả cache
10 phút trong phiên. Trợ lý AI tính theo token Bedrock (khoảng 0,03–0,10 USD một câu, docs/AI-AGENT.md).

## Test

`pytest tests/portal`: SQL dựng an toàn (chặn injection qua client_id/region), gợi ý bỏ sản phẩm đã có và
không tin nhóm quá nhỏ, thông báo lỗi quyền dễ hiểu. Không cần AWS hay Streamlit.

## Hạn chế

- Chạy cục bộ bằng profile AWS của người dùng; chưa có đăng nhập web (Cognito) hay triển khai dùng chung.
- Gợi ý Berka chỉ gồm khoản vay và thẻ (nhãn bảo hiểm, hộ gia đình bị lệch). Gợi ý bằng model chỉ có cho khách MBD, cần `recsys/` và model đã train (`python -m recsys.mbd`).
- Gợi ý là baseline thống kê; chưa có model.
- Trợ lý AI cần Bedrock đã điền form use case Anthropic.
