# Nguồn OneDrive cá nhân: người dùng upload file, job kéo về mỗi ngày

Reader `onedrive` (`sourcing/readers/onedrive.py`) · entry mẫu `sourcing_etl/sources/onedrive/onedrive_uploads.yml`

Người dùng (BA, đối tác) kéo thả file CSV hoặc Excel vào một thư mục OneDrive. Mỗi sáng 08:00 giờ VN,
Glue `src-shell` kéo các file mới hoặc vừa sửa về `landing/onedrive/...`. Không ai phải đụng tới S3 hay AWS
Console.

```
Secrets Manager (refresh token) ──> login.microsoftonline.com ──access token 1h──> Graph /me/drive
        ▲                                   │ refresh token MỚI                     │ liệt kê thư mục
        └────────── reader ghi lại ─────────┘                                       ▼
                                                     downloadUrl (đã ký sẵn) ──> fileparse ──> landing/
```

## 1. Mật khẩu tài khoản Microsoft: KHÔNG lưu ở đâu cả

| | |
|---|---|
| Framework có cần mật khẩu không? | **Không.** Graph API không cho đăng nhập bằng mật khẩu (ROPC) với tài khoản Microsoft cá nhân. Quyền ứng dụng không cần người (client credentials) cũng chỉ có cho tenant tổ chức. |
| Vậy job chạy tự động bằng gì? | **Refresh token** OAuth. Bạn đăng nhập **một lần** trên trang của Microsoft bằng trình duyệt (device code). Script chỉ nhận token rồi ghi vào `fingraph/src/oauth/onedrive`. |
| Token có hết hạn không? | Refresh token của tài khoản cá nhân sống khoảng 90 ngày **kể từ lần dùng cuối**. Mỗi lần chạy, reader nhận token mới và ghi đè vào secret, nên job chạy hằng ngày thì không phải đăng nhập lại. |
| Phạm vi quyền | Chỉ `Files.Read` (đọc file của chính tài khoản đó) + `offline_access`. Không ghi, không xoá, không đọc mail. |
| Mất token thì sao? | Thu hồi tại https://account.live.com/consent/Manage. Đổi mật khẩu tài khoản Microsoft cũng làm token cũ mất hiệu lực. Khi đó chạy lại bước 3. |

Mật khẩu tài khoản Microsoft vừa tạo:
- Lưu trong **trình quản lý mật khẩu** của bạn (Bitwarden, 1Password, Google Password Manager). Không ghi vào
  repo, `.env`, YAML, Terraform hay Secrets Manager.
- **Bật xác minh hai bước** cho tài khoản: https://account.live.com/proofs/manage → Advanced security options →
  Two-step verification. Device code vẫn chạy bình thường, chỉ cần duyệt thêm một bước lúc đăng nhập.
- Không gửi mật khẩu qua chat/Claude. Framework không bao giờ hỏi tới nó.

## 2. Đăng ký app trên Azure (một lần, miễn phí)

1. Vào https://portal.azure.com, đăng nhập bằng **tài khoản Microsoft cá nhân vừa tạo** → **Microsoft Entra ID** →
   **App registrations** → **New registration**.
   - Name: `fingraph-onedrive-reader`
   - Supported account types: **Personal Microsoft accounts only**
   - Redirect URI: để trống
   > Nếu portal báo tài khoản chưa có directory: đăng ký **Azure free account** (Microsoft xác minh bằng thẻ,
   > không trừ tiền, app registration không tính phí). Đăng ký xong portal tự tạo directory mặc định.
2. **Authentication** → *Advanced settings* → **Allow public client flows = Yes** → Save.
   Device code bắt buộc bước này. App không có client secret nên không có bí mật nào phải giữ.
3. **API permissions** → Add → Microsoft Graph → **Delegated** → chọn `Files.Read` và `offline_access`.
   Tài khoản cá nhân không cần admin consent: lúc đăng nhập bạn tự đồng ý.
4. **Overview** → chép **Application (client) ID**. Đây không phải bí mật.

## 3. Đăng nhập một lần → secret

Cần repo hạ tầng apply trước (mục 6). Chạy từ gốc repo ETL:

```powershell
. ..\Hai_nam_graduation_project\docs\iam\assume.ps1    # hoặc để script tự dùng profile fingraph
.venv\Scripts\python.exe scripts\onedrive_login.py --client-id <Application (client) ID> --folder "Tài liệu"
# -> in ra: "To sign in, use a web browser to open https://www.microsoft.com/link and enter the code XXXXXXX"
#    mở link, nhập mã, đăng nhập, bấm Accept
# -> "Dang nhap OK: drive personal cua <ten>", "'Tài liệu': CO", "Da ghi refresh token vao secret ..."
```

Đăng nhập lại khi job báo `refresh token het han hoac bi thu hoi`:
`.venv\Scripts\python.exe scripts\onedrive_login.py` (bỏ `--client-id` thì script lấy lại `client_id` cũ trong secret).

## 4. Cấu hình entry

```yaml
onedrive:
  secret_name: fingraph/src/oauth/onedrive
  # MỘT trong hai. folder_id (ưu tiên) = tham số resid= khi mở link chia sẻ https://1drv.ms/f/...
  # (curl -I <link> -> header Location). Theo ID: đổi tên/di chuyển thư mục vẫn đọc đúng.
  folder_id: "AFEECB8F63339F68!s891c5821359e4271b88e13a60880a460"
  # folder_path: "Tài liệu/fingraph_inbox"   # tính từ gốc OneDrive
  name_pattern: "*.csv"                   # fnmatch, không phân biệt hoa thường
  file_format: csv                        # csv | ndjson | json | xlsx
  # xlsx:
  # sheet: "data"                         # rỗng = sheet đầu
  # header_row: 2                         # dòng chứa tên cột
  min_age_seconds: 300                    # file vừa sửa < 5 phút: để lần sau
  recursive: false
```

| Chi tiết | Hành vi |
|---|---|
| Thư mục `Tài liệu` | Link OneDrive web `.../Documents/Ta%CC%80i%20lie%CC%A3%CC%82u` lưu tên ở dạng **tổ hợp** (NFD). Gõ trong YAML ra dạng dựng sẵn (NFC). Reader so theo NFC, không phân biệt hoa thường, nên hai dạng đều khớp |
| Mốc CDC1 | `<lastModifiedDateTime>\|<đường dẫn>` của file cuối đã nạp. Upload **đè** file cùng tên thì thời điểm sửa đổi, file được nạp lại (bản ghi cũ vẫn ở raw: silver khử trùng theo khoá) |
| CDC5 | Mỗi lần chạy nạp lại mọi file khớp, ghi đè partition raw |
| File bị bỏ qua | `~$*` (file khoá tạm khi Office đang mở), `.`/`_` đầu tên, thư mục con (trừ `recursive: true`), file sửa chưa đủ `min_age_seconds` |
| Cột truy vết | `_src_file` (đường dẫn), `_src_row`, `_src_item_id` (ID OneDrive, không đổi khi đổi tên), `_src_modified` |
| XLSX | Lấy **giá trị** đã tính của ô công thức. Ngày giờ ghi dạng ISO, số giữ kiểu số. Cột header rỗng thành `_col<N>`. File > 64 MB bị từ chối, xuất CSV ở nguồn |
| Giới hạn | Mỗi lần chạy tối đa `max_files_per_run` file, lần sau đi tiếp từ checkpoint |

Kiểm trên máy trước khi bật lịch: đọc OneDrive thật, không ghi S3. Vẫn cần credential AWS, vì reader đọc
secret và **ghi lại** refresh token mới. Việc ghi này là bình thường.

```powershell
.\scripts\run-source.ps1 -Job onedrive_uploads -Mode dry
```

Rồi đặt `enabled: true`, merge, chạy tay qua Lambda trigger. Chạy được rồi mới bật `schedule.enabled: true`
(`cron(0 1 * * ? *)` = 08:00 giờ VN).

## 5. Chi phí (ap-southeast-1, một entry, chạy hằng ngày)

| Hạng mục | Cách tính | USD/tháng |
|---|---|---|
| Glue Python Shell `src-shell` | 0,0625 DPU × 0,44 USD/DPU-giờ, tính theo giây, tối thiểu 1 phút. Ước 3 phút/lần (gồm cài `openpyxl`) × 30 lần = 1,5 giờ | **≈ 0,04** |
| Secrets Manager | 0,40 USD/secret/tháng + 0,05 USD/10.000 lệnh gọi (~60 lệnh/tháng) | **0,40** |
| Lambda trigger + Step Functions + EventBridge Scheduler | 30 lần × vài chục transition. Nằm trong free tier, kể cả khi hết free tier vẫn dưới 0,01 | ≈ 0 |
| DynamoDB `job_log` (on-demand) | ~5 lần ghi/lần chạy | ≈ 0 |
| S3 `landing/` | 0,025 USD/GB. 100 MB file/tháng ≈ 0,003. Lifecycle xoá `landing/` sau 30 ngày. Dữ liệu từ internet vào AWS không tính phí | ≈ 0 |
| Microsoft | Tài khoản cá nhân + OneDrive 5 GB + Graph API + app registration: **miễn phí** | 0 |
| **Tổng tầng sourcing** | | **≈ 0,45** |
| *(Tuỳ chọn)* silver hằng ngày | Glue Spark `slv-spark` G.1X × 2, tối thiểu 1 phút, ~2 phút/lần ≈ 0,03 USD × 30 | ≈ 0,9 |

Thêm entry thứ hai (Excel, thư mục khác) chỉ cộng thêm phần Glue (~0,04). Secret dùng chung, không tốn thêm 0,40.
Chi phí lớn nhất là secret, không phải compute.

## 6. Hạ tầng cần có (repo `lakehouse_ontology_project`)

| Thay đổi | File |
|---|---|
| Vỏ secret `fingraph/src/oauth/onedrive` (không có giá trị trong state) | `terraform/envs/dev/secrets.tf` |
| Glue sourcing được `secretsmanager:PutSecretValue` **chỉ** dưới `fingraph/src/oauth/*`, để xoay vòng token | `terraform/modules/sourcing/iam.tf` |
| Profile `src-shell` cài thêm `openpyxl==3.1.5` | `terraform/modules/sourcing/variables.tf` |

## 7. Lỗi hay gặp

| Thông báo | Nghĩa là | Làm gì |
|---|---|---|
| `refresh token het han hoac bi thu hoi (invalid_grant)` | Quá 90 ngày không chạy, đổi mật khẩu tài khoản Microsoft, hoặc đã thu hồi quyền app | Chạy lại `scripts/onedrive_login.py` |
| `Khong lay duoc device code: ... AADSTS7000218` / `unauthorized_client` | App chưa bật *Allow public client flows* | Bước 2.2 |
| `AADSTS50020` / `AADSTS9002331` | App đăng ký sai loại tài khoản (chỉ tổ chức) | Tạo lại app với *Personal Microsoft accounts only* |
| `khong co thu muc 'X' trong '/...'` | Sai `folder_path`. Thông báo liệt kê đúng chỗ dừng | `onedrive_login.py --folder ...` in tên thư mục ở gốc |
| `CANH BAO: khong ghi lai duoc refresh token moi` | Glue thiếu `PutSecretValue` | Apply repo hạ tầng (mục 6). Token cũ vẫn chạy được tới khi hết hạn |
| Job `SUCCESS`/`NO_DATA` nhưng `records_read = 0`, file vừa upload | File sửa chưa đủ `min_age_seconds` (5 phút) lúc job **đọc thư mục**, tính cả ~20 s Glue khởi động. Reader bỏ qua có chủ ý, checkpoint không tiến | Chờ thêm rồi kích lại. Lần chạy sau tự nhặt |
| Step Functions `FAILED`, lỗi `JobLocked` | Kích lại khi execution trước **chưa nhả khoá `LOCK`**: Glue đã xong nhưng Step Functions chưa chạy tới `ReleaseLease`. Đây là cơ chế chặn chạy chồng, không phải lỗi | Đợi execution trước kết thúc rồi kích lại. Khoá cũng tự hết hạn theo `lease_until` |
| `ModuleNotFoundError: openpyxl` | Profile chưa cài `openpyxl` | Apply repo hạ tầng (mục 6) |
| `UnicodeEncodeError: 'charmap'` khi chạy trên máy | Console Windows dùng cp1252 | `run-source.ps1` đã đặt `PYTHONUTF8=1`. Chạy python trực tiếp thì đặt `$env:PYTHONUTF8 = "1"` |
