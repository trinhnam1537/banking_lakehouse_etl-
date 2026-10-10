# Orchestrator — một Lambda kích mọi luồng job

Chỉ cần truyền `job_id`. Không cần script trên máy, không cần `start-job-run` bằng tay.

```
 EventBridge Schedule (mỗi job 1 lịch) ─┐
 Lambda console "Test" / aws lambda    ─┼─▶ Lambda  <prefix>-trigger ─▶ SFN <prefix>-job-runner ─▶ Glue job
 Rule "Job Run Succeeded"              ─┘        ▲                             │
                                                 └──── PutEvents (chain) ──────┘
```

| Thành phần | Vai trò |
|---|---|
| `orchestrator/trigger.py` | Chuẩn hoá event → đọc `job_config` MỘT lần → áp override → kiểm tra bằng `from_dict()` → ghi `ORCH#<run_id>` có điều kiện → `StartExecution` → (tuỳ chọn) đợi và trả tóm tắt |
| `orchestrator/job_types.py` | `src` (sourcing) và `slv` (silver): tên Glue job, tham số, chống trùng manifest, ai chạy sau ai |
| `terraform/modules/orchestrator` | Lambda, state machine `job-runner` dùng chung, rule `Job Run Succeeded`, lịch cho từng job (tắt mặc định) |

## `job_id`

`<loại>.<tên entry>`: `src.gleif_lei`, `slv.gleif_legal_entity`. Không có tiền tố thì
mặc định là `src`.

## Các trường của event

| Trường | Mặc định | Ý nghĩa |
|---|---|---|
| `job_id` / `job_ids` | bắt buộc | Một job, hoặc danh sách |
| `action` | `run` | `run` · `validate` (không ghi gì, không tốn Glue) · `status` |
| `wait_seconds` | `0` | Đợi execution xong (tối đa 840) rồi trả `RUN#` + `ORCH#` |
| `dry_run` | `false` | Chỉ `src`: đọc nguồn, không ghi S3, không tiến mốc, không chain |
| `overrides` | `{}` | Chỉ `src`, ví dụ `{"api.max_pages": 1}`. Cấm: `name`, `source_type`, `enabled`, `runtime.*`, `schedule.*`. Có override thì **tự tắt chain** |
| `allow_checkpoint_advance` | `false` | Override trên job **cdc1** mà không dry-run sẽ tiến checkpoint thật, nên phải bật cờ này để xác nhận |
| `chain` | `true` | Chạy xong có kích luồng sau (silver) không |
| `manifest` / `data_date` / `rebuild_from` | – | Chỉ `slv`, cần một trong hai trường đầu |
| `force` | `false` | `slv`: chạy lại manifest đã xử lý |
| `run_id` | tự sinh | Đặt tay khi muốn biết trước run_id |

Lỗi của **yêu cầu** (job không có, override sai) được trả về dạng `{"ok": false, "error": ...}`,
không raise, để EventBridge không retry một yêu cầu sai. Lỗi hạ tầng thì raise.

## Test

Event mẫu nằm trong [events/](events/). Có test canh để mọi `job_id` trong đó luôn là entry đang bật.

| File | Làm gì |
|---|---|
| `01_src_validate.json` | Xem config + tham số Glue sẽ truyền. **Chạy cái này trước tiên** |
| `02_src_dry_run_wait.json` | Gọi nguồn thật trên Glue, không ghi S3, đợi kết quả |
| `03_src_run_wait_no_chain.json` | Chạy thật, ghi landing/, **không** kích silver |
| `04_src_override_test_dataset.json` | 1 trang, ghi vào `landing/_test/...`, không đụng dataset thật |
| `05_src_run_chain_to_silver.json` | Chạy thật, xong thì silver `binance_ticker_24h` tự chạy theo |
| `06_slv_run_by_date.json` | Chạy silver theo ngày |
| `07_slv_rerun_manifest.json` | Chạy lại silver cho một manifest (thay `<run_id>`) |
| `08_src_status.json` / `09_src_status_one_run.json` | Checkpoint + 5 lần chạy gần nhất / một run_id |
| `10_src_many_jobs.json` | Nhiều job một lần |
| `11_schedule_tick.json` | Giả lập Scheduler. Gửi hai lần: lần hai trả `DUPLICATE` |
| `12_upstream_succeeded_event.json` | Giả lập event job-runner bắn khi sourcing xong |

**Trên console:** Lambda → `fingraph-dev-trigger` → tab *Test* → dán nội dung file → *Test*.

**Bằng CLI** (credential còn hạn: `. .\docs\iam\assume.ps1`):

```powershell
aws lambda invoke --function-name fingraph-dev-trigger `
  --cli-binary-format raw-in-base64-out --cli-read-timeout 900 `
  --payload file://orchestrator/events/02_src_dry_run_wait.json out.json
Get-Content out.json
```

`--cli-read-timeout 900` là bắt buộc khi có `wait_seconds`. Mặc định CLI chỉ đợi 60 giây, và
khi hết giờ CLI có thể **tự gọi lại**, tức kích job thêm một lần (khác run_id nên không bị chặn).

Test không cần AWS: `pytest tests/orchestrator`.
