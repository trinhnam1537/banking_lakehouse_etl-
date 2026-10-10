# sourcing — framework kéo dữ liệu về tầng raw

Đây là phần **code**. Cấu hình nằm ở [`../sourcing_etl/`](../sourcing_etl/).

Sửa thư mục này khi thêm một **loại nguồn** mới (ví dụ: FTP, MongoDB).
Thêm một **nguồn cụ thể** thì không đụng vào đây.

---

## Các file

```
config.py       doc + kiem tra entry, no ${BIEN_MOI_TRUONG}
state.py        2 bang DynamoDB: job_config va job_log
writer.py       ghi raw NDJSON.gz xuong S3
runner.py       luong chung source -> raw
main.py         diem vao cua Glue job va chay tay
readers/
  base.py       giao dien chung
  api.py        REST API (phan trang, rate limit, secret)
  jdbc.py       PostgreSQL / MySQL
  kafka.py      Redpanda, moc la offset tung partition
  s3file.py     file CSV/JSON tren S3
```

---

## Luồng chạy

```
doc cau hinh (DynamoDB)
  -> ghi RUN#... trang thai RUNNING
    -> doc checkpoint (CKPT#LATEST)
      -> reader sinh tung lo
        -> ghi lo xuong S3
        -> sau moi 20 lo: ghi anh chup PARTIAL
  -> SUCCESS + tien moc, hoac FAILED
```

**Hai quy tắc không được phá:**

1. **Ghi dữ liệu trước, tiến mốc sau.** Đảo lại thì job chết giữa chừng sẽ
   bỏ sót đúng khoảng vừa đọc mà chưa kịp lưu, và không có gì báo lỗi.
2. **Mốc chỉ tiến với PARTIAL hoặc SUCCESS**, không bao giờ với FAILED.

Hệ quả có chủ ý: dữ liệu có thể **trùng** (at-least-once). Tầng bronze phải
khử trùng lặp theo khoá nghiệp vụ.

---

## Hai bảng DynamoDB

```
job_config   cau hinh tinh, Terraform seed tu YAML trong git

job_log      RUN#<iso>#<run_id>    vong doi mot lan chay
             CKPT#<iso>#<run_id>   anh chup tien do   (TTL 90 ngay)
             CKPT#LATEST           checkpoint hien hanh (KHONG TTL)
             ORCH#<run_id>         goc nhin tu Step Functions
```

TTL chỉ xoá item **có** thuộc tính `expires_at`. `CKPT#LATEST` cố ý không
có, nên checkpoint sống mãi trong khi nhật ký tự dọn.

---

## Chạy thử trên máy

```powershell
# Nap bien moi truong tu .env
Get-Content .env | ForEach-Object {
  if ($_ -match '^([A-Z_]+)=(.*)$') { Set-Item "Env:$($Matches[1])" $Matches[2] }
}

# 1. Kiem tra cu phap moi entry — chi can PyYAML, khong can AWS
python -c "from sourcing.config import load_all; print(len(load_all('sourcing_etl/sources')), 'entry OK')"

# 2. Doc nguon that nhung KHONG ghi S3, KHONG tien moc
python -m sourcing.main --config sourcing_etl/sources/berka/berka_order.yml `
  --RAW_BUCKET $env:LAKEHOUSE_BUCKET `
  --JOBLOG_TABLE fingraph-dev-src-job-log `
  --dry-run

# 3. Chay that (bo --dry-run)
python -m sourcing.main --config sourcing_etl/sources/berka/berka_order.yml `
  --RAW_BUCKET $env:LAKEHOUSE_BUCKET `
  --JOBLOG_TABLE fingraph-dev-src-job-log
```

Thư viện cần: `pip install -r sourcing/requirements.txt`.
`confluent-kafka` và `psycopg2` chỉ cần khi chạy nguồn tương ứng — Glue nạp
chúng qua `runtime.additional_python_modules` trong entry YAML.

---

## Thêm một loại nguồn mới

1. Viết lớp con `Reader` trong `readers/<ten>.py`, cài đặt `read()`
2. Thêm một dòng vào `readers/__init__.py`
3. Thêm dataclass cấu hình + giá trị enum `SourceType` trong `config.py`

Runner không cần biết gì — mọi khác biệt gói sau đúng một phương thức.

```python
class MyReader(Reader):
    def read(self, checkpoint):
        since = self.effective_since(checkpoint)   # moc da tru lookback
        for chunk in doc_nguon(since):
            yield Batch(
                records=[self.stamp(r) for r in chunk],
                watermark=moc_moi,
            )
```

`self.stamp()` gắn `_src_job`, `_src_type`, `_src_cdc`, `_src_ingest_ts` vào
từng bản ghi — không có chúng thì khi bronze phát hiện bản ghi lạ sẽ không
lần ngược về job nào sinh ra nó được.

---

## Xem tiến độ và nhật ký

```powershell
# Checkpoint hien tai
aws dynamodb get-item --table-name fingraph-dev-src-job-log `
  --key '{\"job_name\":{\"S\":\"berka_order\"},\"run_key\":{\"S\":\"CKPT#LATEST\"}}' `
  --profile fingraph

# 5 lan chay gan nhat
aws dynamodb query --table-name fingraph-dev-src-job-log `
  --key-condition-expression "job_name = :j AND begins_with(run_key, :p)" `
  --expression-attribute-values '{\":j\":{\"S\":\"berka_order\"},\":p\":{\"S\":\"RUN#\"}}' `
  --no-scan-index-forward --max-items 5 --profile fingraph

# Buoc job keo lai tu dau
aws dynamodb delete-item --table-name fingraph-dev-src-job-log `
  --key '{\"job_name\":{\"S\":\"berka_order\"},\"run_key\":{\"S\":\"CKPT#LATEST\"}}' `
  --profile fingraph
```
