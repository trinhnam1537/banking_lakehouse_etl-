# Chạy thử một job sourcing từ đầu đến cuối: nguồn -> S3 raw -> job_log.
#
#   .\scripts\run-source.ps1                                  # binance_ticker_24hr, chay tren Glue
#   .\scripts\run-source.ps1 -Job gleif_lei                   # job khac
#   .\scripts\run-source.ps1 -Mode local                      # chay tren may, ghi S3 that
#   .\scripts\run-source.ps1 -Mode dry                        # chi doc nguon, khong ghi gi
#
# Ba chế độ, từ rẻ đến đắt:
#
#   dry    Đọc nguồn thật, đếm bản ghi. Không cần AWS. Dùng để kiểm YAML.
#   local  Chạy framework trên máy, ghi S3 và DynamoDB thật. Không tốn Glue.
#   glue   Kích Glue job mà Terraform đã tạo (fingraph-<env>-src-<job>).
#          Đây là đường chạy thật khi có lịch — test đúng IAM, đúng gói
#          framework trên S3, đúng tham số mặc định.
#
# Yêu cầu cho local/glue: credential còn hạn (. .\docs\iam\assume.ps1).

param(
    [string]$Job = "binance_ticker_24hr",
    [ValidateSet("dry", "local", "glue")]
    [string]$Mode = "glue",
    [string]$EnvName = "dev",
    [string]$Region = "ap-southeast-1"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }

# Tìm file YAML theo tên job — tên file là duy nhất trên toàn cây (xem sourcing_etl/README.md)
$yml = Get-ChildItem (Join-Path $repoRoot "sourcing_etl\sources") -Recurse -Filter "$Job.yml" |
    Select-Object -First 1
if (-not $yml) { throw "Khong thay sourcing_etl/sources/**/$Job.yml" }
$relYml = $yml.FullName.Substring($repoRoot.Length + 1).Replace("\", "/")

$prefix = "fingraph-$EnvName"
$jobLogTable = "$prefix-src-job-log"

Push-Location $repoRoot
try {
    # ---------- 1. Doc nguon: luon chay truoc, re va bat loi YAML som ----------
    # Goi reader truc tiep, KHONG qua `sourcing.main --dry-run`: runner van
    # ghi RUN# vao DynamoDB ke ca khi dry-run, nen buoc nay khong can AWS.
    Write-Host "== [1] Doc nguon $Job ($relYml)" -ForegroundColor Cyan
    & $python -c @"
from sourcing.config import load_config
from sourcing.readers import build_reader
cfg = load_config('$relYml')
lines, n, first = 0, 0, None
with build_reader(cfg) as r:
    for b in r.read(None):
        lines += len(b.records)
        n += b.meta.get('records', len(b.records))
        first = first or (b.records[0] if b.records else None)
print('  ', lines, 'dong raw |', n, 'ban ghi nghiep vu')
# raw_mode=response: ban ghi nghiep vu nam trong _response theo _records_path
rec = first
if first and '_response' in first:
    rec = first['_response']
    for part in filter(None, (first.get('_records_path') or '').split('.')):
        rec = rec.get(part) if isinstance(rec, dict) else None
    rec = rec[0] if isinstance(rec, list) and rec else rec
if isinstance(rec, dict):
    cols = [k for k in rec if not k.startswith('_src_')]
    print('  ', len(cols), 'cot:', ', '.join(cols[:25]), '...' if len(cols) > 25 else '')
    print('   mau:', {k: rec[k] for k in cols[:12]})
"@
    if ($LASTEXITCODE -ne 0) { throw "Doc nguon that bai — sua YAML truoc khi chay tiep" }
    if ($Mode -eq "dry") { return }

    # ---------- 2. Credential ----------
    if (-not $env:AWS_ACCESS_KEY_ID) {
        throw "Chua co credential. Chay assume.ps1 cua repo ha tang (docs\iam\assume.ps1)"
    }
    # Bucket: bien moi truong LAKEHOUSE_BUCKET, hoac .env o goc repo nay.
    # .env sinh tu terraform output, chay tu REPO HA TANG:
    #   .\scripts\gen-env.ps1 -Out ..\banking_lakehouse_etl\.env
    $bucket = $env:LAKEHOUSE_BUCKET
    $envFile = Join-Path $repoRoot ".env"
    if (-not $bucket -and (Test-Path $envFile)) {
        $bucket = (Get-Content $envFile |
            Where-Object { $_ -match '^LAKEHOUSE_BUCKET=' }) -replace '^LAKEHOUSE_BUCKET=', ''
    }
    if (-not $bucket) {
        throw "Thieu LAKEHOUSE_BUCKET. Dat bien moi truong, hoac sinh .env bang gen-env.ps1 -Out (repo ha tang)"
    }
    Write-Host "== [2] Bucket: $bucket | job_log: $jobLogTable" -ForegroundColor Cyan

    $startedAt = (Get-Date).ToUniversalTime()

    # ---------- 3. Chay ----------
    if ($Mode -eq "local") {
        Write-Host "== [3] Chay tren may, ghi S3 that" -ForegroundColor Cyan
        & $python -m sourcing.main --config $relYml --RAW_BUCKET $bucket --JOBLOG_TABLE $jobLogTable
        if ($LASTEXITCODE -ne 0) { throw "Job that bai (exit $LASTEXITCODE)" }
    }
    else {
        $glueJob = "$prefix-src-$Job"
        Write-Host "== [3] Kich Glue job $glueJob" -ForegroundColor Cyan
        # Bo --RUN_ID: framework tu sinh. Step Functions moi can truyen de
        # hai ben cung ghi vao mot ban ghi RUN#.
        $runId = aws glue start-job-run --job-name $glueJob --query JobRunId --output text --region $Region
        if ($LASTEXITCODE -ne 0) {
            throw "Khong kich duoc $glueJob. Job co ton tai khong? (enabled: true + terraform apply)"
        }
        Write-Host "   JobRunId: $runId"

        # Python Shell khoi dong ~30-60s, chay binance vai giay
        $terminal = "SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"
        do {
            Start-Sleep -Seconds 15
            $state = aws glue get-job-run --job-name $glueJob --run-id $runId `
                --query JobRun.JobRunState --output text --region $Region
            Write-Host ("   {0:HH:mm:ss} {1}" -f (Get-Date), $state)
        } while ($terminal -notcontains $state)

        if ($state -ne "SUCCEEDED") {
            aws glue get-job-run --job-name $glueJob --run-id $runId `
                --query JobRun.ErrorMessage --output text --region $Region
            Write-Host "   Log: CloudWatch /aws-glue/python-jobs/error, stream $runId" -ForegroundColor Yellow
            throw "Glue job $state"
        }
    }

    # ---------- 4. Kiem ket qua ----------
    Write-Host "== [4] File moi tren S3" -ForegroundColor Cyan
    $dataset = & $python -c "from sourcing.config import load_config; print(load_config('$relYml').raw_path)"
    aws s3api list-objects-v2 --bucket $bucket --prefix "$dataset/" --region $Region `
        --query "Contents[?LastModified>='$($startedAt.ToString("yyyy-MM-ddTHH:mm:ss"))'].[Key,Size]" --output table

    Write-Host "== [5] Lan chay gan nhat trong job_log" -ForegroundColor Cyan
    # PS 5.1 nuot dau nhay kep khi truyen cho aws.exe -> ghi JSON ra file (xem SETUP-LOG muc 7)
    $tmp = [System.IO.Path]::GetTempFileName()
    $vals = '{":j":{"S":"' + $Job + '"},":p":{"S":"RUN#"}}'
    [System.IO.File]::WriteAllText($tmp, $vals, (New-Object System.Text.UTF8Encoding($false)))
    aws dynamodb query --table-name $jobLogTable --region $Region `
        --key-condition-expression "job_name = :j AND begins_with(run_key, :p)" `
        --expression-attribute-values "file://$tmp" `
        --no-scan-index-forward --max-items 1 `
        --query "Items[0].{run:run_key.S,status:status.S,records:records_written.N,files:files_written.N}" --output table
    Remove-Item $tmp
}
finally {
    Pop-Location
}
