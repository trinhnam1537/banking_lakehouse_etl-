# Rà soát framework đọc/ghi tầng sourcing (13-10-2026)

Rà soát `sourcing/` (reader, writer, runner) khi chuẩn bị nạp 7 nguồn mới, đối chiếu với cách các
hệ thống lớn (Kafka Connect, Debezium, Airbyte, Sqoop, Firehose) giải cùng bài toán. Mục 1 là
những gì **đã sửa** trong đợt này, có test. Mục 2 là những gì **còn lại**, xếp theo mức ưu tiên.

## 1. Đã sửa

### 1.1 JDBC: câu SQL nằm trong entry

| Trước | Sau |
|---|---|
| Reader tự dựng `SELECT * FROM schema.table WHERE <mốc> > %s [AND extra_where] ORDER BY <mốc>` | Entry khai **nguyên câu SQL** (`jdbc.query`). Reader chỉ bind `:watermark`, `:chunk_size`, `:run_ts`, `:<params>` |
| Không JOIN được, không chọn cột được, không che dữ liệu nhạy cảm được | Viết được JOIN, chọn cột, ép kiểu, **che số thẻ ngay tại nguồn** (`corebank_card`) |
| `SELECT *`: nguồn thêm cột là raw đổi hình dạng | Cột liệt kê tường minh |

**Đối chiếu.** Sqoop có `--query "... WHERE $CONDITIONS"` và Kafka Connect JDBC có `query` mode,
nhưng cả hai **tự chèn** điều kiện vào câu của người dùng, nên dễ vỡ khi câu có `GROUP BY` hay
`UNION`. Ở đây tham số **tường minh**: tác giả entry tự đặt `:watermark` vào đúng chỗ.

Các hàng rào đi kèm (`sourcing/sqltemplate.py`, `config._validate_jdbc`):

- Kiểm tra tĩnh **trong pytest**: đúng một câu, bắt đầu bằng `SELECT`/`WITH`. CDC1 phải có
  `:watermark` (nếu không, mỗi lần chạy kéo lại toàn bộ). CDC5 không được có `:watermark`. Tham số
  phải khai. `chunk_size` cần `LIMIT :chunk_size` và mốc kiểu `int`.
- Tokenizer bỏ qua chuỗi, comment và `::` của Postgres. Tự thoát `%`.
- Phiên **READ ONLY** ở phía database (`set_session(readonly=True)` với Postgres,
  `SET SESSION TRANSACTION READ ONLY` với MySQL).
- Bind đúng kiểu: mốc `int` thành số thật (so chuỗi thì `'9' > '10'`), `timestamp` với MySQL thành
  `YYYY-MM-DD HH:MM:SS` UTC (MySQL không hiểu `T` và `+00:00`).
- Kết quả thiếu cột mốc thì **báo lỗi ở dòng đầu**. Trước đây mốc lặng lẽ không bao giờ tiến.
- Manifest ghi `columns` từ `cursor.description`, để silver phát hiện schema drift mà không phải đọc file.

### 1.2 JDBC: đọc chia khúc theo keyset

`jdbc.chunk_size > 0`: chạy lại câu với `:watermark` = mốc của khúc trước, cho tới khi một khúc
trả ít hơn N dòng. Mỗi khúc là một truy vấn ngắn, sau đó **kết thúc transaction**.

- Không giữ cursor hay snapshot hàng giờ. Postgres không bị giữ dead tuple. MariaDB không cắt kết
  nối vì `net_write_timeout`.
- Đứt mạng chỉ mất một khúc.
- Chặn lặp vô hạn: khúc đầy mà mốc không tiến thì báo lỗi (dấu hiệu câu SQL lọc sai).

**Đối chiếu:** Debezium *incremental snapshot* và Airbyte đọc bảng lớn đúng theo cách này.

### 1.3 Writer: gom nhiều lô vào một file, cắt theo ngưỡng

| Trước | Sau |
|---|---|
| Mỗi lô của reader thành ≥ 1 file. JDBC đọc lô 5.000 dòng, nên bảng 1 triệu dòng thành **212 file nhỏ** | Bản ghi được nén gzip **ngay khi vào** và cuộn file khi chạm `max_records_per_file` hoặc `max_file_mb` (64 MB nén) |
| Bộ đệm là list dict | Bộ đệm là byte đã nén, nhỏ hơn khoảng 10 lần |

Bất biến "ghi dữ liệu trước, tiến mốc sau" được giữ nhờ **runner gọi `writer.flush()` trước
mỗi lần tiến checkpoint** (PARTIAL và SUCCESS). Có test canh: tại thời điểm ghi mốc, mọi bản ghi
tới mốc đó đã nằm trên S3.

**Đối chiếu:** `flush.size` / `rotate.interval.ms` của Kafka Connect S3 Sink, buffer size và
interval của Firehose.

### 1.4 Writer: CDC5 ghi mới trước, xoá cũ sau

| Trước | Sau |
|---|---|
| Xoá partition hôm nay **trước** lô đầu tiên | Ghi file mới, ghi manifest, **rồi mới** xoá file cũ trong partition hôm nay (`cleanup_superseded`) |
| Job chết giữa chừng thì hôm đó không còn ảnh chụp nào, và silver đang đọc manifest cũ gặp file đã bị xoá | Job chết thì ảnh chụp cũ còn nguyên. File dở dang được lần thành công sau dọn |

Đây là mô hình *write-audit-publish*: ảnh chụp mới chỉ thay ảnh cũ khi đã đủ.

### 1.5 Đọc file theo luồng (`s3file`, `httpfile` mới)

| Trước | Sau |
|---|---|
| `s3file` gọi `Body.read()` rồi parse thành list. File 470 MB cần vài GB RAM, trong khi Glue Python Shell có 1 GB | Luồng byte → gzip → giải mã → parser, gom lô `batch_size`. **Đo:** CSV 74 MB, RAM đỉnh 211 MB |
| Mốc gắn vào lô của cả file | Chỉ **lô cuối** của mỗi file mang mốc. File đọc dở không bao giờ được tính là đã xong. File chỉ có header vẫn tiến mốc |
| Không có khoá cho file không có mã bản ghi | Thêm `_src_file` + `_src_row`, khoá tự nhiên để khử trùng khi file bị đọc lại |
| `DictReader` ghi đè cột trùng tên **im lặng** | Header trùng tên thì báo lỗi (IBM AML có hai cột `Account`) |
| CSV có header lặp giữa file bị lệch cột im lặng | `csv_repeated_header: true` (file tỷ giá năm của CNB) |
| `csv.field_size_limit` 128 KB | Nâng lên, vì OpenSanctions có ô dài hàng chục KB |
| Bỏ qua thư mục và file rỗng | Bỏ qua thêm file đánh dấu (`_SUCCESS`, `.keep`) |

**Reader mới `httpfile`**: tải file qua HTTP theo luồng, lấy danh sách từ `urls` tĩnh hoặc từ một
index JSON nhiều bước (`index_follow`). CDC1 theo khoá file (phiên bản). Có `require_contiguous`:
nguồn chỉ giữ N phiên bản thì phát hiện được việc **mất** delta ở giữa, thay vì nạp tiếp và để
thiếu dữ liệu mà không ai biết.

### 1.6 API: cửa sổ ngày và `Retry-After`

- `api.date_window`: một request cho mỗi ngày từ sau mốc tới hôm nay − N (theo múi giờ nguồn).
  Mốc là ngày đã gọi xong. Có trần `max_windows` để backfill dài chia ra nhiều lần chạy. Đây là
  `DatetimeBasedCursor` của Airbyte low-code.
- 429/503 tôn trọng header `Retry-After` (trần 60 giây). Không có header thì backoff cấp số nhân
  cộng **jitter**, để nhiều job bị chặn cùng lúc không thử lại đúng cùng một khoảnh khắc.

### 1.7 Linh tinh

| Sửa | Vì sao |
|---|---|
| So mốc `timestamp` theo **thời điểm**, không so chuỗi | Driver trả datetime không múi giờ, mốc khởi tạo có `+00:00`. So chuỗi giữa hai định dạng hay hai múi giờ cho kết quả sai |
| `--RAW_BUCKET` được đẩy vào biến môi trường | Entry `s3file` dùng `"${RAW_BUCKET}"` trước đây không được thay giá trị trên Glue |
| Manifest có `etl_release` | Biết lần chạy dùng bản code nào |
| `PENDING_PROFILES` | Entry đang bật mà dùng profile repo hạ tầng chưa tạo thì bị chặn trong pytest |

---

## 2. Còn lại (đề xuất)

| # | Vấn đề | Mức | Đề xuất | Đối chiếu |
|---|---|---|---|---|
| 1 | `advance_checkpoint` là đọc rồi ghi, không điều kiện. Hai lần chạy chồng nhau có thể làm mốc **lùi** | Trung bình (đã có khoá theo entry ở Step Functions) | `ConditionExpression` theo `run_id` hoặc so mốc mới ≥ mốc cũ trước khi ghi | Kafka consumer group fencing (generation id) |
| 2 | CDC1 theo `updated_at` vẫn có thể sót dòng commit muộn hơn `lookback_seconds` | Thấp với nguồn hiện tại | Dài hạn: CDC theo log (Debezium, Postgres logical replication) thay cho truy vấn | Debezium |
| 3 | CDC1 theo truy vấn **không thấy DELETE cứng** | Thấp (nguồn giả lập xoá mềm) | Entry CDC5 định kỳ để đối soát, hoặc CDC theo log | |
| 4 | `httpfile` chỉ thử lại lúc **mở** kết nối. Đứt giữa file 370 MB thì cả lần chạy hỏng | Thấp | HTTP Range để đọc tiếp từ byte đã nhận | |
| 5 | Writer chỉ ghi NDJSON gzip | Thấp ở raw | Giữ nguyên ở raw (không áp schema). Parquet là việc của silver | |
| 6 | Mốc `s3file` theo khoá S3 bỏ sót file đến muộn có khoá nhỏ hơn mốc | Trung bình với đối tác gửi bù | Đọc theo S3 Event qua SQS cho nguồn không bảo đảm thứ tự | Firehose, S3 event notifications |
| 7 | `sourcing.main --dry-run` vẫn ghi `RUN#` | Thấp (đã ghi ở SOURCING-ARCHITECTURE mục 14) | Runner nhận store qua constructor | |
| 8 | Kafka reader yield **mỗi partition một lô**: 6 partition sinh 6 lô nhỏ mỗi vòng | Đã giảm nhờ writer gom file | — | |
| 9 | Không có **data contract** ở raw | Trung bình | Manifest đã có `columns`. Bước tiếp: so với lần trước và cảnh báo khi cột biến mất | Airbyte schema change detection, dlt |
| 10 | Secret đọc mỗi lần chạy | Không đáng kể | — | |

## 3. Kiểm chứng

- pytest: 331 test pass (trước đợt này là 292). Mới thêm `test_reader_jdbc.py`,
  `test_reader_httpfile.py`, cùng test cho writer cuộn file, CDC5 ghi trước xoá sau, flush trước
  checkpoint, đọc file theo luồng, header trùng tên và header lặp, `Retry-After`, cửa sổ ngày.
- Chạy thật trên dữ liệu thật: Berka CTU (chia khúc, READ ONLY), VCB, OpenSanctions (bản đầy đủ và
  delta), CNB.
- Chạy end-to-end trên stack local: 37 entry. Kết quả ở [README.md](README.md#6-chạy-toàn-bộ-trên-máy-stack-local).
