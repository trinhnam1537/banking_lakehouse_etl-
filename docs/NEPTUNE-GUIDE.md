# Sổ tay Amazon Neptune cho FinGraph: lý thuyết chi tiết và cách dùng

[NEPTUNE.md](NEPTUNE.md) trả lời *có nên dùng Neptune không, thiết kế ra sao, tốn bao nhiêu*. Sổ tay này đi sâu
hơn vào hai phần còn lại:
- **Neptune hoạt động bên trong thế nào** (phần I), đủ để giải thích các con số và lỗi gặp trên dev;
- **dùng Neptune của FinGraph thế nào**, từng lệnh (phần II–IV), kèm truy vấn mẫu và cách xử lý lỗi đã gặp.

Số liệu trong sổ tay là số đo thật trên dev (07–08/11/2026), không phải ước lượng.

---

## Mục lục

**Phần I. Lý thuyết**
1. [Graph database: mô hình và cách lưu](#1-graph-database-mô-hình-và-cách-lưu)
2. [Kiến trúc Neptune Database](#2-kiến-trúc-neptune-database)
3. [Chỉ mục và cách Neptune trả lời truy vấn](#3-chỉ-mục-và-cách-neptune-trả-lời-truy-vấn)
4. [openCypher trên Neptune](#4-opencypher-trên-neptune)
5. [Bulk loader](#5-bulk-loader)
6. [Bảo mật: IAM, SigV4, mạng](#6-bảo-mật-iam-sigv4-mạng)
7. [Neptune Analytics và thuật toán đồ thị](#7-neptune-analytics-và-thuật-toán-đồ-thị)

**Phần II. Neptune trong FinGraph**
8. [Kiến trúc đã triển khai](#8-kiến-trúc-đã-triển-khai)
9. [Mô hình dữ liệu trên cụm](#9-mô-hình-dữ-liệu-trên-cụm)

**Phần III. Cách dùng**
10. [Một phiên làm việc từ đầu đến cuối](#10-một-phiên-làm-việc-từ-đầu-đến-cuối)
11. [Lambda `neptune-ops`: từng action](#11-lambda-neptune-ops-từng-action)
12. [Sổ tay truy vấn openCypher](#12-sổ-tay-truy-vấn-opencypher)
13. [Dùng từ agent](#13-dùng-từ-agent)

**Phần IV. Vận hành**
14. [Sự cố đã gặp và cách xử lý](#14-sự-cố-đã-gặp-và-cách-xử-lý)
15. [Hiệu năng và giới hạn của `db.t4g.medium`](#15-hiệu-năng-và-giới-hạn-của-dbt4gmedium)
16. [Chi phí một phiên thật](#16-chi-phí-một-phiên-thật)
17. [Đóng góp cho luận văn](#17-đóng-góp-cho-luận-văn)

---

# Phần I. Lý thuyết

## 1. Graph database: mô hình và cách lưu

### 1.1 Đồ thị là gì về mặt dữ liệu

Một đồ thị gồm **đỉnh** (vertex, node) và **cạnh** (edge, relationship). Trong ngân hàng:
- đỉnh là khách, tài khoản, ngân hàng, vụ rửa tiền, sản phẩm;
- cạnh là "giữ tài khoản", "chuyển tiền cho", "thuộc sở hữu của", "tham gia vụ".

Bảng quan hệ cũng biểu diễn được điều này (bảng `transfer(from, to, amount)`). Khác biệt nằm ở **cách truy vấn
đi theo quan hệ**:

| | CSDL quan hệ | Graph database |
|---|---|---|
| Đi từ A sang các đỉnh kề | JOIN: tra chỉ mục khoá ngoại hoặc hash/sort cả bảng | Tra chỉ mục theo đỉnh A, chỉ đọc cạnh của A |
| Chi phí k bước nhảy | Mỗi bước một JOIN, tập trung gian có thể nổ | Tỉ lệ với số cạnh thực sự đi qua |
| Độ dài thay đổi `1..k` | CTE đệ quy (Athena/Trino hỗ trợ hạn chế) | Có sẵn trong ngôn ngữ: `-[:transfersTo*1..k]->` |
| Lược đồ | Cố định theo bảng | Linh hoạt: thêm loại cạnh không phải sửa bảng |

### 1.2 Property graph (LPG) và RDF

Neptune Database hỗ trợ **cả hai** mô hình, nhưng trên cùng cụm hai mô hình **không nhìn thấy nhau**.

- **Property graph (Labeled Property Graph):**
  - đỉnh có một hoặc nhiều **nhãn** (`:Account`, `:Party`), cạnh có đúng một **loại** (`:transfersTo`);
  - cả đỉnh và cạnh có **thuộc tính** key–value (`weight: 12`, `valid_from: 2022-09-01`);
  - ngôn ngữ: **openCypher** (khai báo, kiểu SQL cho đồ thị) và **Gremlin** (duyệt từng bước, kiểu lập trình).
- **RDF:**
  - mọi thứ là bộ ba `chủ thể – vị từ – đối tượng`, định danh bằng IRI;
  - ngôn ngữ **SPARQL**; ngữ nghĩa gắn với OWL/RDFS;
  - thuộc tính trên cạnh phải *reify* (tạo node trung gian) hoặc dùng RDF-star.

FinGraph viết ontology bằng OWL (RDF) nhưng **chiếu sang property graph** khi nạp vào Neptune. Lý do ở
[NEPTUNE.md mục 4](NEPTUNE.md#4-thiết-kế-mô-hình-dữ-liệu): cạnh của FinGraph có nhiều thuộc tính (`triple_id`,
`valid_from/to`, `weight`, `confidence`), và suy luận OWL đã tính sẵn ở Spark.

### 1.3 "Index-free adjacency" và Neptune

Một số graph DB (Neo4j) lưu con trỏ vật lý từ đỉnh tới cạnh kề, gọi là *index-free adjacency*. **Neptune không làm
vậy.** Neptune lưu mọi thứ (đỉnh, nhãn, thuộc tính, cạnh) thành **bộ bốn (quad)** trong lớp lưu trữ phân tán, và đi
theo cạnh bằng **tra chỉ mục B-tree**. Mỗi bước nhảy là một lần tra chỉ mục (log n), không phải một lần theo con
trỏ (O(1)).

Hệ quả thực tế:
- duyệt vẫn rất nhanh so với JOIN trên S3 (Athena), vì chỉ đọc đúng phần chỉ mục cần;
- nhưng **mỗi bước nhảy vẫn có giá**, nên truy vấn mở rộng không giới hạn (`*1..12` trên đỉnh có bậc cao) có thể
  chạm trần bộ nhớ hoặc thời gian. Mục 15 có số đo.

---

## 2. Kiến trúc Neptune Database

```
                ┌───────────── Cụm (DB cluster) ─────────────┐
 client ──8182──▶ Instance ghi (primary)   Replica đọc (0..15) │   ← máy tính toán: RAM = bộ đệm + bộ nhớ truy vấn
                │        │                       │             │
                │        └────────┬──────────────┘             │
                │   Cluster volume (lớp lưu trữ dùng chung)     │   ← tách khỏi máy tính toán
                │   6 bản sao / 3 AZ, đoạn 10 GB, tự tăng       │
                └─────────────────────────────────────────────┘
```

### 2.1 Lớp lưu trữ (cluster volume)

- Dữ liệu chia thành **đoạn 10 GB**; mỗi đoạn có **6 bản sao trên 3 Availability Zone**.
- Ghi thành công khi **4/6** bản sao xác nhận; đọc cần **3/6**. Mất cả một AZ (2 bản) vẫn ghi được; mất thêm một bản
  vẫn đọc được.
- Dung lượng tự tăng tới 128 TiB, tính tiền theo GB **đã dùng** (không đặt trước).
- Lưu trữ **tồn tại độc lập** với instance: dừng cụm thì instance tắt, dữ liệu vẫn còn, chỉ trả tiền lưu trữ.
  FinGraph dựa vào điều này để chạy **theo phiên**.

### 2.2 Instance

- **Primary** nhận cả đọc và ghi; **replica** chỉ đọc, dùng chung volume nên không phải sao chép dữ liệu (độ trễ
  nhất quán thường dưới 100 ms).
- RAM của instance dùng cho: **bộ đệm trang** (buffer cache, giữ phần chỉ mục nóng) và **bộ nhớ thực thi truy
  vấn** (tập trung gian, sắp xếp, gộp). Trên `db.t4g.medium` chỉ có **4 GB** cho cả hai, và đây là giới hạn
  FinGraph chạm nhiều nhất (mục 15).
- `t4g` là dòng **burstable**: CPU có mức nền, vượt mức nền thì tiêu *CPU credit*. Bulk load dài có thể tiêu hết
  credit.

### 2.3 Endpoint

| Endpoint | Trỏ tới | Dùng khi |
|---|---|---|
| Cluster endpoint | Primary hiện tại | Ghi, nạp, truy vấn cần dữ liệu mới nhất |
| Reader endpoint | Cân bằng giữa các replica | Đọc tải cao (dev **không có** replica, nên trỏ về primary) |
| Instance endpoint | Một instance cụ thể | Gỡ lỗi |

Mọi endpoint chỉ có **địa chỉ trong VPC**. Neptune **không có endpoint công khai**.

### 2.4 Giao dịch

- Mỗi truy vấn là một giao dịch ACID.
- Truy vấn chỉ đọc chạy trên **snapshot** tại thời điểm bắt đầu, nên không bị khoá bởi ghi.
- Truy vấn ghi giữ khoá trên phạm vi chỉ mục chạm tới. Bulk loader ghi theo lô lớn; FinGraph **không** truy vấn
  trong lúc nạp.

### 2.5 Dừng / bật và nạp lại

- `stop-db-cluster`: tắt mọi instance; tối đa **7 ngày** rồi AWS tự bật lại để vá lỗi.
- `start-db-cluster`: khoảng 5–10 phút.
- **Fast reset** (`/system`, hai bước lấy token rồi thực hiện): xoá sạch dữ liệu nhanh hơn nhiều so với `DELETE`
  từng đỉnh, và không cần tạo lại cụm. Sau reset, **engine khởi động lại khoảng 1 phút** trong khi trạng thái cụm
  vẫn là `available` (đã gặp, mục 14).

---

## 3. Chỉ mục và cách Neptune trả lời truy vấn

### 3.1 Mọi thứ là quad

Neptune biểu diễn đồ thị property graph thành các quad `(S, P, O, G)`:

| Thành phần đồ thị | Quad (giản lược) |
|---|---|
| Đỉnh `v1` có nhãn `Account` | `(v1, label, "Account", default)` |
| Thuộc tính `v1.label = "TK AML 1-A1"` | `(v1, "label", "TK AML 1-A1", default)` |
| Cạnh `e1: v1 -transfersTo-> v2` | `(v1, "transfersTo", v2, e1)`: id cạnh nằm ở vị trí G |
| Thuộc tính cạnh `e1.weight = 3` | `(e1, "weight", 3, default)` |

### 3.2 Ba chỉ mục mặc định

| Chỉ mục | Thứ tự khoá | Trả lời nhanh |
|---|---|---|
| **SPOG** | chủ thể → vị từ → đối tượng | "Cạnh đi ra của `v1` loại `transfersTo`", "thuộc tính của `v1`" |
| **POGS** | vị từ → đối tượng → ... | "Mọi đỉnh có nhãn `Account`", "đỉnh có `label = 'X'`" |
| **GPSO** | đồ thị (id cạnh) → ... | "Thuộc tính của cạnh `e1`" |

Chỉ mục thứ tư **OSGP** (đối tượng → ...) là **tuỳ chọn, tắt mặc định**. Không có nó, đi **ngược** cạnh
(`(a)<-[:transfersTo]-(b)` khi biết `a`) phải dùng POGS với vị từ đã biết, nên vẫn chạy nhưng kém hơn khi loại cạnh
không cố định. Truy vấn của FinGraph luôn ghi rõ loại cạnh, nên chưa cần bật OSGP (bật phải tạo lại cụm).

### 3.3 Từ câu Cypher đến kế hoạch

Engine openCypher của Neptune (*DFE*, Dataflow Engine) dịch câu truy vấn thành chuỗi phép tra chỉ mục + join
trong bộ nhớ:

```cypher
MATCH (a)-[:isHeldBy]->(p)<-[:isHeldBy]-(b) WHERE id(a) = $uid RETURN b
```

1. `id(a) = $uid`: tra trực tiếp theo id, không quét.
2. `(a)-[:isHeldBy]->(p)`: SPOG với S = `a`, P = `isHeldBy`.
3. `(p)<-[:isHeldBy]-(b)`: POGS với P = `isHeldBy`, O = `p`.

Ngược lại, `MATCH (n) WHERE n.label CONTAINS 'Khách'` phải **quét** mọi giá trị thuộc tính `label`, vì Neptune
không có chỉ mục toàn văn sẵn (cần tích hợp OpenSearch). Vì vậy tool `find_entity` của agent vẫn chạy trên Athena.

**Quy tắc viết truy vấn nhanh trên Neptune:**
1. Neo điểm bắt đầu bằng `id(n) = $x` (rẻ nhất), hoặc nhãn + thuộc tính chọn lọc.
2. Luôn ghi loại cạnh (`[:transfersTo]`), tránh `[r]` trống.
3. Giới hạn độ dài đường đi (`*1..2`, không `*`) và `LIMIT`.
4. Tránh gộp trên toàn đồ thị (`MATCH (n) ... count(*)`) trên instance nhỏ.

Muốn xem kế hoạch thực thi: gửi tới `/openCypher/explain` với `explain=dynamic`. Lambda `neptune-ops` hiện chưa
mở action này (xem mục 11, việc tiếp theo).

---

## 4. openCypher trên Neptune

### 4.1 Cú pháp cơ bản

```cypher
MATCH (a:Account)-[t:transfersTo]->(b:Account)     // mẫu: đỉnh có nhãn, cạnh có loại
WHERE id(a) = $uid AND t.weight >= 3                // điều kiện, $uid là tham số
OPTIONAL MATCH (b)-[:involvedIn]->(v)               // như LEFT JOIN: không có thì v = null
WITH a, b, count(v) AS cases                        // chuyển tiếp + gộp (như GROUP BY ngầm)
RETURN id(b) AS uid, b.label, cases                 // kết quả
ORDER BY cases DESC LIMIT 20
```

| Cấu trúc | Ý nghĩa |
|---|---|
| `(n:Label)` | Đỉnh có nhãn |
| `-[r:TYPE]->`, `<-[r:TYPE]-`, `-[r:TYPE]-` | Cạnh theo chiều / ngược chiều / không quan tâm chiều |
| `-[:A\|B]->` | Một trong nhiều loại cạnh |
| `-[:TYPE*1..3]->` | Đường đi độ dài 1 tới 3 |
| `p = (...)` | Gán cả đường đi; dùng `nodes(p)`, `relationships(p)`, `length(p)` |
| `collect(x)`, `count(DISTINCT x)` | Gộp |
| `UNWIND list AS x` | Mở danh sách thành dòng |
| `[x IN list \| expr]` | List comprehension |

### 4.2 Đặc thù của Neptune (đã kiểm trên engine 1.4.8.1)

| Điểm | Trên Neptune | Cách FinGraph xử lý |
|---|---|---|
| Id đỉnh/cạnh | Chuỗi do người nạp đặt (`:ID` trong CSV). Đọc bằng `id(n)` | `id(n)` = `uid` của Iceberg, `id(r)` = `triple_id`, nên trích dẫn tra ngược được |
| `shortestPath()` / `allShortestPaths()` | **Không hỗ trợ** | Agent tăng dần độ dài `*1`, `*2`, ... và dừng ở độ dài đầu tiên có kết quả. Đó chính là đường ngắn nhất |
| `all(x IN list WHERE ...)` | **Không hỗ trợ** trong điều kiện đường đi | Lọc sau bằng `WITH` / `UNWIND` |
| Tham số | Gửi JSON ở trường `parameters` (form-encoded) | Mọi giá trị người dùng đi qua tham số; độ dài đường đi là số nguyên đã kẹp biên |
| Độ dài đường đi theo tham số (`*1..$h`) | Không được | Chèn số nguyên sau khi `int()` + `min/max` |
| Nhiều nhãn trên một đỉnh | Có (`:A;B;C` trong CSV) | Nhãn = lớp + mọi lớp tổ tiên, nên `MATCH (p:CreditProduct)` trả cả `GoldCard` |
| Kiểu `Date` | Có | `valid_from` nạp kiểu `Date`, so sánh với `date('2022-09-01')` |
| Thời gian chờ truy vấn | Mặc định 120 s (tham số cụm `neptune_query_timeout`) | Lambda gọi với timeout 100 s |
| Ghi trong openCypher | Có (`CREATE`, `MERGE`, `SET`, `DELETE`) | Lambda **chặn** từ khoá ghi; dữ liệu chỉ vào qua bulk loader |

### 4.3 Gửi truy vấn qua HTTP

```
POST https://<endpoint>:8182/openCypher
Content-Type: application/x-www-form-urlencoded

query=MATCH (a) WHERE id(a) = $uid RETURN a.label&parameters={"uid":"9f2c..."}
```

Trả về `{"results": [ {...}, ... ]}`. Có ký SigV4 (mục 6).

---

## 5. Bulk loader

### 5.1 Vì sao không ghi bằng `CREATE`

Ghi từng câu: mỗi câu một giao dịch, mỗi giao dịch đi qua quorum 4/6 của lớp lưu trữ. Với 14 triệu bản ghi thì
hàng giờ và tốn I/O. Bulk loader đọc file **thẳng từ S3**, ghi theo lô lớn, chạy song song nhiều luồng, nhanh hơn
nhiều bậc.

### 5.2 Định dạng CSV openCypher

Hai loại file, phân biệt bằng header:

```
# vertices/*.csv
:ID,:LABEL,label:String,source_system:String,valid_from:Date,occupation:String
9f2c...,Customer;Person;Party,Khách 2,berka,1993-02-26,

# edges/*.csv
:ID,:START_ID,:END_ID,:TYPE,triple_id:String,valid_from:Date,weight:Double,confidence:Double,is_inferred:Bool
a71b...,9f2c...,4bf0...,holdsAccount,a71b...,1993-02-26,,1.0,false
```

- `:ID` bắt buộc và duy nhất. Nạp lại cùng `:ID` thì cập nhật thuộc tính, không tạo bản sao.
- Cột thuộc tính có kiểu sau dấu `:` (`String`, `Int`, `Double`, `Bool`, `Date`). Ô rỗng là không có thuộc tính.
- `:LABEL` nhiều nhãn cách nhau bằng `;`.
- Cạnh chỉ nạp được khi **cả hai đỉnh đã có**. Loader nạp file đỉnh trước, file cạnh sau.

### 5.3 Tham số của một lần nạp

| Tham số | FinGraph dùng | Ý nghĩa |
|---|---|---|
| `source` | `s3://.../_ops/neptune/<run_id>/<scope>/` | Prefix: loader nạp **mọi file** bên dưới (vì vậy không ghi `_SUCCESS`) |
| `format` | `opencypher` | |
| `iamRoleArn` | role `neptune-loader` | Role cụm "đeo" để đọc S3; chỉ đọc được `_ops/neptune/*` |
| `parallelism` | **`LOW`** | `LOW` / `MEDIUM` / `HIGH` / `OVERSUBSCRIBE`: số luồng. `HIGH` làm `db.t4g.medium` hết bộ nhớ |
| `failOnError` | `TRUE` | Dừng ở lỗi đầu tiên thay vì nạp một nửa |
| `mode` | mặc định `AUTO` | `NEW` / `RESUME` / `AUTO`: `AUTO` nhớ file đã nạp **theo đường dẫn**, nạp tiếp phần còn lại |
| `queueRequest` | `TRUE` | Xếp hàng nếu đang có lần nạp khác |
| `updateSingleCardinalityProperties` | `TRUE` | Nạp lại thì ghi đè giá trị thuộc tính |

### 5.4 Theo dõi

`GET /loader/<loadId>?details=true&errors=true` trả `overallStatus.status`:

| Trạng thái | Nghĩa |
|---|---|
| `LOAD_IN_QUEUE`, `LOAD_NOT_STARTED` | Đang chờ |
| `LOAD_IN_PROGRESS` | Đang nạp; `totalRecords` tăng dần |
| `LOAD_COMPLETED` | Xong, kiểm `insertErrors = 0` |
| `LOAD_FAILED`, `LOAD_CANCELLED_DUE_TO_ERRORS`, `LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED` | Lỗi; mã chi tiết (ví dụ `OUT_OF_MEMORY_ERROR` đã gặp) ở `errors.errorLogs` |
| `LOAD_S3_ACCESS_DENIED_ERROR`, `LOAD_S3_READ_ERROR` | Role hoặc đường mạng tới S3 sai |

---

## 6. Bảo mật: IAM, SigV4, mạng

### 6.1 Xác thực

Cụm bật `iam_database_authentication_enabled`. Mỗi HTTP request phải **ký SigV4** với service name
`neptune-db`, bằng credential của một IAM principal có quyền:

| Action IAM | Cho phép |
|---|---|
| `neptune-db:ReadDataViaQuery` | Truy vấn đọc |
| `neptune-db:WriteDataViaQuery`, `DeleteDataViaQuery` | Truy vấn ghi / xoá |
| `neptune-db:StartLoaderJob`, `GetLoaderJobStatus` | Bulk loader |
| `neptune-db:ResetDatabase` | Fast reset |
| `neptune-db:GetEngineStatus` | `/status` |

Quyền ở **mức cụm**: ai đọc được thì đọc được **mọi** đỉnh. Neptune không có lọc dòng hay che cột như Lake
Formation. Vì vậy FinGraph **không nạp PII và `natural_key`**, và persona `customer` không bao giờ dùng Neptune.

### 6.2 Mạng

- Cụm nằm trong **private subnet**, SG `neptune` chỉ nhận cổng 8182 từ SG `neptune-client`.
- Bulk loader chạy **trong** cụm và đọc S3, nên SG của cụm cần **egress 443 tới prefix list S3** (đi qua S3
  gateway endpoint). Thiếu rule này là lỗi "Unable to connect to s3 endpoint" (mục 14).
- Máy cá nhân, Glue và agent ở ngoài VPC đi qua **Lambda `neptune-ops`** đặt trong VPC (SG `neptune-client`).
  Quyền gọi Lambda (`lambda:InvokeFunction`) là cửa duy nhất, không cần VPN hay bastion.

---

## 7. Neptune Analytics và thuật toán đồ thị

**Neptune Analytics** là sản phẩm riêng: toàn bộ đồ thị trong bộ nhớ, thuật toán gọi từ openCypher:

```cypher
CALL neptune.algo.pageRank({edgeLabels: ["transfersTo"]}) YIELD node, rank
RETURN id(node), rank ORDER BY rank DESC LIMIT 20
```

| Nhóm thuật toán | Ví dụ | Dùng trong FinGraph |
|---|---|---|
| Trung tâm | PageRank, degree, closeness | Tài khoản "trung chuyển" trong lưới rửa tiền |
| Cộng đồng | Louvain, label propagation, WCC | Cụm tài khoản liên kết; cụm `sameAs` |
| Đường đi | BFS, SSSP, `shortestPath` | Có ở Analytics (khác Database) |
| Tương đồng | Jaccard, overlap, vector KNN | "Khách giống bạn đã mua gì" (O9, O10) |

Vòng đời: tạo đồ thị từ S3 hoặc snapshot, chạy thuật toán, xuất kết quả, **xoá**. FinGraph chưa dùng; phương án
khoảng 0 USD là Spark GraphFrames trên Glue ([NEPTUNE.md 3.3](NEPTUNE.md#33-model-gợi-ý-m-đặc-trưng-đồ-thị)).

---

# Phần II. Neptune trong FinGraph

## 8. Kiến trúc đã triển khai

```
 repo ETL                                   repo hạ tầng (Terraform, modules/neptune)
 ─────────                                  ──────────────────────────────────────────
 track_a_onto.entity / triple / subclass    ┌──────────────── VPC, private subnet ───────────────┐
        │  (Iceberg, nguồn sự thật)          │                                                     │
        ▼                                    │  Lambda fingraph-dev-neptune-ops  ──8182, SigV4──▶  │
 Glue onto-spark: onto.export_neptune        │   (SG neptune-client)                Cụm           │
        │  --SCOPE aml_investigation         │                                 fingraph-dev-graph │
        ▼                                    │                                 db.t4g.medium      │
 s3://fingraph-lakehouse-dev-*/_ops/neptune/ │                                 engine 1.4.8.1     │
   <session>/<run_id>/<scope>/{vertices,edges}/*.csv ◀── bulk loader (role neptune-loader,       │
                                             │                        egress 443 → S3 prefix list) │
                                             └─────────────────────────────────────────────────────┘
                                                        ▲ lambda:InvokeFunction
 máy dev: scripts/neptune-session.ps1 ──────────────────┤
 agent:   NeptuneBackend (agent/tools.py) ──────────────┘
```

| Thành phần | Ở đâu | Vai trò |
|---|---|---|
| Cụm `fingraph-dev-graph` | `terraform/modules/neptune/main.tf` | 1 instance `db.t4g.medium`, mã hoá, IAM auth, audit log |
| Role `neptune-loader` | cùng file | Bulk loader đọc **chỉ** `_ops/neptune/*` |
| SG `neptune`, `neptune-client` | cùng file | 8182 chỉ từ client SG; egress 443 tới S3 |
| Lambda `neptune-ops` | `terraform/modules/neptune/lambda/neptune_ops.py` | Cửa vào duy nhất: status, load, load_status, summary, query (chỉ đọc), reset |
| Job `onto.export_neptune` | repo ETL `transforms/onto/export_neptune.py` | Iceberg → CSV openCypher, cắt vùng điều tra, ~20 nghìn dòng/file |
| Script phiên | repo hạ tầng `scripts/neptune-session.ps1` | start / load / summary / stop / status |
| Bật/tắt hạ tầng | `envs/dev/terraform.tfvars`: `neptune_enabled` | `false` thì Terraform xoá cụm |

## 9. Mô hình dữ liệu trên cụm

### 9.1 Quy tắc chiếu

| Iceberg | Neptune |
|---|---|
| `entity.uid` | `id(n)` |
| `class_iri` + `also_types` + mọi lớp tổ tiên | Nhãn (bỏ tiền tố `fin:`): `GoldCard;CreditCard;CreditProduct;Product` |
| `label`, `source_system`, `valid_from`, `attributes[*]` | Thuộc tính đỉnh |
| `triple` có `object_uid` | Cạnh; `id(r)` = `triple_id`; loại = tên quan hệ (`transfersTo`) |
| `triple` literal (`occupation`, `holdingStatus`, `amount`) | Thuộc tính của đỉnh chủ thể (giá trị mới nhất) |
| `natural_key` | **Không nạp** |
| `same_as` (confidence ≥ 0.9) | Cạnh `sameAs`, `is_inferred = true` |
| `same_as_review` (0.6–0.9) | **Không nạp**: chưa được duyệt thì không thành cạnh |

### 9.2 Vùng điều tra AML (`--SCOPE aml_investigation`)

| Bước | Lấy gì |
|---|---|
| Hạt giống | Tài khoản có cạnh `involvedIn` tới một vụ rửa tiền |
| Mở rộng | Đối tác `transfersTo` một bước (hai chiều); tài khoản cùng chủ sở hữu (`isHeldBy`) |
| Đỉnh phụ | Ngân hàng, chủ sở hữu của tài khoản trong vùng, mọi vụ, khái niệm kiểu hình |
| Cạnh AML | Chỉ khi cả hai đầu trong vùng |
| Nguồn khác | Corebank, Berka, UC2, **OpenSanctions** giữ nguyên |

### 9.3 Số liệu trên cụm (nạp 07-11, trước khi có OpenSanctions)

| | Số lượng |
|---|---|
| Đỉnh | **719.324** (khớp tuyệt đối với export) |
| Cạnh | **1.580.836** |
| Tài khoản AML trong vùng | 342.784 |
| Pháp nhân AML | 331.765 |
| Ngân hàng AML | 52.554 |
| Vụ rửa tiền | 2.756 (8 kiểu hình, mỗi loại khoảng 320–370 vụ) |
| Thời gian nạp | 2.338 s (116 file, 14,3 triệu bản ghi, `LOW`) |

So với đồ thị đầy đủ trên Iceberg (3,2 triệu đỉnh, 8 triệu cạnh), vùng điều tra nhỏ hơn khoảng 4–5 lần.
Lần nạp tiếp theo sẽ có thêm khoảng 72 nghìn thực thể OpenSanctions, 4.081 cạnh sở hữu và cạnh `sameAs`.

### 9.4 Loại cạnh hay dùng

| Loại cạnh | Từ → tới | Thuộc tính đáng chú ý |
|---|---|---|
| `transfersTo` | Account → Account | `weight` = số giao dịch của cặp, `valid_from` = ngày đầu |
| `isHeldBy` | Account → Party | Chủ sở hữu (AML) |
| `atBank` | Account → Bank | |
| `involvedIn` | Account → LaunderingAttempt | Có nhãn từ IBM AML |
| `hasTypology` | LaunderingAttempt → Typology (khái niệm SKOS) | `FAN-OUT`, `CYCLE`, `SCATTER-GATHER`, ... |
| `isDirectlyOwnedBy` | Party → Party | OpenSanctions, `weight` = tỉ lệ sở hữu |
| `sameAs` | Customer ↔ Party | `confidence`, `is_inferred = true` |
| `holdsAccount`, `isAuthorizedOn` | Customer → Account | Berka / corebank |
| `hasHolding` → `ofProduct` | Customer → ProductHolding → Product | Sản phẩm đang giữ |
| `locatedIn`, `partOf` | Customer → District → Region | |

---

# Phần III. Cách dùng

## 10. Một phiên làm việc từ đầu đến cuối

Chạy ở thư mục repo hạ tầng, PowerShell. Cần credential còn hạn (đăng nhập MFA trước).

```powershell
cd Hai_nam_graduation_project
aws configure export-credentials --profile fingraph --format powershell | Invoke-Expression

# 1. Bật cụm (5–10 phút nếu đang dừng)
.\scripts\neptune-session.ps1 status
.\scripts\neptune-session.ps1 start

# 2. Nạp đồ thị mới nhất (export Glue ~3 phút + reset + bulk load ~40 phút cho vùng điều tra)
.\scripts\neptune-session.ps1 load                       # mặc định -Scope aml_investigation
.\scripts\neptune-session.ps1 load -SkipExport           # dùng bản export mới nhất đã có trên S3
.\scripts\neptune-session.ps1 load -Scope full           # toàn bộ đồ thị: cần db.r6g.large trở lên

# 3. Đối soát
.\scripts\neptune-session.ps1 summary

# 4. Làm việc: truy vấn (mục 11–12) hoặc chạy agent với --neptune (mục 13)

# 5. TẮT khi xong
.\scripts\neptune-session.ps1 stop
```

**Khi nào phải nạp lại:** sau mỗi lần `onto.build_graph` chạy lại (đồ thị dựng lại toàn bộ). Không cần nạp lại
khi chỉ bật/tắt cụm, vì dữ liệu nằm ở lớp lưu trữ.

**Nếu script treo ở bước theo dõi** (đã gặp hai lần, loader vẫn chạy bình thường): Ctrl+C rồi kiểm trực tiếp:

```powershell
'{"action":"load_status","load_id":"<loadId>"}' | Set-Content $env:TEMP\p.json -Encoding ascii
aws lambda invoke --profile fingraph --function-name fingraph-dev-neptune-ops `
  --cli-binary-format raw-in-base64-out --cli-read-timeout 300 --payload file://$env:TEMP\p.json $env:TEMP\o.json
Get-Content $env:TEMP\o.json
```

## 11. Lambda `neptune-ops`: từng action

Gọi bằng `aws lambda invoke` (như trên) với payload JSON:

| Payload | Kết quả | Ghi chú |
|---|---|---|
| `{"action":"status"}` | `{"status":"healthy", "dbEngineVersion":...}` | Dùng để chờ engine sau reset |
| `{"action":"load","source":"s3://.../"}` | `{"payload":{"loadId":...}}` | `source` phải nằm dưới `EXPORT_PREFIX`; `parallelism` mặc định `LOW` |
| `{"action":"load_status","load_id":"..."}` | Tiến độ, 5 lỗi đầu | |
| `{"action":"summary"}` | Đếm đỉnh theo nhãn, cạnh theo loại | Trên vùng điều tra có thể gặp `MemoryLimitExceeded`, xem mục 15 |
| `{"action":"query","cypher":"...","params":{...}}` | `{"results":[...]}` | **Chỉ đọc**: chặn `CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `DROP`, `LOAD CSV`, `CALL` |
| `{"action":"reset","confirm":"RESET"}` | Xoá sạch dữ liệu | Hai bước token theo API Neptune |

Ví dụ truy vấn có tham số:

```powershell
@{ action = "query"; cypher = "MATCH (a)-[:involvedIn]->(v) WHERE id(a) = `$uid RETURN v.label";
   params = @{ uid = "<32 hex>" } } | ConvertTo-Json -Compress | Set-Content $env:TEMP\p.json -Encoding ascii
aws lambda invoke --profile fingraph --function-name fingraph-dev-neptune-ops `
  --cli-binary-format raw-in-base64-out --payload file://$env:TEMP\p.json $env:TEMP\o.json
```

(`` `$uid `` vì PowerShell sẽ nội suy `$uid` trong chuỗi nháy kép.)

## 12. Sổ tay truy vấn openCypher

Mọi truy vấn dưới đây đã chạy trên cụm dev hoặc là biến thể trực tiếp của truy vấn trong
[agent/tools.py](../agent/tools.py). `$uid` là `uid` lấy từ Athena (`track_a_onto.entity`) hoặc từ tool
`find_entity`.

### 12.1 Kiểm tra nhanh

```cypher
// Một đỉnh và nhãn của nó
MATCH (n) WHERE id(n) = $uid RETURN n.label, labels(n), n.source_system

// Đếm một nhãn (rẻ; đếm mọi nhãn cùng lúc dễ hết bộ nhớ trên t4g)
MATCH (n:LaunderingAttempt) RETURN count(n)

// Đếm một loại cạnh
MATCH ()-[r:transfersTo]->() RETURN count(r)
```

### 12.2 AML: vụ, kiểu hình, chủ sở hữu (CQ9, CQ10)

```cypher
// Số vụ theo kiểu hình
MATCH (v:LaunderingAttempt)-[:hasTypology]->(t)
RETURN t.label AS typology, count(v) AS attempts ORDER BY attempts DESC

// Tài khoản này dính vụ nào, kiểu gì, ai sở hữu  (= phần đầu của tool aml_subgraph)
MATCH (a) WHERE id(a) = $uid
OPTIONAL MATCH (a)-[i:involvedIn]->(v)-[:hasTypology]->(t)
OPTIONAL MATCH (a)-[o:isHeldBy]->(p)
RETURN a.label, collect(DISTINCT {attempt: v.label, typology: t.label, cite: id(i)}) AS attempts,
       collect(DISTINCT {owner: p.label, cite: id(o)}) AS owners

// Tài khoản khác cùng chủ sở hữu (CQ10)
MATCH (a)-[:isHeldBy]->(p)<-[:isHeldBy]-(b) WHERE id(a) = $uid AND id(b) <> $uid
RETURN id(b) AS uid, b.label, p.label AS owner LIMIT 50
```

### 12.3 AML: lưới chuyển tiền và hình dạng

```cypher
// Đối tác 1–2 bước, đánh dấu đối tác đã nằm trong một vụ (= phần hai của aml_subgraph)
MATCH p = (a)-[:transfersTo*1..2]-(b) WHERE id(a) = $uid
UNWIND relationships(p) AS r
WITH DISTINCT r, startNode(r) AS s, endNode(r) AS e
OPTIONAL MATCH (e)-[:involvedIn]->(ve)
RETURN id(r) AS cite, s.label, e.label, r.weight AS n_txn, count(ve) > 0 AS to_flagged
ORDER BY n_txn DESC LIMIT 50

// CYCLE: tiền quay về chính tài khoản trong 2..6 bước
MATCH p = (a)-[:transfersTo*2..6]->(a) WHERE id(a) = $uid
RETURN [r IN relationships(p) | id(r)] AS cite, length(p) LIMIT 5

// FAN-OUT: tài khoản trong vùng chuyển cho >= 5 đối tác
MATCH (a:Account)-[:transfersTo]->(b:Account)
WITH a, count(DISTINCT b) AS n WHERE n >= 5
RETURN id(a), a.label, n ORDER BY n DESC LIMIT 20

// FAN-IN / SCATTER-GATHER: nhiều nguồn cùng đổ về một đích qua một trung gian
MATCH (s)-[:transfersTo]->(m)-[:transfersTo]->(d) WHERE id(d) = $uid
RETURN count(DISTINCT s) AS sources, count(DISTINCT m) AS middles
```

### 12.4 Liên hệ giữa hai thực thể (CQ8, tool `explain_link`)

Neptune không có `shortestPath`. Tăng dần độ dài, dừng ở độ dài đầu tiên có kết quả:

```cypher
// h = 1, 2, 3, ... (số nguyên chèn sau khi kẹp biên, không phải tham số)
MATCH p = (a)-[:transfersTo|isHeldBy|involvedIn|isDirectlyOwnedBy|sameAs*2]-(b)
WHERE id(a) = $a AND id(b) = $b
RETURN [n IN nodes(p) | n.label] AS path, [r IN relationships(p) | id(r)] AS cite LIMIT 1
```

### 12.5 Sở hữu và cấm vận (sau lần nạp có OpenSanctions)

```cypher
// Chuỗi sở hữu 1..4 tầng từ một pháp nhân tới đối tượng bị cấm vận
MATCH p = (c)-[:isDirectlyOwnedBy*1..4]->(s:SanctionedEntity) WHERE id(c) = $uid
RETURN [n IN nodes(p) | n.label] AS chain, [r IN relationships(p) | r.weight] AS pct,
       [r IN relationships(p) | id(r)] AS cite LIMIT 10

// Khách nội bộ đã được xác nhận trùng (sameAs >= 0.9) với thực thể cấm vận
MATCH (c:Customer)-[s:sameAs]-(x:SanctionedEntity)
RETURN id(c), c.label, x.label, s.confidence, id(s) AS cite
```

### 12.6 Khách hàng (nhóm A, thường dùng Athena; Neptune để so sánh)

```cypher
// Sản phẩm đang giữ (CQ1)
MATCH (c)-[h:hasHolding]->(hd)-[:ofProduct]->(p) WHERE id(c) = $uid
RETURN p.label, labels(p), h.valid_from, id(h) AS cite

// Số khách giữ sản phẩm tín dụng (CQ2), nhờ nhãn tổ tiên đã nạp sẵn
MATCH (c:Customer)-[:hasHolding]->()-[:ofProduct]->(:CreditProduct)
RETURN count(DISTINCT c)
```

## 13. Dùng từ agent

```powershell
cd banking_lakehouse_etl
.\.venv\Scripts\python.exe -m agent.loop "Tài khoản <uid> dính vụ rửa tiền nào, ai sở hữu?" `
  --workgroup fingraph-dev-analyst --provider bedrock --neptune fingraph-dev-neptune-ops
```

- `NeptuneBackend` gọi Lambda `query`, nên agent cần `lambda:InvokeFunction` trên đúng hàm.
- Hai tool dùng Neptune: `aml_subgraph`, `explain_link`. Chi tiết ở [AI-AGENT.md](AI-AGENT.md).
- Cụm tắt thì hai tool trả lỗi rõ ràng ("Neptune chưa bật"); các tool Athena vẫn chạy.

---

# Phần IV. Vận hành

## 14. Sự cố đã gặp và cách xử lý

| # | Triệu chứng | Nguyên nhân | Cách xử lý (đã áp dụng) |
|---|---|---|---|
| 1 | Loader: `Unable to connect to s3 endpoint` | SG do Terraform quản lý **không có egress mặc định** | Rule egress 443 tới prefix list S3 (`neptune_to_s3`) |
| 2 | Loader bị từ chối kết nối ngay sau reset | Engine khởi động lại ~1 phút, cụm vẫn `available` | Script chờ `/status` = `healthy` |
| 3 | `aws lambda invoke` lỗi read timeout | Gọi loader mất ~61 s, CLI chờ mặc định 60 s | `--cli-read-timeout 300` |
| 4 | Loader lỗi `OUT_OF_MEMORY_ERROR` | `parallelism HIGH` + file 28 MB (Spark chia lệch theo phân vùng) trên 4 GB RAM | Export ~20 nghìn dòng/file, nạp `LOW` |
| 5 | Nạp lại sau reset bỏ qua file | `mode AUTO` nhớ file đã nạp theo đường dẫn | Mỗi phiên export vào thư mục mới `_ops/neptune/s<giờ>/` |
| 6 | Loader nạp cả file `_SUCCESS` | Loader nạp **mọi** file dưới prefix | Tắt `marksuccessfuljobs` khi ghi CSV |
| 7 | `MemoryLimitExceededException` ở `summary` | Gộp trên toàn đồ thị (`UNWIND labels(n)`, `type(r), count(*)`) | Đếm từng nhãn / loại cạnh; gộp lớn cần `db.r6g.large`+ |
| 8 | Lỗi cú pháp với `all(...)`, `shortestPath` | Neptune openCypher không hỗ trợ | Lọc sau bằng `WITH`; tăng dần độ dài đường đi |
| 9 | Script PowerShell treo khi theo dõi | Một lần `aws lambda invoke` không trả về | Kiểm trực tiếp `load_status` (mục 10) |
| 10 | Lệnh AWS treo chờ nhập mã MFA | Credential hết hạn | Đăng nhập lại, `export-credentials` |

## 15. Hiệu năng và giới hạn của `db.t4g.medium`

| Việc | Đo trên dev | Ghi chú |
|---|---|---|
| Bulk load 0,7 triệu cạnh (nhóm A + UC2) | 1.122 s, 7,3 triệu bản ghi | `LOW` |
| Bulk load vùng điều tra AML (1,58 triệu cạnh) | 2.338 s, 14,3 triệu bản ghi | `LOW`, 116 file |
| Truy vấn neo theo `id`, 1–2 bước | 1,5–1,8 s **tính cả CLI + Lambda** | Phần Neptune dưới 100 ms; phần lớn là khởi động CLI và Lambda |
| Vòng tiền `*2..12` quanh một tài khoản CYCLE | Chạy được | Neo ở `id(a)` nên không bùng nổ |
| Gộp trên toàn đồ thị | Hết bộ nhớ | Giới hạn của 4 GB |

"Bản ghi" của loader là số **quad** (đỉnh, nhãn, thuộc tính, cạnh, thuộc tính cạnh), nên lớn hơn nhiều số đỉnh + cạnh.

**Khi nào lên `db.r6g.large` / `db.r7g.large` (16 GB):** cần `-Scope full` (2,1 triệu tài khoản AML), cần gộp
lớn trên toàn đồ thị, hoặc load `LOW` quá chậm cho lịch làm việc. Đổi `neptune_instance_class` trong tfvars rồi
apply (khoảng 10 phút, dữ liệu giữ nguyên vì lưu trữ tách khỏi instance).

## 16. Chi phí một phiên thật

Giá Singapore: `db.t4g.medium` 0,1433 USD/giờ, lưu trữ 0,11 USD/GB-tháng, I/O 0,22 USD/triệu.

| Phiên | Thời gian | Ước tính |
|---|---|---|
| Bật + nạp vùng điều tra + 1 giờ truy vấn + tắt | ~2 giờ | 0,29 USD instance + I/O nạp ~0,5 USD |
| Để quên chạy 1 tuần | 168 giờ | ~24 USD |
| Dừng cả tháng | 0 giờ | Chỉ lưu trữ: vài GB × 0,11, dưới 1 USD |

Tháng đầu: free tier 750 giờ `db.t4g.medium` (của riêng Neptune, cho tài khoản chưa từng dùng Neptune).

## 17. Đóng góp cho luận văn

| Điểm | Bằng chứng trong hệ thống |
|---|---|
| Neptune là **bản chiếu dựng lại được**, không phải nguồn sự thật | `uid`/`triple_id` giữ nguyên; đối soát khớp tuyệt đối số đỉnh/cạnh sau mỗi lần nạp |
| Truy vấn AML là hình dạng đồ thị | CYCLE, FAN-OUT, chủ sở hữu chung chạy bằng vài dòng Cypher, so với self-join nhiều tầng trên Athena |
| Giới hạn tài nguyên được đo, không giả định | Vùng điều tra thay vì toàn bộ AML; số đo hết bộ nhớ / thời gian nạp |
| Bảo mật bù cho phân quyền mức cụm | Không nạp PII / `natural_key`; Lambda chỉ đọc; tham số hoá truy vấn |
| Chi phí theo phiên | Script start/stop; so sánh 14 USD/tháng với 105–145 USD/tháng chạy 24/7 |

**Thí nghiệm nên chạy để đưa vào luận văn:** cùng bộ câu hỏi use case B, đo độ trễ và độ chính xác khi agent dùng
Athena (`neighborhood` lặp nhiều lần) so với Neptune (`aml_subgraph`, `explain_link`).
