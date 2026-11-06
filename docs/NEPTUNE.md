# Amazon Neptune cho FinGraph: lý thuyết, vai trò, thiết kế, chi phí

Tài liệu này trả lời bốn câu hỏi:
- Neptune là gì và hoạt động thế nào (mục 1).
- Nó đóng vai trò gì trong hệ thống FinGraph, khi nào **cần** và khi nào **không cần** (mục 2–3).
- Thiết kế cụ thể nếu dùng (mục 4–6).
- Chi phí bao nhiêu (mục 7).

Mục 8 là khuyến nghị và lộ trình.

Tài liệu liên quan:
- [ONTOLOGY.md](ONTOLOGY.md) (thiết kế ontology, mục 10 so sánh nơi lưu đồ thị);
- [ONTOLOGY-BUILD.md](ONTOLOGY-BUILD.md) (các bước dựng đồ thị);
- [agent/README.md](../agent/README.md) (AI agent).

**Kết luận ngắn:** đồ thị hiện tại (nhóm A + UC2) vẫn chạy tốt trên Iceberg + Athena với chi phí cố định khoảng 0
USD, nên **chưa cần Neptune**. Neptune đáng tiền khi đưa **IBM AML** (2 triệu tài khoản, hàng triệu cạnh chuyển
tiền) lên đồ thị, vì use case AML cần duyệt nhiều bước nhảy có độ dài thay đổi mà Athena làm chậm và đắt. Khi đó:
- dùng **Neptune Database `db.t4g.medium` bật theo phiên**, khoảng **13–20 USD/tháng**;
- dùng **Neptune Analytics theo lần chạy** cho thuật toán đồ thị, khoảng **1–2 USD/lần**;
- **không** chạy 24/7: khoảng 105–145 USD/tháng chưa tính mạng.

---

## Mục lục

1. [Lý thuyết: graph database và Neptune](#1-lý-thuyết-graph-database-và-neptune)
2. [Vai trò trong FinGraph](#2-vai-trò-trong-fingraph)
3. [Ứng dụng cụ thể](#3-ứng-dụng-cụ-thể)
4. [Thiết kế mô hình dữ liệu](#4-thiết-kế-mô-hình-dữ-liệu)
5. [Thiết kế luồng nạp](#5-thiết-kế-luồng-nạp)
6. [Thiết kế hạ tầng và bảo mật](#6-thiết-kế-hạ-tầng-và-bảo-mật)
7. [Đánh giá chi phí](#7-đánh-giá-chi-phí)
8. [Khuyến nghị và lộ trình](#8-khuyến-nghị-và-lộ-trình)

---

## 1. Lý thuyết: graph database và Neptune

### 1.1 Vì sao có graph database

Trong CSDL quan hệ, quan hệ giữa hai bản ghi là **phép JOIN tính lúc truy vấn**. Hỏi "A chuyển cho ai, người đó
chuyển tiếp cho ai, … tới 5 bước" là 5 phép self-join trên bảng giao dịch. Mỗi bước nhân số dòng trung gian, và
câu hỏi "độ dài bất kỳ" (`*1..5`) phải viết bằng CTE đệ quy. Athena/Trino không đảm bảo hỗ trợ tốt CTE đệ quy, và
mỗi bước lại quét lại bảng trên S3.

Graph database lưu quan hệ là **dữ liệu hạng nhất**: đi từ một đỉnh sang các đỉnh kề là một lần tra chỉ mục theo
đỉnh. Chi phí tỉ lệ với **số cạnh thực sự đi qua**, không tỉ lệ với kích thước bảng. Vì vậy truy vấn nhiều bước
nhảy, đường đi ngắn nhất, chu trình nhanh hơn nhiều bậc khi đồ thị lớn.

### 1.2 Hai mô hình đồ thị

| | **Property graph (LPG)** | **RDF** |
|---|---|---|
| Đơn vị | Đỉnh và cạnh, đều có nhãn và **thuộc tính** (key–value) | Bộ ba `chủ thể – vị từ – đối tượng` (+ named graph) |
| Ngôn ngữ trên Neptune | **openCypher**, Gremlin | **SPARQL 1.1** |
| Thuộc tính trên cạnh | Có sẵn: `(:Account)-[:transfersTo {amount, month}]->(:Account)` | Phải reify (thêm node trung gian) hoặc RDF-star |
| Ngữ nghĩa / suy luận | Không có | Gắn với OWL/RDFS. Neptune Database **không** chạy reasoner OWL |
| Hợp với | Duyệt đường đi, thuật toán đồ thị, ứng dụng | Tích hợp dữ liệu liên kết, chuẩn W3C |

Trên Neptune Database, hai mô hình **tách biệt**: dữ liệu nạp dạng RDF không truy vấn được bằng openCypher và
ngược lại.

### 1.3 Amazon Neptune Database

Dịch vụ graph database được quản lý của AWS:

- **Lưu trữ:**
  - tách khỏi máy tính toán, cùng kiến trúc với Aurora;
  - **6 bản sao trên 3 AZ**, tự tăng dung lượng tới 128 TiB;
  - tính phí theo GB đã dùng.
- **Máy tính toán:**
  - một instance ghi (primary) và tối đa 15 bản đọc (read replica) dùng chung lớp lưu trữ;
  - chọn instance có cỡ cố định (`db.t4g.medium`, `db.r6g.large`…) hoặc **Serverless** (tự co giãn theo NCU, mỗi NCU khoảng 2 GB RAM).
- **Chỉ mục:**
  - mọi dữ liệu là bộ bốn (quad) `S-P-O-G`;
  - Neptune giữ sẵn 3 chỉ mục `SPOG`, `POGS`, `GPSO` nên tra theo đỉnh, theo loại quan hệ, hay theo đồ thị con đều có chỉ mục.
- **Giao dịch:** ACID. Đọc trên replica là nhất quán sau (thường dưới 100 ms).
- **Nạp hàng loạt:** *bulk loader* đọc CSV (property graph) hoặc N-Triples/N-Quads/Turtle (RDF) thẳng từ S3, nhanh hơn ghi từng câu nhiều bậc.
- **Tính năng kèm:**
  - *Neptune Streams* (luồng thay đổi, kiểu CDC);
  - tìm toàn văn qua OpenSearch;
  - *Neptune ML* (GNN qua SageMaker);
  - xác thực IAM (SigV4) ở mức **cụm**.
- **Mạng:** chỉ trong VPC (không có endpoint công khai). Client phải ở trong VPC hoặc đi qua VPN/bastion.
- **Dừng được:** stop cluster thì chỉ trả tiền lưu trữ, tối đa 7 ngày rồi AWS tự bật lại.

### 1.4 Amazon Neptune Analytics

Một sản phẩm khác, dùng để **phân tích**:

- **Mô hình:** toàn bộ đồ thị nằm trong bộ nhớ, tính phí theo *m-NCU* (bộ nhớ).
- **Thuật toán dựng sẵn**, gọi trong openCypher (`CALL neptune.algo.pageRank(...)`): PageRank, Louvain (cộng đồng),
  đường đi ngắn nhất, connected components, độ tương đồng (Jaccard, overlap).
- **Tìm kiếm vector** trên embedding đỉnh.
- **Nạp:** từ S3 hoặc từ snapshot Neptune Database.
- **Vòng đời:** tạo ra, chạy, xuất kết quả, rồi **xoá**. Không có lớp lưu trữ bền như Neptune Database.

Hợp với tính đặc trưng đồ thị cho model theo lô, không hợp làm kho phục vụ lâu dài.

---

## 2. Vai trò trong FinGraph

### 2.1 Nguồn sự thật vẫn là lakehouse

```
 silver_* ──▶ onto.build_graph ──▶ track_a_onto.entity / triple   (Iceberg: NGUỒN SỰ THẬT, có snapshot, LF)
                                        │
                                        ├─▶ Athena: tool agent hiện tại, CQ, đối soát, pre-train
                                        │
                                        └─▶ onto.export_neptune ──▶ S3 CSV ──▶ bulk load ──▶ Neptune
                                                                                  (BẢN CHIẾU để duyệt nhanh)
                                                    Neptune Analytics ──▶ kết quả thuật toán ──▶ S3 ──▶ Iceberg
```

Neptune là **bản chiếu phục vụ truy vấn** (serving projection), dựng lại được hoàn toàn từ Iceberg. Lý do:

| Iceberg giữ | Neptune không giữ được (hoặc không nên) |
|---|---|
| Lịch sử đồ thị (snapshot, time travel) | Neptune chỉ có trạng thái hiện tại |
| Phân quyền mức bảng/cột (Lake Formation) | Neptune chỉ có IAM mức cụm |
| Bất biến và đối soát mỗi lần dựng (O7) | Lỗi nạp phát hiện muộn |
| Chi phí khoảng 0 khi không dùng | Instance tính tiền theo giờ |

Hệ quả thiết kế: **mọi thứ ghi vào Neptune đều sinh từ `entity`/`triple`, uid và triple_id giữ nguyên**. Câu trả
lời của agent trích `triple_id` từ Neptune vẫn tra ngược được về Iceberg và về dòng silver gốc.

### 2.2 Khi nào cần, khi nào không

| Câu hỏi | Athena trên `triple` | Neptune |
|---|---|---|
| Hồ sơ 360 một khách (1–2 bước) | Tốt (vài giây). Nhanh hơn: DynamoDB hot store | Không cần |
| Đếm theo lớp sản phẩm (closure tính sẵn) | Tốt | Không cần |
| "Tiền từ A đi tới B qua ≤ 6 bước?" | Kém: 6 self-join hoặc CTE đệ quy, quét lại mỗi bước | **Tốt**: `MATCH p=(a)-[:transfersTo*1..6]->(b)` |
| Phát hiện chu trình (typology CYCLE) | Rất kém | **Tốt** |
| Chuỗi sở hữu tới đối tượng cấm vận | Được nếu tính sẵn closure (O9) | Tốt và linh hoạt độ sâu |
| PageRank, cộng đồng trên 2 triệu đỉnh | Không làm được bằng SQL | Neptune Analytics (hoặc Spark GraphFrames) |
| Truy vấn của agent cần dưới 1 giây khi duyệt nhiều bước | Không | **Tốt** |

**Quy mô thực tế** (lần dựng trên dev 06-11, release `498af33`):

| Phạm vi | Node | Cạnh | Ghi chú |
|---|---|---|---|
| Nhóm A + UC2 hiện tại | **265.456** (đo 06-11) | **788.338** | 197 nghìn khách (Berka + corebank ~10,8 nghìn, Bank Marketing 86 nghìn, MBD ~100 nghìn), ~550 nghìn cạnh `transfersTo` gộp theo tháng |
| + IBM AML (giai đoạn 2) | + 2,1 triệu tài khoản | + vài triệu tới ~10 triệu cạnh gộp theo (cặp, ngày) | 31,9 triệu giao dịch gốc, 2.756 vụ rửa tiền có nhãn |
| + OpenSanctions | + ~0,3 triệu thực thể | + quan hệ sở hữu | |

0,8 triệu cạnh vẫn nằm gọn trong Athena. Ngưỡng đổi chiều là AML: nhiều triệu cạnh và câu hỏi đường đi độ dài
thay đổi.

---

## 3. Ứng dụng cụ thể

### 3.1 AML (use case B của agent): ứng dụng chính

Các typology có nhãn trong IBM AML đều là **hình dạng đồ thị**, nên đây là chỗ Neptune thể hiện rõ giá trị:

```cypher
// FAN-OUT: một tài khoản chuyển cho >= 5 tài khoản khác trong cùng ngày
MATCH (a:Account)-[t:transfersTo]->(b:Account)
WHERE t.valid_from = date('2022-09-01')
WITH a, count(DISTINCT b) AS n WHERE n >= 5
RETURN a.uid, a.label, n ORDER BY n DESC LIMIT 20

// CYCLE: tiền quay về chính tài khoản xuất phát trong 3..6 bước
MATCH p = (a:Account {uid: $uid})-[:transfersTo*3..6]->(a)
RETURN [r IN relationships(p) | r.triple_id] AS cite, length(p) LIMIT 10

// Đường đi từ tài khoản nghi vấn tới tài khoản đã gắn nhãn rửa tiền (giải thích cho điều tra viên)
MATCH p = shortestPath((a:Account {uid: $uid})-[:transfersTo*..6]-(b:Account)-[:involvedIn]->(:LaunderingAttempt))
RETURN [n IN nodes(p) | n.label], [r IN relationships(p) | r.triple_id]

// Chuỗi sở hữu tới đối tượng bị cấm vận (OpenSanctions)
MATCH p = (c:Party {uid: $uid})-[:isDirectlyOwnedBy*1..4]->(s:SanctionedEntity)
RETURN p
```

Mọi kết quả trả về `triple_id` của từng cạnh, đúng hợp đồng grounding của agent ([agent/README.md A5](../agent/README.md)).

### 3.2 Agent (A): tool cần duyệt nhiều bước

Hợp đồng tool không đổi, chỉ đổi backend ([agent/tools.py](../agent/tools.py) tách `Backend` cho việc này):

| Tool | Backend hiện tại | Khi có Neptune |
|---|---|---|
| `customer_360`, `count_holders`, `describe` | Athena / catalog | Giữ nguyên (đã đủ nhanh) |
| `neighborhood` | Athena | Neptune (dưới 100 ms) |
| `explain_link(uid_a, uid_b)` (mới) | Chỉ làm được khi có closure tính sẵn | Neptune `shortestPath` |
| `aml_subgraph(uid, hops)` (mới) | Không làm | Neptune |

### 3.3 Model gợi ý (M): đặc trưng đồ thị

Neptune Analytics chạy theo lô, xuất kết quả về S3, rồi nạp vào Iceberg thành `track_a_onto.graph_feature` /
`node_embedding` (O9):

- PageRank / degree trên đồ thị khách–sản phẩm, đo mức "trung tâm" của khách;
- Louvain, chia cộng đồng khách giữ sản phẩm giống nhau, làm đặc trưng phân khúc;
- Jaccard giữa khách theo tập sản phẩm, làm đặc trưng "khách giống bạn đã mua gì".

**Bắt buộc point-in-time:** chỉ nạp các cạnh có `valid_from < VALID_START` vào đồ thị phân tích, đúng quy tắc
chống rò rỉ của O9. Vì vậy mỗi lần chạy Analytics dùng một bản export lọc theo mốc thời gian, không dùng đồ thị
"hiện tại".

**Phương án thay thế khoảng 0 USD:** Spark GraphFrames trên Glue tính được PageRank, connected components, label
propagation. Nên **thử GraphFrames trước**, chỉ dùng Neptune Analytics khi cần thuật toán GraphFrames không có
(Louvain, vector similarity) hoặc khi lặp nhanh trong lúc thử nghiệm.

### 3.4 Hợp nhất thực thể (O8)

Cụm `sameAs` (connected components trên cạnh `sameAs` có `confidence >= 0.9`) và trực quan hoá đồ thị con khi
người duyệt xem ứng viên khớp OpenSanctions trong khoảng `[0.6, 0.9)`.

---

## 4. Thiết kế mô hình dữ liệu

**Chọn property graph + openCypher**, không chọn RDF/SPARQL, dù TBox viết bằng OWL. Lý do:

1. Cạnh của FinGraph có thuộc tính quan trọng: `triple_id` (trích dẫn), `valid_from/to` (bitemporal), `weight`,
   `confidence`. LPG lưu sẵn; RDF phải reify, làm số bộ ba tăng gấp 3–4 lần.
2. Suy luận OWL đã **tính sẵn ở Spark** (bảng `subclass`, closure O9). Neptune Database không chạy reasoner OWL,
   nên giữ RDF không được thêm gì.
3. Truy vấn đường đi và thuật toán (Neptune Analytics) đều dùng openCypher. LLM cũng viết Cypher quen hơn SPARQL,
   nếu sau này cho agent sinh truy vấn có kiểm soát.

TBox vẫn là nguồn sự thật về nghĩa. Neptune nhận **kết quả** của TBox:

| `track_a_onto` | → Neptune | Quy tắc |
|---|---|---|
| `entity.uid` | `~id` của đỉnh | Giữ nguyên, để trích dẫn tra ngược được |
| `entity.class_iri` + `also_types` + **mọi lớp tổ tiên** (bảng `subclass`) | Nhãn đỉnh (nhiều nhãn) | `fin:GoldCard` → nhãn `GoldCard;CreditCard;CreditProduct;Product`. Nhờ vậy `MATCH (p:CreditProduct)` trả mọi lớp con mà không cần suy luận |
| `entity.label`, `source_system`, `attributes[*]` | Thuộc tính đỉnh | **Không** nạp `natural_key` (đường ngược về khoá gốc, LF đang che) |
| `triple` có `object_uid` | Cạnh, `~id` = `triple_id`, nhãn = tên quan hệ (`holdsAccount`) | Thuộc tính: `valid_from`, `valid_to`, `weight`, `confidence`, `is_inferred`, `source_dataset` |
| `triple` có `object_literal` (`occupation`, `holdingStatus`, `amount`) | Thuộc tính của đỉnh chủ thể | Một giá trị hiện hành mỗi thuộc tính |
| Node sản phẩm (`source_system = ontology`) | Đỉnh sản phẩm, nhãn theo cây | |

---

## 5. Thiết kế luồng nạp

| Bước | Thành phần | Chi tiết |
|---|---|---|
| 1 | `onto.build_graph` (đã có) | Dựng `entity`/`triple`, bất biến xanh |
| 2 | `onto.export_neptune` (job mới, cùng Glue `onto-spark`) | Đọc `entity`, `triple`, `subclass` → ghi CSV định dạng openCypher của Neptune: `s3://…/_ops/neptune/<run_id>/vertices/*.csv`, `edges/*.csv`. Tham số `--AS_OF` để xuất bản point-in-time cho Analytics |
| 3 | Bulk loader | `POST https://<endpoint>:8182/loader` với `format: opencypher`, `iamRoleArn` đọc prefix `_ops/neptune/`, `parallelism: HIGH`. Theo dõi `GET /loader/<loadId>` |
| 4 | Đối soát sau nạp | Đếm đỉnh/cạnh theo nhãn trên Neptune bằng openCypher và so với `entity`/`triple` của cùng `run_id`. Lệch thì không chuyển agent sang Neptune |

**Cập nhật thế nào:** đồ thị dựng lại toàn bộ mỗi lần (O7), nên Neptune cũng **nạp lại toàn bộ**:
- dev: `fast reset` (xoá sạch dữ liệu cụm), rồi bulk load. Với khoảng 10 triệu cạnh, mất cỡ vài phút tới vài chục phút trên `db.t4g.medium`;
- production: xanh/lục (blue/green), tức nạp vào cụm mới rồi đổi endpoint, để không có lúc đồ thị trống.

Cập nhật gia tăng (Neptune Streams, upsert) chỉ đáng làm khi có luồng CDC (đang tạm dừng) và cần độ trễ dưới ngày.

**Định dạng CSV** (bulk loader openCypher):

```
vertices:  :ID,:LABEL,label:String,source_system:String,gender:String,birth_year:Int,...
           9f2c…,Customer;Person;Party,Khách 2,berka,M,1945,...
edges:     :ID,:START_ID,:END_ID,:TYPE,valid_from:Date,valid_to:Date,weight:Double,confidence:Double,source_dataset:String
           a71b…,9f2c…,4bf0…,holdsAccount,1993-02-26,,,1.0,silver_berka.disp
```

---

## 6. Thiết kế hạ tầng và bảo mật

Thêm module `terraform/modules/neptune` ở repo hạ tầng. Đây là **bản thiết kế, chưa apply**: apply tốn tiền theo
giờ, nên làm khi bắt đầu use case AML và có duyệt.

```hcl
resource "aws_neptune_subnet_group" "this" {
  name       = "${var.prefix}-neptune"
  subnet_ids = var.private_subnet_ids              # subnet riêng, không ra Internet
}

resource "aws_security_group" "neptune" {
  name   = "${var.prefix}-neptune"
  vpc_id = var.vpc_id
  ingress {                                       # chỉ agent runtime + Glue onto-spark
    from_port       = 8182
    to_port         = 8182
    protocol        = "tcp"
    security_groups = var.client_security_group_ids
  }
}

resource "aws_neptune_cluster" "this" {
  cluster_identifier                  = "${var.prefix}-graph"
  engine                              = "neptune"
  neptune_subnet_group_name           = aws_neptune_subnet_group.this.name
  vpc_security_group_ids              = [aws_security_group.neptune.id]
  iam_database_authentication_enabled = true      # SigV4, không mật khẩu
  iam_roles                           = [aws_iam_role.loader.arn]
  storage_encrypted                   = true
  kms_key_arn                         = var.kms_key_arn
  backup_retention_period             = 1         # dev: dựng lại được từ Iceberg
  skip_final_snapshot                 = true      # dev
  deletion_protection                 = false     # dev
}

resource "aws_neptune_cluster_instance" "primary" {
  cluster_identifier = aws_neptune_cluster.this.id
  instance_class     = var.instance_class          # dev: db.t4g.medium
}

# Role cho bulk loader: CHỈ đọc prefix export
resource "aws_iam_role" "loader" { … }             # s3:GetObject, s3:ListBucket trên _ops/neptune/*
# Bulk loader cần S3 gateway endpoint trong VPC (miễn phí)
```

**Bảo mật, điểm khác Lake Formation:** Neptune chỉ phân quyền **mức cụm** (IAM `neptune-db:ReadDataViaQuery`,
`WriteDataViaQuery`…). Không có lọc dòng hay che cột. Vì vậy:

1. **Không nạp PII:**
   - không nạp `natural_key`;
   - không nạp họ tên;
   - nhãn đã không có PII (O5).
2. **Persona `customer`** (use case D) **không** dùng Neptune, mà đi đường Athena + row filter Lake Formation.
3. Quyền đọc cụm chỉ cấp cho role agent-runtime và DE. Analyst truy cập qua tool của agent, không truy cập thẳng.
4. Mọi truy vấn openCypher do tool sinh từ **mẫu có tham số** (`$uid`), không ghép chuỗi. Bật audit log của Neptune
   vào CloudWatch.

**Bật theo phiên** (như phiên Kafka): script `scripts/neptune-session.ps1 start|stop` gọi
`aws neptune start-db-cluster` / `stop-db-cluster`. Lúc bật thì nạp lại nếu `run_id` của đồ thị mới hơn bản đã nạp
(ghi ở SSM `/fingraph/dev/neptune/loaded_run_id`).

---

## 7. Đánh giá chi phí

**Giá Neptune tại Singapore (ap-southeast-1)** lấy từ AWS Price List API ngày 06-11-2026, theo giờ, trả theo mức
dùng (on-demand). Giá đổi theo thời gian, kiểm lại trước khi apply.

| Hạng mục | Giá | Ghi chú |
|---|---|---|
| `db.t4g.medium` (2 vCPU, 4 GB) | **0,1433 USD/giờ** | Standard. I/O-Optimized: 0,193 USD/giờ |
| `db.r7g.large` / `db.r6g.large` (2 vCPU, 16 GB) | 0,333 / 0,3967 USD/giờ | Khi đồ thị AML không vừa 4 GB RAM |
| Serverless | **0,1941 USD / NCU-giờ** | Không về 0 khi rảnh: luôn tính ít nhất mức NCU tối thiểu |
| Lưu trữ | 0,11 USD/GB-tháng | I/O-Optimized: 0,247 USD/GB-tháng, không tính I/O |
| I/O | 0,22 USD / triệu yêu cầu | Chỉ ở Standard |
| Backup vượt dung lượng cụm | 0,023 USD/GB-tháng | |
| CPU credit (T4g vượt baseline) | 0,15 USD / vCPU-giờ | Nạp lớn có thể chạm |
| Neptune Analytics 16 m-NCU | khoảng 0,58 USD/giờ khi chạy; 0,0581 USD/giờ khi dừng | Price List trả giá chạy kèm nhãn ap-northeast-1, giá dừng là của Singapore (bằng 10% giá chạy). **Cần xác nhận lại** trên trang giá |
| Free tier (tài khoản chưa dùng Neptune) | 750 giờ `db.t4g.medium`, 10 triệu I/O, 1 GB lưu trữ, trong 30 ngày | Đủ thử toàn bộ thiết kế miễn phí trong tháng đầu |

**Ước lượng kích thước:** 0,8 triệu cạnh hiện tại (đo 06-11), và khoảng 10 triệu cạnh khi có AML, mỗi cạnh có 6
thuộc tính. Ước vài trăm byte mỗi cạnh trên Neptune, tức **1–5 GB**, nên chi phí lưu trữ dưới 1 USD/tháng. Chi phí
nằm ở **giờ chạy instance**.

**So sánh phương án mỗi tháng** (730 giờ = 24/7; "theo phiên" = 4 giờ × 22 ngày = 88 giờ):

| Phương án | Tính toán | USD/tháng | Hợp với |
|---|---|---|---|
| **0. Không Neptune (hiện tại)** | Athena 5 USD/TB quét; `track_a_onto` vài GB, mỗi câu agent quét vài MB | **≈ 0–2** | Nhóm A + UC2 |
| 1. `db.t4g.medium` 24/7 | 0,1433 × 730 + lưu trữ ~0,5 | **≈ 105** | Demo liên tục, không khuyến nghị cho đồ án |
| **2. `db.t4g.medium` theo phiên** | 0,1433 × 88 + lưu trữ (tính cả lúc dừng) ~0,5 + I/O nạp ~1 | **≈ 14** | **Khuyến nghị khi có AML** |
| 3. Serverless 1 NCU 24/7 | 0,1941 × 730 | **≈ 142** | Không có lợi ở quy mô này: không về 0 khi rảnh |
| 4. `db.r7g.large` theo phiên | 0,333 × 88 + ~1,5 | **≈ 31** | Khi AML vượt 4 GB RAM (đo khi nạp) |
| 5. Neptune Analytics theo lần | 16 m-NCU × ~2 giờ ≈ 1,2 USD/lần × 4 lần | **≈ 5** | Đặc trưng đồ thị (O9), chạy theo lô rồi xoá |
| 6. GraphFrames trên Glue (thay 5) | Glue G.1X × 2 worker × ~0,3 giờ ≈ 0,26 USD/lần | **≈ 1** | Thử trước phương án 5 |

**Chi phí ẩn, hay bị quên:**

| Khoản | Vì sao phát sinh | Cách giữ thấp |
|---|---|---|
| Mạng VPC | Neptune chỉ trong VPC; agent (Lambda) và Glue phải vào VPC | S3 gateway endpoint miễn phí; Bedrock/STS/Athena qua NAT instance nhỏ (đã chọn cho streaming, khoảng 3–4 USD/tháng) thay vì interface endpoint (khoảng 8–10 USD/tháng **mỗi** endpoint ở 1 AZ) |
| Quên tắt | 1 tuần quên tắt `db.t4g.medium` ≈ 24 USD | Script phiên + **AWS Budgets** cảnh báo, tag `Owner`; cụm dừng quá 7 ngày sẽ tự bật lại nên phải dừng lại |
| Nạp lại toàn bộ | I/O khi bulk load | Khoảng vài trăm nghìn tới vài triệu I/O mỗi lần, dưới 1 USD |
| Bản sao đọc | Mỗi replica tính như một instance | Dev không cần replica |

**Tổng cho đồ án khi bật Neptune (phương án 2 + 6 + mạng):** khoảng **15–20 USD/tháng**, tháng đầu gần 0 nhờ free
tier. Phương án 1 hoặc 3 tốn 105–145 USD/tháng mà không thêm năng lực nào cho đồ án.

**Production (tham khảo cho luận văn):** 1 primary + 1 replica `db.r7g.large` 24/7:
2 × 0,333 × 730 ≈ **486 USD/tháng**, cộng lưu trữ và I/O. Đổi lại: đọc dưới 100 ms, chịu lỗi AZ.

---

## 8. Khuyến nghị và lộ trình

| Giai đoạn | Neptune | Điều kiện chuyển | Việc |
|---|---|---|---|
| **N0 (bây giờ)** | Không dùng | | Agent dùng Athena. Giữ `Backend` tách rời trong `agent/tools.py` |
| **N1** | Neptune Database `db.t4g.medium` **theo phiên** | O5 lượt 2 (IBM AML) có trên `triple`, use case B bắt đầu | Module Terraform (mục 6), job `onto.export_neptune`, script phiên, đối soát sau nạp, tool `aml_subgraph` / `explain_link` |
| **N2** | Neptune Analytics theo lần | GraphFrames không đủ (cần Louvain / vector similarity) | Export point-in-time `--AS_OF`, chạy thuật toán, nạp kết quả vào `graph_feature` |
| N3 (ngoài phạm vi đồ án) | Primary + replica 24/7, blue/green | Có người dùng thật | Neptune Streams nếu có CDC |

**Checklist trước khi apply N1:**
- [ ] Đồ thị AML đã qua bất biến O7 trên Iceberg
- [ ] Budget alert cho dịch vụ Neptune (ví dụ 25 USD/tháng)
- [ ] Đường mạng từ agent/Glue tới cụm (subnet riêng, SG, S3 gateway endpoint, NAT cho Bedrock)
- [ ] Đo kích thước thật sau lần nạp đầu, quyết định `t4g.medium` hay `r7g.large`
- [ ] Đối soát đỉnh/cạnh Neptune = `entity`/`triple` cùng `run_id`
- [ ] Bộ câu hỏi agent use case B chạy trên cả Athena và Neptune: ghi độ trễ, độ chính xác (đóng góp của Neptune trong luận văn)
- [ ] Ghi mọi thay đổi hạ tầng vào `docs/SETUP-LOG.md` của repo hạ tầng
