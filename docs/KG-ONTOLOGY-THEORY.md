# Ontology và Knowledge Graph — lý thuyết nền cho FinGraph

Tài liệu này giải thích **các khái niệm** nằm sau tầng ontology / knowledge graph (KG) của dự án: chúng là gì, đến
từ đâu, vì sao hệ thống chọn cách này mà không chọn cách khác. Mỗi khái niệm có ví dụ lấy thẳng từ code của
FinGraph và trích nguồn `[n]` ở mục 15 để dùng lại khi viết luận văn.

Các tài liệu khác trả lời câu hỏi khác:

| Tài liệu | Trả lời |
|---|---|
| **Tài liệu này** | *Vì sao* và *là gì*: lý thuyết, thuật ngữ, so sánh với các cách làm khác |
| [ONTOLOGY.md](ONTOLOGY.md) | Thiết kế ontology FinGraph: lớp, quan hệ, bảng, use case |
| [ONTOLOGY-BUILD.md](ONTOLOGY-BUILD.md) | Các bước O0–O12 đã làm để dựng nó |
| [NEPTUNE-GUIDE.md](NEPTUNE-GUIDE.md) | Graph database Neptune: bên trong, cách dùng, vận hành |
| [AI-AGENT.md](AI-AGENT.md) | Agent dùng đồ thị để trả lời câu hỏi |

## Mục lục

1. [Vấn đề cần giải quyết](#1-vấn-đề-cần-giải-quyết)
2. [Ontology, knowledge graph và các khái niệm lân cận](#2-ontology-knowledge-graph-và-các-khái-niệm-lân-cận)
3. [Mô hình dữ liệu đồ thị: RDF và property graph](#3-mô-hình-dữ-liệu-đồ-thị-rdf-và-property-graph)
4. [Ngôn ngữ ontology: RDFS, OWL 2 và logic mô tả](#4-ngôn-ngữ-ontology-rdfs-owl-2-và-logic-mô-tả)
5. [Thế giới mở, thế giới đóng và SHACL](#5-thế-giới-mở-thế-giới-đóng-và-shacl)
6. [SKOS: từ vựng, mã nguồn và căn chỉnh](#6-skos-từ-vựng-mã-nguồn-và-căn-chỉnh)
7. [Định danh, nguồn gốc và thời gian](#7-định-danh-nguồn-gốc-và-thời-gian)
8. [Suy diễn](#8-suy-diễn)
9. [Hợp nhất thực thể (entity resolution)](#9-hợp-nhất-thực-thể-entity-resolution)
10. [Xây ontology: phương pháp luận](#10-xây-ontology-phương-pháp-luận)
11. [Đưa dữ liệu vào đồ thị: ánh xạ, vật chất hoá và ảo hoá](#11-đưa-dữ-liệu-vào-đồ-thị-ánh-xạ-vật-chất-hoá-và-ảo-hoá)
12. [Lưu trữ và truy vấn đồ thị](#12-lưu-trữ-và-truy-vấn-đồ-thị)
13. [Knowledge graph cho machine learning và LLM](#13-knowledge-graph-cho-machine-learning-và-llm)
14. [So sánh với các ontology / KG khác](#14-so-sánh-với-các-ontology--kg-khác)
15. [Tài liệu tham khảo](#15-tài-liệu-tham-khảo)
16. [Phụ lục A — bảng thuật ngữ](#phụ-lục-a--bảng-thuật-ngữ)
17. [Phụ lục B — câu hỏi tự kiểm tra (chuẩn bị bảo vệ)](#phụ-lục-b--câu-hỏi-tự-kiểm-tra-chuẩn-bị-bảo-vệ)

---

## 1. Vấn đề cần giải quyết

Lakehouse có 6 nguồn đã lên đồ thị (Berka, core banking, Bank Marketing, MBD, IBM AML, OpenSanctions), mỗi nguồn
một lược đồ riêng. Cùng một ý "**khách giữ thẻ tín dụng**" được ghi theo ba cách khác nhau:

| Nguồn | Cách ghi "khách giữ thẻ tín dụng" |
|---|---|
| Berka | Bảng `card.type IN ('classic','junior','gold')`, nối `card → disp → client` |
| Santander | Cột cờ `ind_tjcr_fin_ult1 = 1` trên dòng khách-tháng |
| Core banking | Bảng `card`, cột `card_type` |

Ba hệ quả nếu không có tầng nghĩa chung:

1. **Câu hỏi xuyên nguồn không viết được một lần.** "Bao nhiêu khách giữ sản phẩm tín dụng?" phải viết ba câu SQL
   khác nhau, và người viết phải tự biết `classic` là thẻ tín dụng, khoản vay cũng là sản phẩm tín dụng.
2. **LLM viết SQL thẳng trên schema thô sẽ sai mà không ai biết.** Mô hình không thể đoán `ind_tjcr_fin_ult1`
   nghĩa là thẻ tín dụng; câu SQL vẫn chạy, chỉ là trả lời sai.
3. **Quan hệ nhiều bước khó hỏi.** "Khách này có liên quan tới đối tượng cấm vận nào qua chuỗi sở hữu / chuyển
   tiền không" là bài toán đường đi trên đồ thị, SQL thuần làm rất vụng.

Tầng ontology + KG giải quyết đúng ba việc đó:

- **Ontology** là *từ điển nghĩa chung*: định nghĩa `fin:CreditCard`, nói nó là con của `fin:CreditProduct`, và
  ghi mã gốc của từng nguồn trỏ vào nó.
- **Knowledge graph** là *dữ liệu đã được dịch sang từ điển đó*: mỗi khách, tài khoản, sản phẩm là một node có id
  ổn định; mỗi quan hệ là một cạnh có nguồn gốc.
- **Suy diễn** tính sẵn những gì suy ra được (thẻ classic ⇒ thẻ tín dụng ⇒ sản phẩm tín dụng), để SQL, model và
  agent chỉ cần đọc.

---

## 2. Ontology, knowledge graph và các khái niệm lân cận

### 2.1 Ontology

Định nghĩa được trích nhiều nhất là của Gruber [1]: *"An ontology is an explicit specification of a
conceptualization"* — một **đặc tả tường minh** của một **cách khái niệm hoá** thế giới. Studer và cộng sự [2] bổ
sung thành *"a formal, explicit specification of a shared conceptualization"*:

| Từ khoá | Nghĩa | Trong FinGraph |
|---|---|---|
| **conceptualization** | Cách nhìn một miền: có những loại vật gì, quan hệ gì | Ngân hàng có bên tham gia, tài khoản, sản phẩm, vị trí, giao dịch, kiểu hình rửa tiền |
| **explicit** | Viết ra, không nằm trong đầu người | `ontology/fingraph.ttl` |
| **formal** | Máy đọc và suy luận được | OWL 2 + SHACL, kiểm bằng reasoner trong pytest |
| **shared** | Nhiều bên đồng thuận dùng chung | Một ontology cho cả 6 nguồn, cho model, SQL và agent |

Guarino, Oberle và Staab [3] nhấn mạnh rằng ontology mô tả **nghĩa**, không mô tả cách lưu: cùng một ontology có
thể được lưu thành bảng, file RDF hay graph database.

Một ontology thường gồm:

- **Lớp (class / concept):** `fin:Customer`, `fin:Loan`.
- **Quan hệ (property / relation):** `fin:holdsAccount` nối `Customer` với `Account`.
- **Thuộc tính giá trị (datatype property):** `fin:birthYear` là số nguyên.
- **Tiên đề (axiom):** "mọi `Loan` đều là `CreditProduct`", "`partOf` có tính bắc cầu".
- **Chú thích (annotation):** nhãn tiếng Việt, định nghĩa, mã nguồn — không tham gia suy luận nhưng rất cần cho người
  và cho LLM.

FinGraph 1.0.0 có **53 lớp, 19 quan hệ đối tượng, 10 thuộc tính giá trị** và 20 câu hỏi năng lực
(`ontology/competency_questions.yml`, xem 10.2).

### 2.2 Knowledge graph

Thuật ngữ trở nên phổ biến khi Google ra mắt *Knowledge Graph* năm 2012 với khẩu hiệu *"things, not strings"* [4]:
tìm kiếm theo **thực thể** chứ không theo chuỗi ký tự. Định nghĩa học thuật chưa thống nhất [5]; định nghĩa được
dùng nhiều là của Hogan và cộng sự [6]:

> *"a graph of data intended to accumulate and convey knowledge of the real world, whose nodes represent entities
> of interest and whose edges represent relations between these entities."*

Ba đặc điểm làm KG khác một bảng dữ liệu thông thường:

1. **Node là thực thể có định danh toàn cục**, không phải dòng trong một bảng. Khách 2 ở core banking và khách 2
   ở Berka là hai node, nối bằng cạnh `fin:sameAs`.
2. **Lược đồ linh hoạt, gắn với ontology**: thêm loại cạnh mới không cần đổi cấu trúc bảng.
3. **Tri thức tường minh lẫn suy ra**: cạnh lấy từ dữ liệu (`is_inferred = false`) và cạnh suy diễn (bảng `closure`).

### 2.3 Ontology và KG khác nhau thế nào

Hay gặp nhất là cách chia của logic mô tả (mục 4.3):

| | **TBox** (Terminological box) | **ABox** (Assertional box) |
|---|---|---|
| Nói về | Khái niệm và luật chung | Cá thể cụ thể |
| Ví dụ | `fin:ClassicCard ⊑ fin:CreditCard` | "Khách 2 giữ tài khoản 1" |
| Thay đổi | Hiếm, qua review như code | Mỗi lần chạy ETL |
| Trong FinGraph | `fingraph.ttl` → `tbox.json` → bảng `class`, `property`, `subclass` | Bảng `entity`, `triple` ở `track_a_onto`, rồi Neptune |

Nói gọn: **ontology ≈ TBox (bản thiết kế), knowledge graph ≈ ABox đổ theo bản thiết kế đó** (thường kèm cả TBox).
MUFG [30] tổ chức đúng như vậy: một ontology quy trình chung, mỗi chi nhánh là một instance graph.

### 2.4 Phổ các mức "ontology"

Lassila và McGuinness [7] xếp các mức biểu diễn nghĩa từ yếu tới mạnh. Hữu ích để biết hệ thống mình đứng đâu:

| Mức | Có gì | Ví dụ | Máy suy luận được |
|---|---|---|---|
| Danh mục (catalog) / từ điển thuật ngữ | Danh sách tên | Danh sách mã sản phẩm | Không |
| Glossary | Tên + định nghĩa bằng lời | Từ điển nghiệp vụ trong wiki | Không |
| Thesaurus | Thêm quan hệ rộng/hẹp/liên quan | SKOS (mục 6) | Rất ít |
| **Taxonomy** | Cây phân loại cha–con nghiêm ngặt | Cây `fin:Product` | Thừa kế lớp |
| Lược đồ có kiểu (schema) | Lớp, thuộc tính, kiểu giá trị | Lược đồ quan hệ, JSON Schema, RDFS | Kiểm kiểu |
| **Ontology hình thức** | Thêm tiên đề logic: bắc cầu, đối xứng, ràng buộc lực lượng, phủ định | OWL 2 | Phân loại, kiểm nhất quán |

FinGraph đứng ở mức **taxonomy + OWL 2 phần nhẹ** (bắc cầu, đối xứng, domain/range), kèm SKOS cho mã nguồn và
SHACL cho ràng buộc dữ liệu. Dự án **cố ý không** dùng các tiên đề OWL nặng (phủ định, lượng từ phức tạp), vì
chúng làm suy diễn chậm và khó giải thích, mà các câu hỏi năng lực không cần tới.

### 2.5 Phân biệt với các khái niệm hay nhầm

| Khái niệm | Khác ontology / KG ở đâu |
|---|---|
| **Lược đồ CSDL quan hệ (schema)** | Mô tả *cách lưu* (bảng, khoá, kiểu cột) của *một* hệ thống. Ontology mô tả *nghĩa* và dùng chung cho nhiều hệ thống [3]. Một ontology có thể ánh xạ vào nhiều schema (mục 11) |
| **Mô hình dữ liệu chiều (star schema, Kimball)** | Tối ưu cho phân tích tổng hợp theo fact/dimension. Không có suy luận, không nối thực thể giữa các mart |
| **Semantic layer / metrics layer** (dbt, Cube, LookML) | Định nghĩa *chỉ số* và join cho BI; gần với tool `metric` của agent. Không có phân cấp khái niệm hay suy diễn |
| **Data catalog** (Glue Catalog, DataHub) | Mô tả *tài sản dữ liệu* (bảng nào, ai sở hữu). Ontology mô tả *thế giới nghiệp vụ* mà các bảng đó phản ánh |
| **Graph database** | Là *công cụ lưu trữ*. KG có thể lưu trong graph DB, nhưng FinGraph lưu bản gốc trên bảng Iceberg (mục 12) |
| **Vector store / embedding** | Biểu diễn *gần đúng* theo độ tương đồng; không có quan hệ tường minh, không trích dẫn được từng cạnh (mục 13) |

---

## 3. Mô hình dữ liệu đồ thị: RDF và property graph

Có hai mô hình dữ liệu đồ thị chính [6][8]. FinGraph dùng **cả hai**: RDF cho ontology, property graph cho Neptune.

### 3.1 RDF (Resource Description Framework)

Chuẩn W3C [9]. Mọi thông tin là **bộ ba (triple)** `chủ ngữ — vị ngữ — tân ngữ`:

```turtle
@prefix fin:  <https://fingraph.hust.edu.vn/ontology#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

fin:ClassicCard  rdfs:subClassOf  fin:CreditCard .          # TBox
fin:ClassicCard  rdfs:label       "Thẻ classic"@vi .        # literal có ngôn ngữ
<.../uid/9f2c…>  fin:holdsAccount <.../uid/7a1e…> .         # ABox
```

Các thành phần:

| Thành phần | Là gì | Ghi chú |
|---|---|---|
| **IRI** | Định danh toàn cục dạng URL | `fin:CreditCard` là viết tắt của `https://fingraph.hust.edu.vn/ontology#CreditCard`. Nhờ IRI, hai ontology khác nhau có thể trỏ tới nhau (căn chỉnh FIBO, mục 6) |
| **Literal** | Giá trị: chuỗi, số, ngày, có kiểu `xsd:` hoặc thẻ ngôn ngữ `@vi` | |
| **Blank node** | Node không tên, chỉ có nghĩa trong một file | FinGraph tránh dùng ngoài SHACL |
| **Named graph / quad** | Bộ bốn: thêm thành phần *đồ thị* để nói "bộ ba này thuộc nguồn nào" | Neptune lưu nội bộ theo quad [26] |

**Turtle** [10] là cú pháp viết RDF dễ đọc nhất; `fingraph.ttl`, `alignment/fibo.ttl` và `shapes/*.ttl` đều viết
bằng Turtle.

Điểm yếu kinh điển của RDF: **muốn gắn thông tin lên một cạnh** (độ tin cậy, thời gian hiệu lực, nguồn) thì phải
*reify* — biến cạnh thành một node trung gian — làm đồ thị phình gấp nhiều lần. RDF-star [11] (đưa vào RDF 1.2) cho
phép một triple làm chủ ngữ của triple khác để giải quyết việc này.

### 3.2 Property graph (LPG — labeled property graph)

Mô hình của Neo4j, Neptune openCypher/Gremlin, và chuẩn ISO GQL [12]:

- **Node** có một hoặc nhiều **nhãn** (`:Customer:Person`) và các cặp **thuộc tính** `key = value`.
- **Cạnh** có hướng, có **một loại** (`:holdsAccount`) và **cũng có thuộc tính**.

```
(:Customer {label: "Khách 2"}) -[:holdsAccount {valid_from: 1993-02-26, confidence: 1.0}]-> (:Account)
```

Thuộc tính trên cạnh là tự nhiên, không cần reify — đây là lý do property graph được ưa trong ứng dụng.

### 3.3 So sánh

| | RDF | Property graph |
|---|---|---|
| Đơn vị | Triple (hoặc quad) | Node, cạnh, thuộc tính |
| Định danh | IRI toàn cục | Id nội bộ của CSDL |
| Thuộc tính trên cạnh | Cần reify hoặc RDF-star | Có sẵn |
| Lược đồ / ngữ nghĩa | Chuẩn W3C: RDFS, OWL, SHACL, SKOS — máy suy luận được | Không có chuẩn ngữ nghĩa; nhãn chỉ là tên |
| Liên kết dữ liệu ngoài | Dễ (cùng IRI là cùng thứ) | Phải tự quy ước |
| Ngôn ngữ truy vấn | SPARQL [13] | openCypher [14], Gremlin [15], GQL [12] |
| Mạnh ở | Tích hợp, chuẩn hoá nghĩa, suy diễn | Duyệt đường đi, ứng dụng, hiệu năng |

### 3.4 FinGraph dùng cả hai thế nào

```
fingraph.ttl (RDF/OWL)  ── tbox.py ──►  tbox.json  ──►  bảng class / property / subclass (Iceberg)
                                                          │
mappings/*.yml  + silver ── build_graph ──►  entity, triple (Iceberg)   ◄── bản GỐC của KG
                                                          │
                                           export_neptune ▼
                                      CSV openCypher ──► Neptune (property graph)
```

- **Ontology viết bằng RDF/OWL** để dùng được chuẩn W3C, reasoner và căn chỉnh FIBO.
- **Dữ liệu KG lưu thành bảng `triple`** trên Iceberg: mỗi dòng là một cạnh, có cột `weight`, `valid_from`,
  `valid_to`, `confidence`, `source_dataset`, `is_inferred`, `mapping_id`. Về bản chất đây là **RDF có thuộc tính trên
  cạnh** — giải được bài toán reify bằng cột bảng.
- **Neptune nhận bản property graph** (`id(n) = uid`, `id(r) = triple_id`) để duyệt đường đi AML nhiều bước.

Vì `uid` và `triple_id` giữ nguyên ở cả hai nơi, câu trả lời của agent trích từ Athena hay từ Neptune đều trỏ về
cùng một dòng gốc.

---

## 4. Ngôn ngữ ontology: RDFS, OWL 2 và logic mô tả

### 4.1 RDFS — mức nhẹ nhất

RDF Schema [16] thêm vài từ vựng để nói về lớp và quan hệ:

| Từ vựng | Nghĩa | Trong FinGraph |
|---|---|---|
| `rdfs:Class`, `rdf:type` | Lớp, và "cá thể thuộc lớp" | Cột `class_iri` của `entity` |
| `rdfs:subClassOf` | Lớp con | Cây sản phẩm, cây bên tham gia |
| `rdfs:subPropertyOf` | Quan hệ con | Chưa dùng |
| `rdfs:domain`, `rdfs:range` | Chủ ngữ / tân ngữ của quan hệ thuộc lớp nào | `holdsAccount`: `Customer → Account` |
| `rdfs:label`, `rdfs:comment` | Nhãn, chú thích | Nhãn `@vi` / `@en` |

> **Bẫy hay gặp:** trong RDFS/OWL, `domain` và `range` là luật **suy diễn**, *không phải* ràng buộc. Nếu có cạnh
> `X holdsAccount Y` mà X chưa khai báo là `Customer`, reasoner **không báo lỗi** mà **suy ra X là Customer**. Muốn
> *chặn* dữ liệu sai thì phải dùng SHACL (mục 5). Đó là lý do FinGraph có cả hai.

### 4.2 OWL 2

Web Ontology Language [17] mở rộng RDFS bằng tiên đề có ngữ nghĩa logic chặt chẽ. Các cấu trúc chính, và FinGraph
dùng cái nào:

| Cấu trúc OWL 2 | Nghĩa | FinGraph |
|---|---|---|
| `owl:Class`, `owl:ObjectProperty`, `owl:DatatypeProperty`, `owl:AnnotationProperty` | Khai báo | Có |
| `owl:TransitiveProperty` | `a R b ∧ b R c ⇒ a R c` | `fin:partOf` (quận ⊂ vùng ⊂ nước) |
| `owl:SymmetricProperty` | `a R b ⇒ b R a` | `fin:sameAs` |
| `owl:inverseOf` | `R⁻` | Không (tránh nhân đôi cạnh; truy vấn đi ngược cạnh là đủ) |
| `owl:FunctionalProperty`, lực lượng `minCount/maxCount` | Tối đa / tối thiểu bao nhiêu giá trị | Đặt ở SHACL thay vì OWL (mục 5) |
| `owl:disjointWith` | Hai lớp không giao nhau | Chưa dùng (xem Phụ lục B, câu 6) |
| Hạn chế `owl:someValuesFrom`, `allValuesFrom` | Định nghĩa lớp bằng điều kiện | Không dùng |
| `owl:sameAs` | Hai IRI là **cùng một vật** | **Cố ý không dùng** (mục 9.3) |
| `owl:equivalentClass` | Hai lớp tương đương | Không; căn chỉnh FIBO dùng `skos:closeMatch` |

**Punning** (OWL 2 cho phép một IRI vừa là lớp vừa là cá thể [18]): node sản phẩm trong đồ thị *chính là* IRI lớp
`fin:Loan` (`ids.product_uid`). Cạnh `ofProduct` trỏ vào lớp, nhờ đó "khách giữ sản phẩm thuộc cây `CreditProduct`"
chỉ cần một phép join với bảng `subclass`. SHACL `OfProductShape` kiểm tân ngữ phải nằm trong cây `fin:Product`.

### 4.3 Logic mô tả (Description Logic, DL)

OWL 2 DL có nền là logic mô tả **SROIQ(D)** [19][20]. Ba điều cần nắm:

1. **Ngữ nghĩa hình thức.** Mỗi tiên đề có nghĩa toán học chính xác (diễn giải tập hợp: lớp là tập cá thể,
   `C ⊑ D` nghĩa là tập C nằm trong tập D). Vì vậy "suy ra" là một khái niệm được định nghĩa, không phụ thuộc
   phần mềm.
2. **Quyết định được (decidable)**: mọi câu hỏi suy luận chuẩn đều có thuật toán trả lời trong thời gian hữu hạn —
   đổi lại, độ phức tạp trường hợp xấu nhất của SROIQ rất cao (2NEXPTIME). Đó là lý do có các **profile** nhẹ hơn.
3. **Các tác vụ suy luận chuẩn:**

| Tác vụ | Câu hỏi | FinGraph dùng ở đâu |
|---|---|---|
| Kiểm nhất quán (consistency) | Ontology có mâu thuẫn không? | HermiT trong `tests/ontology/test_tbox.py` |
| Thoả được (satisfiability) | Lớp này có thể có cá thể không? (lớp rỗng thường là lỗi thiết kế) | Cùng test |
| Phân loại (classification) | Tính toàn bộ cây lớp con kể cả quan hệ ẩn | Tính bằng Python thành bảng `subclass` |
| Nhận dạng (realization) | Cá thể này thuộc những lớp nào? | Join `entity.class_iri` với `subclass` |

### 4.4 Các profile OWL 2

OWL 2 định nghĩa ba tập con có độ phức tạp đa thức [21]:

| Profile | Thiết kế cho | Thuật toán | Liên hệ FinGraph |
|---|---|---|---|
| **OWL 2 EL** | Ontology rất nhiều lớp (SNOMED CT ~350.000 khái niệm) | Phân loại thời gian đa thức (reasoner ELK) | — |
| **OWL 2 QL** | Hỏi dữ liệu lớn nằm trong CSDL quan hệ | **Viết lại truy vấn** thành SQL (Ontop) | Hướng ảo hoá của Accelerator (mục 11) |
| **OWL 2 RL** | Suy diễn bằng luật trên dữ liệu lớn | **Luật kiểu Datalog**, vật chất hoá | Gần với cách FinGraph làm: luật bắc cầu chạy trên Spark |

Các tiên đề FinGraph dùng (subClassOf, domain/range, transitive, symmetric) đều nằm gọn trong OWL 2 RL, nên suy diễn
được bằng luật lặp đơn giản (mục 8) mà không cần reasoner đầy đủ ở thời điểm chạy.

---

## 5. Thế giới mở, thế giới đóng và SHACL

### 5.1 Giả định thế giới mở (OWA) và giả định tên duy nhất

OWL theo **giả định thế giới mở (Open World Assumption)**: điều gì không biết thì *chưa biết*, không phải *sai*.
CSDL quan hệ theo **giả định thế giới đóng (CWA)** [22]: cái gì không có trong bảng là không có.

| Tình huống | CSDL / SHACL (đóng) | OWL (mở) |
|---|---|---|
| `ProductHolding` không có `validFrom` | Lỗi: thiếu dữ liệu bắt buộc | Không lỗi: có ngày nhưng chưa ghi |
| Một holding có hai `ofProduct` khác IRI | Lỗi: vi phạm "đúng một" | Không lỗi: **suy ra hai IRI là một** (OWL không có giả định tên duy nhất) |
| Khách không có cạnh tới đối tượng cấm vận | Không liên quan | Không biết có liên quan hay không |

OWA đúng triết lý web (không ai có toàn bộ dữ liệu), nhưng **ngân hàng cần kiểm chất lượng dữ liệu** — tức cần
thế giới đóng. Hai thế giới này cùng tồn tại trong FinGraph:

- **Ontology (OWL)** để nói nghĩa, suy diễn, căn chỉnh.
- **SHACL** để kiểm dữ liệu.

Ý OWA vẫn quan trọng cho agent: không tìm thấy đường đi tới đối tượng cấm vận trong phạm vi đã nạp **không có
nghĩa** là khách sạch. Quy tắc của agent buộc nói rõ phạm vi đã xét thay vì kết luận "không liên quan"
([AI-AGENT.md](AI-AGENT.md) mục 5).

### 5.2 SHACL

Shapes Constraint Language [23] là chuẩn W3C để **kiểm** một đồ thị RDF theo "hình dạng" (shape):

```turtle
fin:HoldsAccountShape a sh:NodeShape ;
    sh:targetSubjectsOf fin:holdsAccount ;           # áp cho mọi chủ ngữ của holdsAccount
    sh:class fin:Customer ;                           # phải là Customer
    sh:property [ sh:path fin:holdsAccount ; sh:class fin:Account ;
                  sh:message "holdsAccount phai noi Customer voi Account" ] .
```

FinGraph kiểm SHACL ở hai nơi:

| Ở đâu | Bằng gì | Dữ liệu |
|---|---|---|
| pytest | `pyshacl` trên `shapes/fingraph.shacl.ttl` | Đồ thị mẫu `tests/ontology/fixtures/sample_abox.ttl` |
| Job `build_graph` | Bất biến tương đương viết bằng Spark | Bảng `triple` thật (hàng trăm nghìn tới triệu cạnh) |

Lý do không chạy `pyshacl` trên dữ liệu thật: SHACL engine Python chạy trong bộ nhớ, không mở rộng được tới hàng
triệu cạnh. Đây là cách làm phổ biến: SHACL làm **đặc tả**, việc kiểm ở quy mô lớn dịch sang SQL/Spark.

### 5.3 SHACL so với OWL so với ràng buộc CSDL

| | OWL | SHACL | CHECK / FK của CSDL |
|---|---|---|---|
| Mục đích | Suy ra tri thức mới | Kiểm dữ liệu | Kiểm dữ liệu |
| Thế giới | Mở | Đóng | Đóng |
| Thiếu giá trị | Không sao | Vi phạm | Vi phạm (NOT NULL) |
| Đặt trên | Nghĩa (lớp, quan hệ) | Đồ thị bất kỳ | Một bảng |
| Đường đi nhiều bước | Có (chuỗi quan hệ) | Có (`sh:path` với `zeroOrMorePath`) | Không |

---

## 6. SKOS: từ vựng, mã nguồn và căn chỉnh

SKOS (Simple Knowledge Organization System) [24] là chuẩn W3C cho **hệ từ vựng**: thesaurus, danh mục phân loại,
bộ mã. SKOS nhẹ hơn OWL: quan hệ `broader`/`narrower` không mang nghĩa tập hợp chặt như `subClassOf`.

FinGraph dùng SKOS ở ba chỗ:

| Cấu trúc SKOS | Dùng để | Ví dụ |
|---|---|---|
| `skos:notation` | **Mã gốc của nguồn** gắn vào lớp chung | `fin:Loan skos:notation "berka:loan", "corebank:loan", "santander:ind_pres_fin_ult1"` — từ mã nguồn tra ra lớp bằng một phép tra |
| `skos:definition`, `skos:prefLabel` | Định nghĩa cho người và cho agent | Semantic catalog của agent sinh từ đây |
| `skos:ConceptScheme` / `skos:Concept` | Danh mục **khái niệm** không phải lớp | `fin:typologyScheme`: 8 kiểu hình rửa tiền của IBM AML (FAN-OUT, CYCLE…) |
| `skos:closeMatch`, `skos:broadMatch` | **Căn chỉnh** sang ontology ngoài mà không cam kết tương đương logic | `fin:Account skos:closeMatch fibo:…/Account` |

Vì sao căn chỉnh FIBO bằng `skos:closeMatch` mà không dùng `owl:equivalentClass`: tương đương OWL buộc *mọi* tiên đề
của FIBO áp vào lớp của mình. FIBO có hàng nghìn lớp với ràng buộc phức tạp; nhập vào sẽ làm suy diễn chậm và có
thể sinh mâu thuẫn. `closeMatch` chỉ nói "nghĩa gần như nhau" — đủ để người đọc và công cụ khác đối chiếu [24][31].

Kiểu hình rửa tiền là `skos:Concept` chứ không phải lớp, vì chúng là **giá trị phân loại** (một nhóm giao dịch
"có kiểu hình FAN-OUT") chứ không phải loại thực thể có cá thể riêng.

---

## 7. Định danh, nguồn gốc và thời gian

### 7.1 Định danh ổn định

KG chỉ dùng được cho trích dẫn khi mỗi node, mỗi cạnh có **id không đổi qua các lần chạy lại**. Linked Data [25]
dùng IRI dereference được; FinGraph dùng băm tất định ([transforms/onto/ids.py](../transforms/onto/ids.py)):

```
uid       = sha256(class_iri | source_system | natural_key)[:32]
triple_id = sha256(subject_uid | predicate_iri | object | valid_from | source_dataset)[:32]
```

| Tính chất | Hệ quả |
|---|---|
| Tất định (cùng đầu vào → cùng id) | Chạy lại ETL không đổi id; agent, embedding, `Recommendation` cũ vẫn trỏ đúng |
| Có `source_system` trong uid | Khách 2 ở Berka và ở core banking là hai node — gộp hay không là việc của entity resolution (mục 9) |
| Không chứa PII | `natural_key` là `client_id=2`, không phải họ tên |
| 32 hex = 128 bit | Xác suất trùng không đáng kể ở quy mô dự án |

Có id ổn định cũng là thứ cho phép **kiểm trích dẫn** của agent: id nào trong câu trả lời không có trong kết quả
tool là bịa (mục 13.3).

### 7.2 Nguồn gốc dữ liệu (provenance)

PROV-O [27] là ontology W3C cho nguồn gốc: *entity* được *generated by* *activity* do *agent* thực hiện. FinGraph
không nhập PROV-O nhưng giữ cùng ý:

| Ý PROV | Trong FinGraph |
|---|---|
| `prov:wasDerivedFrom` | `fin:derivedFrom`; cột `source_dataset`, `source_key` trên mỗi cạnh |
| `prov:wasGeneratedBy` (activity) | `mapping_id`, `run_id`, `ontology_version` trên mỗi dòng |
| Phân biệt dữ liệu gốc / suy ra | `is_inferred`; bảng `closure` tách khỏi `triple` |
| Độ tin cậy | `confidence` (1,0 cho dữ liệu nguồn; 0,6–0,95 cho khớp cấm vận) |

### 7.3 Thời gian kép (bitemporal)

Ngân hàng phải trả lời "**tại ngày X**, khách giữ gì" (kiểm toán, và để model không học từ tương lai). Lý thuyết dữ
liệu thời gian [28] phân biệt hai trục:

| Trục | Câu hỏi | Cột FinGraph |
|---|---|---|
| **Valid time** (thời gian hiệu lực) | Sự thật đúng *ngoài đời* từ khi nào tới khi nào | `valid_from`, `valid_to` trên cạnh |
| **Transaction time** (thời gian ghi nhận) | Hệ thống *biết* điều đó từ khi nào | `recorded_at` / snapshot Iceberg |

RDF thuần không có thời gian; các KG lớn giải bằng reify hoặc qualifier (Wikidata [32]). FinGraph đặt thẳng thành
cột trên bảng `triple`, và Iceberg giữ snapshot cho trục ghi nhận.

---

## 8. Suy diễn

### 8.1 Hai cách suy diễn

| Cách | Làm gì | Ưu | Nhược |
|---|---|---|---|
| **Vật chất hoá (forward chaining)** | Chạy luật trước, **ghi kết quả suy ra** thành dữ liệu | Truy vấn nhanh, đơn giản; SQL/model đọc được | Phải tính lại khi dữ liệu đổi; tốn chỗ |
| **Viết lại truy vấn (backward chaining / query rewriting)** | Giữ dữ liệu gốc; lúc hỏi thì **mở rộng câu hỏi** theo ontology | Luôn mới, không tốn chỗ | Truy vấn phức tạp hơn; cần engine hiểu ontology (Ontop, Stardog) |

FinGraph chọn **vật chất hoá bằng Spark**, vì:

- Athena không có đệ quy đáng tin cậy để tự mở rộng truy vấn bắc cầu;
- model ML cần đặc trưng dạng bảng, không gọi được reasoner;
- LLM **không được tự suy luận bắc cầu** — nó dễ sai; nó chỉ đọc kết quả đã tính
  ([transforms/onto/derive.py](../transforms/onto/derive.py)).

MUFG [30] cũng đặt suy diễn **ngoài LLM** (dùng HermiT giữa Neptune và agent); FinGraph giữ đúng nguyên tắc đó nhưng
thay reasoner bằng luật tính sẵn.

### 8.2 Bao đóng bắc cầu và đánh giá nửa ngây thơ

Bao đóng bắc cầu `R*` của quan hệ `R` là tập mọi cặp `(a, c)` nối được bằng một chuỗi cạnh `R`. Viết bằng Datalog [29]:

```prolog
reach(A, B) :- edge(A, B).
reach(A, C) :- reach(A, B), edge(B, C).
```

Cách tính chuẩn là **đánh giá nửa ngây thơ (semi-naive evaluation)** [29]: mỗi vòng chỉ nối *các cặp mới tìm được ở
vòng trước* (frontier) với cạnh, rồi bỏ các cặp đã có, dừng khi không có cặp mới. Hàm `closure()` trong `derive.py`
làm đúng như vậy bằng Spark:

```
frontier = edges
lặp d = 2..MAX_DEPTH:
    step = frontier ⋈ edges                 # đi thêm một bước
    new  = step − reach                     # left_anti: chỉ giữ cặp CHƯA có
    nếu new rỗng: dừng                      # điểm bất động
    reach = reach ∪ new ; frontier = new
```

`MAX_DEPTH = 6` vừa chặn chi phí vừa chặn vòng lặp do chu trình. Bảng `closure` lưu `(start_uid, node_uid,
predicate_iri, depth)` cho `fin:partOf` (quận → vùng → nước) và `fin:isDirectlyOwnedBy` (chuỗi sở hữu).

**Bao đóng lớp con** (`subclass`) tính tương tự nhưng trên TBox (nhỏ), bằng Python trong `tbox.py`. Câu hỏi "khách
giữ sản phẩm tín dụng" khi đó chỉ là:

```sql
SELECT ... FROM triple t
JOIN subclass s ON s.class_iri = <lớp của t.object_uid> AND s.ancestor_iri = 'fin:CreditProduct'
```

### 8.3 Suy diễn logic và suy diễn thống kê

Cần phân biệt rõ khi viết luận văn:

| | Logic (`subclass`, `closure`) | Thống kê (`product_transition`) |
|---|---|---|
| Kết quả | **Chắc chắn đúng** nếu dữ liệu và tiên đề đúng | **Ước lượng**: P(mua B trong H tháng ∣ đã có A), lift |
| Nền | Ngữ nghĩa OWL / Datalog | Đếm tần suất trên lịch sử |
| Rủi ro | Tiên đề sai | Rò rỉ tương lai, censoring, đếm trùng |
| FinGraph chống rủi ro | Test reasoner + SHACL | Mốc cắt train riêng từng nguồn; chỉ đếm A mua sớm hơn mốc ít nhất H tháng; bỏ khách trùng giữa corebank và Berka |

`product_transition` dùng *cây lớp của ontology* (đếm theo `CreditCard`, không theo `ind_tjcr_fin_ult1`), nên một
định nghĩa dùng chung được cho mọi nguồn — đây là giá trị của ontology đối với model, không phải suy luận logic.

---

## 9. Hợp nhất thực thể (entity resolution)

### 9.1 Bài toán

Cùng một người/tổ chức xuất hiện ở nhiều nguồn với định danh khác nhau. Ngành thống kê gọi là **record linkage**,
ngành dữ liệu gọi là **entity resolution / data matching** [33][34]. Lý thuyết nền là mô hình Fellegi–Sunter [33]:
với mỗi cặp bản ghi, so sánh từng trường, cộng **trọng số bằng chứng** của các trường khớp/không khớp, rồi chia ba
vùng theo hai ngưỡng:

```
tổng điểm ≥ ngưỡng trên      → coi là trùng (link)
ngưỡng dưới ≤ điểm < trên    → người duyệt (possible link)
điểm < ngưỡng dưới           → không trùng
```

### 9.2 FinGraph áp dụng thế nào

[transforms/onto/resolve.py](../transforms/onto/resolve.py), hai luật:

| Luật | Cặp | Cách khớp | confidence |
|---|---|---|---|
| `corebank_berka_client` | khách core banking ↔ khách Berka | cùng `natural_key` (core banking được sinh từ Berka) | 1,0 |
| `sanctions_name` | khách core banking ↔ người trong OpenSanctions | tên mờ + ngày sinh + quốc gia | 0,6–0,95 |

Luật cấm vận theo đúng sơ đồ Fellegi–Sunter, đơn giản hoá:

1. **Chuẩn hoá tên** (bỏ dấu, chữ thường, bỏ ký tự lạ).
2. **Blocking** [34]: chỉ so hai tên có chung ít nhất một từ ≥ 4 ký tự — tránh so mọi cặp (tích Descartes). Chặn
   theo *mọi* từ, không theo một từ, để không bỏ sót lỗi chính tả ở đúng từ đó.
3. **So sánh**: tỉ lệ giống theo khoảng cách Levenshtein `1 − lev(a,b) / max(|a|,|b|)`, giữ cặp ≥ 0,85.
4. **Cộng bằng chứng**: tên trùng hẳn 0,80, tên gần 0,60–0,75; ngày sinh khớp đủ +0,15, khớp năm +0,10; có quốc
   tịch CZ +0,05; trần 0,95.
5. **Hai ngưỡng**: ≥ 0,9 thành cạnh `fin:sameAs`; [0,6; 0,9) vào hàng đợi `same_as_review` cho người duyệt.

`evidence` chỉ ghi cờ (`name=fuzzy:0.91;birth=year;country=no`), **không ghi tên** — tên là PII.

### 9.3 Vì sao không dùng `owl:sameAs`

`owl:sameAs` trong OWL nghĩa là **hai IRI là đúng một vật**: mọi thuộc tính của cái này tự động là của cái kia, và
quan hệ có tính bắc cầu. Halpin và cộng sự [35] chỉ ra trên Linked Data thật, `owl:sameAs` bị dùng cho "gần giống",
gây lan truyền sai hàng loạt.

Với AML, gộp nhầm một khách bình thường với một đối tượng cấm vận là **sai nghiêm trọng và không gỡ được** sau khi
suy diễn đã lan. Vì vậy FinGraph dùng `fin:sameAs`:

- đối xứng (`owl:SymmetricProperty`) nhưng **không** bắc cầu, không gộp thuộc tính;
- là một cạnh có `confidence`, `rule_id`, `evidence`, xoá được;
- agent bị cấm kết luận khách là đối tượng cấm vận từ cạnh dưới 0,9 hoặc từ hàng đợi duyệt.

---

## 10. Xây ontology: phương pháp luận

### 10.1 Các phương pháp phổ biến

| Phương pháp | Ý chính | FinGraph lấy gì |
|---|---|---|
| **Ontology Development 101** [36] (Noy & McGuinness) | 7 bước: xác định phạm vi → cân nhắc tái sử dụng → liệt kê thuật ngữ → lớp và cây lớp → thuộc tính → ràng buộc → tạo cá thể | Thứ tự các bước O1–O5 |
| **Câu hỏi năng lực** [37] (Grüninger & Fox) | Ontology "xong" khi trả lời được một danh sách câu hỏi viết trước | `competency_questions.yml`: 20 câu, mỗi khái niệm phải trỏ tới ít nhất một câu |
| **NeOn** [38] | Xây ontology trong mạng ontology có sẵn: tái sử dụng, căn chỉnh, mô-đun hoá | Căn chỉnh FIBO thay vì tự định nghĩa lại hoặc nhập nguyên |
| Ontology tầng trên (BFO [39], DOLCE, gUFO) | Bộ khung triết học chung (vật thể, quá trình, vai trò…) cho mọi miền | **Không dùng**: quá trừu tượng cho phạm vi đồ án; FIBO đã gánh phần chuẩn hoá miền tài chính |

### 10.2 Câu hỏi năng lực làm gì trong dự án

Mỗi câu (ví dụ CQ1 "Khách X đang giữ những sản phẩm nào tại ngày D?") khai báo `concepts` cần có. Test kiểm:

- mọi khái niệm trong câu hỏi phải tồn tại trong `fingraph.ttl` — chặn ontology thiếu;
- mọi lớp/quan hệ phải phục vụ ít nhất một câu hỏi — chặn ontology phình ra vô ích.

Đây là thước đo phạm vi duy nhất khách quan: không có nó, ontology rất dễ thành bản sao một phần FIBO.

### 10.3 Ontology như code

FinGraph đối xử với ontology như mã nguồn: ở git, có `CHANGELOG.md` và `owl:versionInfo`, có test (cú pháp,
reasoner, SHACL, taxonomy, mapping), có review. Mỗi dòng của đồ thị ghi `ontology_version` để biết nó được dựng
theo phiên bản nào. MUFG [30] mô tả cùng xu hướng: chuyên gia nghiệp vụ chuyển từ **viết** sang **duyệt**.

---

## 11. Đưa dữ liệu vào đồ thị: ánh xạ, vật chất hoá và ảo hoá

### 11.1 Ánh xạ (mapping)

Cần một lớp khai báo "**cột nào của bảng nào thành node/cạnh nào của ontology**". Chuẩn W3C là R2RML [40]; RML mở
rộng cho CSV/JSON. FinGraph dùng YAML riêng ([transforms/onto/mappings/](../transforms/onto/mappings/)) cùng tinh thần:

```yaml
- id: customer
  class: fin:Customer
  also: [fin:Person]
  sql: |                     # SQL trên silver, chỉ bản hiện hành
    SELECT c.client_id, ... FROM {silver_berka.client} c ...
  key: [client_id]           # -> natural_key -> uid
  label: "concat('Khách ', client_id)"   # KHÔNG chứa PII
  valid_from: since
```

| | R2RML | Mapping YAML của FinGraph |
|---|---|---|
| Chuẩn | W3C | Riêng dự án |
| Đầu ra | RDF triple | Dòng bảng `entity` / `triple` (Spark) |
| Thời gian, độ tin cậy, nguồn | Phải tự thêm triple | Cột có sẵn |
| Kiểm | Tuỳ công cụ | Test: mọi lớp/quan hệ phải có trong TBox, cột phải có trong silver; đối soát số dòng sau khi dựng |

Lý do không dùng R2RML: đầu ra cần là bảng Iceberg có cột thời gian và độ tin cậy, không phải file RDF.

### 11.2 Vật chất hoá (ETL) và ảo hoá (OBDA)

| | **Vật chất hoá** (FinGraph) | **Ảo hoá / OBDA** [41][42] (Accelerator, Ontop) |
|---|---|---|
| Cách làm | Chạy job dựng đồ thị, **ghi** thành bảng | Không sao chép: câu SPARQL được **dịch thành SQL** chạy thẳng trên nguồn |
| Dữ liệu mới | Phải chạy lại job | Luôn mới |
| Suy diễn | Tính sẵn (mục 8) | Viết lại truy vấn (OWL 2 QL) |
| Hiệu năng truy vấn | Nhanh, ổn định | Phụ thuộc SQL sinh ra |
| Đối soát, kiểm toán | Dễ: có bảng, có snapshot | Khó hơn |
| Hợp với | Quy mô đồ án, cần model đọc bảng | Nguồn thay đổi liên tục, không được sao chép |

FinGraph chọn vật chất hoá vì cùng chỗ lưu, cùng phân quyền Lake Formation, và model/agent đọc thẳng bảng. Đây là
đánh đổi có chủ ý, so sánh được với Context Ontology Accelerator [31] khi viết luận văn.

---

## 12. Lưu trữ và truy vấn đồ thị

### 12.1 Ba lựa chọn lưu trữ

| Lựa chọn | Ví dụ | Duyệt đường đi | FinGraph |
|---|---|---|---|
| **Triple store (RDF)** | GraphDB, Stardog, Apache Jena, Neptune SPARQL | SPARQL property path | Không dùng |
| **Property graph DB** | Neo4j, Neptune openCypher/Gremlin, TigerGraph | Rất tốt | **Neptune**, cho vùng điều tra AML |
| **Bảng quan hệ / lakehouse** | Iceberg + Athena/Spark | Kém (join lặp) → tính sẵn `closure` | **Bản gốc** của KG |

Lakehouse [43] làm bản gốc vì rẻ, cùng quản trị với phần còn lại của nền tảng. Neptune làm **bản chiếu** cho câu hỏi
cần duyệt đường đi tuỳ ý — điều bảng không làm tốt.

### 12.2 Index-free adjacency

Neo4j quảng bá **index-free adjacency** [44]: mỗi node giữ con trỏ trực tiếp tới cạnh của nó, đi một bước có chi phí
hằng, không phụ thuộc kích thước đồ thị. Neptune **không** làm vậy: nó lưu quad và tra qua các chỉ mục
SPOG/POGS/GPSO [26] — mỗi bước là một lần tra chỉ mục. Hệ quả thực tế và các điểm khác cách hiểu thông thường về
Neptune được ghi ở [NEPTUNE-GUIDE.md](NEPTUNE-GUIDE.md) mục 1.3 và 3.

### 12.3 Ngôn ngữ truy vấn

| Ngôn ngữ | Mô hình | Kiểu | Ví dụ "tài khoản của khách" |
|---|---|---|---|
| **SPARQL 1.1** [13] | RDF | Khai báo, khớp mẫu bộ ba | `SELECT ?a WHERE { ?c fin:holdsAccount ?a }` |
| **openCypher** [14] | Property graph | Khai báo, khớp mẫu ASCII-art | `MATCH (c:Customer)-[:holdsAccount]->(a) RETURN a` |
| **Gremlin** [15] | Property graph | Mệnh lệnh, duyệt từng bước | `g.V(uid).out('holdsAccount')` |
| **GQL** (ISO/IEC 39075:2024) [12] | Property graph | Chuẩn ISO đầu tiên cho truy vấn đồ thị, gần Cypher | — |
| **SQL** trên bảng `triple` | Quan hệ | Join | `SELECT object_uid FROM triple WHERE subject_uid = ? AND predicate_iri = 'fin:holdsAccount'` |

FinGraph dùng **SQL** (Athena, tool `query`/`neighborhood`) và **openCypher** (Neptune, tool AML). Lưu ý openCypher
trên Neptune không có `shortestPath` / `all()` — tool `explain_link` tìm đường bằng đào sâu dần
([NEPTUNE-GUIDE.md](NEPTUNE-GUIDE.md)).

---

## 13. Knowledge graph cho machine learning và LLM

### 13.1 KG cho machine learning

| Cách | Ý chính | FinGraph |
|---|---|---|
| **Đặc trưng thủ công từ đồ thị** | Bậc node, số láng giềng loại X, tỉ lệ trong vùng… | Đặc trưng ở ONTOLOGY.md 6.3 cho UC2; `product_transition` |
| **KG embedding** (TransE [45] và các biến thể) | Học vector cho node/quan hệ sao cho `h + r ≈ t` | Chưa làm; ứng viên cho bước sau |
| **Graph Neural Network** (GraphSAGE [46], GCN, GAT) | Tổng hợp thông tin láng giềng để dự đoán node/cạnh | Chưa làm; IBM AML [47] là bộ dữ liệu benchmark GNN cho phát hiện rửa tiền |

Giá trị chính của ontology đối với model trong dự án: **một định nghĩa sản phẩm dùng chung** cho mọi nguồn, nên
huấn luyện trên Santander và đánh giá trên Berka ở mức lớp `CreditCard` là có nghĩa.

### 13.2 KG và LLM

Pan và cộng sự [48] chia ba hướng kết hợp: KG giúp LLM, LLM giúp KG, và hai bên hiệp đồng. Các khái niệm liên quan:

| Khái niệm | Là gì | FinGraph |
|---|---|---|
| **Hallucination** [49] | LLM sinh nội dung trôi chảy nhưng không có căn cứ | Vấn đề chính cần chặn |
| **RAG** [50] | Truy xuất tài liệu liên quan rồi đưa vào ngữ cảnh khi sinh | Không dùng RAG văn bản cho dữ liệu nghiệp vụ |
| **GraphRAG** [51] | Dựng đồ thị (thường do LLM trích tự động) từ văn bản, truy xuất theo cộng đồng/láng giềng | Chỉ phù hợp cho phần tài liệu chính sách — đồ thị trích tự động không bám ontology riêng (ONTOLOGY.md 9.5) |
| **KG-grounded agent** | LLM gọi tool truy vấn KG có cấu trúc, câu trả lời phải dựa vào kết quả tool | **Cách FinGraph làm** |
| **LLM hỗ trợ xây ontology** | LLM đề xuất lớp/ánh xạ, người duyệt | Chỉ ở vai gợi ý (ONTOLOGY.md 7.3) |

### 13.3 Grounding trong FinGraph

Ý tưởng từ MUFG [30]: LLM tự do sẽ bịa bước không có trong nguồn; ràng buộc nó vào subgraph lấy từ đồ thị và bắt
trích node/cạnh thì đầu ra mới dùng được. FinGraph cụ thể hoá thành:

1. Tool trả về kèm `uid` / `triple_id` (nhờ id ổn định, mục 7.1).
2. Câu trả lời phải trích các id đó.
3. Một bước kiểm **không dùng LLM** đối chiếu mọi id trong câu trả lời với tập id đã thấy trong phiên; id lạ ⇒ chặn.
4. Ontology đi vào system prompt dưới dạng **semantic catalog** sinh tự động từ TBox, nên agent biết "thẻ tín dụng"
   là lớp nào mà không cần đoán cột.

Chi tiết ở [AI-AGENT.md](AI-AGENT.md) mục 8.

---

## 14. So sánh với các ontology / KG khác

### 14.1 Các KG / ontology tiêu biểu

| Hệ thống | Loại | Quy mô / đặc điểm | Mô hình | Khác FinGraph ở đâu |
|---|---|---|---|---|
| **Google Knowledge Graph** [4] | KG doanh nghiệp, đóng | Hàng tỉ sự kiện; phục vụ tìm kiếm | Nội bộ | Mục tiêu tìm kiếm thực thể công khai; không công bố ontology |
| **Wikidata** [32] | KG mở, cộng đồng biên tập | Hơn 100 triệu item | RDF + qualifier, SPARQL | Thông tin trên cạnh dùng *qualifier*; FinGraph dùng cột bảng |
| **DBpedia** [52] | KG trích từ Wikipedia | Ontology DBpedia ~ vài trăm lớp | RDF | Dựng bằng trích tự động từ infobox |
| **YAGO** [53] | KG trích từ Wikipedia + WordNet | Nổi tiếng độ chính xác cao, có thời gian/không gian | RDF | Cũng gắn thời gian vào sự kiện, giống ý bitemporal |
| **Schema.org** [54] | Từ vựng chung cho web | ~800 kiểu | RDFS nhẹ | Ưu tiên rộng và đơn giản, không suy diễn |
| **FIBO** [55] | Ontology miền tài chính (EDM Council) | Hàng nghìn lớp, OWL 2 DL | OWL | FinGraph **căn chỉnh** 16 lớp chính, không nhập toàn bộ |
| **SNOMED CT** / **Gene Ontology** [56] | Ontology y sinh | SNOMED ~350.000 khái niệm (OWL 2 EL); GO ~40.000 thuật ngữ | OWL / OBO | Ví dụ ontology khổng lồ cần reasoner đặc thù |
| **Palantir Foundry Ontology** [57] | "Ontology vận hành" trong sản phẩm | Object, link, action trên dữ liệu doanh nghiệp | Riêng | Ontology gắn cả *hành động* nghiệp vụ, không phải chuẩn W3C |
| **MUFG Bank** [30] | Ontology quy trình ngân hàng + agent | Ontology chung, instance graph mỗi chi nhánh, HermiT, Neptune, Bedrock | RDF/OWL | Miền khác (quy trình/flowchart). Cùng nguyên tắc grounding và suy diễn ngoài LLM |
| **Context Ontology Accelerator** [31] | Mã nguồn mở của AWS | Scan → Model → Serve; Ontop ảo hoá; MCP | RDF/OWL | Ảo hoá thay vì vật chất hoá (mục 11.2) |
| **Bedrock KB GraphRAG** [58] | Dịch vụ AWS | Neptune Analytics + vector, trích thực thể tự động | Property graph | Không bám ontology riêng; hợp cho tài liệu, không cho dữ liệu nghiệp vụ |

### 14.2 Vị trí của FinGraph

| Chiều | FinGraph |
|---|---|
| Phạm vi | Ontology **miền hẹp** (ngân hàng bán lẻ + AML), không phải KG mở |
| Cách dựng | **Ánh xạ khai báo** từ dữ liệu có cấu trúc, không trích từ văn bản |
| Biểu diễn | OWL/SKOS/SHACL cho nghĩa; bảng triple Iceberg làm gốc; property graph Neptune làm bản chiếu |
| Suy diễn | OWL 2 RL-like, **vật chất hoá** bằng Spark |
| Chất lượng | SHACL + đối soát số dòng + test reasoner |
| Dùng cho | SQL xuyên nguồn, đặc trưng model, agent có kiểm trích dẫn |

Đóng góp có thể nêu trong luận văn: kết hợp **chuẩn ngữ nghĩa W3C** với **lakehouse** (không cần triple store),
**id tất định** làm nền cho trích dẫn kiểm được, và **entity resolution có độ tin cậy** thay vì `owl:sameAs`.

---

## 15. Tài liệu tham khảo

Nền tảng ontology và KG
- [1] T. R. Gruber. *A translation approach to portable ontology specifications.* Knowledge Acquisition 5(2):199–220, 1993.
- [2] R. Studer, V. R. Benjamins, D. Fensel. *Knowledge engineering: Principles and methods.* Data & Knowledge Engineering 25(1–2):161–197, 1998.
- [3] N. Guarino, D. Oberle, S. Staab. *What is an ontology?* In Handbook on Ontologies, 2nd ed., Springer, 2009, pp. 1–17.
- [4] A. Singhal. *Introducing the Knowledge Graph: things, not strings.* Google Official Blog, 16/05/2012.
- [5] L. Ehrlinger, W. Wöß. *Towards a definition of knowledge graphs.* SEMANTiCS 2016 (Posters & Demos), CEUR-WS 1695.
- [6] A. Hogan et al. *Knowledge Graphs.* ACM Computing Surveys 54(4), Article 71, 2021. arXiv:2003.02320.
- [7] O. Lassila, D. McGuinness. *The role of frame-based representation on the Semantic Web.* Stanford KSL-01-02, 2001.
- [8] R. Angles, C. Gutierrez. *Survey of graph database models.* ACM Computing Surveys 40(1), 2008.

Chuẩn W3C / ISO
- [9] W3C. *RDF 1.1 Concepts and Abstract Syntax.* Recommendation, 2014. https://www.w3.org/TR/rdf11-concepts/
- [10] W3C. *RDF 1.1 Turtle.* Recommendation, 2014. https://www.w3.org/TR/turtle/
- [11] O. Hartig. *Foundations of RDF\* and SPARQL\*.* AMW 2017; W3C RDF 1.2 Concepts (Working Draft). https://www.w3.org/TR/rdf12-concepts/
- [12] ISO/IEC 39075:2024. *Information technology — Database languages — GQL.*
- [13] W3C. *SPARQL 1.1 Query Language.* Recommendation, 2013. https://www.w3.org/TR/sparql11-query/
- [14] N. Francis et al. *Cypher: An evolving query language for property graphs.* SIGMOD 2018; openCypher: https://opencypher.org/
- [15] M. A. Rodriguez. *The Gremlin graph traversal machine and language.* DBPL 2015; Apache TinkerPop: https://tinkerpop.apache.org/
- [16] W3C. *RDF Schema 1.1.* Recommendation, 2014. https://www.w3.org/TR/rdf-schema/
- [17] W3C. *OWL 2 Web Ontology Language Document Overview (2nd ed.).* Recommendation, 2012. https://www.w3.org/TR/owl2-overview/
- [18] W3C. *OWL 2 New Features and Rationale* (mục punning). 2012. https://www.w3.org/TR/owl2-new-features/
- [21] W3C. *OWL 2 Web Ontology Language Profiles (2nd ed.).* 2012. https://www.w3.org/TR/owl2-profiles/
- [23] W3C. *Shapes Constraint Language (SHACL).* Recommendation, 2017. https://www.w3.org/TR/shacl/
- [24] W3C. *SKOS Simple Knowledge Organization System Reference.* Recommendation, 2009. https://www.w3.org/TR/skos-reference/
- [27] W3C. *PROV-O: The PROV Ontology.* Recommendation, 2013. https://www.w3.org/TR/prov-o/
- [40] W3C. *R2RML: RDB to RDF Mapping Language.* Recommendation, 2012. https://www.w3.org/TR/r2rml/

Logic, suy diễn, CSDL
- [19] F. Baader, D. Calvanese, D. McGuinness, D. Nardi, P. Patel-Schneider (eds.). *The Description Logic Handbook.* Cambridge University Press, 2003.
- [20] I. Horrocks, O. Kutz, U. Sattler. *The even more irresistible SROIQ.* KR 2006.
- [22] R. Reiter. *On closed world data bases.* In Logic and Data Bases, Plenum, 1978.
- [28] R. T. Snodgrass. *Developing Time-Oriented Database Applications in SQL.* Morgan Kaufmann, 1999.
- [29] S. Abiteboul, R. Hull, V. Vianu. *Foundations of Databases.* Addison-Wesley, 1995 (ch. 12–13: Datalog, semi-naive evaluation).
- [41] A. Poggi et al. *Linking data to ontologies.* Journal on Data Semantics X, LNCS 4900, 2008.
- [42] D. Calvanese et al. *Ontop: Answering SPARQL queries over relational databases.* Semantic Web 8(3):471–487, 2017.
- [43] M. Armbrust, A. Ghodsi, R. Xin, M. Zaharia. *Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics.* CIDR 2021.
- [44] I. Robinson, J. Webber, E. Eifrem. *Graph Databases*, 2nd ed. O'Reilly, 2015.

Hợp nhất thực thể
- [33] I. P. Fellegi, A. B. Sunter. *A theory for record linkage.* JASA 64(328):1183–1210, 1969.
- [34] P. Christen. *Data Matching.* Springer, 2012.
- [35] H. Halpin, P. Hayes, J. McCusker, D. McGuinness, H. Thompson. *When owl:sameAs isn't the same.* ISWC 2010.

Phương pháp xây ontology
- [36] N. F. Noy, D. L. McGuinness. *Ontology Development 101.* Stanford KSL-01-05, 2001.
- [37] M. Grüninger, M. S. Fox. *Methodology for the design and evaluation of ontologies.* IJCAI-95 Workshop on Basic Ontological Issues in Knowledge Sharing, 1995.
- [38] M. C. Suárez-Figueroa, A. Gómez-Pérez, E. Motta, A. Gangemi (eds.). *Ontology Engineering in a Networked World.* Springer, 2012.
- [39] R. Arp, B. Smith, A. D. Spear. *Building Ontologies with Basic Formal Ontology.* MIT Press, 2015.

ML, LLM
- [45] A. Bordes et al. *Translating embeddings for modeling multi-relational data.* NeurIPS 2013.
- [46] W. Hamilton, R. Ying, J. Leskovec. *Inductive representation learning on large graphs.* NeurIPS 2017.
- [47] E. Altman et al. *Realistic synthetic financial transactions for anti-money laundering models.* NeurIPS 2023 Datasets & Benchmarks.
- [48] S. Pan et al. *Unifying large language models and knowledge graphs: A roadmap.* IEEE TKDE 36(7), 2024.
- [49] Z. Ji et al. *Survey of hallucination in natural language generation.* ACM Computing Surveys 55(12), 2023.
- [50] P. Lewis et al. *Retrieval-augmented generation for knowledge-intensive NLP tasks.* NeurIPS 2020.
- [51] D. Edge et al. *From local to global: A Graph RAG approach to query-focused summarization.* arXiv:2404.16130, 2024.

Hệ thống, sản phẩm
- [25] C. Bizer, T. Heath, T. Berners-Lee. *Linked Data — the story so far.* IJSWIS 5(3), 2009.
- [26] AWS. *Amazon Neptune User Guide* — Neptune graph data model (quads, SPOG/POGS/GPSO indexes). https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-data-model.html
- [30] AWS Database Blog. *How MUFG Bank aims to cut flowchart work by up to 90% with ontology-grounded AI on Amazon Neptune and Amazon Bedrock.* https://aws.amazon.com/blogs/database/how-mufg-bank-aims-to-cut-flowchart-work-by-up-to-90-with-ontology-grounded-ai-on-amazon-neptune-and-amazon-bedrock/
- [31] AWS. *Context Ontology Accelerator* (Apache-2.0). https://github.com/aws/context-ontology-accelerator
- [32] D. Vrandečić, M. Krötzsch. *Wikidata: a free collaborative knowledgebase.* Communications of the ACM 57(10):78–85, 2014.
- [52] J. Lehmann et al. *DBpedia — A large-scale, multilingual knowledge base extracted from Wikipedia.* Semantic Web 6(2):167–195, 2015.
- [53] F. M. Suchanek, G. Kasneci, G. Weikum. *YAGO: a core of semantic knowledge.* WWW 2007.
- [54] R. V. Guha, D. Brickley, S. Macbeth. *Schema.org: Evolution of structured data on the web.* Communications of the ACM 59(2), 2016.
- [55] EDM Council. *FIBO — Financial Industry Business Ontology.* https://spec.edmcouncil.org/fibo/
- [56] M. Ashburner et al. *Gene Ontology: tool for the unification of biology.* Nature Genetics 25:25–29, 2000; SNOMED International: https://www.snomed.org/
- [57] Palantir. *Foundry Ontology — Overview* (tài liệu sản phẩm). https://www.palantir.com/docs/foundry/ontology/overview/
- [58] AWS. *Amazon Bedrock Knowledge Bases GraphRAG with Neptune Analytics.* https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-build-graphs-build.html

> Số liệu quy mô ở bảng 14.1 (Wikidata, SNOMED, Schema.org, GO) thay đổi theo thời gian; khi đưa vào luận văn nên
> lấy số mới nhất từ trang chính thức kèm ngày truy cập.

---

## Phụ lục A — bảng thuật ngữ

| Tiếng Anh | Tiếng Việt dùng trong dự án | Giải thích một dòng |
|---|---|---|
| Ontology | Ontology | Đặc tả hình thức, tường minh của các khái niệm dùng chung |
| Knowledge graph (KG) | Đồ thị tri thức | Dữ liệu dạng node–cạnh theo ontology |
| TBox / ABox | Phần khái niệm / phần cá thể | Luật chung / dữ liệu cụ thể |
| Class, instance | Lớp, cá thể | `fin:Customer`, "Khách 2" |
| Object / datatype property | Quan hệ / thuộc tính giá trị | Nối hai node / nối node với giá trị |
| Axiom | Tiên đề | Câu logic: `Loan ⊑ CreditProduct` |
| Taxonomy | Cây phân loại | Cây lớp cha–con |
| IRI | Định danh toàn cục | URL dùng làm tên |
| Triple / quad | Bộ ba / bộ bốn | Chủ–vị–tân (+ đồ thị) |
| Reification | Vật hoá cạnh | Biến cạnh thành node để gắn thông tin |
| Labeled property graph | Đồ thị thuộc tính có nhãn | Mô hình Neo4j / Neptune openCypher |
| Reasoner | Bộ suy diễn | HermiT, ELK, Pellet |
| Consistency / classification | Nhất quán / phân loại | Không mâu thuẫn / tính cây lớp đầy đủ |
| Open / closed world assumption | Giả định thế giới mở / đóng | Không biết ≠ sai / không có = sai |
| Unique name assumption | Giả định tên duy nhất | Hai tên khác nhau là hai vật khác nhau (OWL không giả định) |
| Shape (SHACL) | Hình dạng ràng buộc | Luật kiểm dữ liệu đồ thị |
| Materialization | Vật chất hoá | Tính sẵn kết quả suy diễn thành dữ liệu |
| Query rewriting / OBDA | Viết lại truy vấn / truy cập dữ liệu qua ontology | Mở rộng câu hỏi lúc chạy, không sao chép dữ liệu |
| Transitive closure | Bao đóng bắc cầu | Mọi cặp nối được qua chuỗi cạnh |
| Semi-naive evaluation | Đánh giá nửa ngây thơ | Mỗi vòng chỉ mở rộng từ cặp mới |
| Entity resolution / record linkage | Hợp nhất thực thể | Nhận ra hai bản ghi là cùng một vật |
| Blocking | Chặn (phân khối) | Chỉ so cặp có chung khoá thô, tránh so mọi cặp |
| Competency question | Câu hỏi năng lực | Câu hỏi ontology phải trả lời được |
| Alignment | Căn chỉnh | Nối khái niệm của mình với ontology ngoài |
| Provenance | Nguồn gốc dữ liệu | Dữ liệu từ đâu, do job nào |
| Valid / transaction time | Thời gian hiệu lực / ghi nhận | Đúng ngoài đời khi nào / hệ thống biết khi nào |
| Grounding | Bám căn cứ | Câu trả lời dựa vào và trích được dữ liệu thật |
| Hallucination | Bịa (ảo giác) | LLM sinh nội dung không có căn cứ |
| Semantic catalog | Danh mục ngữ nghĩa | Bản rút gọn ontology cho agent |

---

## Phụ lục B — câu hỏi tự kiểm tra (chuẩn bị bảo vệ)

Mỗi câu có gợi ý trả lời ngắn và mục để đọc lại.

1. **Ontology khác lược đồ CSDL ở đâu?** — Ontology mô tả nghĩa, dùng chung nhiều hệ thống, có suy luận; schema mô
   tả cách lưu của một hệ thống. (2.5)
2. **Vì sao không dùng thẳng graph database làm bản gốc?** — Cùng chỗ lưu và phân quyền với lakehouse, rẻ, model đọc
   bảng; Neptune chỉ làm bản chiếu cho duyệt đường đi AML. (11.2, 12.1)
3. **`rdfs:domain` có chặn dữ liệu sai không?** — Không, nó suy ra kiểu; chặn là việc của SHACL. (4.1, 5)
4. **Vì sao cần cả OWL lẫn SHACL?** — OWL thế giới mở để suy diễn; SHACL thế giới đóng để kiểm chất lượng. (5.1)
5. **Vì sao không dùng `owl:sameAs`?** — Gộp cứng, bắc cầu, không gỡ được; AML cần liên kết có độ tin cậy và người
   duyệt. (9.3)
6. **Có nên thêm `owl:disjointWith` giữa `Person` và `Organization` không?** — Có lợi: reasoner bắt được node gán cả
   hai lớp. Cần cân nhắc vì một số nguồn gán lớp mơ hồ; nếu thêm thì kiểm bằng SHACL trên dữ liệu thật trước.
7. **Suy diễn chạy ở đâu, vì sao không để LLM tự suy?** — Spark vật chất hoá; LLM suy bắc cầu dễ sai và không kiểm
   được. (8.1)
8. **Bao đóng bắc cầu dừng khi nào nếu đồ thị có chu trình?** — Khi vòng lặp không thêm cặp mới (left anti join),
   hoặc chạm `MAX_DEPTH`. (8.2)
9. **`product_transition` có phải suy diễn ontology không?** — Không, đó là thống kê dùng cây lớp của ontology; có
   rủi ro rò rỉ tương lai nên cắt mốc riêng từng nguồn. (8.3)
10. **Vì sao id là băm tất định?** — Chạy lại không đổi id, nên trích dẫn và kết quả cũ còn đúng; agent kiểm được id
    bịa. (7.1, 13.3)
11. **FinGraph đứng đâu so với FIBO?** — Căn chỉnh 16 lớp chính bằng `skos:closeMatch` / `broadMatch`, không nhập. (6, 14.1)
12. **Khác Context Ontology Accelerator ở đâu?** — Vật chất hoá thay vì ảo hoá qua Ontop. (11.2)
13. **Khác GraphRAG ở đâu?** — Đồ thị dựng bằng ánh xạ khai báo theo ontology, không trích tự động từ văn bản; mỗi
    cạnh có nguồn và trích dẫn được. (13.2)
14. **Không tìm thấy đường tới đối tượng cấm vận có nghĩa khách sạch không?** — Không; giả định thế giới mở và phạm
    vi dữ liệu đã nạp có hạn. (5.1)
