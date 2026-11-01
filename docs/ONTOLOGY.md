# Tầng Ontology và Knowledge Graph — thiết kế, xây dựng, phục vụ model và AI agent

> **Trạng thái (10-30, kiểm lại 10-31):** tài liệu **thiết kế**. Đã có trên hạ tầng: database `track_a_onto`
> (**rỗng**: bảng `triple` CHƯA được tạo; lược đồ 6 cột mới chỉ nằm trong job khung `jobs/glue/onto_closure.py`,
> job này đọc bảng đó nhưng chưa chạy bao giờ; mục 4.2 là thiết kế), các tầng nguồn
> nó dựa vào (`silver_berka`, `silver_corebank`, `track_a_gold.berka_*`, `track_a_pretrain.uc2_*`), cùng
> phân quyền persona + Lake Formation (`docs/` repo hạ tầng). Mọi thứ còn lại là việc cần làm, theo lộ trình ở mục 11.

## Mục lục

1. [Vì sao cần ontology trong hệ thống này](#1-vì-sao-cần-ontology-trong-hệ-thống-này)
2. [Nguyên tắc thiết kế](#2-nguyên-tắc-thiết-kế)
3. [TBox — lược đồ khái niệm](#3-tbox--lược-đồ-khái-niệm)
4. [ABox — knowledge graph thực thể](#4-abox--knowledge-graph-thực-thể)
5. [Ánh xạ từng nguồn vào đồ thị](#5-ánh-xạ-từng-nguồn-vào-đồ-thị)
6. [Tầng suy diễn tính sẵn](#6-tầng-suy-diễn-tính-sẵn)
7. [Pipeline xây dựng](#7-pipeline-xây-dựng)
8. [Phục vụ model (UC2 gợi ý sản phẩm)](#8-phục-vụ-model-uc2-gợi-ý-sản-phẩm)
9. [Knowledge graph cho AI agent](#9-knowledge-graph-cho-ai-agent)
10. [Lưu trữ, phục vụ và chi phí](#10-lưu-trữ-phục-vụ-và-chi-phí)
11. [Lộ trình và tiêu chí nghiệm thu](#11-lộ-trình-và-tiêu-chí-nghiệm-thu)
12. [Tham khảo](#12-tham-khảo)

---

## 1. Vì sao cần ontology trong hệ thống này

Lakehouse đang có 12 nhóm nguồn. Cùng một khái niệm nghiệp vụ mang 12 cái tên khác nhau:

| Khái niệm | Berka / corebank | Santander | MBD | Bank Marketing | IBM AML | OpenSanctions |
|---|---|---|---|---|---|---|
| Khách hàng | `client.client_id` | `ncodpers` | `client_id` (hash) | một dòng = một khách (không có ID) | chủ tài khoản | `entity` |
| Khách **giữ** sản phẩm | `card`, `loan`, giao dịch `SIPO`/`POJISTNE` | 24 cờ `ind_*_ult1` | `target_1..4` | `y`, `loan`, `housing` | | |
| Quan hệ giữa các bên | `disp` OWNER / DISPONENT | | | | chuyển tiền giữa tài khoản | sở hữu, liên đới |
| Vị trí | `district`, `region` | `cod_prov`, `nomprov` | geohash | | | quốc gia |

Thiếu một lớp nghĩa chung thì mỗi người tiêu thụ (model, BA, agent) phải tự hiểu lại 12 bộ bảng, và hiểu mỗi
người một kiểu. Tầng ontology giải quyết bằng hai thứ:

- **Ontology (TBox):** bộ từ vựng chung. Gồm các lớp (`Customer`, `CreditCard`), quan hệ (`holdsProduct`),
  và luật (`GoldCard ⊂ CreditCard ⊂ CreditProduct`). Nhỏ, ít đổi, nằm trong git, có review.
- **Knowledge graph (ABox):** các thực thể và quan hệ thật, đổ từ silver/gold theo bộ từ vựng đó, có thời gian
  hiệu lực, và mỗi cạnh chỉ ngược được về nguồn.

Ba nhóm người dùng hưởng lợi:

| Người dùng | Nhận được gì |
|---|---|
| **Model** (UC2 gợi ý sản phẩm, AML) | Đặc trưng đồ thị, taxonomy sản phẩm chung giữa 4 bộ dữ liệu, đường đi đồ thị làm **lý do** cho gợi ý |
| **AI agent** | Một bản đồ ngữ nghĩa để hiểu câu hỏi, công cụ truy vấn có ràng buộc, câu trả lời **trích được nguồn** (node/cạnh) thay vì tự bịa |
| **BA / DA** | Hỏi theo khái niệm nghiệp vụ ("khách giữ sản phẩm tín dụng") thay vì nhớ cột nào ở bảng nào |

## 2. Nguyên tắc thiết kế

Các nguyên tắc dưới đây rút từ yêu cầu của đồ án, và từ hai tham khảo thực tế: case study MUFG Bank trên Amazon
Neptune + Bedrock, và dự án mã nguồn mở Context Ontology Accelerator của AWS (mục 12).

1. **Tách TBox / ABox.** Ontology là bản thiết kế, knowledge graph là dữ liệu đổ theo bản thiết kế đó. MUFG làm
   đúng như vậy: một ontology quy trình chung của trụ sở, rồi mỗi chi nhánh là một instance graph ánh xạ vào
   ontology. Ở đây thì một ontology ngân hàng chung, và mỗi nguồn (Berka, Santander, MBD…) là một phần của
   instance graph.
2. **Ontology là hợp đồng, không phải chú thích.** File `.ttl` có kiểm tra tự động (SHACL trong pytest). Job xây
   đồ thị từ chối cạnh dùng predicate chưa khai báo. Thêm khái niệm thì phải qua review như code.
3. **Grounding: mọi câu trả lời của agent phải trích được.** Kết luận chính của MUFG là LLM tự do sinh ra bước
   không có trong nguồn, còn khi bị **ràng buộc vào subgraph lấy từ đồ thị** và mỗi bước phải trích node/cạnh
   nguồn thì hallucination giảm mạnh. Ở đây cũng vậy: tool của agent trả về kèm `uid`/`triple_id`, và câu trả lời
   phải dẫn các id đó.
4. **Suy diễn nằm ngoài LLM.** MUFG đặt bộ suy diễn OWL (HermiT) giữa đồ thị và agent. Ở đây phần suy diễn cần
   thiết (bao đóng lớp con, bao đóng sở hữu, chuyển tiếp sản phẩm) được **tính sẵn bằng Spark** thành bảng. LLM
   không tự suy luận bắc cầu; nó đọc kết quả đã tính. Có thể dùng thêm bộ suy diễn OWL offline để kiểm tính nhất
   quán của TBox (mục 3.6).
5. **Có thời gian (bitemporal).** Ngân hàng cần hỏi "tại thời điểm X khách giữ gì": để chống rò rỉ tương lai khi
   huấn luyện, và để kiểm toán. Mỗi cạnh có `valid_from`/`valid_to` (thời gian nghiệp vụ) và `recorded_at`
   (thời điểm hệ thống biết).
6. **Vật chất hoá trên lakehouse, ảo hoá khi cần.** Đồ thị gốc là bảng Iceberg trong `track_a_onto`, cùng một chỗ
   lưu, cùng phân quyền Lake Formation với phần còn lại. Accelerator của AWS chọn ảo hoá (SPARQL federation qua
   Ontop) để khỏi sao chép dữ liệu. Ở quy mô đồ án, vật chất hoá đơn giản hơn, rẻ hơn và kiểm chứng được.
7. **Phân quyền đi theo dữ liệu, không theo lời nhắc.** Agent chạy dưới role persona (DE / analyst / customer).
   Lake Formation lọc dòng ở tầng dữ liệu, nên prompt injection không vượt được.
8. **Con người duyệt.** MUFG nhấn mạnh chuyên gia nghiệp vụ chuyển từ người *viết* sang người *duyệt*. Ở đây,
   ánh xạ nguồn → ontology và các luật suy diễn đều do người duyệt. LLM chỉ được dùng để **gợi ý** ánh xạ (mục 7.3).

## 3. TBox — lược đồ khái niệm

### 3.1 Namespace

| Tiền tố | IRI | Dùng cho |
|---|---|---|
| `fin:` | `https://fingraph.hust.edu.vn/ontology#` | Lớp và quan hệ của dự án (đã dùng trong `onto_closure.py`: `fin:isDirectlyOwnedBy`) |
| `fid:` | `https://fingraph.hust.edu.vn/id/` | IRI thực thể: `fid:<uid>` |
| `fibo-*` | `https://spec.edmcouncil.org/fibo/ontology/...` | Chỉ để **căn chỉnh** (`rdfs:subClassOf` / `skos:closeMatch`), không nhập toàn bộ FIBO |
| `skos:`, `rdfs:`, `owl:`, `xsd:`, `prov:` | chuẩn W3C | Nhãn, phân cấp, kiểu, nguồn gốc |

Vì sao không nhập nguyên FIBO: FIBO có hàng nghìn lớp, phần lớn không dùng tới, và làm việc suy diễn chậm đi. Mình
chỉ lấy khoảng 40 lớp cần thiết, mỗi lớp ghi IRI FIBO tương ứng. Như vậy vẫn căn chỉnh được với chuẩn ngành mà
lược đồ đủ nhỏ để đọc và review trong một buổi.

### 3.2 Lớp chính

| Lớp `fin:` | Cha | Căn chỉnh FIBO | Ý nghĩa |
|---|---|---|---|
| `Party` | | `fibo-fnd-pty-pty:Party` | Bên tham gia: người hoặc tổ chức |
| `Customer` | `Party` | `fibo-fbc-pas-fpas:Customer` (≈) | Bên có quan hệ với ngân hàng |
| `Person`, `Organization` | `Party` | `fibo-fnd-aap-ppl:Person`, `fibo-be-le-lp:LegalPerson` | |
| `SanctionedEntity` | `Party` | | Thực thể trong danh sách cấm vận (OpenSanctions) |
| `Account` | | `fibo-fbc-pas-caa:Account` | Tài khoản tiền gửi |
| `Product` | | `fibo-fbc-pas-fpas:FinancialProduct` | Gốc của taxonomy sản phẩm (3.3) |
| `ProductHolding` | | | **Sự kiện** khách giữ một sản phẩm, có thời gian (n-ary, mục 3.4) |
| `Transaction` | | `fibo-fbc-pas-fpas:Transaction` | Chỉ dùng ở mức tổng hợp (3.4) |
| `Location` → `Country`, `Region`, `District`, `GeoCell` | | `fibo-fnd-plc-loc:Location` | Berka district/region, Santander tỉnh, MBD geohash |
| `Dataset`, `SourceSystem` | | `prov:Entity` | Nguồn gốc: cạnh nào đến từ đâu |
| `ModelScore`, `Recommendation` | | | Kết quả model ghi ngược vào đồ thị (mục 8.4) |

### 3.3 Taxonomy sản phẩm: trái tim của UC2

Một cây sản phẩm chung để bốn bộ dữ liệu nói cùng một ngôn ngữ:

```
fin:Product
├── fin:DepositProduct
│   ├── fin:CurrentAccount        Santander ind_cco, ind_ctop, ind_ctpp, ind_ctma · Berka account
│   ├── fin:PayrollAccount        Santander ind_cno
│   ├── fin:JuniorAccount         Santander ind_ctju
│   ├── fin:SavingsAccount        Santander ind_ahor, ind_viv
│   ├── fin:EAccount              Santander ind_ecue
│   └── fin:TermDeposit           Bank Marketing y
│       ├── fin:ShortTermDeposit  Santander ind_deco
│       ├── fin:MediumTermDeposit Santander ind_deme
│       └── fin:LongTermDeposit   Santander ind_dela
├── fin:CreditProduct
│   ├── fin:Loan                  Berka loan · Santander ind_pres · Bank Marketing loan (đặc trưng)
│   │   └── fin:Mortgage          Santander ind_hip · Bank Marketing housing (đặc trưng)
│   ├── fin:CreditCard            Santander ind_tjcr
│   │   ├── fin:ClassicCard       Berka card_type=classic
│   │   ├── fin:JuniorCard        Berka card_type=junior
│   │   └── fin:GoldCard          Berka card_type=gold
│   └── fin:Guarantee             Santander ind_aval
├── fin:InvestmentProduct
│   ├── fin:Fund                  Santander ind_fond
│   ├── fin:Securities            Santander ind_valo
│   ├── fin:Derivative            Santander ind_cder
│   └── fin:PensionPlan           Santander ind_plan
├── fin:InsuranceProduct          Berka k_symbol=POJISTNE
└── fin:PaymentService
    ├── fin:RecurringPayment      Berka k_symbol=SIPO · Santander ind_recibo
    ├── fin:PayrollService        Santander ind_nomina
    ├── fin:PensionReceipt        Santander ind_nom_pens · Berka k_symbol=DUCHOD (đặc trưng)
    └── fin:TaxPayment            Santander ind_reca
fin:AnonymousProduct              MBD target_1..4 (bộ dữ liệu ẩn danh tên sản phẩm)
```

Ghi chú:
- **Santander có 24 cột sản phẩm**, không phải 22 như một số mô tả. Hai cột `ind_ahor` và `ind_aval` gần như
  không bao giờ được thêm mới, nên nhiều lời giải Kaggle bỏ chúng và chỉ còn 22 nhãn.
- Sản phẩm MBD bị ẩn danh: chỉ có `target_1..4`. Chúng là lớp con của `fin:AnonymousProduct`, **không** gắn vào
  cây trên. Model vẫn học được trên MBD, nhưng không chuyển giao theo lớp sản phẩm với các nguồn khác được.
- Mỗi lá giữ `skos:notation` = mã gốc (`ind_tjcr_fin_ult1`, `classic`) và `fin:sourceDataset`. Từ một mã nguồn
  tìm ra lớp chung bằng đúng một phép tra, không cần bảng ánh xạ riêng.

### 3.4 Quan hệ (properties)

| Quan hệ | Miền → Đích | Kiểu | Thời gian | Ý nghĩa / nguồn |
|---|---|---|---|---|
| `fin:holdsAccount` | `Customer` → `Account` | object | có | Berka `disp` OWNER |
| `fin:isAuthorizedOn` | `Customer` → `Account` | object | có | Berka `disp` DISPONENT (người được uỷ quyền) |
| `fin:hasHolding` | `Customer` → `ProductHolding` | object | | |
| `fin:ofProduct` | `ProductHolding` → `Product` (lớp) | object | | |
| `fin:viaAccount` | `ProductHolding` → `Account` | object | | Khoản vay/bảo hiểm gắn tài khoản nào |
| `fin:locatedIn` | `Party` / `Account` → `Location` | object | có | Berka district, Santander tỉnh |
| `fin:partOf` | `Location` → `Location` | object, **bắc cầu** | | District ⊂ Region ⊂ Country |
| `fin:transfersTo` | `Account` → `Account` | object, **tổng hợp** | theo tháng | Giao dịch chuyển khoản gộp theo (from, to, tháng); `weight` = tổng tiền |
| `fin:isDirectlyOwnedBy` | `Party` → `Party` | object | có | Sở hữu pháp nhân (OpenSanctions, IBM AML). Đã dùng trong `onto_closure` |
| `fin:sameAs` | `Party` → `Party` | object, **đối xứng** | | Cùng một thực thể ở hai nguồn (mục 4.4) |
| `fin:frequentlyFollowedBy` | `Product` → `Product` | object, **suy diễn** | | Từ `product_transition` (mục 6.2) |
| `fin:age`, `fin:gender`, `fin:segment`, `fin:income` | `Person` → literal | datatype | có | Thuộc tính hồ sơ |
| `fin:balanceEndOfMonth`, `fin:inflow3m` … | `Customer` → literal | datatype | theo tháng | Chỉ trỏ tới gold, **không** đổ vào triple (mục 4.5) |

**Vì sao `ProductHolding` là một node (n-ary) chứ không là một cạnh thẳng `Customer → Product`?** Vì việc giữ một
sản phẩm có thuộc tính riêng: ngày bắt đầu, ngày kết thúc, qua tài khoản nào, số tiền, trạng thái (khoản vay
`A`/`B`/`C`/`D` của Berka). RDF không gắn thuộc tính lên cạnh được, trừ khi dùng RDF-star. Mẫu n-ary là cách chuẩn
và tương thích mọi công cụ. Bảng `triple` vẫn có cột `weight`, `valid_from`, `valid_to` cho các cạnh chỉ cần
chừng đó thông tin.

### 3.5 Tổ chức file

```
ontology/
├── fingraph.ttl              TBox: lớp, quan hệ, taxonomy sản phẩm (OWL 2 DL, Turtle)
├── alignment/fibo.ttl        rdfs:subClassOf / skos:closeMatch sang FIBO
├── mappings/                 nguồn -> ontology, dạng YAML (mục 5), người duyệt
│   ├── berka.yml
│   ├── santander_products.yml
│   └── ...
├── shapes/fingraph.shacl.ttl SHACL: ràng buộc dữ liệu (mục 3.6)
└── CHANGELOG.md              phiên bản ontology (semver) — ghi vào cột ontology_version
```

### 3.6 Kiểm tra (chạy trong pytest, không tốn AWS)

| Kiểm tra | Công cụ | Bắt lỗi gì |
|---|---|---|
| Cú pháp + OWL 2 DL | `rdflib` + `owlready2` (HermiT, offline) | Lớp tự mâu thuẫn, vòng lặp `subClassOf` |
| Taxonomy đầy đủ | pytest | Mã sản phẩm trong mapping chưa có lớp tương ứng; lá thiếu `skos:notation` |
| Dữ liệu mẫu đúng hình | `pyshacl` trên đồ thị mẫu nhỏ | `ProductHolding` thiếu `ofProduct`, `valid_to < valid_from`, `holdsAccount` trỏ vào thứ không phải `Account` |
| Mapping ↔ silver | pytest + DDL silver | Cột mapping tham chiếu không có trong entry silver |

`owlready2` là LGPL-3.0, cũng đúng thư viện Accelerator của AWS dùng. Nó chỉ chạy trong pytest/CI, không đóng
gói vào Glue.

## 4. ABox — knowledge graph thực thể

### 4.1 Bảng

Tất cả nằm trong database `track_a_onto` (Iceberg, `s3://.../track-a/onto/`).

| Bảng | Khoá | Mô tả |
|---|---|---|
| `class`, `property`, `subclass` | `iri` | TBox nạp từ `.ttl`, để Spark/Athena join được |
| `entity` | `uid` | Mỗi node một dòng: lớp, nhãn, nguồn, khoá gốc, thuộc tính tĩnh |
| `triple` | `triple_id` | Mỗi cạnh một dòng. **Mở rộng** lược đồ đang có (4.2) |
| `same_as` | `(uid_a, uid_b)` | Cặp thực thể trùng nhau giữa các nguồn, kèm độ tin cậy |
| `closure` | `(start_uid, node_uid, predicate)` | Bao đóng bắc cầu tính sẵn (mục 6.1) |
| `product_transition` | `(from_product, to_product, horizon)` | Xác suất mua tiếp (mục 6.2) |
| `node_embedding` | `(uid, model_version)` | Vector node (mục 6.4) |

### 4.2 Lược đồ

```sql
-- Mo rong bang triple DANG CO (giu nguyen 6 cot cu de onto_closure.py khong phai sua)
CREATE TABLE glue_catalog.track_a_onto.triple (
  subject_uid      string,     -- cot cu
  predicate_iri    string,     -- cot cu, vd 'fin:holdsAccount'
  object_uid       string,     -- cot cu; NULL neu doi tuong la literal
  weight           double,     -- cot cu: ty le so huu, tong tien chuyen, xac suat
  valid_from       date,       -- cot cu: thoi gian nghiep vu
  valid_to         date,       -- cot cu: NULL = con hieu luc
  -- them moi:
  triple_id        string,     -- sha256(subject|predicate|object|valid_from|source) — de agent TRICH DAN
  object_literal   string,     -- doi tuong la gia tri (tuoi, gioi tinh)
  object_datatype  string,     -- 'xsd:int', 'xsd:date'...
  source_dataset   string,     -- 'silver_berka.disp'
  source_key       string,     -- khoa dong goc, vd 'disp_id=1'
  confidence       double,     -- 1.0 cho canh lay thang; < 1 cho same_as / suy dien
  is_inferred      boolean,    -- true = sinh boi luat, khong lay tu nguon
  ontology_version string,     -- '1.0.0'
  recorded_at      timestamp,  -- thoi diem he thong biet (truc thu hai cua bitemporal)
  run_id           string
) USING iceberg
PARTITIONED BY (predicate_iri, bucket(16, subject_uid));

CREATE TABLE glue_catalog.track_a_onto.entity (
  uid              string,     -- sha256(class | source | natural_key)[:32]
  class_iri        string,     -- 'fin:Customer'
  label            string,     -- nhan cho nguoi doc / cho LLM
  source_dataset   string,
  natural_key      string,     -- 'client_id=2'
  attributes       map<string,string>,   -- thuoc tinh tinh, nho
  valid_from       date,
  valid_to         date,
  ontology_version string,
  recorded_at      timestamp
) USING iceberg PARTITIONED BY (class_iri);
```

**Phân vùng `triple` theo `predicate_iri`** vì gần như mọi truy vấn đều lọc theo loại quan hệ trước ("mọi cạnh
`holdsAccount` của khách X"). `bucket(subject_uid)` thì để join theo node không phải quét cả bảng.

### 4.3 Định danh `uid`

```
uid = sha256(f"{class_iri}|{source_system}|{natural_key}")[:32]
vd   fin:Customer|berka|client_id=2   ->  9f2c...
```

- **Ổn định:** cùng dữ liệu cho cùng uid, chạy lại job không sinh node mới, và join giữa các lần chạy được.
- **Không lộ khoá gốc:** agent và log thấy uid, không thấy `client_id`. Ánh xạ ngược nằm ở `entity.natural_key`,
  và Lake Formation chặn được cột này theo persona.
- Hai nguồn cùng một người thì là **hai uid** + một dòng `same_as`. Không gộp cứng, vì việc gộp có thể sai.

### 4.4 Thực thể trùng giữa các nguồn (`same_as`)

| Cặp | Cách khớp | `confidence` |
|---|---|---|
| corebank `client` ↔ Berka `client` | Cùng `client_id` gốc (corebank được nạp từ Berka) | 1.0 |
| Khách ↔ OpenSanctions | Khớp mờ tên + ngày sinh + quốc gia | 0.6–0.95, **người duyệt** khi < 0.9 |
| IBM AML account ↔ Berka account | Không có khoá chung | **không nối** |

Truy vấn mặc định chỉ đi qua `same_as` có `confidence >= 0.9`. Agent AML khi trình bày một liên kết có
`confidence` thấp **bắt buộc** phải nói rõ "khả năng trùng khớp X%".

### 4.5 Cái gì KHÔNG đưa vào đồ thị

- **Từng giao dịch.** Berka có 1 triệu, MBD hàng chục triệu, Santander 13,6 triệu dòng khách × tháng. Đồ thị chỉ giữ
  cạnh **tổng hợp** (`transfersTo` theo tháng). Chi tiết vẫn ở silver, và `entity` trỏ sang đó.
- **Đặc trưng số theo tháng** (số dư, dòng tiền): đã có trong `track_a_gold.berka_client_month`. Đồ thị chỉ khai
  báo **thuộc tính đó tồn tại và nằm ở bảng nào** (`fin:measuredIn`), không sao chép giá trị. Agent và model đọc
  thẳng từ gold.

Nguyên tắc: **đồ thị lưu cấu trúc và nghĩa, lakehouse lưu khối lượng.**

## 5. Ánh xạ từng nguồn vào đồ thị

Mapping viết dạng khai báo, người duyệt (`ontology/mappings/*.yml`). Job đồ thị đọc mapping để sinh `entity`/`triple`.

### 5.1 Berka / corebank (silver_berka, silver_corebank)

| Nguồn | → Node / cạnh |
|---|---|
| `client` | `fin:Person` + `fin:Customer` (nhãn: `"Khách {client_id}"`), `fin:gender`, `fin:age` (theo `birth_date`) |
| `account` | `fin:Account`; `fin:locatedIn` district; `valid_from` = `opened_date` |
| `disp` OWNER | `fin:holdsAccount` (khách → tài khoản) |
| `disp` DISPONENT | `fin:isAuthorizedOn` |
| `loan` | `fin:ProductHolding` `ofProduct fin:Loan`, `viaAccount`, `valid_from` = `granted_date`, `valid_to` = `maturity_date`, thuộc tính `loan_status`, `amount` |
| `card` | `fin:ProductHolding` `ofProduct fin:ClassicCard` / `JuniorCard` / `GoldCard`, `valid_from` = `issued_date` |
| `trans` `k_symbol = POJISTNE` / `SIPO` | `ProductHolding` `InsuranceProduct` / `RecurringPayment`, `valid_from` = tháng giao dịch đầu tiên. **Đúng định nghĩa ở `track_a_gold.berka_product_event`** để đồ thị và pre-train dùng chung một sự thật |
| `trans` chuyển khoản | `fin:transfersTo` gộp theo (from, to, tháng) |
| `district` | `fin:District` `partOf` `fin:Region` `partOf` `fin:Country` (CZ) |
| silver corebank `scd2` | `valid_from`/`valid_to` lấy thẳng từ lịch sử SCD2. Đây là nguồn duy nhất có lịch sử **thay đổi thật** |

### 5.2 Santander (sau khi có silver)

| Nguồn | → |
|---|---|
| `ncodpers` | `fin:Customer`; `segmento` → `fin:segment`; `renta` → `fin:income`; `cod_prov` → `fin:locatedIn` `fin:Region` (ES) |
| 24 cờ `ind_*` theo `fecha_dato` | Với mỗi cờ chuyển **0 → 1** giữa hai tháng: một `ProductHolding` `valid_from` = tháng đó. Chuyển 1 → 0 thì đóng `valid_to`. Đây chính là nhãn "thêm sản phẩm" của bài Kaggle |

### 5.3 MBD

| Nguồn | → |
|---|---|
| `client_id` | `fin:Customer` (`fin:Organization`, vì MBD là khách doanh nghiệp) |
| `targets.target_k = 1` tại `mon` | `ProductHolding` `ofProduct fin:AnonymousProduct_k` |
| `geo` geohash | `fin:GeoCell` + `fin:activeIn` (tổng hợp theo tháng) |
| `dialog` embedding | **Không** vào đồ thị. Giữ ở silver làm đặc trưng model |

### 5.4 Bank Marketing

Không có ID khách, nên mỗi dòng là một `fin:Customer` ẩn danh (uid từ `_src_file` + `_src_row`). Cạnh
`ProductHolding` `TermDeposit` khi `y = yes`. Giá trị chính ở đây là **taxonomy** (`TermDeposit` dùng chung với
Santander `ind_deco/deme/dela`), không phải mạng quan hệ.

### 5.5 IBM AML, OpenSanctions, OneDrive

- IBM AML: `fin:Account` + `fin:transfersTo` (gộp), nhãn rửa tiền thành cạnh `fin:flaggedAs`.
- OpenSanctions: `fin:SanctionedEntity`, `fin:isDirectlyOwnedBy`, và `same_as` tới khách (4.4).
- OneDrive (file người dùng upload): **chưa ánh xạ**. Lược đồ phụ thuộc file. Khi một loại file ổn định thì viết
  mapping riêng.

## 6. Tầng suy diễn tính sẵn

Mọi suy diễn agent và model cần đều **tính trước** thành bảng. LLM không bao giờ phải tự suy luận bắc cầu.

### 6.1 Bao đóng (`closure`)

| Loại | Luật | Dùng cho |
|---|---|---|
| Sở hữu | `isDirectlyOwnedBy` bắc cầu, nhân `weight` | AML: ai thực sự sở hữu ai, qua mấy tầng. **`jobs/glue/onto_closure.py` đã có** (left_anti chống chu trình, localCheckpoint, dừng sớm) |
| Lớp con | `subClassOf*` | Khách giữ `GoldCard` thì cũng giữ `CreditCard` và `CreditProduct`. Truy vấn theo lớp cha không cần đệ quy |
| Vị trí | `partOf*` | District → Region → Country |

Athena không đệ quy tin cậy được (`WITH RECURSIVE`), nên bảng bao đóng là **bắt buộc**, không phải tối ưu hoá.

### 6.2 Chuyển tiếp sản phẩm (`product_transition`)

Từ các `ProductHolding` có thời gian:

```
P(B | A, H) = #khách đang giữ A tại m và mua B trong (m, m+H]  /  #khách đang giữ A và chưa giữ B tại m
lift(B | A) = P(B | A, H) / P(B, H)
```

Tính **chỉ trên khoảng thời gian của tập train** (trước `VALID_START` của pre-train), không thì đặc trưng này rò rỉ
tương lai sang tập valid/test. Kết quả sinh cạnh suy diễn `fin:frequentlyFollowedBy` (`is_inferred = true`,
`weight` = lift) cho các cặp có lift > 1,5 và đủ số mẫu.

### 6.3 Đặc trưng đồ thị (cho pre-train)

| Đặc trưng | Từ | Point-in-time |
|---|---|---|
| `n_products_by_category` (credit / deposit / payment / insurance) | `ProductHolding` + closure lớp con | `valid_from <= m` |
| `max_transition_score`, `sum_transition_score` cho sản phẩm ứng viên | `product_transition` × sản phẩm đang giữ | bảng tính trên train |
| `region_adoption_rate(p)` | Tỉ lệ khách cùng region đang giữ `p` | tại `m` |
| `n_authorized_users`, `is_authorized_elsewhere` | `isAuthorizedOn` | tại `m` |
| `transfer_out_degree_3m`, `distinct_counterparties_3m` | `transfersTo` | 3 tháng tới `m` |

### 6.4 Embedding node

node2vec hoặc TransE trên đồ thị khách–sản phẩm–vùng–tài khoản, **chỉ dùng cạnh có `valid_from` < `VALID_START`**.
Đồ thị cỡ Berka (khoảng 10 nghìn node) chạy trong vài phút trên một Glue job hoặc trên máy. Ghi kết quả vào
`node_embedding` kèm `model_version`.

## 7. Pipeline xây dựng

### 7.1 Job

Theo đúng khuôn tầng gold/pre-train (`docs/TRANSFORMS.md`): PySpark tự viết, Glue job dùng chung, `--TRANSFORM` chọn logic.

```
silver_* + track_a_gold.berka_product_event
   │
   ▼  fingraph-dev-onto-spark  --TRANSFORM onto.load_schema     (.ttl -> class/property/subclass)
   ▼                           --TRANSFORM onto.build_graph     (mapping -> entity, triple)
   ▼                           --TRANSFORM onto.resolve         (same_as)
   ▼                           --TRANSFORM onto.derive          (closure, product_transition, canh suy dien)
   ▼                           --TRANSFORM onto.export_hot      (ho so 360 + top-k -> DynamoDB, muc 9.4)
track_a_onto.*
   │
   ▼  pretrain-spark (UC2) join dac trung do thi (muc 6.3)
```

Hạ tầng thêm: một job `onto-spark` trong `modules/transforms` (chỉ thêm một khoá vào `local.jobs`), grant LF
`ALL` trên `track_a_onto` cho role `xfm`, và `SELECT` cho DE/analyst. Đây là công việc nhỏ.

### 7.2 Bất biến và kiểm tra mỗi lần chạy

- **Idempotent:** `createOrReplace` như gold, Iceberg giữ snapshot cũ để so sánh hai phiên bản đồ thị.
- **Không cạnh mồ côi:** mọi `subject_uid`/`object_uid` phải có trong `entity`. Có thì job FAILED.
- **Predicate đã khai báo:** mọi `predicate_iri` phải có trong `property`, đúng hợp đồng ở nguyên tắc 2.
- **Đối soát số lượng:** số `holdsAccount` = số dòng `disp` OWNER, số `ProductHolding Loan` = số dòng `loan`. Ghi
  vào bảng `ops`.

### 7.3 LLM hỗ trợ xây ontology: có, nhưng chỉ ở vai gợi ý

MUFG dùng Bedrock để **suy ra** mô tả ontology từ tài liệu, rồi chuyên gia duyệt. Ở đây có thể làm tương tự cho phần
khó nhất là **mapping**: đưa cho LLM schema silver + từ điển cột nguồn (ví dụ 48 cột Santander) + TBox, để nó đề xuất
`mappings/santander_products.yml`. Người duyệt từng dòng rồi commit. Pytest kiểm (3.6) chặn mọi mapping trỏ tới lớp
không tồn tại. LLM **không bao giờ** ghi thẳng vào đồ thị.

## 8. Phục vụ model (UC2 gợi ý sản phẩm)

### 8.1 Đặc trưng đồ thị vào pre-train

Pre-train hiện tại (`transforms/pretrain/uc2_next_product.py`, 797.737 dòng, xem `docs/TRANSFORMS.md`) chỉ có đặc
trưng hồ sơ và hành vi. Bước tiếp theo là join thêm đặc trưng ở mục 6.3, rồi đánh giá **có và không có** đặc trưng
đồ thị trên cùng tập test thời gian:

| Chỉ số | Không đồ thị | Có đồ thị |
|---|---|---|
| AUC-PR theo từng sản phẩm | | |
| Recall@3, MAP@7 (chỉ số của cuộc thi Santander) theo khách | | |

Bảng này là kết quả thực nghiệm chính cho phần ontology của luận văn.

### 8.2 Một taxonomy, nhiều bộ dữ liệu

- **Huấn luyện chéo:** học trên Santander (13,6 triệu dòng) ở mức **lớp sản phẩm** (`CreditCard`, `Loan`,
  `TermDeposit`), rồi tinh chỉnh hoặc đánh giá trên Berka.
- **Bảng chỉ số thống nhất:** cùng một định nghĩa "mua sản phẩm lớp X trong H tháng" cho cả bốn nguồn.

### 8.3 Lý do cho gợi ý

Từ khách tới sản phẩm được gợi ý, lấy các đường đi ngắn qua `product_transition`, `region_adoption_rate` và sản
phẩm đang giữ, rồi chọn 2–3 đường có `weight` cao nhất. Ví dụ:

> Gợi ý **Loan** cho khách `9f2c…`. Lý do: (1) đang giữ `ClassicCard` từ 1996-03, mà lift(Loan | ClassicCard) =
> 2,4 [`triple:7a1e…`]; (2) 18% khách cùng vùng *Praha* đã vay [`closure:…`]; (3) dòng tiền vào 3 tháng tăng 35%
> [`gold.berka_client_month 1997-11`].

### 8.4 Ghi kết quả model ngược vào đồ thị

Điểm model ghi thành node `fin:Recommendation` (khách, sản phẩm, điểm, `model_version`, `scored_at`). Agent đọc
được cả gợi ý lẫn phiên bản model sinh ra gợi ý đó, và kiểm toán được "ai được gợi ý gì, khi nào, bởi model nào".

## 9. Knowledge graph cho AI agent

### 9.1 Vấn đề agent gặp nếu không có ontology

Hai cách làm ngây thơ, cả hai đều hỏng ở ngân hàng:

| Cách | Hỏng ở đâu |
|---|---|
| Đưa LLM đọc thẳng 12 bộ bảng (text-to-SQL trên schema thô) | Không biết `ind_tjcr_fin_ult1` là thẻ tín dụng, không biết khách giữ thẻ ở Berka phải đi qua `disp → card`. SQL chạy được mà trả lời sai, và không ai phát hiện |
| RAG trên tài liệu | Trả lời được câu "thẻ gold là gì", không trả lời được "khách này có gì, liên quan tới ai" |

Bài học từ MUFG: LLM tự do **bịa bước không có trong nguồn**. Khi bị ràng buộc vào subgraph lấy từ đồ thị, và mỗi
bước phải trích node/cạnh nguồn, đầu ra mới dùng được.

### 9.2 Kiến trúc

```
                       ┌────────────────── Semantic catalog (sinh tu TBox) ──────────────────┐
                       │ lop, quan he, mo ta tieng Viet, bang/cot vat chat hoa, vi du truy van │
                       └───────────────────────────────┬──────────────────────────────────────┘
                                                       │ nap vao system prompt (rut gon) / tool `describe`
 Nguoi dung ─► Agent (Claude tren Bedrock, Strands Agents hoac SDK) ─► MCP server "fingraph" ─► Lambda tool
                       ▲                                                                              │
                       │ cau tra loi + trich dan (uid / triple_id)                    chay DUOI role persona
                       │                                                                              ▼
                       └──────── kiem tra dau ra (9.6) ◄──── DynamoDB nong · Athena (track_a_onto, gold) · Lake Formation
```

- **MCP server** đóng gói tool để mọi client agent dùng chung: Claude Desktop/Code, Bedrock Agents, Strands Agents.
  Accelerator của AWS cũng phục vụ ngữ cảnh cho agent qua MCP.
- **Mỗi phiên agent assume đúng role persona** của người hỏi (DE / analyst / customer). Tool gọi Athena/DynamoDB
  bằng credential của role đó.

### 9.3 Semantic catalog: bản đồ cho agent

Sinh tự động từ `fingraph.ttl`, **không viết tay**, để nó không bao giờ lệch khỏi ontology:

```yaml
- class: fin:CreditCard
  label_vi: Thẻ tín dụng
  parents: [fin:CreditProduct, fin:Product]
  children: [fin:ClassicCard, fin:JuniorCard, fin:GoldCard]
  source_codes: {berka: "card_type in (classic,junior,gold)", santander: "ind_tjcr_fin_ult1"}
  query_hint: "Khach giu the: triple predicate fin:hasHolding -> ofProduct thuoc closure subclass cua fin:CreditCard"
  materialized_in: [track_a_onto.triple, track_a_gold.berka_product_event]
```

Agent nhận bản rút gọn (lớp + mô tả) trong system prompt, và gọi tool `describe(concept)` khi cần chi tiết. Nhờ vậy
prompt nhỏ mà agent vẫn "biết" toàn bộ lược đồ.

### 9.4 Tool

Nguyên tắc: **tool hẹp, có kiểu, trả về kèm trích dẫn.** SQL tự do chỉ là phương án cuối.

| Tool | Đầu vào | Trả về | Đọc từ |
|---|---|---|---|
| `describe(concept)` | tên/khái niệm | định nghĩa, lớp cha/con, mã nguồn, chỗ vật chất hoá | semantic catalog |
| `find_entity(text, class?)` | tên, mã | danh sách `{uid, class, label, source}` | `entity` |
| `customer_360(uid, as_of?)` | uid, ngày | hồ sơ + sản phẩm đang giữ + chỉ số hành vi tại `as_of` | DynamoDB (nóng) / gold + triple |
| `neighborhood(uid, predicates?, hops≤2, as_of?)` | uid | **subgraph** (node + cạnh + `triple_id`) | `triple`, `closure` |
| `explain_link(uid_a, uid_b, max_hops)` | 2 uid | các đường đi ngắn nhất, kèm `confidence` từng cạnh | `closure`, `triple`, `same_as` |
| `recommend(uid, k, as_of?)` | uid | top-k sản phẩm + điểm + **lý do** (8.3) | `Recommendation` + `product_transition` |
| `metric(name, filters)` | chỉ số đã định nghĩa | giá trị + câu SQL đã chạy | gold (định nghĩa chỉ số tập trung, như *metric service* của Accelerator) |
| `query(sql)` | SQL | dòng kết quả | Athena, **chỉ đọc**, dưới role persona, trần quét theo workgroup |

**Đường nóng:** `customer_360` và `recommend` là hai tool gọi nhiều nhất. Job `onto.export_hot` đẩy sẵn hồ sơ 360 +
top-k cho từng khách vào DynamoDB `fingraph-dev-hot` (`pk = CUST#<uid>`, có TTL), nên tool trả lời dưới 10 ms
thay vì vài giây chờ Athena.

### 9.5 Các kiểu truy xuất (retrieval)

| Kiểu | Khi nào | Cách |
|---|---|---|
| **Subgraph quanh thực thể** (kiểu MUFG) | Câu hỏi về một khách / một tài khoản | `find_entity` → `neighborhood(hops=2)` → LLM chỉ được dùng các node/cạnh trong subgraph này |
| **Truy vấn theo ontology** | Câu hỏi tổng hợp ("bao nhiêu khách giữ sản phẩm tín dụng ở Praha") | Catalog dịch khái niệm → bảng/cạnh → `metric` hoặc `query`. SQL sinh ra đi qua closure, không đoán join |
| **Đường đi** | "A liên quan B thế nào" | `explain_link` trên `closure` + `same_as` |
| **GraphRAG trên tài liệu** (giai đoạn sau) | Câu hỏi về chính sách, điều khoản sản phẩm | Chia tài liệu sản phẩm/chính sách thành đoạn, mỗi đoạn gắn cạnh `fin:describes` tới lớp sản phẩm. Truy xuất theo vector rồi mở rộng theo đồ thị. Có bản dựng sẵn: **Bedrock Knowledge Bases GraphRAG + Neptune Analytics** (GA 03/2025), nhưng việc trích thực thể của nó là tự động, không bám ontology riêng. Vì vậy chỉ hợp cho phần tài liệu, không thay được đồ thị nghiệp vụ |

### 9.6 Hợp đồng grounding và kiểm tra đầu ra

Agent được hướng dẫn, **và bị kiểm tra**, theo bốn luật:

1. Mọi khẳng định về một thực thể phải kèm ít nhất một `uid`/`triple_id` lấy được từ tool trong cùng phiên.
2. Không khẳng định quan hệ không có trong subgraph đã truy xuất. Thiếu thông tin thì nói thiếu.
3. Liên kết `same_as` có `confidence < 0.9` phải được nói rõ là "khả năng".
4. Số liệu phải đến từ `metric`/`query`, kèm câu SQL. Không tự tính nhẩm.

Một bước kiểm tra sau khi sinh câu trả lời (Lambda, không dùng LLM):
- Trích ra mọi id trong câu trả lời, rồi kiểm từng id có nằm trong kết quả tool của phiên không. Id không có thì
  đánh dấu **không có căn cứ**.
- Kiểm cạnh được nhắc tới có thoả SHACL không (ví dụ `holdsAccount` phải nối `Customer` với `Account`).
- Câu trả lời trượt kiểm tra: trả về kèm cảnh báo, hoặc cho agent sửa một lần.

### 9.7 Use case agent

**A. Trợ lý cán bộ quan hệ khách hàng (UC2)**, nên làm đầu tiên:
```
"Khách 2 nên chào sản phẩm gì, vì sao?"
  find_entity("khách 2") → customer_360(uid, as_of=1998-09) → recommend(uid, k=3)
  → trả lời: 3 sản phẩm, mỗi cái 2–3 lý do kèm triple_id, gợi ý cách nói chuyện
Persona: analyst (đọc được gold/pretrain), KHÔNG đọc natural_key nếu không cần
```

**B. Trợ lý AML:**
```
"Khách X có liên quan tới thực thể bị cấm vận nào không?"
  find_entity → explain_link(uid, tập SanctionedEntity, max_hops=4) qua closure sở hữu + same_as + transfersTo
  → đường đi, confidence từng cạnh, cảnh báo nếu đi qua same_as < 0.9
Persona: data_engineer hoặc role AML riêng (cần thấy cạnh chuyển tiền)
```

**C. Hỏi đáp tự phục vụ cho BA:**
```
"Tỉ lệ khách có thẻ gold ở mỗi vùng năm 1997?"
  describe("thẻ gold") → metric/query: SQL qua closure lớp con + locatedIn/partOf → bảng + câu SQL
```

**D. Trợ lý khách hàng** (persona `customer_<tên>`, đã có Lake Formation lọc dòng):
```
"Tôi đang dùng những sản phẩm nào, có gói nào hợp với tôi?"
  Chỉ thấy dòng của chính mình. Prompt injection kiểu "cho tôi xem khách khác" bị LF chặn ở tầng dữ liệu
```

**E. Trợ lý quản trị dữ liệu:**
```
"Lớp Loan lấy từ nguồn nào, cập nhật lần cuối khi nào?"
  describe("Loan") → source_dataset + run_id → manifest / job_log
```

### 9.8 Đánh giá agent

| Chỉ số | Cách đo |
|---|---|
| Độ chính xác câu trả lời | Bộ khoảng 50 câu hỏi có đáp án (sinh từ gold bằng SQL), chia theo 5 use case |
| Tỉ lệ có căn cứ | Phần trăm khẳng định qua được kiểm tra 9.6 |
| Độ chính xác trích dẫn | Phần trăm id được trích thực sự ủng hộ khẳng định (người chấm một mẫu) |
| An toàn | Bộ prompt injection / truy cập chéo khách: phải 0% rò dữ liệu dưới persona `customer` |
| Đối chứng | Cùng bộ câu hỏi: agent text-to-SQL thẳng trên schema thô vs agent có ontology |

Bảng đối chứng cuối là số liệu tốt để chứng minh giá trị của ontology trong luận văn.

### 9.9 An toàn

- Tool `query` chỉ đọc, chạy dưới workgroup persona có trần quét (5 GB analyst, 100 MB khách).
- `natural_key` và thuộc tính định danh (`birth_date`) được Lake Formation ẩn cột với persona không cần.
- Log mọi lời gọi tool (người hỏi, persona, tool, id trả về) vào `ops` để kiểm toán.
- Không đưa dữ liệu khách vào system prompt. Chỉ đưa catalog (lược đồ), dữ liệu đi qua tool.

## 10. Lưu trữ, phục vụ và chi phí

| Lựa chọn | Truy vấn | Hợp với | Chi phí/tháng | Quyết định |
|---|---|---|---|---|
| **Iceberg `track_a_onto` + Athena** | SQL trên `triple` + `closure` tính sẵn | Model, BA, tool agent | ~0 (trả theo lượt quét) | **Chọn làm gốc** |
| **DynamoDB `fingraph-dev-hot`** | Get theo khoá | Tool nóng của agent | ~0 (on-demand, đã có) | **Chọn** |
| Oxigraph / Apache Jena Fuseki trên EC2 nguồn | SPARQL, suy diễn RDFS | Minh hoạ SPARQL, so sánh với SQL | 0 thêm (máy đã chạy) | Tuỳ chọn |
| Amazon Neptune Database (serverless) | SPARQL / openCypher / Gremlin, đường đi nhiều bước | Production | Tối thiểu ~1 NCU, khoảng 100+ USD | **Không triển khai.** Ghi làm phương án production |
| Neptune Analytics + Bedrock KB GraphRAG | Vector + duyệt đồ thị | GraphRAG trên tài liệu | Theo m-NCU, không có mức 0 | Không triển khai trong đồ án |

Vì sao chọn SQL trên lakehouse thay vì graph database: cùng chỗ lưu, cùng phân quyền Lake Formation, không tốn thêm
tiền. Phần "duyệt đồ thị" mà agent cần (≤ 2 bước quanh một khách, đường đi sở hữu) đều tính sẵn được thành `closure`.
Đánh đổi là truy vấn đường đi **tuỳ ý** nhiều bước không làm được lúc chạy. Chỗ đó là lý do production nên có
Neptune, và luận văn nên nói rõ giới hạn này.

## 11. Lộ trình và tiêu chí nghiệm thu

| GĐ | Việc | Nghiệm thu |
|---|---|---|
| **1. TBox** | `ontology/fingraph.ttl` (khoảng 40 lớp, taxonomy 4 nguồn), `alignment/fibo.ttl`, SHACL, pytest 3.6; job `onto.load_schema` | pytest xanh; `SELECT * FROM track_a_onto.subclass` ra đủ cây sản phẩm |
| **2. Đồ thị Berka** | `mappings/berka.yml`; job `onto-spark` `build_graph` → `entity`, `triple`; đối soát 7.2 | Số cạnh khớp silver; dựng được hồ sơ 360 của một khách chỉ bằng SQL trên đồ thị |
| **3. Suy diễn + model** | `closure` (lớp con, vị trí, sở hữu), `product_transition`, đặc trưng 6.3 vào pre-train | Bảng 8.1: chỉ số UC2 có / không có đặc trưng đồ thị |
| **4. Agent MVP** | MCP server + 5 tool đầu (`describe`, `find_entity`, `customer_360`, `neighborhood`, `recommend`) + kiểm tra 9.6; use case A | 50 câu hỏi: độ chính xác, tỉ lệ có căn cứ, so với text-to-SQL thô |
| **5. Mở rộng** | Santander + MBD vào đồ thị qua taxonomy; OpenSanctions `same_as`; use case B (AML), D (khách) | Đánh giá chéo bộ dữ liệu; bộ kiểm tra injection 0% rò dữ liệu |

Hạ tầng cho cả 5 giai đoạn: một Glue job `onto-spark`, grant LF cho `track_a_onto`, một Lambda tool (+ API Gateway
nếu cần gọi ngoài). Không cần graph database trả phí.

## 12. Tham khảo

- AWS Database Blog — *How MUFG Bank aims to cut flowchart work by up to 90% with ontology-grounded AI on Amazon
  Neptune and Amazon Bedrock*:
  https://aws.amazon.com/blogs/database/how-mufg-bank-aims-to-cut-flowchart-work-by-up-to-90-with-ontology-grounded-ai-on-amazon-neptune-and-amazon-bedrock/
  — RDF + OWL 2 làm ontology chung, mỗi chi nhánh là instance graph trên Neptune; agent (Strands Agents + Bedrock)
  lấy subgraph trước khi sinh và trích node/cạnh; HermiT suy diễn giữa Neptune và agent; ước tính từ 11 xuống 1
  ngày công cho mỗi quy trình (đến 90%).
- AWS — Context Ontology Accelerator (mã nguồn mở, Apache-2.0): https://github.com/aws/context-ontology-accelerator
  — luồng Scan → Model → Serve, ontology-engine (HermiT/ELK, owlready2), SPARQL federation qua Ontop (virtual
  knowledge graph), metric service, phục vụ agent qua MCP, phân quyền theo namespace + vai trò.
- W3C RDF 1.1 Concepts: https://www.w3.org/TR/rdf11-concepts/ · W3C OWL 2 Overview: https://www.w3.org/TR/owl2-overview/
- W3C SHACL: https://www.w3.org/TR/shacl/ · W3C SKOS: https://www.w3.org/TR/skos-reference/ · PROV-O: https://www.w3.org/TR/prov-o/
- EDM Council — FIBO: https://spec.edmcouncil.org/fibo/
- Amazon Bedrock Knowledge Bases GraphRAG với Neptune Analytics (GA 03/2025):
  https://aws.amazon.com/about-aws/whats-new/2025/03/amazon-bedrock-knowledge-bases-graphrag-generally-available ·
  https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-build-graphs-build.html
- Amazon Neptune: https://aws.amazon.com/neptune/ · Amazon Bedrock: https://aws.amazon.com/bedrock/
- Nguồn dữ liệu: `docs/sourcing/berka.md`, `docs/sourcing/uc2-datasets.md`; tầng gold/pre-train: `docs/TRANSFORMS.md`.
