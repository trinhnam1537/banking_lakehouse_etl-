# Xây tầng ontology: các bước, giải thích, checklist

Tài liệu này là **runbook xây dựng** tầng ontology và knowledge graph. Nó trả lời "làm gì, theo thứ tự nào,
kiểm gì thì coi là xong". Còn **thiết kế** (lớp, quan hệ, taxonomy sản phẩm, kiến trúc agent, lý do chọn
lakehouse thay vì graph DB) nằm ở [ONTOLOGY.md](ONTOLOGY.md). Hai tài liệu dẫn chiếu nhau: mỗi bước dưới đây ghi
mục thiết kế tương ứng, để khỏi chép lại.

Tầng ontology phục vụ **hai mục đích**, và mọi quyết định trong tài liệu đều quy về một trong hai:

| Mục đích | Cần từ ontology | Tiêu chí thành công (bước O12) |
|---|---|---|
| **M. Model gợi ý sản phẩm (UC2)** | Taxonomy sản phẩm chung cho nhiều bộ dữ liệu, sự kiện "khách giữ sản phẩm" có thời gian, đặc trưng đồ thị **không rò rỉ tương lai**, lý do gợi ý | Chỉ số UC2 có đặc trưng đồ thị tốt hơn không có, trên cùng tập test thời gian |
| **A. AI agent** | Bộ từ vựng để hiểu câu hỏi, thực thể có định danh ổn định, subgraph quanh thực thể, mọi câu trả lời trích được nguồn, phân quyền theo persona | Bộ ~50 câu hỏi: độ chính xác và tỉ lệ có căn cứ cao hơn agent text-to-SQL trên schema thô |

Ký hiệu ở cột "Phục vụ": **M** model, **A** agent, **M+A** cả hai.

**Trạng thái (04-11-2026):** giai đoạn 1 (O0–O4 + `onto.load_schema`) **xong**:
- `ontology/fingraph.ttl` có 53 lớp, 29 quan hệ, 8 kiểu hình rửa tiền; kèm 20 câu hỏi năng lực.
- 17 test trong `tests/ontology` đều qua.
- Job `fingraph-dev-onto-spark` chạy SUCCEEDED trên dev và tạo 4 bảng `track_a_onto.class/property/subclass/concept`.

Còn chờ: quyền SELECT `track_a_onto` cho persona analyst (cần duyệt cấp quyền). Bước tiếp theo: O5–O7 (mapping
corebank/Berka, Bank Marketing, MBD → `entity`, `triple`).

---

## 0. Bức tranh tổng thể

```
 silver_* (đã có, 02-11-2026)                        track_a_onto (Iceberg, Athena)                     Người dùng
 ─────────────────────────────                       ───────────────────────────────                    ──────────
 silver_corebank / silver_berka ─┐   O3 TBox (.ttl) ──▶ class, property, subclass
 silver_bank_marketing           │   O5 mapping (.yml)
 silver_mbd                      ├──▶ O7 build_graph ─▶ entity, triple ─┐
 silver_cards                    │   O8 resolve ──────▶ same_as         ├─▶ O9 derive ─▶ closure,          ─▶ O10 pretrain (M)
 silver_aml                      │                                      │               product_transition,    UC2 có đặc trưng đồ thị
 silver_opensanctions            │                                      │               graph_feature,
 silver_paysim, silver_fx  ──────┘                                      │               node_embedding
 (chờ: Santander, Home Credit)                                          └─▶ O11 catalog + hot store ─▶ MCP tool ─▶ agent (A)
```

| Bước | Nội dung | Phục vụ | Phụ thuộc |
|---|---|---|---|
| O0 | Chốt phạm vi và quyết định nền | M+A | |
| O1 | Câu hỏi năng lực (competency questions) | M+A | O0 |
| O2 | Kiểm kê silver → ma trận nguồn × khái niệm | M+A | O1 |
| O3 | TBox: lớp, quan hệ, taxonomy sản phẩm | M+A | O2 |
| O4 | Hợp đồng: SHACL + pytest | M+A | O3 |
| O5 | Mapping nguồn → ontology | M+A | O3, O4 |
| O6 | Định danh `uid` và thời gian (bitemporal) | M+A | O3 |
| O7 | Job dựng đồ thị: `entity`, `triple` | M+A | O5, O6 |
| O8 | Hợp nhất thực thể: `same_as` | A (+M) | O7 |
| O9 | Suy diễn tính sẵn: closure, chuyển tiếp sản phẩm, đặc trưng, embedding | M+A | O7 |
| O10 | Đưa vào model gợi ý | M | O9 |
| O11 | Phục vụ agent: catalog, hot store, tool, grounding | A | O7–O9 |
| O12 | Đánh giá và nghiệm thu | M+A | O10, O11 |
| O13 | Vận hành: lịch, giám sát, phiên bản, chi phí, phân quyền | M+A | O7 trở đi |

---

## O0. Chốt phạm vi và quyết định nền

**Vì sao làm trước:** ontology rất dễ phình ra (FIBO có hàng nghìn lớp). Không chốt phạm vi thì sẽ mô hình hoá
những thứ không model nào dùng, không câu hỏi nào hỏi. Mỗi lớp thêm vào là thêm mapping, thêm test, thêm chỗ sai.

**Quyết định (đã chốt trong [ONTOLOGY.md mục 2, 10](ONTOLOGY.md)):**

| Quyết định | Chọn | Lý do ngắn |
|---|---|---|
| Nơi lưu đồ thị | Bảng Iceberg `track_a_onto` + Athena | Cùng chỗ lưu và cùng Lake Formation với lakehouse, ~0 USD. Neptune ghi làm phương án production |
| Ngôn ngữ TBox | OWL 2 DL, Turtle (`.ttl`), căn chỉnh FIBO bằng `skos:closeMatch` | Chuẩn W3C, kiểm được bằng reasoner, không nhập nguyên FIBO |
| Suy diễn | **Tính sẵn bằng Spark** thành bảng (closure) | Athena không đệ quy tin cậy. LLM không tự suy luận bắc cầu |
| Mức chi tiết | Đồ thị giữ **cấu trúc và nghĩa**. Khối lượng (từng giao dịch, số dư theo tháng) ở lại silver/gold | 32 triệu giao dịch AML không thành 32 triệu cạnh: gộp `transfersTo` theo cặp tài khoản và kỳ |
| Phạm vi giai đoạn 1 | Nhóm A (core banking/Berka) + UC2 (Bank Marketing, MBD) | Đủ để chứng minh cả M và A. Santander vào khi có dữ liệu |
| Phạm vi giai đoạn 2 | AML (IBM AML, OpenSanctions, thẻ) | Use case agent AML |

**Ba "vũ trụ" dữ liệu không nối thực thể với nhau** (giải thích ở [sourcing/README.md mục 2.2](sourcing/README.md)):
core banking, IBM AML, PaySim, và các bộ UC2. Ontology nối chúng ở **tầng khái niệm** (cùng lớp `fin:Account`, cùng
taxonomy sản phẩm), **không** bịa cạnh giữa thực thể của hai vũ trụ.

**Checklist O0**
- [ ] Ghi phạm vi giai đoạn 1 và 2 vào mục O12 (bảng nghiệm thu)
- [ ] Xác nhận nguồn đã có silver: `silver_corebank`, `silver_berka`, `silver_bank_marketing`, `silver_mbd`, `silver_cards`, `silver_aml`, `silver_opensanctions`, `silver_paysim`, `silver_fx`
- [ ] Ghi nguồn còn thiếu và ảnh hưởng: **Santander** (taxonomy 24 sản phẩm, nguồn UC2 lớn nhất) và **Home Credit**. Cả hai bị chặn vì chưa chấp nhận luật thi Kaggle

---

## O1. Câu hỏi năng lực (competency questions)

**Vì sao:** đây là bước chuẩn của phương pháp xây ontology (METHONTOLOGY, NeOn). Ontology tốt là ontology **trả lời
được một danh sách câu hỏi viết trước**. Danh sách này vừa định phạm vi (O3 chỉ thêm lớp khi có câu hỏi cần), vừa
là bộ test nghiệm thu (O12). Không có nó thì không biết khi nào xong.

**Làm gì:** viết 20–30 câu hỏi, mỗi câu ghi: ai hỏi, phục vụ M hay A, khái niệm cần có, đáp án lấy từ đâu để chấm.
Lưu ở `ontology/competency_questions.yml`.

| # | Câu hỏi | Phục vụ | Khái niệm / quan hệ cần |
|---|---|---|---|
| CQ1 | Khách X đang giữ những sản phẩm nào tại ngày D? | M+A | `Customer`, `ProductHolding`, `ofProduct`, `valid_from/to` |
| CQ2 | Khách giữ **sản phẩm tín dụng** (mọi loại thẻ, khoản vay) ở vùng R là bao nhiêu? | A | taxonomy + closure `subClassOf*`, `locatedIn`, `partOf*` |
| CQ3 | Sau khi có thẻ classic, khách hay mua gì tiếp trong 3 tháng? | M | `product_transition`, `frequentlyFollowedBy` |
| CQ4 | Vì sao gợi ý Loan cho khách X? | M+A | đường đi khách → sản phẩm đang giữ → transition → sản phẩm gợi ý, có `triple_id` |
| CQ5 | "Tiền gửi có kỳ hạn" ở Bank Marketing và Santander có phải cùng một sản phẩm? | M+A | `fin:TermDeposit`, `skos:notation` theo nguồn |
| CQ6 | Ai là người được uỷ quyền trên tài khoản của khách X? | A | `isAuthorizedOn` |
| CQ7 | Giao dịch thẻ của khách X ở đâu, nhóm merchant nào? | A | `Card` ↔ `Customer` qua `card_token` |
| CQ8 | Khách X có trùng tên/ngày sinh với đối tượng bị cấm vận nào không, độ tin cậy bao nhiêu? | A | `SanctionedEntity`, `same_as`, `confidence` |
| CQ9 | Tài khoản Y (IBM AML) có nằm trong vụ rửa tiền kiểu FAN-OUT nào không? | A | `LaunderingAttempt`, `typology`, `involvedIn` |
| CQ10 | Pháp nhân sở hữu tài khoản Y còn sở hữu những tài khoản nào, ở ngân hàng nào? | A | `Account` → `Party` (`isHeldBy`), `Bank` |
| CQ11 | Dữ liệu về lớp `Loan` đến từ bảng nguồn nào, cập nhật lần cuối khi nào? | A | `Dataset`, `prov:wasDerivedFrom`, `source_dataset`, `run_id` |
| CQ12 | Tỉ lệ đăng ký tiền gửi có kỳ hạn theo nghề nghiệp? | A | Bank Marketing → `TermDeposit`, thuộc tính hồ sơ |

**Checklist O1**
- [ ] ≥ 20 câu hỏi, có cả M và A, phủ đủ 5 use case agent ([ONTOLOGY.md mục 9.7](ONTOLOGY.md))
- [ ] Mỗi câu có đáp án **chấm được** bằng SQL trên silver/gold (dùng lại ở O12)
- [ ] Mỗi khái niệm trong cột cuối xuất hiện trong O3. Khái niệm không câu hỏi nào cần thì không thêm

---

## O2. Kiểm kê silver → ma trận nguồn × khái niệm

**Vì sao:** cùng một khái niệm mang tên khác nhau ở mỗi nguồn (`client_id`, `ncodpers`, `entity_id`...). Ma trận này
là **đầu vào trực tiếp của mapping (O5)** và cho thấy trước khái niệm nào chỉ có ở một nguồn (không dùng được để
học chéo), quan hệ nào nối được, quan hệ nào không.

**Hiện trạng silver (02-11-2026), quy mô ảnh hưởng thiết kế:**

| Database | Bảng chính | Quy mô | Thành gì trên đồ thị |
|---|---|---|---|
| `silver_corebank` | client, account, disp, card, loan, standing_order, trans, district | ~5,4 nghìn khách, ~1,06 triệu giao dịch | `Person/Customer`, `Account`, `holdsAccount`, `isAuthorizedOn`, `ProductHolding` (Loan, thẻ, bảo hiểm, thanh toán định kỳ), `District ⊂ Region ⊂ Country`, `transfersTo` gộp theo tháng |
| `silver_berka` | 8 bảng gốc | như trên (1993–1998) | Như corebank, nối `same_as` 1.0 |
| `silver_bank_marketing` | contact_full, contact_additional | 45.211 + 41.188 | `Customer` ẩn danh, `ProductHolding TermDeposit` khi `subscribed` |
| `silver_mbd` | target_month, client_split | 1,2 triệu dòng, ~100 nghìn khách | `Organization/Customer`, `ProductHolding AnonymousProduct_k` |
| `silver_cards` | card_authorization | ~150 nghìn, tăng ~170 nghìn/ngày | **Không** từng giao dịch: cạnh tổng hợp khách → nhóm merchant theo tháng; `card_token` nối thẻ corebank |
| `silver_aml` | transfer, account, laundering_attempt, label | 31,9 triệu giao dịch, 2,09 triệu tài khoản, 2.756 vụ | `Account`, `Bank`, `Party` (entity_id), `transfersTo` **gộp theo cặp và ngày**, `LaunderingAttempt` + `typology`, `flaggedAs` |
| `silver_opensanctions` | target, entity, entity_change | 73.803 đối tượng, 309 nghìn thực thể | `SanctionedEntity`, `isDirectlyOwnedBy` (FtM Ownership), ứng viên `same_as` với khách |
| `silver_paysim` | transaction, fraud_label | 6,36 triệu | Ngoài phạm vi giai đoạn 1–2 (model gian lận dùng thẳng silver) |
| `silver_fx` | cnb_czk_rate, vcb_vnd_rate | nhỏ | Không vào đồ thị; dùng quy đổi ở gold |

**Làm gì:** điền bảng `ontology/source_matrix.md`: hàng = khái niệm của O1, cột = nguồn, ô = `bảng.cột` hoặc biểu
thức. Đánh dấu ô "không có" và ô "suy ra được" (ví dụ bảo hiểm của Berka suy từ giao dịch `k_symbol = POJISTNE`).

**Checklist O2**
- [ ] Mọi khái niệm của O1 có ít nhất một ô điền
- [ ] Ghi rõ khoá nối thật giữa nguồn: corebank↔Berka (`client_id` gốc), thẻ↔corebank (`card_token`), AML account↔entity (`entity_id`). Không có khoá thì ghi "không nối"
- [ ] Ước lượng số node và cạnh cho mỗi nguồn. AML: đếm số cặp `(from, to)` khác nhau trước khi quyết định mức gộp

---

## O3. TBox: lớp, quan hệ, taxonomy sản phẩm

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 3](ONTOLOGY.md).

**Vì sao là trái tim của cả hai mục đích:**
- **M:** taxonomy sản phẩm cho phép bốn bộ dữ liệu nói cùng một ngôn ngữ (`fin:TermDeposit` = Bank Marketing `y` =
  Santander `ind_deco/deme/dela`). Nhờ đó học chéo được, và định nghĩa nhãn "mua sản phẩm lớp X trong H tháng" thống nhất.
- **A:** agent hiểu "thẻ tín dụng" là `fin:CreditCard` cùng mọi lớp con, không phải đoán tên cột.

**Làm gì:**

| File | Nội dung |
|---|---|
| `ontology/fingraph.ttl` | ~40 lớp (`Party`, `Customer`, `Person`, `Organization`, `Account`, `Bank`, `Product` + cây sản phẩm, `ProductHolding`, `Location`..., `SanctionedEntity`, `LaunderingAttempt`, `Dataset`, `Recommendation`), ~25 quan hệ có `rdfs:domain`/`rdfs:range`, nhãn `rdfs:label` **tiếng Việt và tiếng Anh**, `skos:definition` |
| `ontology/alignment/fibo.ttl` | `skos:closeMatch` / `rdfs:subClassOf` sang FIBO cho từng lớp chính |
| `ontology/CHANGELOG.md` | Phiên bản semver, ghi vào cột `ontology_version` của mọi dòng đồ thị |

Bổ sung so với thiết kế gốc, theo silver mới có: `fin:Bank` (IBM AML `bank_id`), `fin:LaunderingAttempt` +
`fin:involvedIn` + `fin:typology` (giá trị SKOS: FAN-OUT, CYCLE...), `fin:isHeldBy` (`Account` → `Party`, cho AML),
`fin:spendsAt` (khách → nhóm merchant, cạnh tổng hợp từ thẻ).

**Quy ước đặt tên** (để agent và người đọc đoán được): lớp `UpperCamelCase`, quan hệ `lowerCamelCase` dạng động từ
(`holdsAccount`), mọi lá taxonomy có `skos:notation` = mã gốc ở nguồn.

**Checklist O3**
- [ ] Mỗi lớp/quan hệ trả lời ít nhất một CQ (truy vết ngược được về O1)
- [ ] Mỗi lớp có nhãn vi + en và định nghĩa một câu. Catalog cho agent (O11) sinh từ đây, thiếu là agent thiếu
- [ ] Mỗi lá taxonomy có `skos:notation` theo nguồn và `fin:sourceDataset`
- [ ] Không vòng `subClassOf`. Reasoner (HermiT qua `owlready2`) báo nhất quán
- [ ] `ontology_version` = `1.0.0` trong CHANGELOG

---

## O4. Hợp đồng: SHACL + pytest

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 3.6](ONTOLOGY.md).

**Vì sao:** "ontology là hợp đồng, không phải chú thích". Nếu không có kiểm tra tự động, mapping sẽ dần trỏ tới lớp
không tồn tại, cạnh nối sai kiểu, và **agent trích dẫn một cạnh vô nghĩa mà không ai biết**. Kiểm trong pytest thì
lỗi hiện ra trên máy, trước khi tốn một đồng Glue nào.

**Làm gì:** `ontology/shapes/fingraph.shacl.ttl` và `tests/ontology/`:

| Test | Bắt lỗi |
|---|---|
| Cú pháp + OWL 2 DL + reasoner nhất quán | Lớp tự mâu thuẫn, vòng lặp |
| Taxonomy đầy đủ | Mã sản phẩm trong mapping chưa có lớp; lá thiếu `skos:notation` |
| SHACL trên đồ thị mẫu nhỏ | `ProductHolding` thiếu `ofProduct`; `valid_to < valid_from`; `holdsAccount` trỏ vào thứ không phải `Account` |
| Mapping ↔ DDL silver | Mapping dùng cột không có trong DDL `silver_etl/ddl/**` |
| Mapping ↔ TBox | Predicate/lớp trong mapping chưa khai báo |
| CQ có lớp | Khái niệm trong `competency_questions.yml` đều có trong TBox |

Thư viện chỉ cho test, không đóng gói vào Glue: `rdflib`, `pyshacl`, `owlready2` (thêm vào `requirements-dev.txt`).

**Checklist O4**
- [ ] `pytest tests/ontology` xanh trên máy và trên CI
- [ ] Một mapping cố tình sai (lớp không tồn tại) làm test đỏ. Viết đúng một test như vậy để chứng minh hàng rào chạy

---

## O5. Mapping nguồn → ontology

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 5](ONTOLOGY.md).

**Vì sao khai báo (YAML) thay vì viết code cho từng nguồn:** mapping là **tri thức nghiệp vụ**, cần người duyệt (chuyên
gia đọc YAML được, không đọc PySpark). Một job đọc mọi mapping, nên thêm nguồn không thêm code. LLM có thể **gợi ý**
mapping ([ONTOLOGY.md mục 7.3](ONTOLOGY.md)), người duyệt từng dòng, pytest (O4) chặn chỗ sai.

**Làm gì:** mỗi nguồn một file `ontology/mappings/<nguồn>.yml`, mỗi mục sinh node hoặc cạnh từ một bảng silver:

```yaml
# ontology/mappings/corebank.yml (minh hoạ)
source_system: corebank
entities:
  - class: fin:Customer
    from: silver_corebank.client            # bảng hiện hành: ds_is_current AND ds_record_status <> 'D'
    natural_key: "client_id"
    label: "concat('Khách ', client_id)"     # KHÔNG dùng full_name: nhãn đi vào prompt agent
    valid_from: ds_valid_from
    attributes: {gender: gender, birth_year: "year(birth_date)"}
edges:
  - predicate: fin:holdsAccount
    from: silver_corebank.disp
    where: "disp_type = 'OWNER'"
    subject: {class: fin:Customer, key: client_id}
    object:  {class: fin:Account,  key: account_id}
    valid_from: ds_valid_from
  - predicate: fin:hasHolding                # n-ary: tạo node ProductHolding + 3 cạnh
    holding: {product: fin:Loan, from: silver_corebank.loan, key: loan_id,
              customer_via: "account_id -> disp OWNER -> client_id",
              valid_from: granted_date, valid_to: maturity_date,
              attributes: {status: loan_status, amount: amount}}
```

Thứ tự viết mapping theo phạm vi O0:

| Lượt | File | Nội dung |
|---|---|---|
| 1 | `corebank.yml`, `berka.yml` | Khách, tài khoản, quyền, vay, thẻ, bảo hiểm/thanh toán (theo đúng định nghĩa của `track_a_gold.berka_product_event`), vị trí |
| 1 | `bank_marketing.yml`, `mbd.yml` | Khách ẩn danh / doanh nghiệp, `TermDeposit`, `AnonymousProduct_k` |
| 2 | `cards.yml` | `spendsAt` gộp theo (khách, nhóm merchant, tháng) |
| 2 | `aml.yml` | `Account`, `Bank`, `Party`, `isHeldBy`, `transfersTo` gộp, `LaunderingAttempt`, `flaggedAs` |
| 2 | `opensanctions.yml` | `SanctionedEntity`, `isDirectlyOwnedBy` từ thực thể FtM loại `Ownership` |
| 3 | `santander.yml` | Khi có dữ liệu: 24 cờ `ind_*` → `ProductHolding` theo chuyển 0→1 |

**Quy tắc bắt buộc:**
- **Một sự thật một chỗ:** định nghĩa "khách giữ bảo hiểm" ở mapping phải **trùng** định nghĩa ở gold
  (`berka_product_event`). Lệch nhau thì model (đọc gold) và agent (đọc đồ thị) trả lời khác nhau cho cùng câu hỏi.
- **Nhãn không chứa PII:** `label` đi vào prompt agent, nên dùng "Khách 2", không dùng họ tên. Họ tên ở silver, có Lake Formation che.
- **Chỉ đọc phiên bản hiện hành** của bảng SCD2 khi dựng trạng thái hiện tại. Lịch sử đi vào `valid_from/valid_to`.

**Checklist O5**
- [ ] Mỗi mapping có người duyệt (ghi tên trong file)
- [ ] pytest O4 xanh với mapping mới
- [ ] Định nghĩa sản phẩm của Berka/corebank khớp `track_a_gold.berka_product_event` (test đối chiếu số dòng ở O7)

---

## O6. Định danh `uid` và thời gian

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 2.5, 4.3](ONTOLOGY.md).

**Vì sao tách bước:** hai quyết định này **không sửa được sau khi đã có người dùng**. Đổi cách sinh `uid` thì mọi trích
dẫn agent đã đưa ra, mọi embedding đã train, mọi `Recommendation` đã ghi đều mất nghĩa.

| Quyết định | Quy tắc | Vì sao |
|---|---|---|
| `uid` | `sha256(class_iri | source_system | natural_key)[:32]` | Ổn định qua các lần chạy, không lộ khoá gốc, cùng người ở hai nguồn = hai uid + `same_as` |
| `triple_id` | `sha256(subject | predicate | object | valid_from | source)` | Agent trích dẫn từng cạnh |
| Thời gian nghiệp vụ | `valid_from`, `valid_to` (NULL = còn hiệu lực) | **M:** đặc trưng tại tháng `m` chỉ dùng cạnh có `valid_from <= m`, chống rò tương lai. **A:** "tại ngày D khách giữ gì" |
| Thời gian hệ thống | `recorded_at` | Kiểm toán: hệ thống biết điều này từ khi nào |
| Trục thời gian | Theo trục chung của simulator (Berka dời +24 năm, `SIM_EPOCH` 2023-01-01) | Các nguồn so sánh được trên một trục ([sourcing/README.md mục 2.1](sourcing/README.md)) |

**Checklist O6**
- [ ] Hàm `make_uid` và `make_triple_id` có test: cùng đầu vào cùng kết quả, đổi một trường thì đổi kết quả
- [ ] Mọi cạnh có `valid_from`. Cạnh không xác định được thời điểm thì ghi rõ quy tắc (ví dụ: ngày mở tài khoản)

---

## O7. Job dựng đồ thị: `entity`, `triple`

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 4.1–4.2, 7](ONTOLOGY.md).

**Vì sao theo khuôn gold/pre-train** ([TRANSFORMS.md](TRANSFORMS.md)): logic dựng đồ thị (n-ary, gộp, đường nối
`account → disp → client`) là PySpark tự viết, không khai báo hoá được hết. Glue job dùng chung, `--TRANSFORM` chọn
logic, `createOrReplace` Iceberg nên chạy lại cho cùng kết quả và còn snapshot cũ để so hai phiên bản đồ thị.

**Làm gì:** module `transforms/onto/`:

| `--TRANSFORM` | Đọc | Ghi |
|---|---|---|
| `onto.load_schema` | `ontology/*.ttl` (đóng gói trong etl.zip) | `track_a_onto.class`, `property`, `subclass` |
| `onto.build_graph` | mapping + `silver_*` | `track_a_onto.entity`, `track_a_onto.triple` (lược đồ ở ONTOLOGY.md 4.2, phân vùng `predicate_iri`, `bucket(16, subject_uid)`) |

Hạ tầng: thêm job `onto-spark` vào `modules/transforms` (một khoá trong `local.jobs`), grant Lake Formation `ALL` trên
`track_a_onto` cho role transforms, `SELECT` cho DE và analyst. Cỡ máy: đồ thị nhóm A nhỏ (cỡ S), AML (2 triệu
tài khoản, vài triệu cạnh gộp) cỡ M.

**Bất biến kiểm mỗi lần chạy** (vi phạm thì job FAILED, bảng giữ phiên bản cũ):

| Bất biến | Vì sao |
|---|---|
| Không cạnh mồ côi: mọi `subject_uid`/`object_uid` có trong `entity` | Agent không được trích một node không tồn tại |
| Mọi `predicate_iri` có trong `property` | Hợp đồng TBox |
| Đối soát số lượng: số `holdsAccount` = số `disp OWNER` hiện hành; số `ProductHolding` Loan = số `loan`; số `TermDeposit` = số dòng `subscribed` | Phát hiện mapping lọc sai |
| `uid` duy nhất trong `entity`, `triple_id` duy nhất trong `triple` | Trích dẫn không mơ hồ |

**Checklist O7**
- [ ] Chạy local trên Spark + Iceberg với mẫu nhỏ (như test silver), có test cho từng loại mapping (node, cạnh, n-ary, gộp)
- [ ] Chạy trên AWS, bảng đối soát ra đúng
- [ ] **Dựng được hồ sơ 360 của một khách chỉ bằng SQL trên `entity` + `triple`** (nghiệm thu giai đoạn 2 của ONTOLOGY.md)
- [ ] Ghi số node/cạnh theo lớp và predicate vào [sourcing/ingest-progress.md](sourcing/ingest-progress.md) hoặc tài liệu riêng

---

## O8. Hợp nhất thực thể: `same_as`

**Phục vụ:** A (CQ8 cấm vận), M (nối hồ sơ Berka và corebank). **Thiết kế:** [ONTOLOGY.md mục 4.4](ONTOLOGY.md).

**Vì sao không gộp cứng:** gộp sai hai người khác nhau thì không gỡ được và sinh ra kết luận AML sai. Giữ hai `uid` +
một dòng `same_as` có `confidence`, truy vấn mặc định chỉ đi qua `confidence >= 0.9`, agent bắt buộc nói "khả năng"
với liên kết yếu hơn.

| Cặp | Cách khớp | `confidence` | Người duyệt |
|---|---|---|---|
| corebank `client` ↔ Berka `client` | cùng `client_id` gốc | 1.0 | không |
| corebank `card` ↔ thẻ Kafka | `card_token` (md5 số thẻ) | 1.0 | không |
| Khách corebank ↔ OpenSanctions | tên chuẩn hoá (bỏ dấu, thường hoá) + ngày sinh + quốc gia, điểm theo trọng số | 0.6–0.95 | **có**, khi < 0.9 |
| IBM AML ↔ corebank, PaySim ↔ corebank | không có khoá | **không nối** | |

Đáp án để đo chất lượng khớp có sẵn: `corebank.seed --sanction-hits N` gieo N ca trúng, đáp án ở `data/ground_truth/`.

**Checklist O8**
- [ ] Precision/recall của khớp OpenSanctions đo trên đáp án gieo sẵn
- [ ] Hàng đợi duyệt cho `confidence` trong [0.6, 0.9): bảng `track_a_onto.same_as_review`
- [ ] Không có `same_as` nào giữa hai vũ trụ không có khoá chung

---

## O9. Suy diễn tính sẵn

**Phục vụ:** M+A. **Thiết kế:** [ONTOLOGY.md mục 6](ONTOLOGY.md).

**Vì sao tính sẵn:** Athena không đệ quy tin cậy, LLM suy luận bắc cầu sai, còn model cần đặc trưng dạng bảng. Mọi
suy diễn thành **bảng Iceberg**, tool agent và pre-train chỉ đọc.

| Bảng | Luật | Phục vụ | Lưu ý bắt buộc |
|---|---|---|---|
| `closure` lớp con | `subClassOf*` | M+A | Khách giữ `GoldCard` thì giữ `CreditCard`, `CreditProduct` |
| `closure` vị trí | `partOf*` | A | District → Region → Country |
| `closure` sở hữu | `isDirectlyOwnedBy*`, nhân `weight` | A (AML) | Đã có `jobs/glue/onto_closure.py` (chống chu trình, cắt lineage) |
| `product_transition` | P(B | A, H), lift | M+A | **Chỉ tính trên khoảng train** (trước `VALID_START`). Tính trên toàn bộ là rò nhãn sang test |
| `graph_feature` | Đặc trưng mục 6.3 theo (khách, tháng) | M | **Point-in-time**: tại tháng `m` chỉ dùng cạnh `valid_from <= m` |
| `node_embedding` | node2vec / TransE | M+A | Chỉ cạnh có `valid_from < VALID_START`, có `model_version` |

Cạnh suy diễn (`frequentlyFollowedBy`) ghi vào `triple` với `is_inferred = true` và `confidence` < 1, để agent phân
biệt "dữ liệu nói" với "hệ thống suy ra".

**Checklist O9**
- [ ] Test rò rỉ: đổi dữ liệu **sau** `VALID_START` không làm đổi bất kỳ giá trị nào của `product_transition`, `graph_feature` trong khoảng train
- [ ] `closure` không có chu trình, độ sâu ≤ `MAX_DEPTH`
- [ ] Mỗi cạnh suy diễn truy ngược được về luật và các cạnh gốc

---

## O10. Đưa vào model gợi ý (UC2)

**Phục vụ:** M. **Thiết kế:** [ONTOLOGY.md mục 8](ONTOLOGY.md).

**Vì sao làm thành thí nghiệm có đối chứng:** luận văn cần chứng minh ontology **có ích**, không chỉ "có". Cách thuyết
phục duy nhất là cùng tập test thời gian, cùng model, chỉ khác có hay không có đặc trưng đồ thị.

**Làm gì:**
1. `transforms/pretrain/uc2_next_product.py` join `track_a_onto.graph_feature` theo (khách, tháng). Bật/tắt bằng tham số `--GRAPH_FEATURES`.
2. Định nghĩa nhãn theo **lớp sản phẩm của taxonomy** thay vì mã nguồn: "mua sản phẩm thuộc closure của `fin:X` trong H tháng". Cùng định nghĩa chạy được cho Berka, Bank Marketing, MBD, và Santander khi có.
3. Huấn luyện hai bản (có / không đặc trưng đồ thị), ghi bảng kết quả.
4. Điểm model ghi ngược thành `fin:Recommendation` (khách, sản phẩm, điểm, `model_version`, `scored_at`) để agent đọc và kiểm toán được.
5. Lý do gợi ý: 2–3 đường đi có `weight` cao nhất từ khách tới sản phẩm, mỗi đường có `triple_id` ([ONTOLOGY.md mục 8.3](ONTOLOGY.md)).

| Chỉ số | Không đồ thị | Có đồ thị |
|---|---|---|
| AUC-PR theo từng lớp sản phẩm | | |
| Recall@3, MAP@7 theo khách | | |

**Checklist O10**
- [ ] Chia tập theo **thời gian**, không chia ngẫu nhiên (chia ngẫu nhiên là rò tương lai)
- [ ] Bảng kết quả điền đủ, kèm `model_version` và phiên bản đồ thị (snapshot Iceberg)
- [ ] `Recommendation` có trong `triple` và `recommend(uid)` (O11) đọc được

---

## O11. Phục vụ AI agent

**Phục vụ:** A. **Thiết kế:** [ONTOLOGY.md mục 9](ONTOLOGY.md).

**Vì sao không cho agent đọc thẳng silver:** text-to-SQL trên schema thô chạy ra SQL đúng cú pháp nhưng trả lời sai
(không biết `ind_tjcr_fin_ult1` là thẻ tín dụng, không biết phải đi `disp → card`). Agent cần **bản đồ nghĩa**
(catalog), **tool hẹp có kiểu** trả về kèm trích dẫn, và **kiểm tra sau khi sinh**.

| Thành phần | Làm gì | Vì sao |
|---|---|---|
| Semantic catalog | Sinh **tự động** từ `fingraph.ttl`: lớp, nhãn vi, cha/con, mã nguồn, bảng vật chất hoá, gợi ý truy vấn | Không bao giờ lệch khỏi ontology. Bản rút gọn vào system prompt, chi tiết qua tool `describe` |
| Hot store | `onto.export_hot`: hồ sơ 360 + top-k gợi ý mỗi khách → DynamoDB (`CUST#<uid>`, TTL) | Hai tool gọi nhiều nhất trả lời dưới 10 ms thay vì vài giây Athena |
| Tool (MCP server) | `describe`, `find_entity`, `customer_360`, `neighborhood`, `explain_link`, `recommend`, `metric`, `query` (chỉ đọc) | Tool hẹp, mỗi kết quả kèm `uid`/`triple_id` |
| Persona | Mỗi phiên agent assume role persona của người hỏi | Lake Formation lọc ở tầng dữ liệu; prompt injection không vượt được |
| Kiểm tra grounding | Lambda không dùng LLM: mọi id trong câu trả lời phải có trong kết quả tool của phiên | Chặn câu trả lời bịa |

Thứ tự: catalog → `describe` + `find_entity` → `customer_360` + `neighborhood` → `recommend` → kiểm tra grounding →
use case A (trợ lý RM, UC2) → B (AML) → C (BA) → D (khách) → E (quản trị dữ liệu).

**Checklist O11**
- [ ] Catalog sinh từ TBox trong CI; sửa TBox là catalog đổi theo
- [ ] Không dữ liệu khách nào trong system prompt
- [ ] Tool `query` chỉ đọc, chạy dưới workgroup persona có trần quét
- [ ] Mọi lời gọi tool ghi log (người hỏi, persona, tool, id trả về)
- [ ] Persona `customer_*` không lấy được dòng của khách khác (bộ prompt injection, kỳ vọng 0% rò)

---

## O12. Đánh giá và nghiệm thu

**Vì sao có bước riêng:** "xong" phải đo được. Bộ CQ của O1 trở thành bộ test, cùng các số liệu so sánh có đối chứng.

| Hạng mục | Đo | Đạt khi |
|---|---|---|
| Ontology trả lời được CQ | Mỗi CQ có truy vấn trên `track_a_onto` cho đúng đáp án SQL silver | 100% CQ giai đoạn tương ứng |
| Chất lượng đồ thị | Bất biến O7, đối soát số lượng | 0 vi phạm |
| Model (M) | Bảng O10 | Có đồ thị ≥ không đồ thị trên AUC-PR và Recall@3; nếu không, ghi rõ và phân tích |
| Agent (A) | ~50 câu hỏi chia 5 use case: độ chính xác, tỉ lệ có căn cứ, độ chính xác trích dẫn | Tốt hơn agent text-to-SQL thô trên cùng bộ câu hỏi |
| An toàn (A) | Bộ prompt injection / truy cập chéo dưới persona `customer` | 0% rò dữ liệu |

---

## O13. Vận hành

| Việc | Cách | Ghi chú |
|---|---|---|
| Lịch | Chuỗi sự kiện như silver: silver xong → `onto.build_graph` → `onto.derive` → pre-train / `export_hot` | Tận dụng EventBridge "Job Run Succeeded" của orchestrator |
| Phiên bản | `ontology_version` trên mọi dòng; snapshot Iceberg cho mỗi lần dựng | So hai phiên bản đồ thị khi đổi TBox |
| Đổi TBox | Thêm lớp/quan hệ: minor. Đổi nghĩa/xoá: major, chạy lại toàn bộ, ghi CHANGELOG | Trích dẫn cũ của agent trỏ `uid` vẫn đúng vì `uid` không phụ thuộc phiên bản |
| Giám sát | Bất biến O7 và số node/cạnh vào luồng giám sát chung (log JSON mỗi lần chạy) | Xem đề xuất giám sát toàn hệ thống |
| Phân quyền | `track_a_onto` vào Lake Formation: DE `ALL`, analyst `SELECT`, ẩn `entity.natural_key` với persona không cần | `natural_key` là đường ngược về khoá gốc |
| Chi phí | Glue onto-spark cỡ S/M theo lần chạy; Athena trả theo lượt quét (phân vùng theo `predicate_iri`) | ~0 USD cố định |

---

## Checklist tổng (theo giai đoạn của ONTOLOGY.md mục 11)

| GĐ | Bước | Xong khi |
|---|---|---|
| **1. TBox** | O0, O1, O2, O3, O4 | pytest ontology xanh; `track_a_onto.subclass` ra đủ cây sản phẩm |
| **2. Đồ thị nhóm A + UC2** | O5 lượt 1, O6, O7 | Đối soát khớp silver; hồ sơ 360 một khách chỉ bằng SQL trên đồ thị |
| **3. Suy diễn + model** | O9, O10 | Bảng kết quả O10 có / không có đặc trưng đồ thị |
| **4. Agent MVP** | O11 (catalog, 5 tool đầu, grounding), use case A | 50 câu hỏi: độ chính xác, tỉ lệ có căn cứ, so với text-to-SQL thô |
| **5. Mở rộng** | O5 lượt 2–3, O8, use case B, D | AML với OpenSanctions + IBM AML; Santander vào taxonomy khi có dữ liệu; injection 0% rò |
