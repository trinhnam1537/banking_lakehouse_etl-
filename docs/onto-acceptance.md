# Nghiệm thu dữ liệu đồ thị FinGraph (2026-11-19)

Sinh bởi `scripts/onto_acceptance.py` (persona data engineer, chỉ đọc). Kiểm tra mã nguồn và logic nằm ở `pytest tests/ontology tests/transforms` (96 test đạt ngày 11-19).

## Kết quả kiểm tra

| Mục | Kết quả | Bằng chứng |
|---|---|---|
| O7: entity/triple cùng một run_id | đạt | entity run_id ['jr_1b6b70726c5c8f3979733afdb420b60634aa3737767c2a735337b0f41f4f9075'], triple run_id ['jr_1b6b70726c5c8f3979733afdb420b60634aa3737767c2a735337b0f41f4f9075'] |
| O7: số node/cạnh theo lớp và predicate đã ghi | đạt | 3,218,537 node, 7,997,661 cạnh (bảng chi tiết bên dưới) |
| O6: mọi cạnh có valid_from | **trượt** | cạnh thiếu valid_from: [{'predicate_iri': 'fin:partOf', 'n': '170'}, {'predicate_iri': 'fin:occupation', 'n': '85781'}, {'predicate_iri': 'fin:involvedIn', 'n': '3842'}, {'predicate_iri': 'fin:hasTypology', 'n': '2756'}, {'predicate_iri': 'fin:hasHolding', 'n': '9929'}, {'predicate_iri': 'fin:isHeldBy', 'n': '2087786'}, {'predicate_iri': 'fin:ofProduct', 'n': '9929'}, {'predicate_iri': 'fin:sameAs', 'n': '5369'}, {'predicate_iri': 'fin:atBank', 'n': '2087786'}, {'predicate_iri': 'fin:isDirectlyOwnedBy', 'n': '4081'}] |
| O8: không có same_as giữa hai vũ trụ không có khoá chung | đạt | cặp nguồn: [('berka', 'corebank', 2704), ('corebank', 'berka', 2665)]; ngoài danh sách cho phép: [] |
| O8: same_as đã xác nhận đều có confidence >= 0.9 | đạt | confidence nhỏ nhất theo cặp: ['1.0', '1.0'] |
| O8: hàng đợi duyệt same_as_review có confidence trong [0.6, 0.9) | đạt | 14 ứng viên, confidence 0.6071428571428572..0.8 |
| O9: closure không chu trình, độ sâu <= MAX_DEPTH (6) | đạt | 5209 dòng, độ sâu lớn nhất 6, vòng về chính nó 0 |
| O9: mỗi cạnh suy diễn truy ngược được về luật (mapping_id) và cạnh gốc (source_key) | đạt | [('fin:sameAs', 5369, 'same_as.corebank_berka_client', 0)] |
| O5: sản phẩm Berka trên đồ thị khớp track_a_gold.berka_product_event (số khách theo sản phẩm) | đạt | (sản phẩm, gold, đồ thị): [('loan', 682, 682), ('card_classic', 659, 659), ('card_junior', 145, 145), ('card_gold', 88, 88), ('household', 3297, 3297), ('insurance', 510, 510)] |
| O7: dựng được hồ sơ 360 một khách chỉ bằng SQL trên entity + triple | đạt | Khách 2 (Berka): 5 dòng, loại ['holding', 'holdsAccount', 'locatedIn'] |
| O10: Recommendation có trong đồ thị | **trượt** | 0 node fin:Recommendation |
| NEPTUNE: đỉnh ngoài vùng AML khớp entity Iceberg | **trượt** | Neptune 719,324 đỉnh / 1,580,836 cạnh; đỉnh không phải AML: Neptune 265,464 vs Iceberg 337,524 |

## Số node theo lớp và nguồn (3,218,537)

| Lớp | Nguồn | Số node |
|---|---|---:|
| `fin:Account` | aml | 2,087,786 |
| `fin:Party` | aml | 668,138 |
| `fin:Bank` | aml | 122,333 |
| `fin:Customer` | mbd | 100,224 |
| `fin:Customer` | bank_marketing | 86,399 |
| `fin:Party` | opensanctions | 72,060 |
| `fin:Account` | corebank | 13,860 |
| `fin:Account` | berka | 12,164 |
| `fin:ProductHolding` | mbd | 12,070 |
| `fin:ProductHolding` | bank_marketing | 9,929 |
| `fin:ProductHolding` | corebank | 9,924 |
| `fin:ProductHolding` | berka | 9,881 |
| `fin:Customer` | corebank | 5,403 |
| `fin:Customer` | berka | 5,369 |
| `fin:LaunderingAttempt` | aml | 2,756 |
| `fin:District` | corebank | 77 |
| `fin:District` | berka | 77 |
| `fin:Bank` | berka | 13 |
| `fin:Bank` | corebank | 13 |
| `fin:Region` | berka | 8 |
| `fin:Typology` | ontology | 8 |
| `fin:Region` | corebank | 8 |
| `fin:DepositProduct` | ontology | 1 |
| `fin:AnonymousProduct` | ontology | 1 |
| `fin:CreditCard` | ontology | 1 |
| `fin:AnonymousProduct_3` | ontology | 1 |
| `fin:InsuranceProduct` | ontology | 1 |
| `fin:ClassicCard` | ontology | 1 |
| `fin:GoldCard` | ontology | 1 |
| `fin:RecurringPayment` | ontology | 1 |
| `fin:Derivative` | ontology | 1 |
| `fin:PensionReceipt` | ontology | 1 |
| `fin:JuniorCard` | ontology | 1 |
| `fin:ShortTermDeposit` | ontology | 1 |
| `fin:AnonymousProduct_2` | ontology | 1 |
| `fin:PayrollAccount` | ontology | 1 |
| `fin:TaxPayment` | ontology | 1 |
| `fin:JuniorAccount` | ontology | 1 |
| `fin:AnonymousProduct_4` | ontology | 1 |
| `fin:PaymentService` | ontology | 1 |
| `fin:LongTermDeposit` | ontology | 1 |
| `fin:SavingsAccount` | ontology | 1 |
| `fin:Guarantee` | ontology | 1 |
| `fin:Product` | ontology | 1 |
| `fin:Mortgage` | ontology | 1 |
| `fin:CurrentAccount` | ontology | 1 |
| `fin:TermDeposit` | ontology | 1 |
| `fin:CreditProduct` | ontology | 1 |
| `fin:Loan` | ontology | 1 |
| `fin:PensionPlan` | ontology | 1 |
| `fin:MediumTermDeposit` | ontology | 1 |
| `fin:InvestmentProduct` | ontology | 1 |
| `fin:Securities` | ontology | 1 |
| `fin:Country` | corebank | 1 |
| `fin:PayrollService` | ontology | 1 |
| `fin:Country` | berka | 1 |
| `fin:Fund` | ontology | 1 |
| `fin:AnonymousProduct_1` | ontology | 1 |
| `fin:EAccount` | ontology | 1 |

## Số cạnh theo predicate (7,997,661)

| Predicate | Suy diễn | Số cạnh |
|---|---|---:|
| `fin:transfersTo` | false | 3,543,799 |
| `fin:atBank` | false | 2,104,776 |
| `fin:isHeldBy` | false | 2,087,786 |
| `fin:occupation` | false | 85,781 |
| `fin:hasHolding` | false | 41,804 |
| `fin:ofProduct` | false | 41,804 |
| `fin:involvedIn` | false | 26,424 |
| `fin:locatedIn` | false | 19,806 |
| `fin:viaAccount` | false | 19,805 |
| `fin:holdsAccount` | false | 9,034 |
| `fin:sameAs` | true | 5,369 |
| `fin:isDirectlyOwnedBy` | false | 4,081 |
| `fin:hasTypology` | false | 2,756 |
| `fin:isAuthorizedOn` | false | 1,738 |
| `fin:amount` | false | 1,364 |
| `fin:holdingStatus` | false | 1,364 |
| `fin:partOf` | false | 170 |
