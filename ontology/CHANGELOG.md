# CHANGELOG ontology FinGraph

Phiên bản (semver) ghi vào cột `ontology_version` của mọi dòng `track_a_onto.*`.

| Loại thay đổi | Tăng | Hệ quả |
|---|---|---|
| Thêm lớp / quan hệ / nhãn, thêm mã nguồn vào `skos:notation` | minor (`1.x.0`) | Dựng lại đồ thị, dữ liệu cũ vẫn đúng nghĩa |
| Sửa nhãn, định nghĩa, chú thích | patch (`1.0.x`) | Chỉ catalog agent đổi |
| Đổi nghĩa, xoá lớp / quan hệ, đổi cách sinh `uid` | **major** | Dựng lại toàn bộ, kiểm lại trích dẫn agent và embedding |

## 1.0.0 (04-11-2026)

- TBox đầu tiên: bên tham gia (`Party`, `Person`, `Organization`, `Customer`, `SanctionedEntity`, `Bank`), `Account`,
  `ProductHolding` (n-ary), taxonomy sản phẩm 4 nhánh + `AnonymousProduct` (MBD), vị trí, `MerchantCategory`,
  AML (`LaunderingAttempt`, `Typology` + 8 kiểu hình SKOS), `Dataset`, `Recommendation`.
- 20 quan hệ đối tượng, 10 thuộc tính giá trị. Căn chỉnh FIBO ở `alignment/fibo.ttl`.
- 20 câu hỏi năng lực (`competency_questions.yml`).
