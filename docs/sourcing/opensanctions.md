# OpenSanctions — danh sách cấm vận

Cơ sở dữ liệu mở về cá nhân và tổ chức bị cấm vận, gộp từ hàng trăm danh sách chính thức (OFAC,
EU, UN, UK...). Dùng cho **sàng lọc AML**, tức là so khách hàng và đối tác với danh sách cấm vận,
và cho **đồ thị quan hệ** (sở hữu, điều hành). Đây là nguồn **thật**, không giả lập.

| | |
|---|---|
| Loại | File bulk qua HTTPS, cập nhật nhiều lần mỗi ngày |
| Giấy phép | **CC BY-NC 4.0**: phi thương mại (đồ án) dùng được, phải ghi nguồn. Thương mại cần mua giấy phép |
| Credential | Không cần (bulk data công khai). API `api.opensanctions.org` cần key, **không dùng** ở sourcing |
| Entry | `opensanctions_targets`, `opensanctions_entities` (CDC5), `opensanctions_delta` (CDC1). Đều `src-shell`, bật |
| Dataset raw | `landing/opensanctions/{targets, entities, delta}/` |

## 1. Chọn bộ dữ liệu (đo ngày 11-10-2026)

| Bộ | Entity | Đối tượng (target) | `entities.ftm.json` | `targets.simple.csv` |
|---|---|---|---|---|
| `default` (cấm vận + PEP + ...) | 4.149.453 | 1.240.874 | **2.639 MB** | 466 MB |
| **`sanctions`** (dùng) | 302.339 | 72.481 | 367 MB | **74 MB** |

Bộ `default` lớn chủ yếu do PEP (người có ảnh hưởng chính trị). Với Glue Python Shell 1 GB RAM và
timeout tính theo phút thì dùng bộ `sanctions`. Có PEP thì sàng lọc đầy đủ hơn, nhưng đó là việc
cho Spark sau này.

## 2. Các file và URL

| File | URL | Định dạng |
|---|---|---|
| Chỉ mục | `https://data.opensanctions.org/datasets/latest/sanctions/index.json` | JSON: `version`, `entity_count`, `resources[]`, **`delta_url`** |
| Bản phẳng | `.../latest/sanctions/targets.simple.csv` | CSV, mỗi dòng một đối tượng |
| Bản đầy đủ | `.../latest/sanctions/entities.ftm.json` | NDJSON, mô hình **FollowTheMoney** |
| Danh sách delta | `delta_url` → `https://data.opensanctions.org/artifacts/sanctions/<version>/delta.json` | `{"versions": {"<version>": "<url entities.delta.json>"}}`, giữ **100 phiên bản** gần nhất (~25 ngày, ~4 bản/ngày) |
| Một delta | `.../artifacts/sanctions/<version>/entities.delta.json` | NDJSON `{"op": "ADD"|"MOD"|"DEL", "entity": {...}}` |

Phiên bản có dạng `20261010194704-euu` (thời điểm UTC + hậu tố), nên **sắp theo thứ tự từ điển
cũng là sắp theo thời gian**. Đây là điều kiện để dùng làm mốc CDC1.

## 3. Schema

### `targets.simple.csv`

| Cột | Ghi chú |
|---|---|
| `id` | Mã entity ổn định (`NK-...`, `ofac-...`) |
| `schema` | `Person`, `Company`, `Organization`, `Vessel`, `Airplane`, `CryptoWallet`... |
| `name`, `aliases` | Tên chính, các tên khác ngăn bằng `;`. Nhiều hệ chữ (Latin, Kirin, Ả Rập...) |
| `birth_date`, `countries`, `addresses`, `identifiers`, `phones`, `emails` | Đa giá trị, ngăn bằng `;` |
| `sanctions` | Mô tả cấm vận, **có thể dài hàng chục KB** (đã nâng `csv.field_size_limit`) |
| `program_ids`, `dataset` | Chương trình cấm vận, danh sách nguồn |
| `first_seen`, `last_seen`, `last_change` | |

### Entity FollowTheMoney (`entities.ftm.json`, `entity` trong delta)

```json
{"id": "NK-38BEqT6evkH57TJTZnikgM", "caption": "Ownership", "schema": "Ownership",
 "referents": ["gb-coh-...", "ofac-..."], "datasets": ["us_ofac_sdn"],
 "first_seen": "...", "last_seen": "...", "last_change": "...",
 "properties": {"asset": ["NK-msL5..."], "owner": ["NK-e7Fj..."], "role": ["Owned or Controlled By"]},
 "target": false}
```

- `properties` là map tên thuộc tính → **mảng** giá trị. Silver phải explode.
- Schema quan hệ (`Ownership`, `Directorship`, `Family`, `Associate`, `UnknownLink`) là **cạnh đồ
  thị** có sẵn: `owner` → `asset`, `director` → `organization`.
- Thuộc tính **`leiCode`** của Company/Organization là khoá nối sang **GLEIF**, khoá thật duy nhất
  giữa các nguồn công khai.
- `target: true` nghĩa là đối tượng chính bị cấm vận. `false` là thực thể liên quan.

---

## 4. Entry sourcing (reader `httpfile`)

| Entry | Lấy gì | CDC | Mốc | Lịch đề xuất |
|---|---|---|---|---|
| `opensanctions_targets` | `targets.simple.csv` (74 MB) | CDC5 | — | Chủ nhật hằng tuần |
| `opensanctions_entities` | `entities.ftm.json` (~370 MB) | CDC5 | — | Chủ nhật hằng tuần |
| `opensanctions_delta` | index → `delta_url` → các `entities.delta.json` mới hơn mốc | CDC1 | `_file_key` = chuỗi phiên bản | 6 giờ một lần |

Reader `httpfile` **tải và parse theo luồng**: không tải về đĩa, không nạp vào RAM. Đo trên
`opensanctions_targets`: 73.595 bản ghi, **RAM đỉnh 211 MB**, 75 giây. Writer cắt file raw theo
ngưỡng 64 MB nén hoặc 100.000 dòng.

**Kiểm tra liên tục** (`require_contiguous: true`): nguồn chỉ giữ 100 delta gần nhất. Nếu mốc đã
lưu cũ hơn phiên bản cũ nhất còn lại, các delta ở giữa đã mất. Job khi đó **báo lỗi** thay vì nạp
tiếp và để silver thiếu thay đổi. Cách xử lý: chạy `opensanctions_entities` để lấy bản đầy đủ, rồi
`reset_checkpoint("opensanctions_delta")`. Chỉ cần chạy delta ít nhất mỗi ~20 ngày là không bao
giờ gặp lỗi này.

Raw mẫu (`opensanctions/delta`):

```json
{"op": "ADD", "entity": {"id": "NK-38BEqT6evkH57TJTZnikgM", "schema": "Ownership", "...": "..."},
 "_src_file": "20260919014701-llq",
 "_src_url": "https://data.opensanctions.org/artifacts/sanctions/20260919014701-llq/entities.delta.json",
 "_src_row": 1, "_src_job": "opensanctions_delta", "_src_type": "httpfile", "_src_cdc": "cdc1", "...": "..."}
```

## 5. Dùng để sàng lọc

- Phía khách hàng: `corebank_client.full_name` (+ `birth_date`), chủ thẻ trong `cards_authorizations`.
- Ca kiểm thử: `simulators.corebank.seed --sanction-hits N` gieo N khách mang tên người trong danh
  sách. Đáp án ở `data/ground_truth/sanction_hits.csv`, dùng để đo recall của bước screening.
- Tên đa hệ chữ (Kirin, Ả Rập): bước so khớp cần chuyển tự (transliteration) và so mờ (fuzzy).
  So bằng nhau tuyệt đối sẽ bỏ sót phần lớn.

## 6. Chất lượng dữ liệu: việc của silver

| Vấn đề | Xử lý |
|---|---|
| Đa giá trị ngăn `;` (CSV) và mảng (FtM) | Explode |
| Entity được gộp (merge) giữa các phiên bản: `id` cũ thành `referents` của `id` mới | Dựng bảng ánh xạ id cũ → id hiện hành từ `referents` |
| Delta `DEL` | Đánh dấu xoá mềm ở silver |
| CDC5 hằng tuần cùng delta 6 giờ chồng lấn | Khử trùng theo `id` + `last_change` |
