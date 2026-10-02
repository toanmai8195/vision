# Bài toán kỹ thuật — phân khúc user (Segmentation / CDP)

> Bước 1 (L0). Chỉ mô tả bài toán; thiết kế ở `CLAUDE.md`, con số quy mô ở `capacity.md`, ví dụ dữ liệu ở `data-flow-examples.md`.
>
> Thứ tự đọc: **thực tế** (§1–§2) → **dữ liệu nền tảng sinh ra** (§3) → **bài toán cần giải** (§4–§5) → **hệ thống vào/ra, ràng buộc** (§6–§7).

> **Phạm vi hiện tại (tối giản): 1 nguồn duy nhất — S2a `user_profile` → `user_city` (MUTEX, STATE).**
> Làm xong cả luồng (bronze → silver → … → activation) với nguồn này rồi mới mở rộng, theo thứ tự:
> bước 11 S1 payment — raw/silver đủ cột, xử lý `MUTEX` (`last_txn_category`) trước; 12 `NOT_MUTEX`; 13 `PARTIAL_VALUE` / `PARTIAL_VALUE_BY_TAG` + `aggFunc`;
> 14 S2b product → `NOT_MUTEX` `STATE`; 15 S4/S5 → `EXTENDED`; 16 S3 churn file → `MUTEX` `EVENT` + REMOVE (xem `checklist.md`).
> Các nguồn còn lại vẫn mô tả đầy đủ ở tài liệu (là đích cuối), nhưng **chưa** ingest/xử lý cho tới khi tới lượt.

## 0. Tóm tắt một đoạn

Nền tảng thanh toán có hàng chục triệu người dùng, mỗi người để lại dấu vết (hồ sơ, giao dịch, sản phẩm, quà, hành vi). Marketer muốn nói "gửi ưu đãi cho nhóm người **như thế này**" và nhận về đúng danh sách người đó, mỗi sáng, cho hàng nghìn nhóm. Vision đọc các dấu vết đó từ DB của nền tảng, tính sẵn ra các "nhóm" và trả lời nhanh các câu hỏi về nhóm. Vision **chỉ đọc**, không tham gia luồng thanh toán.

---

## 1. Thực tế: một ngày của T

T là một người dùng ví/app thanh toán (`user_id = U1001`). Mỗi việc T làm khiến một bộ phận của nền tảng ghi một dòng vào "sổ" (bảng OLTP) của bộ phận đó. Các bảng dưới đây khớp `com/tm/src/ingest/oltp/schema.sql` (schema `src`). Vision chỉ **đọc** các sổ này (qua CDC), không ghi vào.

Cột chung của mọi bảng: `user_id` (TEXT, mã nghiệp vụ như `U1001`; số `uidx` chỉ sinh ở silver, bước 4). `event_ts` (TIMESTAMPTZ, UTC) là lúc sự việc xảy ra; `created_at` (TIMESTAMPTZ) là lúc dòng được ghi vào OLTP. Hai mốc này lệch nhau nhiều ngày = dữ liệu đến muộn.

### 1.1 T đăng ký tài khoản → `user_profile` (sổ hồ sơ)

Khi T đăng ký, dịch vụ hồ sơ tạo một dòng: `user_id = U1001`, `city_code = HCM`. Mỗi người một dòng. Hệ thống cần biết T là ai và ở đâu, vì marketer muốn gửi ưu đãi theo thành phố.
- T chuyển ra Hà Nội, cập nhật hồ sơ: dòng của T được **sửa** từ HCM thành HN.
- Nếu T xoá thông tin thành phố: giá trị thành rỗng (hoặc xoá dòng).
- Đây là **trạng thái hiện tại**, nên Vision phải ghi nhận được "trước là HCM, giờ là HN". Vì vậy bảng đặt `REPLICA IDENTITY FULL` để CDC mang cả giá trị cũ (`before`) lẫn mới (`after`).

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `user_id` | TEXT, **khoá chính** | T là ai; mỗi người đúng một dòng |
| `city_code` | TEXT, cho phép NULL | thành phố hiện tại (`HCM`, `HN`…); NULL = chưa có/đã xoá |
| `created_at` | TIMESTAMPTZ | lúc tạo hồ sơ |
| `updated_at` | TIMESTAMPTZ | lần sửa gần nhất |

Vision dùng để: attribute `user_city` (`MUTEX` + `STATE`).

### 1.2 T mở thêm dịch vụ → `user_product` (sổ sản phẩm đang dùng)

T bật PayLater, rồi mua bảo hiểm. Mỗi sản phẩm là một dòng: `(U1001, paylater)`, `(U1001, insurance)`. Một người có nhiều dòng.
- Hôm sau T đóng PayLater: dòng `(U1001, paylater)` bị **xoá**, dòng bảo hiểm vẫn còn.
- Hệ thống cần biết "T đang dùng sản phẩm nào" để tách nhóm khách có/không có PayLater.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `user_id` | TEXT, khoá chính (1/2) | T là ai |
| `product` | TEXT, khoá chính (2/2) | sản phẩm đang dùng (`paylater`, `insurance`…) |
| `opened_at` | TIMESTAMPTZ | lúc mở sản phẩm |
| `updated_at` | TIMESTAMPTZ | lần sửa gần nhất |

Vision dùng để: attribute `product_holding` (`NOT_MUTEX` + `STATE`). Cũng `REPLICA IDENTITY FULL`.

### 1.3 T quẹt thanh toán → `payment_event` (sổ giao dịch)

T trả 55.000đ ở quán cà phê lúc 10h sáng. Dịch vụ thanh toán ghi **một dòng mới**: ai trả, loại cửa hàng (`mcc 5812` = ăn uống), bao nhiêu, thành công hay thất bại, lúc nào.
- Sổ này chỉ **thêm**, không sửa. T trả 3 lần thì có 3 dòng.
- Giao dịch thất bại cũng được ghi, nhưng Vision không tính vào tổng tiền.
- Vision dùng sổ này để biết "T tiêu bao nhiêu trong 7 ngày" và "T tiêu ở ngành nào".
- Có thể có giao dịch ghi muộn (ngày xảy ra khác ngày ghi), nên có cả `event_ts` và `created_at`.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `event_id` | TEXT, **khoá chính** | mã duy nhất của giao dịch; dùng dedup khi Kafka giao trùng |
| `user_id` | TEXT | ai trả |
| `mcc` | TEXT | mã loại cửa hàng; ánh xạ sang ngành hàng (`5812`,`5814` → fnb; `4722` → travel; `4900` → bill) |
| `amount` | NUMERIC(27,6), ≥ 0 | số tiền |
| `status` | TEXT: `SUCCESS` \| `FAILED` | kết quả; chỉ `SUCCESS` được tính |
| `event_ts` | TIMESTAMPTZ | lúc T trả (UTC) |
| `created_at` | TIMESTAMPTZ | lúc hệ thống ghi |

Vision dùng để: `txn_category` (`NOT_MUTEX`), `txn_amount` (`PARTIAL_VALUE`), `txn_amount_by_category` (`PARTIAL_VALUE_BY_TAG`) — đều `EVENT`.

### 1.4 Khuyến mãi → `voucher_grant` và `oa_follow` *(đề xuất)*

- App tặng T quà `gift_abc` trị giá 50.000đ → thêm dòng vào `voucher_grant` (sổ phát quà). Mã quà là chuỗi tự do, ngày nào cũng có thể có mã mới.
- T bấm theo dõi trang chính thức (OA) của một thương hiệu → thêm dòng `FOLLOW` vào `oa_follow`. Mai T bỏ theo dõi thì thêm dòng `UNFOLLOW` (không sửa dòng cũ).
- Hệ thống cần biết T đã nhận quà gì và đang theo dõi OA nào để nhắc "bạn nhận quà rồi mà chưa theo dõi OA này".

`voucher_grant`:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `event_id` | TEXT, khoá chính | mã duy nhất của lần tặng |
| `user_id` | TEXT | người nhận |
| `voucher_code` | TEXT | mã quà (chuỗi tự do) = **tag** `EXTENDED` |
| `value` | NUMERIC(27,6), ≥ 0 | giá trị quà |
| `event_ts`, `created_at` | TIMESTAMPTZ | như mô tả chung |

`oa_follow`:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `event_id` | TEXT, khoá chính | mã duy nhất của lần thao tác |
| `user_id` | TEXT | người thao tác |
| `oa_code` | TEXT | mã OA (chuỗi tự do) = **tag** `EXTENDED` |
| `action` | TEXT: `FOLLOW` \| `UNFOLLOW` | theo dõi / bỏ theo dõi (UNFOLLOW = signal REMOVE) |
| `event_ts`, `created_at` | TIMESTAMPTZ | như mô tả chung |

Vision dùng để: `gift_value` (`PARTIAL_VALUE_BY_TAG` `SUM`, `EXTENDED`), `oa_follow` (`NOT_MUTEX`, `EXTENDED`).

### 1.5 T dùng app → `app_event` (sổ hành vi) *(đề xuất)*

T mở màn hình khuyến mãi, hệ thống ghi một dòng `view_promo`. Nhìn lại 2 lần thì có 2 dòng. Vision dùng sổ này để đếm số lần T làm một hành vi.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `event_id` | TEXT, khoá chính | mã duy nhất của lần xảy ra |
| `user_id` | TEXT | ai làm |
| `event_name` | TEXT | tên hành vi (`view_promo`, `open_screen_x`…) = **tag** `EXTENDED` |
| `event_ts`, `created_at` | TIMESTAMPTZ | như mô tả chung |

Vision dùng để: `PARTIAL_VALUE_BY_TAG` với `COUNT` (số lần), `NOT_MUTEX` (đã có hành vi X trong window), đều `EXTENDED`.

### 1.6 Điểm rủi ro rời bỏ → file parquet, không có bảng

Mỗi đêm đội AI chạy mô hình, chấm điểm "T có sắp bỏ app không" rồi xuất ra file parquet (không qua OLTP), nên `schema.sql` không có bảng cho nguồn này.

| Cột (file) | Ý nghĩa |
|---|---|
| `user_id` | người được chấm |
| `score` | điểm rời bỏ (0–1); band low `<0.3`, mid `[0.3,0.7)`, high `≥0.7` |
| `model_version` | phiên bản mô hình (vd `churn-v7`) |
| `scored_at` | lúc chấm |

Vision dùng để: `churn_score_band` (`MUTEX` + `EVENT`).

### 1.7 Hai kiểu sổ, và cách dùng chung

- **Sổ sự kiện** (giao dịch, quà, OA, hành vi, điểm ML): chỉ thêm. Mỗi dòng nói "tại lúc này, có chuyện X xảy ra".
- **Sổ trạng thái** (hồ sơ, sản phẩm): sửa/xoá. Mỗi dòng nói "hiện giờ T đang như thế nào". T không đổi gì suốt 2 năm thì **không có dòng mới nào**, nhưng T vẫn đang ở HN — Vision phải hiểu được điều đó (`CLAUDE.md` §3.4, `STATE`).

Marketer hỏi: *"tìm những người ở Hà Nội, có nguy cơ rời bỏ cao, tuần này chưa ăn uống"*. Vision đọc các sổ trên, gom lại thành danh sách người dùng.

---

## 2. Thực tế: quy mô nền tảng

| | Thực tế | Ghi chú |
|---|---|---|
| Số user | **~30M** | thiết kế của Vision vẫn giữ **100M** (`CLAUDE.md` §0) làm headroom |
| Giao dịch (payment event) | **~1M/ngày** (~12/giây trung bình) | nhỏ hơn giả định 10M/ngày ở `capacity.md` → ước lượng chi phí ở đó là bảo thủ |
| Nhịp xử lý | T+1 theo `ds` (Asia/Ho_Chi_Minh) | dữ liệu ngày hôm qua xử lý xong trước sáng nay |

---

## 3. Nguồn dữ liệu và ca biên

Từ góc nhìn Vision, mỗi sổ ở §1 là một **nguồn**. Cột "Ai sinh ra" là vai trò hệ thống, chưa gắn tên team/dịch vụ thật. Ví dụ dữ liệu cụ thể: `data-flow-examples.md` §1–§4, và các khối ví dụ ngay dưới mỗi bảng trong `schema.sql`.

| Nguồn | Ai sinh ra | Dạng | Khoá / thời gian | Vào Vision như loại dữ liệu | Đường vào (L1) |
|---|---|---|---|---|---|
| **S1 Payment** (`payment_event`) | dịch vụ thanh toán, insert-only | **event** | `event_id` · `event_ts` (UTC) · cột raw: `user_id, mcc, amount, status (SUCCESS/FAILED)` | `NOT_MUTEX` (ngành hàng theo MCC), `PARTIAL_VALUE` (tổng tiền), `PARTIAL_VALUE_BY_TAG` (tiền theo ngành) — đều `EVENT` | CDC → Kafka → Flink → bronze |
| **S2a Profile** (`user_profile`) | dịch vụ hồ sơ; có update/delete | **snapshot (trạng thái)** | `user_id` · thời điểm CDC `ts_ms` | `MUTEX` `STATE` (city) | CDC → bronze → SCD2 |
| **S2b Product holding** (`user_product`) | dịch vụ sản phẩm; thêm/đóng sản phẩm | **snapshot (trạng thái)** | `(user_id, product)` · thời điểm CDC | `NOT_MUTEX` `STATE` | CDC → bronze → SCD2 |
| **S3 Churn score** (file ML) | job ML batch hằng ngày, ghi parquet `dt=<ds>`; cột `user_id, score, model_version, scored_at` | **event theo batch** (mỗi dòng = một lần chấm) | `user_id` · `scored_at` | `MUTEX` `EVENT` (band low/mid/high) | file loader (sensor `_SUCCESS`) → bronze |
| **S4 Voucher/quà/OA** *(đề xuất)* (`voucher_grant`, `oa_follow`) | dịch vụ khuyến mãi và dịch vụ OA, insert-only | **event** | `event_id` · `event_ts`; tag = `voucher_code`, `oa_code` (chuỗi tự do) | `EXTENDED`: `PARTIAL_VALUE_BY_TAG` (giá trị quà theo mã), `NOT_MUTEX` (follow OA, có REMOVE khi unfollow) | CDC → Kafka → Flink → bronze (như S1) |
| **S5 Hành vi app** *(đề xuất)* (`app_event`) | app/collector, insert-only | **event** | `event_id` · `event_ts`; tag = `event_name` (vd `view_promo`) | `EXTENDED`: `PARTIAL_VALUE_BY_TAG` với `COUNT` (số lần), `NOT_MUTEX` (đã có hành vi X trong window) | CDC → Kafka → Flink → bronze (như S1) |

> S4, S5 là **đề xuất**, dùng đúng các loại dữ liệu đã có, không cần đổi `CLAUDE.md`. Tag của chúng là chuỗi tự do, cardinality cao (`CLAUDE.md` §3.6). Ví dụ mẫu `data-flow-examples.md` mới có S1, S2, S3 và ví dụ EXTENDED (`gift_value`, `oa_follow`).

### Ca biên phải xử lý (mỗi ca có trong ví dụ/golden)

| Nguồn | Ca biên | Xử lý dự kiến | Ví dụ |
|---|---|---|---|
| S1 | giao dịch **giao trùng** (at-least-once) cùng `event_id` | dedup `event_id` ở silver | `e-9001` §1 |
| S1 | **đến muộn** (`event_ts` cũ hơn ngày đang xử lý) | tính lại `ds` cũ; ≤ 3 ngày tự reprocess, hơn → backfill | `e-9005` §1 |
| S1 | `event_ts` UTC nhưng ngày nghiệp vụ theo **ICT** (lệch ngày) | `ds` = ngày theo Asia/Ho_Chi_Minh | `e-9003` §1 |
| S1 | giao dịch **FAILED** | không vào tag/số tiền (chỉ `SUCCESS`) | `e-9004` §1 |
| S1 | một user **nhiều tag cùng ngày** | hợp lệ với NOT_MUTEX | U1001 §1 |
| S2a | **đổi giá trị** (city HCM → HN) | version cũ đóng (`REMOVED`), version mới mở (`ADDED`) | U1001 §2.1 |
| S2a | **user mới** | cấp `uidx` mới, `ADDED` | U1002 §2.1 |
| S2a | user **không đổi** từ lâu | vẫn thuộc tag ở mọi window (đó là lý do dùng `STATE`) | U1003 §2.1 |
| S2a | **xoá** giá trị (mất city) | `REMOVED`, không tag nào | quy tắc §3.4 `CLAUDE.md` |
| S2b | **đóng** sản phẩm nhưng còn sản phẩm khác | chỉ tag đó `REMOVED`, tag khác giữ (NOT_MUTEX) | U1001 §2.2 |
| S3 | model **chỉ chấm một phần user** mỗi ngày | user không được chấm ngày đó không có signal; window lấy lần chấm gần nhất | §3 |
| S3 | user **đổi band** giữa các ngày | ADD band mới xoá band cũ (MUTEX, không quay lại tag cũ) | U1001 §3 |
| S3 | **REMOVE** sau ADD | không thuộc tag nào | X, Y, Z §4 |
| S3 | file ngày `dt=<ds>` **chạy lại** | silver ghi idempotent theo `ds` | — |
| S4 | **unfollow** OA | signal REMOVE cho tag `oa_code`; window lấy signal gần nhất (NOT_MUTEX) | §7.2 |
| S4 | user nhận quà nhiều lần cùng mã trong window | AGG theo `aggFunc` của attribute (vd `SUM`) | §7.3 |
| S5 | **khối lượng lớn** hơn payment nhiều lần | xem ghi chú dưới bảng | — |
| Chung | tag/giá trị mới xuất hiện ở nguồn EXTENDED | cấp `tag_id` mới trong `tag_dict`; chưa có trong dictionary → bitmap rỗng | §7.2 |

(Cột "Ví dụ": `§n` không kèm tên file là mục của `data-flow-examples.md`.)

**Ghi chú S4/S5 (đề xuất, chờ bạn xác nhận ở `Done khi`):**
- Đi cùng đường CDC như payment để giữ một cách ingest duy nhất; không ghi song song OLAP (`CLAUDE.md` §6).
- Không cần đổi thiết kế: S4 khớp `gift_value`, `oa_follow` ở `data-flow-examples.md` §7. S5 dùng `COUNT` (`CLAUDE.md` §3.2.1).
- S5 ở seed bước 2 chỉ cần vài loại `event_name` với ít dòng. `TODO(verify)`: nếu thực tế mỗi user sinh hàng chục sự kiện/ngày (hàng trăm triệu dòng/ngày), cân nhắc app/collector gộp theo `(user, event_name, ngày)` trước khi vào OLTP rồi dùng `SUM` trên số lần — quyết khi có số đo, không ảnh hưởng bước 2.
- Hoàn tiền/huỷ giao dịch: ngoài phạm vi v1.

**Giả định về nền tảng thật (chưa có tài liệu xác nhận, `TODO(verify)`):** mỗi giao dịch có sẵn mã duy nhất làm `event_id`; "đóng sản phẩm" được ghi bằng xoá dòng (nếu dịch vụ thật chỉ đặt cờ `closed_at`/`status` thì schema phải đổi).

---

## 4. Bài toán: phân khúc user để làm gì

Có dữ liệu ở §3, nghiệp vụ muốn chia người dùng thành **segment** theo hành vi và thuộc tính — vd *"chi F&B ≥ 500K trong 7 ngày, ở Hà Nội, không có nguy cơ rời bỏ cao"* — rồi dùng segment đó cho:

| Mục đích | Cách hệ thống phục vụ |
|---|---|
| Gửi campaign / push | hệ thống khác lấy **danh sách user** của segment (`users by segment`, export async với segment lớn) |
| Cá nhân hoá realtime | app/backend hỏi "user này thuộc segment nào" / "có thuộc segment X không" (`segments by user`, `contains`) |
| Đếm / ước lượng quy mô | xem số user trước khi chạy campaign (`count`, `estimate` lúc tạo segment) |
| Phân tích / báo cáo | export segment ra bảng cho BI, A/B test, đo hiệu quả |

**Ai dùng**
- **Marketer** tự định nghĩa segment (UI/API) bằng rule AND / OR / SUB trên các attribute.
- **Data/CRM team & hệ thống khác** tạo và gọi segment qua API.
- Hệ quả: ~5K segment tạo/rebuild mỗi ngày, phần lớn dùng chung condition (vd `churn A30`, `city`) → cần cache condition.

**Vì sao khó:** 30–100M user × hàng trăm attribute × nhiều khoảng ngày (7, 30, 90, 180 ngày…) × 5K segment mỗi sáng. Quét lại toàn bộ giao dịch trong khoảng ngày cho từng segment là không khả thi, nên Vision tính sẵn trạng thái theo ngày rồi ghép lại (`CLAUDE.md` §0, §4).

---

## 5. Ví dụ segment cụ thể

**Segment đầu tiên (phạm vi hiện tại):** `seg_0001` — "user đang ở HN" = `user_city hn A7` → `{1,3}`, chỉ cần nguồn `user_profile` (`data-flow-examples.md` §0.1). Ba segment dưới là đích cuối, cần thêm nguồn.

Ba segment minh hoạ; dữ liệu qua từng layer (L0 → L7) và kết quả bitmap ở `data-flow-examples.md` §1–§7 (golden test). As-of `ds = 2026-09-15`, 4 user `U1001..U1004`. Mục đích nghiệp vụ dưới đây chỉ để minh hoạ.

| Segment | Ý nghĩa nghiệp vụ | Rule | Loại dữ liệu dùng | Kết quả |
|---|---|---|---|---|
| `seg_1001` | Kéo lại user có nguy cơ rời bỏ ở HN chưa mua F&B gần đây → gửi ưu đãi F&B | `(churn mid/high A30 ∩ city hn A7) − F&B A7` | MUTEX `EVENT` (churn), MUTEX `STATE` (city), NOT_MUTEX `EVENT` (ngành hàng) | `{3}` |
| `seg_1002` | User chi tiêu lớn 7 ngày → chăm sóc VIP / đo tác động | `SUM amount A7 ≥ 1M` ∪ `SUM F&B A7 ≥ 500K` | PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG | `{1,2}` |
| `seg_1003` | Nhận quà ≥ 100K nhưng chưa follow OA cụ thể → nhắc follow | `SUM gift A7 ≥ 100K − follow oa_12345 A7` | PARTIAL_VALUE_BY_TAG `EXTENDED`, NOT_MUTEX `EXTENDED` | `{1}` |

Ba ví dụ này (đích cuối) chạm đủ 4 loại dữ liệu, cả `EVENT`/`STATE` và `STANDARD`/`EXTENDED`. Ca biên đi kèm (trùng `event_id`, đến muộn, ngày ICT ≠ UTC, REMOVE trong MUTEX) ở `data-flow-examples.md` §1, §4.

---

## 6. Vào / ra của hệ thống

**Vào**
- Dữ liệu nguồn ở OLTP (§3): payment, profile, product holding, và đề xuất voucher/OA, hành vi app; điểm rời bỏ từ file ML.
- Định nghĩa segment (DSL): điều kiện trên attribute + tag + date range (+ `valueRange` với dữ liệu số) + lịch build.

**Ra**
- Với mỗi segment: tập user (bitmap) theo **version**, kèm `asOfDs`.
- API: `count`, `users`, `contains`, `segments by user`, export async.

**Bốn loại dữ liệu** hệ thống phải xử lý (đích cuối; hiện làm trước `MUTEX` `STATE`): `MUTEX`, `NOT_MUTEX`, `PARTIAL_VALUE`, `PARTIAL_VALUE_BY_TAG` (`CLAUDE.md` §3.2).

---

## 7. Ràng buộc

**Quy mô** (thiết kế, chưa đo; thực tế ~30M user, ~1M giao dịch/ngày — §2): 100M user · 500 attribute · 500–1000 tag/attribute (≤ 500K tag) · mọi date range mỗi ngày · 5K segment/ngày · lịch sử 400 ngày.

**Thời gian & độ tươi**
- Pipeline chạy **T+1** theo `ds` (Asia/Ho_Chi_Minh). Không realtime trong ngày (`A0` ngoài phạm vi v1).
- SLA (ICT): silver 02:00 · daily 03:30 · range 05:00 · DQ 05:15 · **5K segment published trước 07:00**.
- Late data ≤ 3 ngày tự reprocess; cũ hơn → backfill thủ công.

**Serving (ONLINE)**: `contains` / `count` p99 < 10ms · `segments by user` p99 < 20ms · availability 99.9%.

**Đúng đắn**
- Kết quả phải khớp định nghĩa ở `CLAUDE.md` §3.2 (kiểm bằng reference implementation + golden test).
- Build lại cùng `(segment_id, version)` ra cùng kết quả (idempotent).
- Consumer chỉ thấy dữ liệu của ngày đã qua DQ; DQ mức BLOCK fail → không publish.

**Bảo mật/riêng tư**: bitmap và file segment chỉ chứa `uidx`; không log `user_id`/SĐT ở INFO.

**Ngoài phạm vi v1**: realtime trong ngày (`A0`) · `AVG` / `DISTINCT_COUNT` / `FIRST` / `LAST` cho partial value · `EXTENDED` với MUTEX · hoàn tiền/huỷ giao dịch.
