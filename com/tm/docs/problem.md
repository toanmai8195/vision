# Bài toán kỹ thuật — phân khúc user (Segmentation / CDP)

> Bước 1 (L0). Chỉ mô tả bài toán; thiết kế ở `CLAUDE.md`, con số quy mô ở `capacity.md`, ví dụ dữ liệu ở `data-flow-examples.md`.

## 0. Bối cảnh hệ thống

Hệ thống nguồn là **nền tảng thanh toán** (ví/app) phục vụ người dùng cuối. Vision đọc dữ liệu của nền tảng này để phân khúc user; không tham gia luồng thanh toán.

| | Thực tế | Ghi chú |
|---|---|---|
| Số user | **~30M** | thiết kế của Vision vẫn giữ **100M** (`CLAUDE.md` §0) làm headroom; 30M nằm trong dư địa đó |
| Giao dịch (payment event) | **~1M/ngày** (~12/giây trung bình) | nhỏ hơn giả định 10M/ngày ở `capacity.md` → ước lượng chi phí trong `capacity.md` là bảo thủ |
| Nhịp xử lý | T+1 theo `ds` (Asia/Ho_Chi_Minh) | |

### Dữ liệu raw hệ thống ghi lại

| Nguồn | Nội dung raw | Dạng | Vào Vision như |
|---|---|---|---|
| **Payment** (OLTP) | `event_id, user_id, mcc, amount, status (SUCCESS/FAILED), event_ts` — mỗi giao dịch một dòng, insert-only | event | `NOT_MUTEX` (ngành hàng theo MCC), `PARTIAL_VALUE` (tổng tiền), `PARTIAL_VALUE_BY_TAG` (tiền theo ngành) |
| **Profile** (OLTP, CDC) | `user_id, city_code, …` — có update/delete | snapshot/trạng thái | `MUTEX` `STATE` (city) |
| **Product holding** (OLTP, CDC) | sản phẩm user đang dùng (paylater, bảo hiểm) — thêm/bớt | snapshot/trạng thái | `NOT_MUTEX` `STATE` |
| **Churn score** (file ML) | `user_id, score, model_version, scored_at` — parquet theo ngày | event theo batch | `MUTEX` `EVENT` (band low/mid/high) |
| **Voucher / quà / campaign** | mã quà, mã campaign user nhận/dùng | event | attribute `EXTENDED` (tag là chuỗi tự do, cardinality cao — `CLAUDE.md` §3.6) |
| **Hành vi app** (click/view) | sự kiện dùng app | event | *chưa có trong thiết kế* — xem lưu ý dưới |

**Lưu ý / cần chốt ở các task sau**
- `TODO(verify)`: **Hành vi app** có khối lượng lớn hơn payment nhiều lần, schema chưa rõ (loại sự kiện, trường, số lượng/ngày). Cần chốt trước khi đưa vào bước 2; nếu đưa vào thì thuộc loại dữ liệu nào (nhiều khả năng `NOT_MUTEX` theo màn hình, `PARTIAL_VALUE(_BY_TAG)` với `COUNT`) và `attributeType` nào. Chưa đổi `CLAUDE.md`; đổi thì theo §13.4.
- `TODO(verify)`: schema chi tiết Voucher/campaign (trường, ai sinh, dạng OLTP hay file).
- Ví dụ mẫu `data-flow-examples.md` mới có payment, profile, product, churn score.

## 1. Phân khúc user để làm gì

Nghiệp vụ cần chia người dùng thành **segment** theo hành vi và thuộc tính (vd "chi F&B ≥ 500K trong 7 ngày, ở Hà Nội, không có nguy cơ rời bỏ cao") rồi dùng segment đó cho:

| Mục đích | Cách hệ thống phục vụ |
|---|---|
| Gửi campaign / push | hệ thống khác lấy **danh sách user** của segment (`users by segment`, export async với segment lớn) |
| Cá nhân hoá realtime | app/backend hỏi "user này thuộc segment nào" / "có thuộc segment X không" (`segments by user`, `contains`) |
| Đếm / ước lượng quy mô | xem số user trước khi chạy campaign (`count`, `estimate` lúc tạo segment) |
| Phân tích / báo cáo | export segment ra bảng cho BI, A/B test, đo hiệu quả |

## 2. Ai dùng

- **Marketer** tự định nghĩa segment (UI/API) bằng rule AND / OR / SUB trên các attribute.
- **Data/CRM team & hệ thống khác** tạo và gọi segment qua API.
- Hệ quả: ~5K segment tạo/rebuild mỗi ngày, phần lớn dùng chung condition (vd `churn A30`, `city`) → cần cache condition.

## 3. Vào / ra của hệ thống

**Vào**
- Dữ liệu nguồn ở OLTP: sự kiện thanh toán (payment), trạng thái hồ sơ (profile, product), điểm rời bỏ từ file ML (churn score); dự kiến thêm voucher/campaign và hành vi app (§0).
- Định nghĩa segment (DSL): điều kiện trên attribute + tag + date range (+ `valueRange` với dữ liệu số) + lịch build.

**Ra**
- Với mỗi segment: tập user (bitmap) theo **version**, kèm `asOfDs`.
- API: `count`, `users`, `contains`, `segments by user`, export async.

**Bốn loại dữ liệu** hệ thống phải xử lý: `MUTEX`, `NOT_MUTEX`, `PARTIAL_VALUE`, `PARTIAL_VALUE_BY_TAG` (`CLAUDE.md` §3.2).

## 4. Ràng buộc

**Quy mô** (thiết kế, chưa đo; thực tế ~30M user, ~1M giao dịch/ngày — xem §0): 100M user · 500 attribute · 500–1000 tag/attribute (≤ 500K tag) · mọi date range mỗi ngày · 5K segment/ngày · lịch sử 400 ngày.

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

## 5. Ngoài phạm vi v1
Realtime trong ngày (`A0`) · `AVG` / `DISTINCT_COUNT` / `FIRST` / `LAST` cho partial value · `EXTENDED` với MUTEX.

## 6. Ví dụ segment cụ thể

Ba segment minh hoạ; dữ liệu qua từng layer (L0 → L7) và kết quả bitmap ở `data-flow-examples.md` §1–§7 (golden test). As-of `ds = 2026-09-15`, 4 user `U1001..U1004`. Mục đích nghiệp vụ dưới đây chỉ để minh hoạ.

| Segment | Ý nghĩa nghiệp vụ | Rule | Loại dữ liệu dùng | Kết quả |
|---|---|---|---|---|
| `seg_1001` | Kéo lại user có nguy cơ rời bỏ ở HN chưa mua F&B gần đây → gửi ưu đãi F&B | `(churn mid/high A30 ∩ city hn A7) − F&B A7` | MUTEX `EVENT` (churn), MUTEX `STATE` (city), NOT_MUTEX `EVENT` (ngành hàng) | `{3}` |
| `seg_1002` | User chi tiêu lớn 7 ngày → chăm sóc VIP / đo tác động | `SUM amount A7 ≥ 1M` ∪ `SUM F&B A7 ≥ 500K` | PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG | `{1,2}` |
| `seg_1003` | Nhận quà ≥ 100K nhưng chưa follow OA cụ thể → nhắc follow | `SUM gift A7 ≥ 100K − follow oa_12345 A7` | PARTIAL_VALUE_BY_TAG `EXTENDED`, NOT_MUTEX `EXTENDED` | `{1}` |

Ba ví dụ này chạm đủ 4 loại dữ liệu, cả `EVENT`/`STATE` và `STANDARD`/`EXTENDED`. Ca biên đi kèm (trùng `event_id`, đến muộn, ngày ICT ≠ UTC, REMOVE trong MUTEX) ở `data-flow-examples.md` §1, §4. Ca biên theo từng nguồn sẽ chốt ở task tiếp theo.
