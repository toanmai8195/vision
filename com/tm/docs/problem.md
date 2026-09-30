# Bài toán kỹ thuật — phân khúc user (Segmentation / CDP)

> Bước 1 (L0). Chỉ mô tả bài toán; thiết kế ở `CLAUDE.md`, con số quy mô ở `capacity.md`, ví dụ dữ liệu ở `data-flow-examples.md`.

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
- Dữ liệu nguồn ở OLTP: sự kiện thanh toán (payment), trạng thái hồ sơ (profile, product), điểm rời bỏ từ file ML (churn score).
- Định nghĩa segment (DSL): điều kiện trên attribute + tag + date range (+ `valueRange` với dữ liệu số) + lịch build.

**Ra**
- Với mỗi segment: tập user (bitmap) theo **version**, kèm `asOfDs`.
- API: `count`, `users`, `contains`, `segments by user`, export async.

**Bốn loại dữ liệu** hệ thống phải xử lý: `MUTEX`, `NOT_MUTEX`, `PARTIAL_VALUE`, `PARTIAL_VALUE_BY_TAG` (`CLAUDE.md` §3.2).

## 4. Ràng buộc

**Quy mô** (thiết kế, chưa đo): 100M user · 500 attribute · 500–1000 tag/attribute (≤ 500K tag) · mọi date range mỗi ngày · 5K segment/ngày · lịch sử 400 ngày.

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
