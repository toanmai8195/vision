# Capacity & scale — ước lượng

> Đây là ước lượng để chọn thiết kế, **chưa phải số đo**. Phase P6 phải benchmark và thay bảng này bằng số thật.
> Định nghĩa thuật toán ở `CLAUDE.md` §4.

## 1. Giả định

| Tham số | Giá trị thiết kế | Tham chiếu thực tế |
|---|---|---|
| User (`uidx`) | 100M | segment bitmap lớn nhất ước ~12MB serialized |
| Attribute | 500 | ~80% MUTEX/NOT_MUTEX, ~15% PARTIAL_VALUE, ~5% PARTIAL_VALUE_BY_TAG (giả định) |
| Tag / attribute | 500–1000 → T ≤ 500K | |
| Date range precompute | ≤ 11 (A1, A7, A15, A30, A60, A90, A120, A180, IN_MONTH, LAST_MONTH, ALWAYS_ACTIVE) | lọc theo `supportedDateRanges` |
| Segment build / ngày | 5K, trung bình 5 condition × 10 tag | |
| Retention lịch sử | 400 ngày | |

## 2. Chi phí mỗi ngày — MUTEX / NOT_MUTEX (bitmap)

| Bước | Số phép bitmap | Ghi chú |
|---|---|---|
| Reduce ngày (L3) | ~50 scan (1 scan / source table) | không scan theo attribute |
| Block mới | ~1 OR / (tag hoặc attribute) khấu hao, ngày xấu nhất ~8 | MUTEX chỉ cần block trên `ADD(·,0)` → **theo attribute**, không theo tag |
| LATEST / POS / STATE | 2–3 phép / tag | incremental |
| Window MUTEX | 11 × (≤ 6 OR trên attribute) + 11 AND / tag | phần OR dùng chung cho mọi tag của attribute |
| Window NOT_MUTEX | 11 × (≤ 6 OR + 1 AND) / tag | |
| STATE | 0 phép / window (`= STATE(r)`) | chỉ ghi lại tham chiếu |
| **Tổng** | ≈ 20–80 × T ≈ **10–40M phép/ngày** | song song theo `attrGroupId` |

So sánh cách ngây thơ (quét lại toàn bộ event trong window mỗi lần tính): A180 cho 500K tag = union 180 ngày/tag = 90M phép chỉ riêng A180, và chi phí tăng tuyến tính theo độ dài window.

## 3. Chi phí mỗi ngày — PARTIAL_VALUE / PARTIAL_VALUE_BY_TAG (row)

Giả định 75 attribute PARTIAL_VALUE + 25 attribute PARTIAL_VALUE_BY_TAG (50 tag), ~10M user có event/ngày, ~40M user có giá trị trong window dài.

| Bước | Ước lượng |
|---|---|
| `pv_daily` | ~10M row × 100 attr ≈ 1B row/ngày (BY_TAG nhân theo số tag user chạm) |
| Window qua block | ≤ 6 block × ≤ 40M user ≈ 240M row scan / (attr, window) worst case |
| Tổng scan worst case | 100 attr × 11 window × 240M ≈ **260B row** → **quá nặng nếu precompute mọi window** |

Giảm tải (theo thứ tự áp dụng):
1. Chỉ precompute `supportedDateRanges` và tag có `valueRange` định sẵn; ad-hoc `valueRange` tính on-demand + cache.
2. **Rolling incremental** cho window cố định: `SUM_N(ds) = SUM_N(ds−1) + V(ds) − V(ds−N)` (DECIMAL chính xác) → mỗi window chỉ chạm `|SUM_N(ds−1)| + 2 × daily`. Đối soát lại bằng block hằng tuần.
3. **Bit-Sliced Index (BSI)**: biểu diễn giá trị dưới dạng ~40 bitmap slice; cộng window = cộng BSI, lọc `valueRange` = so sánh BSI → trả thẳng bitmap. StarRocks không có sẵn → cần engine riêng (Go). Chỉ làm nếu (1)+(2) không đạt SLA.

## 4. Storage

| Bảng | Ước lượng | Ghi chú |
|---|---|---|
| `tag_daily` (EVENT) | tỉ lệ số (user, tag) có event/ngày; Roaring ~1–2 byte/phần tử khi thưa | 400 ngày |
| `tag_daily` (STATE) | chỉ `ADDED/REMOVED` (thường < 0.1% user/ngày) + checkpoint tuần | thay vì 100M user × ngày × attribute |
| `tag_block` | ≤ số level × dung lượng daily, thực tế nhỏ hơn nhiều vì union trùng user | |
| `tag_range_bitmap` | ≤ 500K × 11 = 5.5M row/ngày (bỏ rỗng) | giữ 7 ngày |
| `pv_range_value` | lớn nhất trong hệ thống | giữ 2 ngày |
| Segment bitmap | ~12MB / segment lớn @100M user | 5K × trung bình nhỏ hơn nhiều |

## 5. Segment build & serving

- 5K segment trong cửa sổ 2h ≈ 0.7 segment/s. Condition cache khử trùng lặp (nhiều segment dùng chung `churn A30`, `city`…).
- Builder lấy bitmap theo batch; vài chục bitmap lớn có thể lên hàng trăm MB → giới hạn batch theo **byte**, không theo số row.
- activation-api: `segments by user` = 5K × `contains` trên mmap ≈ 1ms; page cache cần ≈ tổng kích thước segment ONLINE.

## 6. Rủi ro cần đo sớm (P3/P6)

1. Kích thước bitmap tag lớn (vd city=HCM ~20M user) khi fetch/decode ở builder.
2. Số partition × bucket × tablet của các bảng PK partition theo `ds` với retention 400 ngày.
3. Chi phí DELETE + INSERT theo `(ds, attr_id)` trên PK table ở quy mô lớn.
4. PARTIAL_VALUE / PARTIAL_VALUE_BY_TAG: scan window (§3) — quyết định sớm có cần rolling incremental/BSI không.
5. Độ trễ `user_id → uidx` (Redis) ở QPS activation cao.
