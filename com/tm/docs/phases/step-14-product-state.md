# Bước 14 — NOT_MUTEX STATE — S2b product holding

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | `src.user_product` (CDC) → `product_holding` (`NOT_MUTEX`, `STATE`) |
| **Output** | `bronze.user_product_cdc_raw`; `silver.user_product_scd2`; `ADDED/REMOVED/STATE` theo từng tag; `tag_range_bitmap` cho `product_holding` |

```
user_product ──CDC──▶ bronze ──Spark MERGE──▶ silver SCD2 ──▶ ADDED/REMOVED ──▶ STATE(d,t) ──▶ tag_range_bitmap
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Bổ sung phần giải thích loại mới vào `data-types.md` (bảng "Sẽ bổ sung khi có nguồn" ở cuối file).
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 14" trong `../checklist.md`.
