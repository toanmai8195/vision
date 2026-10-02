# Bước 11 — NOT_MUTEX — S1 payment, EVENT

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | `src.payment_event` (CDC) → `txn_category` (`NOT_MUTEX`, `EVENT`, `STANDARD`; tag = ngành hàng theo MCC) |
| **Output** | `bronze.payment_event_raw`; `silver.payment_txn`; `gold.tag_daily` (`ADD/DEL/SIG`), `tag_block`, `POS`; `tag_range_bitmap` cho `txn_category` |

```
payment ──CDC──▶ bronze ──Spark (dedup event_id, ds ICT, chỉ SUCCESS)──▶ silver.payment_txn
  ──▶ ADD/DEL/SIG theo từng tag ──▶ block trên SIG ──▶ POS(r,t) ──▶ POS ∩ SIG_window ──▶ tag_range_bitmap
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 11" trong `../checklist.md`.
