# Bước 11 — MUTEX EVENT — S1 payment (`last_txn_category`)

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | `src.payment_event` (CDC, ingest đủ cột) → `last_txn_category` (`MUTEX`, `EVENT`, `STANDARD`): ngành hàng của giao dịch `SUCCESS` gần nhất; chỉ xử lý thông tin MUTEX (`mcc`, `event_ts`, `status`), chưa dùng `amount` |
| **Output** | `bronze.payment_event_raw`; `silver.payment_txn` (đủ cột); `gold.tag_daily` (`ADD/DEL`, `ADD(d,0)`), `tag_block`, `LATEST`; `tag_range_bitmap` cho `last_txn_category` |

```
payment ──CDC──▶ bronze (nguyên văn) ──Spark──▶ silver.payment_txn (đủ cột: dedup event_id, ds ICT, uidx)
  ──▶ chỉ SUCCESS, mcc→tag ──▶ ADD cuối ngày theo (event_ts, event_id) ──▶ block trên ADD(·,0) ──▶ LATEST ──▶ LATEST ∩ SEEN ──▶ tag_range_bitmap
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Bổ sung phần giải thích loại mới vào `data-types.md` (bảng "Sẽ bổ sung khi có nguồn" ở cuối file).
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 11" trong `../checklist.md`.
