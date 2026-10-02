# Bước 12 — PARTIAL_VALUE & PARTIAL_VALUE_BY_TAG + `aggFunc`

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | `txn_amount` (`PARTIAL_VALUE`) và `txn_amount_by_category` (`PARTIAL_VALUE_BY_TAG`), `aggFunc` `SUM`/`COUNT`/`MIN`/`MAX` (cùng nguồn payment, `STANDARD`) |
| **Output** | `gold.pv_daily`, `pv_block`, `pv_range_value`; bucket `valueRange` định sẵn → `tag_range_bitmap` |

```
silver.payment_txn (value DECIMAL(27,6)) ──▶ pv_daily (AGG theo aggFunc) ──▶ pv_block (⊕ = +/min/max)
  ──▶ pv_range_value ──▶ lúc build: lọc valueRange (ad-hoc trong condition) + cache
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 12" trong `../checklist.md`.
