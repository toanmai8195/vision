# Bước 6 — Temporal (L4)

| | Nội dung |
|---|---|
| **Input** | `tag_daily`, `pv_daily` |
| **Output** | `gold.tag_block`, `gold.pv_block`, `gold.tag_latest`, `gold.tag_state_checkpoint` |

```
daily ──▶ dyadic block B(k,s) (OR cho bitmap, ⊕ cho AGG)
daily ──▶ LATEST (MUTEX) · POS (NOT_MUTEX) · STATE (feed STATE) ──▶ checkpoint tuần
```

- Chi phí mỗi ngày tỉ lệ số tag, không tỉ lệ độ dài window (§4.2–4.3).
- Window `[l,r]` ghép ≤ 2·log₂N block rời nhau.
- Done bằng **property test (hypothesis)**: SQL == `reference.py`, ≤ 400 ngày, có REMOVE, late data (hiện nhánh STATE; EVENT khi mở rộng).
- Hiện chỉ nhánh `STATE` của MUTEX (`STATE(r,t)` + checkpoint tuần); block/LATEST/POS/AGG làm khi mở rộng sang `EVENT` và các loại khác.

Checklist: xem mục "Bước 6" trong `../checklist.md`.
