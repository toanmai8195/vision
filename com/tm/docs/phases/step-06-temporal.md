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
- Done bằng **property test (hypothesis)**: SQL == `reference.py`, ≤ 400 ngày, có REMOVE, late data, EVENT và STATE.

## Checklist
**Mục tiêu**: gộp nhiều ngày mà không quét lại event.
- [ ] Dyadic block (`tag_block`, `pv_block`)
- [ ] `LATEST` (MUTEX), `POS` (NOT_MUTEX), `STATE` + checkpoint tuần

**Done khi**: property test (hypothesis) — SQL == `reference.py` với ≤ 400 ngày, có REMOVE, late data, EVENT và STATE.
