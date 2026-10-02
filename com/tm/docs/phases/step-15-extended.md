# Bước 15 — EXTENDED — S4/S5 (voucher, OA, app event)

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | `voucher_grant`, `oa_follow`, `app_event` (CDC) → attribute `EXTENDED` (tag là chuỗi tự do): `PARTIAL_VALUE_BY_TAG` (giá trị quà theo mã, số lần theo `event_name`, `COUNT`), `NOT_MUTEX` (follow OA, REMOVE khi unfollow) |
| **Output** | `silver.tag_dict`; `meta.condition_usage`; `pv_daily`/`pv_block`/POS chỉ cho tag có hoạt động; range usage-driven |

```
3 bảng ──CDC──▶ bronze ──▶ silver + tag_dict (append-only) ──▶ daily/block (chi phí ∝ tag có hoạt động)
  ──▶ range chỉ cho (attr, tag, window) trong meta.condition_usage; tag lần đầu → on-demand + cache
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 15" trong `../checklist.md`.
