# Bước 5 — Daily (L3)

| | Nội dung |
|---|---|
| **Input** | `silver.*`; nhu cầu nghiệp vụ |
| **Output** | Catalog attribute (Postgres `meta`); StarRocks `gold.tag_daily` (`ADD/DEL`), `gold.pv_daily` (AGG theo ngày); `tag_dict` cho EXTENDED; `reference.py` |

```
nhu cầu nghiệp vụ ──▶ chọn dataType/feedMode/aggFunc ──▶ catalog (meta.*)
silver ──StarRocks SQL (1 scan / source)──▶ tag_daily · pv_daily   (DELETE ds,attr + INSERT)
reference.py (ngây thơ, đúng §3.2) ◀── chuẩn so sánh
```

- Thêm **StarRocks** và **Postgres `meta`** vào compose.
- Bắt đầu từ nhu cầu: mỗi attribute trả lời câu hỏi nào → chọn loại (xem `data-types.md`) → mới seed catalog.
- SQL theo template theo `attrGroupId`; tag rule ở catalog, không hard-code.
- Hiện chỉ attribute `user_city` (`MUTEX`, `STATE`): `ADDED`/`REMOVED`/`STATE`. Thiết kế `reference.py` và SQL template **sẵn chỗ** cho 4 loại, `aggFunc`, STANDARD/EXTENDED, nhánh chưa làm báo lỗi tường minh (`CLAUDE.md` §11).
- Kết quả phải khớp `reference.py`.

Checklist: xem mục "Bước 5" trong `../checklist.md`.
