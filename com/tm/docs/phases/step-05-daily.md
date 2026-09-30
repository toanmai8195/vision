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
- Cả 4 loại, `aggFunc` SUM/COUNT/MIN/MAX, STANDARD/EXTENDED. Kết quả phải khớp `reference.py`.

## Checklist
**Mục tiêu**: từ nhu cầu nghiệp vụ, khai báo attribute rồi rút gọn event thành trạng thái theo ngày.
- [ ] Nhu cầu tạo attribute: mỗi attribute trả lời câu hỏi nghiệp vụ nào, chọn `dataType` / `feedMode` / `aggFunc` ra sao (hướng dẫn chọn: `data-types.md`)
- [ ] Catalog Postgres `meta.*` + proto `catalog`; seed attribute theo nhu cầu vừa chốt (đủ 4 loại)
- [ ] `reference.py`: cách tính ngây thơ đúng §3.2, làm chuẩn so sánh cho các bước sau
- [ ] SQL `tag_daily` (`ADD/DEL`) và `pv_daily` (`SUM/COUNT/MIN/MAX`); `tag_dict` cho EXTENDED
- [ ] Ghi idempotent theo `(ds, attr_id)`

**Done khi**: kết quả SQL khớp `reference.py` trên golden, cả 4 loại.
