# Bước 14 — MUTEX EVENT — S3 churn score (file ML)

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | file parquet churn score → `churn_score_band` (`MUTEX`, `EVENT`, `STANDARD`; band low/mid/high) |
| **Output** | `bronze.churn_score_raw`; `silver.churn_score`; `ADD/DEL/ADD(d,0)`, `LATEST`, `SEEN`; `tag_range_bitmap` cho `churn_score_band` |

```
file ML ──PySpark loader (sensor _SUCCESS)──▶ bronze ──▶ silver.churn_score (score→band)
  ──▶ ADD/DEL, ADD(d,0) ──▶ block trên ADD(·,0) ──▶ LATEST ──▶ LATEST ∩ SEEN ──▶ tag_range_bitmap
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 14" trong `../checklist.md`.
