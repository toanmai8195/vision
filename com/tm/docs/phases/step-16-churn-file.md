# Bước 16 — MUTEX EVENT từ file ML — S3 churn score

> Mở rộng sau khi xong luồng 1 nguồn (bước 3→9 với `user_city`). Mỗi task chạy lại qua các layer đã có, chỉ thêm nhánh cho loại/nguồn mới; nhánh chưa làm vẫn báo lỗi tường minh.

| | Nội dung |
|---|---|
| **Input** | file parquet churn score → `churn_score_band` (`MUTEX`, `EVENT`, `STANDARD`; band low/mid/high), có REMOVE |
| **Output** | `bronze.churn_score_raw`; `silver.churn_score`; `tag_range_bitmap` cho `churn_score_band` |

```
file ML ──PySpark loader (sensor _SUCCESS)──▶ bronze ──▶ silver.churn_score (score→band) ──▶ cùng đường MUTEX `EVENT` của bước 11
```

- Thiết kế: `CLAUDE.md` §3.5 (cột loại này), §4; ví dụ/golden: `data-flow-examples.md`; cách chọn loại: `data-types.md`.
- Test theo `CLAUDE.md` §11: parametrize loại mới, có ca REMOVE/late data khi áp dụng; loại đã làm trước đó **không được hồi quy**.
- Bổ sung phần giải thích loại mới vào `data-types.md` (bảng "Sẽ bổ sung khi có nguồn" ở cuối file).
- Chỉ **done** khi chạy đúng ở mọi layer đã có (bronze → activation).

Checklist: xem mục "Bước 16" trong `../checklist.md`.
