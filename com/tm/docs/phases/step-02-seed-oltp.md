# Bước 2 — Seed OLTP (L0)

| | Nội dung |
|---|---|
| **Input** | Bài toán + ví dụ + danh sách nguồn (bước 1) |
| **Output** | Postgres OLTP (nguồn) có schema từng nguồn + data mẫu khớp ví dụ |

```
schema.sql ──▶ Postgres OLTP ◀── seed script (data ví dụ + ca biên) ◀── generator (--users --days)
```

- Thêm **Postgres OLTP** vào compose (chỉ service này).
- Dựng Python trên Bazel (rules_python, `pip.parse`, macro `com_tm_py_image`) cho seed generator.
- Schema theo từng nguồn: bảng, cột, kiểu, khoá, cột thời gian. Bảng event (payment) insert-only; bảng trạng thái (profile, product) có update/delete.
- Seed có cả ca biên: trùng, đến muộn, xoá/đổi giá trị. Script chạy lại được.
- Phân biệt với Postgres `meta` (catalog) — dựng ở bước 5.

Checklist: xem mục "Bước 2" trong `../checklist.md`.
