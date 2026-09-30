# Bước 2 — Seed OLTP (L0)

| | Nội dung |
|---|---|
| **Input** | Bài toán + ví dụ + danh sách nguồn (bước 1) |
| **Output** | Postgres OLTP (nguồn) có schema từng nguồn + data mẫu khớp ví dụ |

```
schema.sql ──▶ Postgres OLTP ◀── seed script (data ví dụ + ca biên) ◀── generator (--users --days)
```

- Thêm **Postgres OLTP** vào compose (chỉ service này).
- Schema theo từng nguồn: bảng, cột, kiểu, khoá, cột thời gian. Bảng event (payment) insert-only; bảng trạng thái (profile, product) có update/delete.
- Seed có cả ca biên: trùng, đến muộn, xoá/đổi giá trị. Script chạy lại được.
- Phân biệt với Postgres `meta` (catalog) — dựng ở bước 5.

## Checklist
**Mục tiêu**: có DB nguồn giống hệ thống thật, chứa data mẫu.
- [ ] Thêm Postgres OLTP vào compose
- [ ] Schema OLTP cho từng nguồn (bảng, cột, kiểu, khoá, cột thời gian) theo bài toán ở bước 1
- [ ] Seed data khớp ví dụ bước 1, gồm cả ca biên (trùng, đến muộn, xoá/đổi giá trị)
- [ ] Script seed chạy lại được; có cách sinh thêm data để test lớn hơn

**Done khi**: query OLTP ra đúng data của ví dụ ở bước 1.
