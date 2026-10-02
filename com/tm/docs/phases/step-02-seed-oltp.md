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
- OLTP hiện chỉ có bảng `src.user_profile` (schema, seed, generator). Các nguồn khác thêm lại ở bước 11–16 (bản cũ có đủ 6 bảng + seed + generator trong git, commit `84ca0a2`).
- Phân biệt với Postgres `meta` (catalog) — dựng ở bước 5.

Chạy seed bằng compose (`com/tm/docker/vision/docker-compose.yml`):
- `docker compose up -d` → service one-shot `oltp-seed` áp `schema.sql` và nạp data ví dụ **nếu chưa có** (tránh sự kiện CDC giả khi chạy lại ở bước 3). Nạp lại: `FORCE_SEED=1 docker compose up oltp-seed`.
- Data lớn: `GENERATE_ARGS="--users 10000 --days 30" docker compose --profile bigseed up oltp-generate`.
- `src/ingest/oltp/seed.sh` vẫn dùng được để chạy tay (từ máy host, cần Bazel cho phần generator).

Checklist: xem mục "Bước 2" trong `../checklist.md`.
