-- Seed OLTP khớp ví dụ golden (L0): com/tm/docs/data-flow-examples.md §2.1 (user_city).
-- Input : schema.sql đã áp. Output: dữ liệu của U1001..U1003 trong src.user_profile.
-- Idempotent: xoá dòng của user seed rồi phát lại đúng chuỗi thao tác (INSERT -> UPDATE) trong 1 transaction,
-- nên CDC thấy lại đúng lịch sử và kết quả cuối luôn như nhau.
--
-- Ánh xạ user: U1001 = uidx 1, U1002 = 2, U1003 = 3.
-- Ca biên: đổi giá trị (city U1001 HCM -> HN), user mới (U1002), không đổi từ lâu (U1003).
--   Ca xoá city (UPDATE city_code = NULL hoặc DELETE dòng) chưa seed, thử tay bằng psql.
-- Các nguồn khác (payment, product, voucher, OA, app, churn file) được thêm lại ở bước 11–16 (bản cũ: git 84ca0a2).

BEGIN;

-- S2a Profile (§2.1): phát lại đúng chuỗi thay đổi.
DELETE FROM src.user_profile WHERE user_id IN ('U1001', 'U1002', 'U1003');
INSERT INTO src.user_profile (user_id, city_code, created_at, updated_at) VALUES
  ('U1003', 'HN',  '2024-06-01T00:00:00Z', '2024-06-01T00:00:00Z'),   -- không đổi từ lâu
  ('U1001', 'HCM', '2025-01-10T00:00:00Z', '2025-01-10T00:00:00Z');
UPDATE src.user_profile SET city_code = 'HN', updated_at = '2026-09-15T03:00:00Z'   -- đổi giá trị (ts_ms 1789441200000)
 WHERE user_id = 'U1001';
INSERT INTO src.user_profile (user_id, city_code, created_at, updated_at) VALUES
  ('U1002', 'HCM', '2026-09-15T04:00:00Z', '2026-09-15T04:00:00Z');   -- user mới (ts_ms 1789444800000)

COMMIT;
