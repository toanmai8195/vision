-- Seed OLTP khớp ví dụ golden (L0): com/tm/docs/data-flow-examples.md §1, §2, §7 và data-types.md §6.
-- Input : schema.sql đã áp. Output: dữ liệu của U1001..U1003 trong schema `src`.
-- Idempotent: bảng event dùng ON CONFLICT DO NOTHING; bảng trạng thái xoá dòng của user seed rồi
-- phát lại đúng chuỗi thao tác (INSERT -> UPDATE -> DELETE) trong 1 transaction, nên CDC thấy lại
-- đúng lịch sử và kết quả cuối luôn như nhau.
--
-- Ánh xạ user: U1001 = uidx 1, U1002 = 2, U1003 = 3, U1004 = 4 (U1004 chỉ có điểm churn, S3 là file).
-- Ca biên:
--   * trùng event_id (e-9001): xảy ra ở Kafka (at-least-once), OLTP có PK nên chỉ 1 dòng -> tạo ở bước 3.
--   * đến muộn: e-9005 (event_ts 09-14, created_at 09-15).
--   * lệch ngày UTC/ICT: e-9003 (18:30Z 09-14 = 01:30 ICT 09-15).
--   * FAILED: e-9004.   * đổi giá trị: city U1001 HCM -> HN.   * xoá: đóng paylater của U1001.
--   * REMOVE trong MUTEX (§4) dùng attribute giả định, không có nguồn OLTP -> không seed.
--   * churn score (S3): xem churn_score_examples.csv (file, không qua OLTP).

BEGIN;

-- S1 Payment (§1). e-9001 chỉ 1 dòng (bản giao trùng nằm ở Kafka).
INSERT INTO src.payment_event (event_id, user_id, mcc, amount, status, event_ts, created_at) VALUES
  ('e-8001', 'U1002', '5812',  600000, 'SUCCESS', '2026-09-10T05:00:00Z', '2026-09-10T05:00:01Z'),
  ('e-9001', 'U1001', '5812',   55000, 'SUCCESS', '2026-09-15T03:02:11Z', '2026-09-15T03:02:12Z'),
  ('e-9002', 'U1001', '4722', 1200000, 'SUCCESS', '2026-09-15T05:10:00Z', '2026-09-15T05:10:01Z'),
  ('e-9003', 'U1002', '5814',   45000, 'SUCCESS', '2026-09-14T18:30:00Z', '2026-09-14T18:30:01Z'),
  ('e-9004', 'U1003', '5812',   30000, 'FAILED',  '2026-09-15T08:00:00Z', '2026-09-15T08:00:01Z'),
  ('e-9005', 'U1003', '4900',  350000, 'SUCCESS', '2026-09-14T09:00:00Z', '2026-09-15T09:00:00Z')  -- đến muộn
ON CONFLICT (event_id) DO NOTHING;

-- S2a Profile (§2.1): phát lại đúng chuỗi thay đổi.
DELETE FROM src.user_profile WHERE user_id IN ('U1001', 'U1002', 'U1003');
INSERT INTO src.user_profile (user_id, city_code, created_at, updated_at) VALUES
  ('U1003', 'HN',  '2024-06-01T00:00:00Z', '2024-06-01T00:00:00Z'),   -- không đổi từ lâu
  ('U1001', 'HCM', '2025-01-10T00:00:00Z', '2025-01-10T00:00:00Z');
UPDATE src.user_profile SET city_code = 'HN', updated_at = '2026-09-15T03:00:00Z'   -- đổi giá trị (ts_ms 1789441200000)
 WHERE user_id = 'U1001';
INSERT INTO src.user_profile (user_id, city_code, created_at, updated_at) VALUES
  ('U1002', 'HCM', '2026-09-15T04:00:00Z', '2026-09-15T04:00:00Z');   -- user mới (ts_ms 1789444800000)

-- S2b Product holding (§2.2): trước 09-15 rồi thay đổi ngày 09-15.
DELETE FROM src.user_product WHERE user_id IN ('U1001', 'U1002', 'U1003');
INSERT INTO src.user_product (user_id, product, opened_at, updated_at) VALUES
  ('U1001', 'paylater',  '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'),
  ('U1001', 'insurance', '2026-08-10T00:00:00Z', '2026-08-10T00:00:00Z'),
  ('U1002', 'paylater',  '2026-08-05T00:00:00Z', '2026-08-05T00:00:00Z');
DELETE FROM src.user_product WHERE user_id = 'U1001' AND product = 'paylater';   -- đóng paylater, insurance giữ
INSERT INTO src.user_product (user_id, product, opened_at, updated_at) VALUES
  ('U1003', 'insurance', '2026-09-15T02:00:00Z', '2026-09-15T02:00:00Z');

-- S4a Voucher/quà (data-types.md §6 gift_value). gift_abc U1001 A7 = 70K+50K(09-10) = 120K.
INSERT INTO src.voucher_grant (event_id, user_id, voucher_code, value, event_ts, created_at) VALUES
  ('g-1', 'U1001', 'gift_abc',  50000, '2026-09-10T03:00:00Z', '2026-09-10T03:00:01Z'),
  ('g-2', 'U1001', 'gift_abc',  70000, '2026-09-14T03:00:00Z', '2026-09-14T03:00:01Z'),
  ('g-3', 'U1002', 'gift_abc',  30000, '2026-09-15T03:00:00Z', '2026-09-15T03:00:01Z'),
  ('g-4', 'U1002', 'gift_xyz', 200000, '2026-09-15T03:00:00Z', '2026-09-15T03:00:01Z')
ON CONFLICT (event_id) DO NOTHING;

-- S4b Follow OA (data-types.md §6 oa_follow). U1001 unfollow oa_12345 ngày 09-13 (signal REMOVE).
INSERT INTO src.oa_follow (event_id, user_id, oa_code, action, event_ts, created_at) VALUES
  ('o-1', 'U1001', 'oa_12345', 'FOLLOW',   '2026-09-02T03:00:00Z', '2026-09-02T03:00:01Z'),
  ('o-2', 'U1002', 'oa_12345', 'FOLLOW',   '2026-09-10T03:00:00Z', '2026-09-10T03:00:01Z'),
  ('o-3', 'U1002', 'oa_777',   'FOLLOW',   '2026-09-10T03:00:00Z', '2026-09-10T03:00:01Z'),
  ('o-4', 'U1001', 'oa_12345', 'UNFOLLOW', '2026-09-13T03:00:00Z', '2026-09-13T03:00:01Z'),
  ('o-5', 'U1003', 'oa_999',   'FOLLOW',   '2026-09-14T03:00:00Z', '2026-09-14T03:00:01Z')
ON CONFLICT (event_id) DO NOTHING;

-- S5 Hành vi app (minh hoạ, chưa có golden; đề xuất).
INSERT INTO src.app_event (event_id, user_id, event_name, event_ts, created_at) VALUES
  ('a-1', 'U1001', 'view_promo',    '2026-09-15T02:00:00Z', '2026-09-15T02:00:01Z'),
  ('a-2', 'U1001', 'view_promo',    '2026-09-15T02:05:00Z', '2026-09-15T02:05:01Z'),
  ('a-3', 'U1002', 'open_screen_x', '2026-09-15T04:00:00Z', '2026-09-15T04:00:01Z')
ON CONFLICT (event_id) DO NOTHING;

COMMIT;
