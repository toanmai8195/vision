-- Schema OLTP (L0): Postgres nguồn nghiệp vụ, tách khỏi Postgres `meta` (bước 5).
-- Input : bài toán + nguồn ở com/tm/docs/problem.md §3. Phạm vi hiện tại: chỉ S2a user_profile.
-- Các nguồn khác (S1 payment, S2b product, S4 voucher/OA, S5 app_event) được thêm lại ở bước 11–16; bản cũ có trong git (commit 84ca0a2).
-- Output: bảng của từng nguồn; Debezium (bước 3) đọc log của các bảng này.
-- Idempotent: chạy lại nhiều lần không lỗi, không mất dữ liệu (IF NOT EXISTS).
--
-- Quy ước
--   * user_id là chuỗi nghiệp vụ ('U1001'); uidx chỉ sinh ở silver (bước 4).
--   * event_ts = thời điểm nghiệp vụ (UTC, timestamptz); ds theo ICT tính ở silver.
--   * created_at = lúc dòng được ghi vào OLTP; created_at > event_ts nhiều ngày = đến muộn.
--   * Bảng trạng thái (S2a): UPDATE/DELETE; REPLICA IDENTITY FULL để CDC có `before`.

CREATE SCHEMA IF NOT EXISTS src;

-- S2a Profile: trạng thái, 1 dòng/user. city_code NULL hoặc DELETE dòng = mất city (REMOVED).
-- birth_date: ngày sinh (tuổi tính ở silver theo `ds`, không lưu tuổi vì bị cũ theo thời gian); NULL = chưa khai báo.
-- gender: 'M' | 'F' | 'O'; NULL = chưa khai báo.
CREATE TABLE IF NOT EXISTS src.user_profile (
    user_id    TEXT        PRIMARY KEY,
    city_code  TEXT,
    birth_date DATE,
    gender     TEXT        CHECK (gender IN ('M', 'F', 'O')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- DB đã tạo từ bản cũ (chưa có 2 cột này): thêm cột, chạy lại không lỗi.
ALTER TABLE src.user_profile ADD COLUMN IF NOT EXISTS birth_date DATE;
ALTER TABLE src.user_profile ADD COLUMN IF NOT EXISTS gender TEXT CHECK (gender IN ('M', 'F', 'O'));
ALTER TABLE src.user_profile REPLICA IDENTITY FULL;
-- Ví dụ (data-flow-examples.md §2.1; CDC thấy mỗi thay đổi dưới dạng op c/u/d):
--   user_id city_code birth_date  gender thay đổi
--   U1003   HN        1985-02-14  NULL   có từ 2024-06-01, không đổi (STATE: vẫn thuộc hn ở mọi window); chưa khai báo giới tính
--   U1001   HCM -> HN 1990-05-20  F      UPDATE city ngày 2026-09-15 (op=u: before HCM, after HN)
--   U1002   HCM       2001-11-03  M      INSERT ngày 2026-09-15 (user mới, op=c)
--   (ca xoá) UPDATE city_code = NULL hoặc DELETE dòng -> REMOVED, không thuộc tag nào
