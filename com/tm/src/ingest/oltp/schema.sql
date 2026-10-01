-- Schema OLTP (L0): Postgres nguồn nghiệp vụ, tách khỏi Postgres `meta` (bước 5).
-- Input : bài toán + nguồn S1..S5 ở com/tm/docs/problem.md §7.
-- Output: bảng của từng nguồn; Debezium (bước 3) đọc log của các bảng này.
-- Idempotent: chạy lại nhiều lần không lỗi, không mất dữ liệu (IF NOT EXISTS).
--
-- Quy ước
--   * user_id là chuỗi nghiệp vụ ('U1001'); uidx chỉ sinh ở silver (bước 4).
--   * event_ts = thời điểm nghiệp vụ (UTC, timestamptz); ds theo ICT tính ở silver.
--   * created_at = lúc dòng được ghi vào OLTP; created_at > event_ts nhiều ngày = đến muộn.
--   * Bảng event (S1, S4, S5): insert-only, không UPDATE/DELETE.
--   * Bảng trạng thái (S2a, S2b): UPDATE/DELETE; REPLICA IDENTITY FULL để CDC có `before`.
--   * S3 churn score là file parquet của job ML (không qua OLTP) -> không có bảng ở đây.
--   * Số tiền NUMERIC(27,6) khớp DECIMAL(27,6) của pv_daily (CLAUDE.md §3.2).

CREATE SCHEMA IF NOT EXISTS src;

-- S1 Payment: event, insert-only. mcc -> ngành hàng (tag NOT_MUTEX / PARTIAL_VALUE_BY_TAG).
-- PARTIAL_VALUE chỉ tính SUCCESS; FAILED vẫn được ghi lại.
CREATE TABLE IF NOT EXISTS src.payment_event (
    event_id   TEXT          PRIMARY KEY,
    user_id    TEXT          NOT NULL,
    mcc        TEXT          NOT NULL,
    amount     NUMERIC(27,6) NOT NULL CHECK (amount >= 0),
    status     TEXT          NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    event_ts   TIMESTAMPTZ   NOT NULL,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payment_event_user_ts_idx ON src.payment_event (user_id, event_ts);

-- S2a Profile: trạng thái, 1 dòng/user. city_code NULL hoặc DELETE dòng = mất city (REMOVED).
CREATE TABLE IF NOT EXISTS src.user_profile (
    user_id    TEXT        PRIMARY KEY,
    city_code  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE src.user_profile REPLICA IDENTITY FULL;

-- S2b Product holding: trạng thái, 1 dòng/(user, sản phẩm) đang dùng.
-- Mở sản phẩm = INSERT; đóng sản phẩm = DELETE (tag khác của user không đổi).
CREATE TABLE IF NOT EXISTS src.user_product (
    user_id    TEXT        NOT NULL,
    product    TEXT        NOT NULL,
    opened_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, product)
);
ALTER TABLE src.user_product REPLICA IDENTITY FULL;

-- S4a Voucher/quà: event, insert-only. voucher_code = tag EXTENDED (chuỗi tự do).
CREATE TABLE IF NOT EXISTS src.voucher_grant (
    event_id     TEXT          PRIMARY KEY,
    user_id      TEXT          NOT NULL,
    voucher_code TEXT          NOT NULL,
    value        NUMERIC(27,6) NOT NULL CHECK (value >= 0),
    event_ts     TIMESTAMPTZ   NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS voucher_grant_user_ts_idx ON src.voucher_grant (user_id, event_ts);

-- S4b Follow OA: event, insert-only. action UNFOLLOW = signal REMOVE; oa_code = tag EXTENDED.
CREATE TABLE IF NOT EXISTS src.oa_follow (
    event_id   TEXT        PRIMARY KEY,
    user_id    TEXT        NOT NULL,
    oa_code    TEXT        NOT NULL,
    action     TEXT        NOT NULL CHECK (action IN ('FOLLOW', 'UNFOLLOW')),
    event_ts   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS oa_follow_user_ts_idx ON src.oa_follow (user_id, event_ts);

-- S5 Hành vi app: event, insert-only. event_name = tag EXTENDED; mỗi dòng = 1 lần xảy ra (COUNT).
CREATE TABLE IF NOT EXISTS src.app_event (
    event_id   TEXT        PRIMARY KEY,
    user_id    TEXT        NOT NULL,
    event_name TEXT        NOT NULL,
    event_ts   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS app_event_user_ts_idx ON src.app_event (user_id, event_ts);
