-- Schema OLTP (L0): Postgres nguồn nghiệp vụ, tách khỏi Postgres `meta` (bước 5).
-- Input : bài toán + nguồn S1..S5 ở com/tm/docs/problem.md §3.
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
-- Ví dụ (data-flow-examples.md §1; event_ts UTC, created_at bỏ qua):
--   event_id user_id mcc  amount   status  event_ts              ghi chú
--   e-8001   U1002   5812   600000 SUCCESS 2026-09-10T05:00:00Z  đã xử lý ngày 09-10
--   e-9001   U1001   5812    55000 SUCCESS 2026-09-15T03:02:11Z  giao trùng ở Kafka (OLTP chỉ 1 dòng nhờ PK)
--   e-9002   U1001   4722  1200000 SUCCESS 2026-09-15T05:10:00Z  U1001 có 2 ngành cùng ngày (NOT_MUTEX)
--   e-9003   U1002   5814    45000 SUCCESS 2026-09-14T18:30:00Z  = 01:30 ICT ngày 15 (lệch ngày UTC/ICT)
--   e-9004   U1003   5812    30000 FAILED  2026-09-15T08:00:00Z  FAILED: không vào tag/số tiền
--   e-9005   U1003   4900   350000 SUCCESS 2026-09-14T09:00:00Z  đến muộn: created_at là 09-15, event_ts 09-14

-- S2a Profile: trạng thái, 1 dòng/user. city_code NULL hoặc DELETE dòng = mất city (REMOVED).
CREATE TABLE IF NOT EXISTS src.user_profile (
    user_id    TEXT        PRIMARY KEY,
    city_code  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE src.user_profile REPLICA IDENTITY FULL;
-- Ví dụ (data-flow-examples.md §2.1; CDC thấy mỗi thay đổi dưới dạng op c/u/d):
--   user_id city_code  thay đổi
--   U1003   HN         có từ 2024-06-01, không đổi (STATE: vẫn thuộc hn ở mọi window)
--   U1001   HCM -> HN  UPDATE ngày 2026-09-15 (op=u: before HCM, after HN)
--   U1002   HCM        INSERT ngày 2026-09-15 (user mới, op=c)
--   (ca xoá) UPDATE city_code = NULL hoặc DELETE dòng -> REMOVED, không thuộc tag nào

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
-- Ví dụ (data-flow-examples.md §2.2):
--   trước 2026-09-15: (U1001, paylater) (U1001, insurance) (U1002, paylater)
--   2026-09-15:       DELETE (U1001, paylater)  -> chỉ tag paylater REMOVED, insurance của U1001 giữ nguyên
--                     INSERT (U1003, insurance)
--   sau đó:           paylater = {U1002}; insurance = {U1001, U1003}

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
-- Ví dụ (data-types.md §6 gift_value; event_id ví dụ minh hoạ):
--   event_id user_id voucher_code value   event_ts
--   g-1      U1001   gift_abc      50000   2026-09-10
--   g-2      U1001   gift_abc      70000   2026-09-14   -> SUM gift_abc A7 của U1001 = 120000
--   g-3      U1002   gift_abc      30000   2026-09-15
--   g-4      U1002   gift_xyz     200000   2026-09-15

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
-- Ví dụ (data-types.md §6 oa_follow; event_id ví dụ minh hoạ):
--   event_id user_id oa_code   action    event_ts
--   o-1      U1001   oa_12345  FOLLOW    2026-09-02
--   o-2      U1002   oa_12345  FOLLOW    2026-09-10
--   o-3      U1002   oa_777    FOLLOW    2026-09-10
--   o-4      U1001   oa_12345  UNFOLLOW  2026-09-13   -> signal REMOVE, A7 oa_12345 chỉ còn U1002
--   o-5      U1003   oa_999    FOLLOW    2026-09-14   -> tag mới, chưa có trong tag_dict

-- S5 Hành vi app: event, insert-only. event_name = tag EXTENDED; mỗi dòng = 1 lần xảy ra (COUNT).
CREATE TABLE IF NOT EXISTS src.app_event (
    event_id   TEXT        PRIMARY KEY,
    user_id    TEXT        NOT NULL,
    event_name TEXT        NOT NULL,
    event_ts   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS app_event_user_ts_idx ON src.app_event (user_id, event_ts);
-- Ví dụ (minh hoạ, chưa có golden — S5 là đề xuất, problem.md §3):
--   event_id user_id event_name     event_ts
--   a-1      U1001   view_promo     2026-09-15T02:00:00Z
--   a-2      U1001   view_promo     2026-09-15T02:05:00Z   -> COUNT view_promo của U1001 = 2
--   a-3      U1002   open_screen_x  2026-09-15T04:00:00Z
