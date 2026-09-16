-- =============================================================================
-- V2: seed 6 attribute ví dụ (com/tm/docs/data-flow-examples.md)
-- Phủ đủ 4 loại dữ liệu; MUTEX / NOT_MUTEX có cả EVENT và STATE.
-- =============================================================================

INSERT INTO meta.attr_group (id, name, source, description) VALUES
    (1, 'payment',      'S1 payment_event (Kafka)',            'Giao dịch thanh toán'),
    (2, 'core_profile', 'S2 core.user_profile / user_product', 'CDC hồ sơ và sản phẩm'),
    (3, 'ml_churn',     'S3 churn_score (parquet)',            'Điểm churn từ team DS');

INSERT INTO meta.attribute (id, name, description, data_type, feed_mode, supported_date_ranges, attr_group_id) VALUES
    (101, 'txn_category',           'Loại giao dịch theo MCC',        'NOT_MUTEX',            'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 1),
    (102, 'txn_amount',             'Tổng tiền giao dịch',            'PARTIAL_VALUE',        'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 1),
    (103, 'txn_amount_by_category', 'Tổng tiền giao dịch theo ngành', 'PARTIAL_VALUE_BY_TAG', 'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 1),
    (201, 'user_city',              'Thành phố hiện tại',             'MUTEX',                'STATE',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 2),
    (202, 'product_holding',        'Sản phẩm đang sở hữu',           'NOT_MUTEX',            'STATE',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 2),
    (301, 'churn_score_band',       'Nhóm điểm churn',                'MUTEX',                'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 3);

INSERT INTO meta.tag (attr_id, id, name) VALUES
    (101, 1, 'fnb'), (101, 2, 'travel'), (101, 3, 'bill'),
    (103, 1, 'fnb'), (103, 2, 'travel'), (103, 3, 'bill'),
    (201, 1, 'hcm'), (201, 2, 'hn'),
    (202, 1, 'paylater'), (202, 2, 'insurance'),
    (301, 1, 'low'), (301, 2, 'mid'), (301, 3, 'high');

-- PARTIAL_VALUE: tag là khoảng giá trị
INSERT INTO meta.tag (attr_id, id, name, value_from, value_from_inclusive, value_to, value_to_inclusive) VALUES
    (102, 1, 'lt_500k',  0,       TRUE, 500000,  FALSE),
    (102, 2, '500k_2m',  500000,  TRUE, 2000000, FALSE),
    (102, 3, 'gte_2m',   2000000, TRUE, NULL,    NULL);
