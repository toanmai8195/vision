-- =============================================================================
-- V4: seed attribute ví dụ cho aggFunc + EXTENDED (com/tm/docs/data-types.md §5–§6)
-- =============================================================================

INSERT INTO meta.attr_group (id, name, source, description) VALUES
    (4, 'engagement', 'S4 OA follow / gift (Kafka)', 'Tương tác OA và quà tặng (tag EXTENDED)');

INSERT INTO meta.attribute (id, name, description, data_type, feed_mode, supported_date_ranges, attr_group_id, agg_func, attribute_type) VALUES
    (104, 'txn_count',  'Số giao dịch',               'PARTIAL_VALUE',        'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 1, 'COUNT', 'STANDARD'),
    (105, 'txn_max',    'Giao dịch lớn nhất',         'PARTIAL_VALUE',        'EVENT',
        ARRAY['A1','A7','A15','A30','A60','A90','A120','A180','IN_MONTH','LAST_MONTH','ALWAYS_ACTIVE'], 1, 'MAX', 'STANDARD'),
    (401, 'oa_follow',  'Follow Official Account',    'NOT_MUTEX',            'EVENT',
        ARRAY['A1','A7','A30','ALWAYS_ACTIVE'], 4, NULL, 'EXTENDED'),
    (402, 'gift_value', 'Giá trị quà nhận theo mã quà', 'PARTIAL_VALUE_BY_TAG', 'EVENT',
        ARRAY['A7','A30','ALWAYS_ACTIVE'], 4, 'SUM', 'EXTENDED');

-- txn_count: bucket theo số giao dịch (ngưỡng chủ yếu vẫn là ad-hoc trong condition)
INSERT INTO meta.tag (attr_id, id, name, value_from, value_from_inclusive, value_to, value_to_inclusive) VALUES
    (104, 1, 'once',   1, TRUE,  1,    TRUE),
    (104, 2, '2_5',    1, FALSE, 5,    TRUE),
    (104, 3, 'gt_5',   5, FALSE, NULL, NULL);
