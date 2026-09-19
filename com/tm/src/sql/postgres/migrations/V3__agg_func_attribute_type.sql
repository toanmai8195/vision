-- =============================================================================
-- V3: aggFunc + attributeType (CLAUDE.md §3.1, §3.2.1, §3.6)
-- Cùng luật với com/tm/src/common/python/catalog/validation.py.
-- =============================================================================

ALTER TABLE meta.attribute
    ADD COLUMN agg_func TEXT CHECK (agg_func IN ('SUM', 'COUNT', 'MIN', 'MAX')),
    ADD COLUMN attribute_type TEXT NOT NULL DEFAULT 'STANDARD' CHECK (attribute_type IN ('STANDARD', 'EXTENDED'));

-- PARTIAL_VALUE(_BY_TAG) mặc định SUM; loại khác không có aggFunc.
UPDATE meta.attribute SET agg_func = 'SUM' WHERE data_type IN ('PARTIAL_VALUE', 'PARTIAL_VALUE_BY_TAG');

ALTER TABLE meta.attribute
    ADD CONSTRAINT attribute_agg_func_only_partial_value CHECK (
        (data_type IN ('PARTIAL_VALUE', 'PARTIAL_VALUE_BY_TAG')) = (agg_func IS NOT NULL)
    ),
    -- EXTENDED chỉ cho NOT_MUTEX và PARTIAL_VALUE_BY_TAG (§3.5 dòng EXTENDED)
    ADD CONSTRAINT attribute_extended_data_type CHECK (
        attribute_type = 'STANDARD' OR data_type IN ('NOT_MUTEX', 'PARTIAL_VALUE_BY_TAG')
    );

-- Tag EXTENDED không khai báo trong meta.tag (dictionary nằm ở silver.tag_dict).
CREATE OR REPLACE FUNCTION meta.check_tag() RETURNS TRIGGER AS $$
DECLARE
    dt        TEXT;
    at        TEXT;
    has_range BOOLEAN := NEW.value_from IS NOT NULL OR NEW.value_to IS NOT NULL;
BEGIN
    SELECT data_type, attribute_type INTO dt, at FROM meta.attribute WHERE id = NEW.attr_id;
    IF at = 'EXTENDED' THEN
        RAISE EXCEPTION 'tag %.%: EXTENDED attribute has no catalog tags', NEW.attr_id, NEW.name;
    END IF;
    CASE dt
        WHEN 'PARTIAL_VALUE' THEN
            IF NOT has_range THEN
                RAISE EXCEPTION 'tag %.%: PARTIAL_VALUE tag requires value range', NEW.attr_id, NEW.name;
            END IF;
        WHEN 'MUTEX', 'NOT_MUTEX', 'PARTIAL_VALUE_BY_TAG' THEN
            IF has_range THEN
                RAISE EXCEPTION 'tag %.%: value range is only allowed for PARTIAL_VALUE (attribute is %)',
                    NEW.attr_id, NEW.name, dt;
            END IF;
        ELSE
            RAISE EXCEPTION 'tag %.%: unsupported data_type %', NEW.attr_id, NEW.name, dt;
    END CASE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Không cho đổi data_type / aggFunc / attributeType khi attribute đã có tag hoặc đang được dùng.
CREATE OR REPLACE FUNCTION meta.check_attribute_update() RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.data_type <> OLD.data_type
        OR NEW.agg_func IS DISTINCT FROM OLD.agg_func
        OR NEW.attribute_type <> OLD.attribute_type)
       AND (EXISTS (SELECT 1 FROM meta.tag WHERE attr_id = OLD.id)
            OR EXISTS (SELECT 1 FROM meta.condition_usage WHERE attr_id = OLD.id)) THEN
        RAISE EXCEPTION 'attribute %: cannot change data_type/agg_func/attribute_type once tags or usages exist',
            OLD.name;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Condition đang được segment ACTIVE dùng → nguồn cho range usage-driven của EXTENDED (§3.6).
-- window: tên DateRange (A1…ALWAYS_ACTIVE) hoặc 'CUSTOM:<from>:<to>'.
CREATE TABLE meta.condition_usage (
    attr_id      INT         NOT NULL REFERENCES meta.attribute (id),
    tag_string   TEXT        NOT NULL,  -- tag name (STANDARD) hoặc tag_string (EXTENDED)
    date_window  TEXT        NOT NULL,
    segment_id   TEXT        NOT NULL REFERENCES meta.segment (segment_id),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (attr_id, tag_string, date_window, segment_id)
);
CREATE INDEX condition_usage_attr ON meta.condition_usage (attr_id, date_window);
