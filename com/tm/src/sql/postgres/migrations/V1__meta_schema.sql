-- =============================================================================
-- V1: schema metadata `meta` (CLAUDE.md §3, §5)
-- Luật 4 loại dữ liệu được enforce ngay tại DB — cùng luật với
-- com/tm/src/common/python/catalog/validation.py.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta;

-- Nhóm attribute theo source table — đơn vị chạy job.
CREATE TABLE meta.attr_group (
    id          INT         PRIMARY KEY CHECK (id > 0),
    name        TEXT        NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_]*$'),
    source      TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE meta.attribute (
    id                    INT         PRIMARY KEY CHECK (id > 0),
    name                  TEXT        NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_]*$'),
    description           TEXT        NOT NULL DEFAULT '',
    data_type             TEXT        NOT NULL
        CHECK (data_type IN ('MUTEX', 'NOT_MUTEX', 'PARTIAL_VALUE', 'PARTIAL_VALUE_BY_TAG')),
    feed_mode             TEXT        NOT NULL CHECK (feed_mode IN ('EVENT', 'STATE')),
    supported_date_ranges TEXT[]      NOT NULL
        CHECK (
            cardinality(supported_date_ranges) > 0
            AND supported_date_ranges <@ ARRAY[
                'A1', 'A7', 'A15', 'A30', 'A60', 'A90', 'A120', 'A180',
                'IN_MONTH', 'LAST_MONTH', 'ALWAYS_ACTIVE'
            ]::TEXT[]
        ),
    attr_group_id         INT         NOT NULL REFERENCES meta.attr_group (id),
    status                TEXT        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('DRAFT', 'ACTIVE', 'DEPRECATED')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- PARTIAL_VALUE(_BY_TAG) luôn là EVENT (CLAUDE.md §3.1)
    CONSTRAINT attribute_partial_value_is_event
        CHECK (data_type IN ('MUTEX', 'NOT_MUTEX') OR feed_mode = 'EVENT')
);

CREATE TABLE meta.tag (
    attr_id              INT            NOT NULL REFERENCES meta.attribute (id),
    id                   INT            NOT NULL CHECK (id >= 1), -- 0 dành cho __any__
    name                 TEXT           NOT NULL CHECK (name ~ '^[a-z0-9_][a-z0-9_.]*$'),
    description          TEXT           NOT NULL DEFAULT '',
    hidden               BOOLEAN        NOT NULL DEFAULT FALSE,     -- vd tombstone __none__
    -- valueRange: chỉ PARTIAL_VALUE (kiểm tra ở trigger bên dưới)
    value_from           NUMERIC(27, 6),
    value_from_inclusive BOOLEAN,
    value_to             NUMERIC(27, 6),
    value_to_inclusive   BOOLEAN,
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (attr_id, id),
    UNIQUE (attr_id, name),
    CONSTRAINT tag_value_from_pair CHECK ((value_from IS NULL) = (value_from_inclusive IS NULL)),
    CONSTRAINT tag_value_to_pair CHECK ((value_to IS NULL) = (value_to_inclusive IS NULL)),
    CONSTRAINT tag_value_range_order CHECK (value_from IS NULL OR value_to IS NULL OR value_from <= value_to)
);

-- Luật tag phụ thuộc data_type của attribute → trigger.
CREATE FUNCTION meta.check_tag() RETURNS TRIGGER AS $$
DECLARE
    dt        TEXT;
    has_range BOOLEAN := NEW.value_from IS NOT NULL OR NEW.value_to IS NOT NULL;
BEGIN
    SELECT data_type INTO dt FROM meta.attribute WHERE id = NEW.attr_id;
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

CREATE TRIGGER tag_check
    BEFORE INSERT OR UPDATE ON meta.tag
    FOR EACH ROW EXECUTE FUNCTION meta.check_tag();

-- Không cho đổi data_type khi attribute đã có tag (đổi loại = attribute mới).
CREATE FUNCTION meta.check_attribute_update() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.data_type <> OLD.data_type AND EXISTS (SELECT 1 FROM meta.tag WHERE attr_id = OLD.id) THEN
        RAISE EXCEPTION 'attribute %: cannot change data_type from % to % once tags exist',
            OLD.name, OLD.data_type, NEW.data_type;
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER attribute_update_check
    BEFORE UPDATE ON meta.attribute
    FOR EACH ROW EXECUTE FUNCTION meta.check_attribute_update();

-- -----------------------------------------------------------------------------
-- Segment (CLAUDE.md §6)
-- -----------------------------------------------------------------------------

CREATE TABLE meta.segment (
    segment_id  TEXT        PRIMARY KEY CHECK (segment_id ~ '^seg_[a-z0-9_]+$'),
    name        TEXT        NOT NULL,
    definition  JSONB       NOT NULL, -- vision.segment.v1.Segment (proto JSON)
    schedule    TEXT        NOT NULL DEFAULT 'DAILY' CHECK (schedule IN ('DAILY', 'ONCE')),
    serving     TEXT        NOT NULL DEFAULT 'OFFLINE' CHECK (serving IN ('ONLINE', 'OFFLINE')),
    status      TEXT        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE meta.build_run (
    id          BIGSERIAL   PRIMARY KEY,
    ds          DATE        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error       TEXT
);

CREATE TABLE meta.segment_version (
    segment_id   TEXT        NOT NULL REFERENCES meta.segment (segment_id),
    version      INT         NOT NULL CHECK (version >= 1),
    ds           DATE        NOT NULL,
    build_run_id BIGINT      REFERENCES meta.build_run (id),
    status       TEXT        NOT NULL DEFAULT 'BUILDING'
        CHECK (status IN ('BUILDING', 'PUBLISHED', 'FAILED', 'SUPERSEDED')),
    cardinality  BIGINT,
    snapshot_uri TEXT,
    checksum     TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    PRIMARY KEY (segment_id, version),
    CONSTRAINT segment_version_published_complete
        CHECK (status <> 'PUBLISHED' OR (cardinality IS NOT NULL AND snapshot_uri IS NOT NULL))
);

-- Mỗi segment tối đa 1 version PUBLISHED cho mỗi ds.
CREATE UNIQUE INDEX segment_version_one_published_per_ds
    ON meta.segment_version (segment_id, ds) WHERE status = 'PUBLISHED';
