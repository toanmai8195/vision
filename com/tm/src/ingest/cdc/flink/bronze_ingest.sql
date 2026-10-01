-- Ingest bronze (L1): Kafka (Debezium CDC của OLTP) -> Iceberg `vision.bronze.*_raw` trên MinIO.
-- Input : topic vision.src.<table>.v1 (value = envelope Debezium JSON nguyên văn: before/after/source/op/ts_ms).
-- Output: 1 bảng bronze/nguồn. Chưa làm sạch: giữ payload gốc + vài cột trích sẵn để tra cứu.
-- Cột bronze (giống nhau mọi bảng):
--   topic, kafka_partition, kafka_offset, kafka_ts : vị trí/thời điểm bản ghi trong Kafka (dedup/truy vết)
--   msg_key      : key Kafka, JSON {"user_id":"..."} (partition theo user_id)
--   payload      : envelope Debezium nguyên văn (JSON string)
--   op           : loại thao tác  c=insert, u=update, d=delete, r=snapshot ban đầu
--   source_ts_ms : thời điểm thay đổi ở DB nguồn (epoch ms, `source.ts_ms`)
--   cdc_ts_ms    : thời điểm Debezium xử lý (epoch ms, `ts_ms`)
--   ingest_ts / ingest_hour : lúc Flink ghi (UTC) và partition theo giờ
-- Idempotent khi chạy lại file: CREATE ... IF NOT EXISTS; job đọc Kafka theo group offset nên resume được.
-- Giao at-least-once: trùng được xử lý ở silver (dedup event_id), không dedup ở bronze.

SET 'pipeline.name' = 'bronze_ingest';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.runtime-mode' = 'streaming';
-- Iceberg sink chỉ commit khi checkpoint.
SET 'execution.checkpointing.interval' = '10s';

CREATE CATALOG vision WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://vision-warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'vision',
  's3.secret-access-key' = 'vision-secret',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS vision.bronze;

CREATE TABLE IF NOT EXISTS vision.bronze.payment_event_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TABLE IF NOT EXISTS vision.bronze.user_profile_cdc_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TABLE IF NOT EXISTS vision.bronze.user_product_cdc_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TABLE IF NOT EXISTS vision.bronze.voucher_grant_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TABLE IF NOT EXISTS vision.bronze.oa_follow_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TABLE IF NOT EXISTS vision.bronze.app_event_raw (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP_LTZ(3),
  msg_key STRING,
  payload STRING,
  op STRING,
  source_ts_ms BIGINT,
  cdc_ts_ms BIGINT,
  ingest_ts TIMESTAMP_LTZ(3),
  ingest_hour STRING
) PARTITIONED BY (ingest_hour);

CREATE TEMPORARY TABLE kafka_payment_event (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.payment_event.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-payment_event',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY TABLE kafka_user_profile (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.user_profile.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-user_profile',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY TABLE kafka_user_product (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.user_product.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-user_product',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY TABLE kafka_voucher_grant (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.voucher_grant.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-voucher_grant',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY TABLE kafka_oa_follow (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.oa_follow.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-oa_follow',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY TABLE kafka_app_event (
  msg_key STRING,
  payload STRING,
  topic STRING METADATA VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'vision.src.app_event.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'vision-bronze-app_event',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'key.format' = 'raw',
  'key.fields' = 'msg_key',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO vision.bronze.payment_event_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_payment_event;

INSERT INTO vision.bronze.user_profile_cdc_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_user_profile;

INSERT INTO vision.bronze.user_product_cdc_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_user_product;

INSERT INTO vision.bronze.voucher_grant_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_voucher_grant;

INSERT INTO vision.bronze.oa_follow_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_oa_follow;

INSERT INTO vision.bronze.app_event_raw
SELECT topic, kafka_partition, kafka_offset, kafka_ts, msg_key, payload,
       JSON_VALUE(payload, '$.op'),
       CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT),
       CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT),
       CURRENT_TIMESTAMP,
       DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH')
FROM kafka_app_event;

END;
