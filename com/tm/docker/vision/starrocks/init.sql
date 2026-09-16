-- Database cho các layer Gold / Segment / metadata mirror (CLAUDE.md §2, §5).
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS seg;
CREATE DATABASE IF NOT EXISTS meta;
CREATE DATABASE IF NOT EXISTS dq;

-- Iceberg (bronze / silver) qua REST catalog.
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_vision
PROPERTIES (
    'type' = 'iceberg',
    'iceberg.catalog.type' = 'rest',
    'iceberg.catalog.uri' = 'http://iceberg-rest:8181',
    'aws.s3.access_key' = 'minioadmin',
    'aws.s3.secret_key' = 'minioadmin',
    'aws.s3.endpoint' = 'http://minio:9000',
    'aws.s3.enable_path_style_access' = 'true',
    'aws.s3.region' = 'us-east-1'
);
