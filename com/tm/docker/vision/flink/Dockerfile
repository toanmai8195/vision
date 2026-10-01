# Flink + jar cho ingest bronze: Kafka source, Iceberg sink (REST catalog + S3FileIO), Hadoop client (Iceberg Flink cần).
# Version pin theo URL Maven Central. TODO(verify): thêm checksum sha256 cho từng jar.
FROM flink:1.20.2-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/flink/lib; \
    for u in \
      org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
      org/apache/iceberg/iceberg-flink-runtime-1.20/1.9.2/iceberg-flink-runtime-1.20-1.9.2.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar \
      org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
      org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar; do \
      curl -fsSLO "$MAVEN/$u"; \
    done
