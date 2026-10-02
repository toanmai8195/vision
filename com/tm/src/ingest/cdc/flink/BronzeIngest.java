package com.tm.vision.ingest.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * Ingest bronze (L1): Kafka (Debezium CDC của OLTP) -> Iceberg `bronze.*_raw` trên MinIO.
 *
 * <pre>
 * Kafka (topic vision.src.user_profile.v1) ──► source + deserializer ──► mỗi topic (hiện 1): filter ──► toRow ──► Iceberg FlinkSink
 * </pre>
 *
 * Bronze chưa làm sạch: giữ envelope Debezium nguyên văn, không dedup. Giao at-least-once: event trùng (sau restart)
 * được xử lý ở silver (dedup event_id). Không dùng event time/watermark vì job chỉ chuyển dữ liệu, không window.
 * Iceberg sink chỉ commit snapshot khi checkpoint hoàn tất nên dữ liệu hiện trong bảng theo từng checkpoint.
 *
 * Chạy application mode trên cluster Flink (xem docker-compose). Checkpoint, state backend và restart strategy cấu hình
 * ở flink-conf (FLINK_PROPERTIES trong compose), không nằm trong code job.
 * Biến môi trường: KAFKA_BOOTSTRAP, ICEBERG_REST_URI, ICEBERG_WAREHOUSE, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY,
 * KAFKA_GROUP_ID.
 */
public final class BronzeIngest {
    public static void main(String[] args) throws Exception {
        CatalogLoader catalog = IcebergBronze.catalogLoader(IcebergBronze.catalogProps(
                env("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
                env("ICEBERG_WAREHOUSE", "s3://vision-warehouse/"),
                env("S3_ENDPOINT", "http://minio:9000"),
                env("S3_ACCESS_KEY", "vision"),
                env("S3_SECRET_KEY", "vision-secret")));
        IcebergBronze.ensureTables(catalog);

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<BronzeRecord> source = KafkaSource.<BronzeRecord>builder()
                .setBootstrapServers(env("KAFKA_BOOTSTRAP", "kafka:9092"))
                .setTopics(IcebergBronze.TOPIC_TO_TABLE.keySet().toArray(new String[0]))
                .setGroupId(env("KAFKA_GROUP_ID", "vision-bronze-ingest"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setDeserializer(new BronzeRecordDeserializer())
                .build();

        DataStream<BronzeRecord> records = senv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc");
        IcebergBronze.TOPIC_TO_TABLE.forEach((topic, table) -> IcebergBronze.append(records, topic, table, catalog));

        senv.execute("bronze_ingest");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }
}
