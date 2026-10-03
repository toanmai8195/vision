package com.tm.vision.ingest.cdc;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

/**
 * Các bảng bronze `bronze.*_raw` trên Iceberg (REST catalog + S3FileIO trên MinIO): mỗi nguồn một bảng, cùng schema.
 * Tạo bảng nếu chưa có, đổi BronzeRecord thành dòng, ghi vào bảng.
 */
public final class IcebergBronze {
    public static final String NAMESPACE = "bronze";

    /** Topic CDC -> tên bảng bronze. Giai đoạn này chỉ có user_profile; thêm nguồn = thêm 1 dòng ở đây. */
    public static final Map<String, String> TOPIC_TO_TABLE = topicToTable();

    /** Cột chung của mọi bảng bronze; tất cả optional (như bảng đã tạo bằng Flink SQL ở bản trước). */
    public static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "topic", Types.StringType.get()),
            Types.NestedField.optional(2, "kafka_partition", Types.IntegerType.get()),
            Types.NestedField.optional(3, "kafka_offset", Types.LongType.get()),
            Types.NestedField.optional(4, "kafka_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(5, "msg_key", Types.StringType.get()),
            Types.NestedField.optional(6, "payload", Types.StringType.get()),
            Types.NestedField.optional(7, "op", Types.StringType.get()),
            Types.NestedField.optional(8, "source_ts_ms", Types.LongType.get()),
            Types.NestedField.optional(9, "cdc_ts_ms", Types.LongType.get()),
            Types.NestedField.optional(10, "ingest_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(11, "ingest_hour", Types.StringType.get()));

    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneOffset.UTC);

    private IcebergBronze() {}

    private static Map<String, String> topicToTable() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("vision.src.user_profile.v1", "user_profile_cdc_raw");
        return m;
    }

    public static Map<String, String> catalogProps(String restUri, String warehouse, String s3Endpoint,
                                                   String accessKey, String secretKey) {
        Map<String, String> p = new HashMap<>();
        p.put("uri", restUri);
        p.put("warehouse", warehouse);
        p.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        p.put("s3.endpoint", s3Endpoint);
        p.put("s3.path-style-access", "true");
        p.put("s3.access-key-id", accessKey);
        p.put("s3.secret-access-key", secretKey);
        p.put("client.region", "us-east-1");
        return p;
    }

    public static CatalogLoader catalogLoader(Map<String, String> props) {
        return CatalogLoader.rest("rest", new Configuration(), props);
    }

    public static TableIdentifier table(String name) {
        return TableIdentifier.of(NAMESPACE, name);
    }

    /** Idempotent: tạo namespace và các bảng (partition theo `ingest_hour`) nếu chưa có. */
    public static void ensureTables(CatalogLoader loader) {
        Catalog catalog = loader.loadCatalog();
        SupportsNamespaces ns = (SupportsNamespaces) catalog;
        Namespace bronze = Namespace.of(NAMESPACE);
        if (!ns.namespaceExists(bronze)) {
            ns.createNamespace(bronze);
        }
        for (String name : TOPIC_TO_TABLE.values()) {
            TableIdentifier id = table(name);
            if (!catalog.tableExists(id)) {
                catalog.createTable(id, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("ingest_hour").build());
            }
        }
    }

    /** Giờ ghi theo UTC, dạng `yyyy-MM-dd-HH` (giá trị partition). */
    public static String ingestHour(long ingestTsMs) {
        return HOUR.format(Instant.ofEpochMilli(ingestTsMs));
    }

    public static RowData toRow(BronzeRecord r, long ingestTsMs) {
        return GenericRowData.of(
                StringData.fromString(r.topic),
                r.kafkaPartition,
                r.kafkaOffset,
                TimestampData.fromEpochMillis(r.kafkaTsMs),
                str(r.msgKey),
                str(r.payload),
                str(r.op),
                r.sourceTsMs,
                r.cdcTsMs,
                TimestampData.fromEpochMillis(ingestTsMs),
                StringData.fromString(ingestHour(ingestTsMs)));
    }

    /** Ghi các record của một topic vào bảng của nó; Iceberg sink chỉ commit snapshot khi checkpoint hoàn tất. */
    public static void append(DataStream<BronzeRecord> records, String topic, String tableName, CatalogLoader loader) {
        DataStream<RowData> rows = records
                .filter(r -> topic.equals(r.topic))
                .name("filter-" + tableName)
                .map(r -> toRow(r, System.currentTimeMillis()))
                .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(SCHEMA)))
                .name("to-row-" + tableName);
        FlinkSink.forRowData(rows).tableLoader(TableLoader.fromCatalog(loader, table(tableName))).append();
    }

    private static StringData str(String s) {
        return s == null ? null : StringData.fromString(s);
    }
}
