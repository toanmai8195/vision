package com.tm.vision.ingest.cdc;

/**
 * Một message Kafka của CDC ở dạng bronze: payload Debezium nguyên văn + vài cột trích sẵn để tra cứu.
 * POJO (public field + constructor rỗng) để Flink dùng POJO serializer thay vì Kryo.
 */
public class BronzeRecord {
    public String topic;
    public int kafkaPartition;
    public long kafkaOffset;
    public long kafkaTsMs;
    /** Key Kafka, JSON {"user_id":"..."}; null nếu message không có key. */
    public String msgKey;
    /** Envelope Debezium nguyên văn (JSON string). */
    public String payload;
    /** c=insert, u=update, d=delete, r=snapshot; null nếu payload không phải envelope hợp lệ. */
    public String op;
    /** `source.ts_ms`: thời điểm thay đổi ở DB nguồn; null nếu không có. */
    public Long sourceTsMs;
    /** `ts_ms`: thời điểm Debezium xử lý; null nếu không có. */
    public Long cdcTsMs;

    public BronzeRecord() {}
}
