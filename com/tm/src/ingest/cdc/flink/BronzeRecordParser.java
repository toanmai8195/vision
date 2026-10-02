package com.tm.vision.ingest.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Dựng BronzeRecord từ message Kafka. Bronze là bản thô nên payload không parse được vẫn được giữ:
 * các cột trích sẵn (op, source_ts_ms, cdc_ts_ms) để null, việc loại bỏ để silver quyết định.
 */
public final class BronzeRecordParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BronzeRecordParser() {}

    public static BronzeRecord parse(String topic, int partition, long offset, long kafkaTsMs, String key, String payload) {
        BronzeRecord r = new BronzeRecord();
        r.topic = topic;
        r.kafkaPartition = partition;
        r.kafkaOffset = offset;
        r.kafkaTsMs = kafkaTsMs;
        r.msgKey = key;
        r.payload = payload;
        try {
            JsonNode n = MAPPER.readTree(payload);
            if (n != null && n.isObject()) {
                r.op = text(n.get("op"));
                r.cdcTsMs = number(n.get("ts_ms"));
                JsonNode src = n.get("source");
                r.sourceTsMs = src == null ? null : number(src.get("ts_ms"));
            }
        } catch (java.io.IOException e) {
            // giữ nguyên payload, cột trích sẵn để null
        }
        return r;
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? null : n.asText();
    }

    private static Long number(JsonNode n) {
        return n == null || !n.isNumber() ? null : n.asLong();
    }
}
