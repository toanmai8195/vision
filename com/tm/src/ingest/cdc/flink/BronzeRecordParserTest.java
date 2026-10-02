package com.tm.vision.ingest.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class BronzeRecordParserTest {
    private static BronzeRecord parse(String payload) {
        return BronzeRecordParser.parse("vision.src.user_profile.v1", 2, 17L, 1700000000000L, "{\"user_id\":\"u1\"}", payload);
    }

    @Test
    public void extractsOpAndTimestampsForEveryDebeziumOp() {
        for (String op : new String[] {"c", "u", "d", "r"}) {
            BronzeRecord r = parse("{\"op\":\"" + op + "\",\"ts_ms\":200,\"source\":{\"ts_ms\":100},\"after\":{\"a\":1}}");
            assertEquals(op, r.op);
            assertEquals(Long.valueOf(100), r.sourceTsMs);
            assertEquals(Long.valueOf(200), r.cdcTsMs);
        }
    }

    @Test
    public void keepsKafkaPositionAndPayloadVerbatim() {
        String payload = "{\"op\":\"c\",\"ts_ms\":2,\"source\":{\"ts_ms\":1}}";
        BronzeRecord r = parse(payload);
        assertEquals("vision.src.user_profile.v1", r.topic);
        assertEquals(2, r.kafkaPartition);
        assertEquals(17L, r.kafkaOffset);
        assertEquals(1700000000000L, r.kafkaTsMs);
        assertEquals("{\"user_id\":\"u1\"}", r.msgKey);
        assertEquals(payload, r.payload);
    }

    @Test
    public void missingFieldsBecomeNull() {
        BronzeRecord r = parse("{\"op\":\"d\"}");
        assertEquals("d", r.op);
        assertNull(r.sourceTsMs);
        assertNull(r.cdcTsMs);
    }

    @Test
    public void invalidJsonIsKeptWithNullExtractedColumns() {
        BronzeRecord r = parse("not json");
        assertEquals("not json", r.payload);
        assertNull(r.op);
        assertNull(r.sourceTsMs);
        assertNull(r.cdcTsMs);
    }

    @Test
    public void nonObjectJsonIsKeptWithNullExtractedColumns() {
        BronzeRecord r = parse("[1,2]");
        assertEquals("[1,2]", r.payload);
        assertNull(r.op);
    }
}
