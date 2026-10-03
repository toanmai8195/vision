package com.tm.vision.ingest.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.flink.table.data.RowData;
import org.junit.Test;

public class IcebergBronzeTest {
    @Test
    public void toRowFollowsSchemaColumnOrder() {
        BronzeRecord r = BronzeRecordParser.parse("vision.src.user_profile.v1", 1, 9L, 1700000000000L, "{\"user_id\":\"u1\"}",
                "{\"op\":\"u\",\"ts_ms\":200,\"source\":{\"ts_ms\":100}}");
        long ingest = 1700003600000L; // 2023-11-14T23:13:20Z
        RowData row = IcebergBronze.toRow(r, ingest);
        assertEquals(IcebergBronze.SCHEMA.columns().size(), row.getArity());
        assertEquals("vision.src.user_profile.v1", row.getString(0).toString());
        assertEquals(1, row.getInt(1));
        assertEquals(9L, row.getLong(2));
        assertEquals(1700000000000L, row.getTimestamp(3, 3).getMillisecond());
        assertEquals("{\"user_id\":\"u1\"}", row.getString(4).toString());
        assertEquals("u", row.getString(6).toString());
        assertEquals(100L, row.getLong(7));
        assertEquals(200L, row.getLong(8));
        assertEquals(ingest, row.getTimestamp(9, 3).getMillisecond());
        assertEquals("2023-11-14-23", row.getString(10).toString());
    }

    @Test
    public void toRowKeepsNullColumnsNull() {
        BronzeRecord r = BronzeRecordParser.parse("t", 0, 0L, 0L, null, "not json");
        RowData row = IcebergBronze.toRow(r, 0L);
        assertTrue(row.isNullAt(4));
        assertTrue(row.isNullAt(6));
        assertTrue(row.isNullAt(7));
        assertTrue(row.isNullAt(8));
        assertNull(r.msgKey);
    }

    @Test
    public void ingestHourIsUtc() {
        assertEquals("1970-01-01-00", IcebergBronze.ingestHour(0L));
        assertEquals("2023-11-14-22", IcebergBronze.ingestHour(1700000000000L));
    }

    @Test
    public void everySourceTopicHasATable() {
        assertEquals(1, IcebergBronze.TOPIC_TO_TABLE.size());
        assertEquals("user_profile_cdc_raw", IcebergBronze.TOPIC_TO_TABLE.get("vision.src.user_profile.v1"));
    }
}
