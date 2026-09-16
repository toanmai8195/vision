package com.tm.vision.activation.api

import com.google.protobuf.util.JsonFormat
import com.tm.vision.proto.catalog.v1.DateRange
import com.tm.vision.proto.segment.v1.Condition
import com.tm.vision.proto.segment.v1.Rule
import com.tm.vision.proto.segment.v1.Segment
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ActivationApiTest {
    /** DSL ví dụ seg_1001 trong CLAUDE.md §6.1 phải parse được bằng proto. */
    @Test
    fun parsesSegmentDslExample() {
        val json = """
            {
              "segmentId": "seg_1001",
              "rule": {"operator": "SUB", "children": [
                {"operator": "AND", "children": [
                  {"condition": {"attr": "churn_score_band", "tags": ["mid", "high"], "tagOp": "OR", "dateRange": "A30"}},
                  {"condition": {"attr": "user_city", "tags": ["hn"], "dateRange": "A7"}}
                ]},
                {"condition": {"attr": "txn_category", "tags": ["fnb"], "dateRange": "A7"}}
              ]},
              "schedule": {"type": "DAILY"},
              "serving": "ONLINE"
            }
        """.trimIndent()

        val builder = Segment.newBuilder()
        JsonFormat.parser().merge(json, builder)
        val segment = builder.build()

        assertEquals(Rule.Operator.SUB, segment.rule.operator)
        val and = segment.rule.getChildren(0)
        assertEquals(Rule.Operator.AND, and.operator)
        assertEquals(DateRange.A30, and.getChildren(0).condition.dateRange)
        assertEquals(Condition.TagOp.OR, and.getChildren(0).condition.tagOp)
        assertEquals(Segment.Serving.ONLINE, segment.serving)
    }

    /** DSL ví dụ seg_1002 (PARTIAL_VALUE + PARTIAL_VALUE_BY_TAG với valueRange). */
    @Test
    fun parsesValueRangeCondition() {
        val json = """
            {"operator": "OR", "children": [
              {"condition": {"attr": "txn_amount", "dateRange": "A7",
                             "valueRange": {"fromValue": "1000000", "fromInclusive": true}}},
              {"condition": {"attr": "txn_amount_by_category", "tags": ["fnb"],
                             "customDateRange": {"fromDate": "2026-09-09", "toDate": "2026-09-15"},
                             "valueRange": {"fromValue": "500000", "fromInclusive": true}}}
            ]}
        """.trimIndent()

        val builder = Rule.newBuilder()
        JsonFormat.parser().merge(json, builder)
        val rule = builder.build()

        val pv = rule.getChildren(0).condition
        assertTrue(pv.hasValueRange())
        assertEquals("1000000", pv.valueRange.fromValue)
        assertTrue(!pv.valueRange.hasToValue())

        val byTag = rule.getChildren(1).condition
        assertEquals(Condition.WindowCase.CUSTOM_DATE_RANGE, byTag.windowCase)
        assertEquals("2026-09-09", byTag.customDateRange.fromDate)
    }

    /** Dagger component dựng được service; /healthz và /metrics hoạt động. */
    @Test
    fun servesHealthAndMetrics() = runBlocking {
        val component = DaggerActivationApiComponent.factory().create(ApiConfig(port = 0))
        val vertx = component.vertx()
        try {
            val verticle = component.apiVerticle()
            vertx.deployVerticle(verticle).coAwait()
            val port = verticle.server.actualPort()
            val client = WebClient.create(vertx)

            val health = client.get(port, "localhost", "/healthz").send().coAwait()
            assertEquals(200, health.statusCode())
            assertEquals("ok", health.bodyAsString())

            val metrics = client.get(port, "localhost", "/metrics").send().coAwait()
            assertEquals(200, metrics.statusCode())
            assertTrue(metrics.bodyAsString().contains("vertx_http_server"))
        } finally {
            vertx.close().coAwait()
        }
    }
}
