# P7 — Observability ⬜

> Biết hệ thống có chạy đúng hạn và đúng dữ liệu không. Dữ liệu ở đây là **metric và số liệu vận hành**, không phải dữ liệu nghiệp vụ.
> Thiết kế gốc: `CLAUDE.md` §9, §10.

## Input / Output

| | Nội dung | Nơi phát ra | Dạng |
|---|---|---|---|
| **Input** | Metric service | Go `client_golang`, Kotlin Micrometer (`/metrics`) | Prometheus exposition |
| **Input** | Metric Airflow | StatsD → statsd-exporter | StatsD |
| **Input** | Metric job batch (Spark, SQL) | Pushgateway | Prometheus |
| **Input** | Metric hạ tầng | StarRocks, Flink, Kafka built-in | Prometheus |
| **Input** | Số liệu theo tag / segment (cardinality cao) | StarRocks `dq.result`, `seg.build_stats` | bảng |
| **Output** | Time series | Prometheus | TSDB |
| **Output** | Dashboard | Grafana `com/tm/src/observability/grafana/` | JSON dashboard |
| **Output** | Alert | Prometheus rules → Alertmanager | rule YAML |

## Flow

```
service /metrics ─────────────┐
Airflow ──StatsD──▶ exporter ──┤
batch ──push──▶ Pushgateway ───┼──scrape──▶ Prometheus ──▶ alert rules ──▶ Alertmanager
StarRocks / Flink / Kafka ─────┘                  │
                                                  ▼
StarRocks dq.result, seg.build_stats ──MySQL datasource──▶ Grafana ◀── Prometheus datasource
```

## Các bước

### Bước 1 — Chuẩn metric
- Tên `vision_<component>_<what>_<unit>`, vd `vision_segment_builder_build_duration_seconds`.
- Label cho phép: `component`, `source`, `attr_group`, `date_range`, `data_type`, `agg_func`, `attribute_type`, `status`, `endpoint`. **Cấm** `user_id`, `uidx`, `tag_id`, `segment_id` → số liệu theo tag / segment để trong StarRocks.

### Bước 2 — Dashboard
Pipeline freshness / SLA · chi phí temporal **theo data_type** · DQ · segment build · activation API · StarRocks.

### Bước 3 — Alert
| Alert | Điều kiện |
|---|---|
| `VisionRangeNotReady` | 05:30 chưa có range ngày `ds` |
| `VisionDQBlocked` | có check BLOCK fail |
| `VisionSegmentBuildFailureRatio` | > 1% segment build lỗi |
| `VisionSegmentPublishLate` | 07:00 chưa publish đủ |
| `VisionApiP99High` | p99 vượt SLO |
| `VisionCollectorKafkaErrors` | collector ghi Kafka lỗi |

## Công nghệ
Prometheus · Grafana · statsd-exporter · Pushgateway · Micrometer · client_golang.

## Kiểm tra
Cố ý làm hỏng 1 DQ / chậm 1 DAG / lỗi API → alert tương ứng bắn; dashboard tách được chi phí và độ trễ theo 4 loại dữ liệu.
