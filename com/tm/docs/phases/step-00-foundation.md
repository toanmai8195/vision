# Bước 0 — Nền móng

> Chỉ dựng chỗ để code và chạy thử. Chưa có nghiệp vụ, chưa có dữ liệu. Việc cần làm: `../phases.md`.

| | Nội dung |
|---|---|
| **Input** | Version pin ở `CLAUDE.md` §8; cần tham khảo layout/macro Bazel thì xem repo `thor` (`/Users/toanmai/Documents/code/thor`) |
| **Output** | Bazel 8 build/test được Go · Kotlin · Python; macro image `com_tm_{py,go,kt}_image`; `docker-compose.yml` rỗng khung |

```
MODULE.bazel + BUILD.bazel ──Bazel 8──▶ binary ──rules_oci──▶ image  (com.tm.<lang>.<name>:v1.0.0)
docker-compose.yml (khung) ◀── mỗi bước sau chỉ thêm service nó cần
```

- Công nghệ: Bazel 8 (bzlmod), rules_go + gazelle, rules_kotlin, rules_python, rules_oci. Version pin ở `CLAUDE.md` §8.
- Không dựng Kafka, Spark, StarRocks… ở đây; xem bảng "Service thêm dần" trong `../phases.md`.

## Service thêm dần theo bước
| Bước | Thêm vào compose |
|---|---|
| 2 | Postgres OLTP (nguồn) |
| 3 | Kafka, Debezium, Flink, MinIO + Iceberg REST |
| 4 | Spark |
| 5 | StarRocks, Postgres `meta` (catalog) |
| 9 | Redis |
| 10 | Airflow, Prometheus, Grafana (Airflow có thể đưa vào sớm hơn khi cần chạy DAG) |

Checklist: xem mục "Bước 0" trong `../checklist.md`.
