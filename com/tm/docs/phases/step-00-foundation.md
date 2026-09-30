# Bước 0 — Nền móng

> Chỉ dựng chỗ để code và chạy thử. Chưa có nghiệp vụ, chưa có dữ liệu. Việc cần làm: `../phases.md`.

| | Nội dung |
|---|---|
| **Input** | Version pin ở `CLAUDE.md` §8; cần tham khảo layout/macro Bazel thì xem repo `thor` (`/Users/toanmai/Documents/code/thor`) |
| **Output** | Bazel 8 chạy được (`bazel test //...` với `sh_test` mẫu); `docker-compose.yml` rỗng khung. Chưa có ngôn ngữ nào |

```
MODULE.bazel + BUILD.bazel ──Bazel 8──▶ sh_test mẫu xanh
docker-compose.yml (khung) ◀── mỗi bước sau chỉ thêm service nó cần
```

- Công nghệ: chỉ Bazel 8 (bzlmod). Version pin ở `CLAUDE.md` §8.
- Ngôn ngữ thêm khi có code thật dùng: Python + macro `com_tm_py_image` ở bước 2 (seed generator); Go, Kotlin + macro image ở bước 8.
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
