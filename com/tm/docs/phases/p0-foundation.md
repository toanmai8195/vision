# P0 — Foundation ✅

> Dựng nền móng: build, contract dữ liệu, catalog, stack local. **Chưa có dữ liệu nghiệp vụ chảy qua.**
> Thiết kế gốc: `CLAUDE.md` §1, §8. Việc cần làm + checkbox: `../phases.md`.

## Input / Output

| | Nội dung | Nơi lưu | Dạng |
|---|---|---|---|
| **Input** | Thiết kế trong `CLAUDE.md`; layout/macro Bazel của repo `pandora` | — | tài liệu, code mẫu |
| **Output 1** | Contract dữ liệu | `com/tm/proto/vision/{event,catalog,segment}/v1/*.proto` | Protobuf → code Go / Java-Kotlin / Python |
| **Output 2** | Catalog attribute/tag + 6 attribute mẫu (đủ 4 loại, EVENT/STATE) | Postgres schema `meta` (`com/tm/src/sql/postgres/migrations/V1__meta_schema.sql`, `V2__seed_example_attributes.sql`) | bảng quan hệ, migration Flyway |
| **Output 3** | Image service | Docker local: `com.tm.{py,go,kt,airflow}.<name>:v1.0.0` | OCI image (rules_oci) |
| **Output 4** | Stack local | `com/tm/docker/vision/docker-compose.yml` | container |

## Flow

```
CLAUDE.md ──▶ proto (.proto) ──protoc (Bazel)──▶ code Go / Kotlin / Python dùng chung
          ──▶ SQL migration ──Flyway──▶ Postgres meta.attribute, meta.tag, meta.tag_rule, …
          ──▶ BUILD.bazel + macro ──Bazel 8──▶ binary ──rules_oci──▶ image Docker
          ──▶ docker-compose ──▶ Kafka · Flink · MinIO + Iceberg REST · Spark · StarRocks · Postgres · Redis · Airflow · Prometheus · Grafana
```

## Các bước

### Bước 1 — Contract bằng Protobuf
- **Công nghệ**: Protobuf 29.3, `rules_proto`, `py_proto_library` / `go_proto_library` / `java_proto_library`.
- **Biến đổi**: định nghĩa trong `.proto` → class sinh tự động cho 3 ngôn ngữ; không có DTO viết tay.

| Proto | Mô tả | Ví dụ |
|---|---|---|
| `event.v1.DataEvent` | event đầu vào của 4 loại | `{event_id:"e-9001", user_id:"U1001", attr:"txn_category", event_ts_ms:…, tag:{tags_add:["fnb"]}}` |
| `catalog.v1.Attribute` / `Tag` | cấu hình attribute | `{id:101, name:"txn_category", data_type:NOT_MUTEX, feed_mode:EVENT, supported_date_ranges:[A1,A7,A30]}` |
| `segment.v1.Rule` / `Condition` | DSL segment | `{operator:SUB, children:[…]}` |

### Bước 2 — Catalog trong Postgres
- **Công nghệ**: Postgres 16, Flyway 11 (service `flyway` trong compose).
- **Biến đổi**: migration tạo schema `meta` và seed 6 attribute trong `data-flow-examples.md` §0:

| attr_id | name | dataType | feedMode |
|---|---|---|---|
| 101 | txn_category | NOT_MUTEX | EVENT |
| 102 | txn_amount | PARTIAL_VALUE | EVENT |
| 103 | txn_amount_by_category | PARTIAL_VALUE_BY_TAG | EVENT |
| 201 | user_city | MUTEX | STATE |
| 202 | product_holding | NOT_MUTEX | STATE |
| 301 | churn_score_band | MUTEX | EVENT |

- Luật catalog (`com/tm/src/common/python/catalog/validation.py`) và CHECK constraint Postgres cùng chặn tổ hợp sai (vd PARTIAL_VALUE + STATE).

### Bước 3 — Build & image
- **Công nghệ**: Bazel 8.7 (bzlmod), rules_python / rules_go + gazelle / rules_kotlin + Dagger2, rules_oci.
- **Biến đổi**: source → binary → image qua macro `com_tm_{py,go,kt,airflow}_image`. Service "hello": `event-collector` (Go), `activation-api` (Kotlin/Vert.x), `simulator` (Python), image Airflow.

### Bước 4 — Stack local
- **Công nghệ**: docker compose.
- **Biến đổi**: không có dữ liệu; chỉ đảm bảo mọi service healthy để P2+ dùng.

## Kiểm tra
- `bazel build //... && bazel test //...` xanh.
- `docker compose -f com/tm/docker/vision/docker-compose.yml up -d` → mọi service healthy.
- `psql … -c "select id, name, data_type, feed_mode from meta.attribute"` → 6 dòng ở trên.
