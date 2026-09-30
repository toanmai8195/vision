# Kế hoạch triển khai — theo từng layer

Đi từ bài toán nghiệp vụ: biết có dữ liệu gì → đưa vào hệ thống → mới khai báo attribute theo nhu cầu.
Quy tắc chung: mọi bước đụng dữ liệu chỉ **done** khi chạy đúng cả 4 loại (MUTEX · NOT_MUTEX · PARTIAL_VALUE · PARTIAL_VALUE_BY_TAG).
Chi tiết thiết kế: `CLAUDE.md`. Flow dữ liệu + **checklist** (việc cần làm, tiêu chí done) nằm trong file từng bước: `phases/step-NN-*.md`. Ví dụ input/output (dùng làm golden test): `data-flow-examples.md`.

| Bước | Layer | Kết quả |
|---|---|---|
| [0](phases/step-00-foundation.md) | Nền móng | repo build được, compose khung |
| [1](phases/step-01-problem-examples.md) | L0 Bài toán + ví dụ | tài liệu: bài toán kỹ thuật, ví dụ cụ thể |
| [2](phases/step-02-seed-oltp.md) | L0 Seed OLTP | DB nguồn (OLTP) có schema + data mẫu |
| [3](phases/step-03-bronze.md) | L1 Bronze (OLAP) | data từ OLTP đi vào Iceberg bronze |
| [4](phases/step-04-silver.md) | L2 Silver | dữ liệu sạch, có `uidx` |
| [5](phases/step-05-daily.md) | L3 Daily | khai báo attribute từ nhu cầu + `ADD/DEL`, `pv_daily` theo ngày |
| [6](phases/step-06-temporal.md) | L4 Temporal | block + LATEST/POS/STATE |
| [7](phases/step-07-range.md) | L5 Range | bitmap / giá trị cho mọi date range |
| [8](phases/step-08-segment.md) | L6 Segment | build segment từ DSL, publish |
| [9](phases/step-09-activation.md) | L7 Activation | API tra cứu segment |
| [10](phases/step-10-operations.md) | Vận hành | DQ, Airflow, metrics, scale |
