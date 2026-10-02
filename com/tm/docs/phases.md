# Kế hoạch triển khai — theo từng layer

Đi từ bài toán nghiệp vụ: biết có dữ liệu gì → đưa vào hệ thống → mới khai báo attribute theo nhu cầu.
**Phạm vi hiện tại (tối giản):** chỉ 1 nguồn — S2a `user_profile` → attribute `user_city` (`MUTEX`, `STATE`). Làm xong cả luồng bước 3→9 với nguồn này, rồi mới mở rộng lần lượt: S1 payment (`NOT_MUTEX` → `PARTIAL_VALUE` / `PARTIAL_VALUE_BY_TAG` + `aggFunc`) → S2b product (`NOT_MUTEX` `STATE`) → S3 churn (`MUTEX` `EVENT`) → S4/S5 (`EXTENDED`). Danh sách ở mục "Mở rộng" của `checklist.md`.

Đích cuối: đủ 4 loại (MUTEX · NOT_MUTEX · PARTIAL_VALUE · PARTIAL_VALUE_BY_TAG). Mỗi lần mở rộng, bước đã làm phải chạy đúng thêm loại mới.
Chi tiết thiết kế: `CLAUDE.md`. Flow dữ liệu từng bước: `phases/step-NN-*.md`. **Checklist tổng hợp (tiến độ): `checklist.md`.** Ví dụ input/output (dùng làm golden test): `data-flow-examples.md`.

| Bước | Layer | Kết quả |
|---|---|---|
| [0](phases/step-00-foundation.md) | Nền móng | Bazel 8 chạy được, compose khung |
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
