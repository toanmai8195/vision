# Bước 4 — Silver (L2)

| | Nội dung |
|---|---|
| **Input** | `bronze.user_profile_cdc_raw` |
| **Output** | Iceberg `silver.user_profile_scd2`; `silver.user_dict` (`user_id → uidx`); `UNIVERSE(ds)`. (Sau này thêm `payment_txn`, `user_product_scd2`, `churn_score` khi mở rộng.) |

```
bronze ──Spark──▶ dedup event_id ──▶ ds theo ICT ──▶ CDC→SCD2 ──▶ silver
                  └─ dòng lỗi ──▶ DLQ          user_id ──▶ uidx (append-only)
```

- Thêm **Spark** vào compose.
- Hiện chỉ nhánh CDC → SCD2 (`user_profile`); dedup `event_id` áp dụng khi có nguồn event (payment…).
- `ds` = ngày theo Asia/Ho_Chi_Minh; thứ tự trong ngày theo `(event_ts, event_id)`.
- `uidx` số nguyên dày, chỉ append, không tái sử dụng.
- Idempotent: chạy lại cùng `ds` ra cùng kết quả. Late data ≤ 3 ngày tự reprocess.

Checklist: xem mục "Bước 4" trong `../checklist.md`.
