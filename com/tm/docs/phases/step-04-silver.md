# Bước 4 — Silver (L2)

| | Nội dung |
|---|---|
| **Input** | `bronze.*_raw` |
| **Output** | Iceberg `silver.*` sạch: `payment_txn`, `user_profile_scd2`, `user_product_scd2`, `churn_score`; `user_dict` (`user_id → uidx`); `UNIVERSE(ds)` |

```
bronze ──Spark──▶ dedup event_id ──▶ ds theo ICT ──▶ CDC→SCD2 ──▶ silver
                  └─ dòng lỗi ──▶ DLQ          user_id ──▶ uidx (append-only)
```

- Thêm **Spark** vào compose.
- `ds` = ngày theo Asia/Ho_Chi_Minh; thứ tự trong ngày theo `(event_ts, event_id)`.
- `uidx` số nguyên dày, chỉ append, không tái sử dụng.
- Idempotent: chạy lại cùng `ds` ra cùng kết quả. Late data ≤ 3 ngày tự reprocess.

## Checklist
**Mục tiêu**: dữ liệu sạch, user có `uidx`.
- [ ] Spark: dedup `event_id`, tính `ds` theo ICT, dòng lỗi vào DLQ
- [ ] CDC → SCD2 (profile, product)
- [ ] Dictionary `user_id → uidx` (chỉ append, không tái sử dụng)

**Done khi**: chạy lại cùng `ds` ra cùng kết quả (idempotent); khớp `data-flow-examples.md`.
