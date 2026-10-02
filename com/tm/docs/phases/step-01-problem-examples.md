# Bước 1 — Bài toán + ví dụ (L0)

> Chỉ là tài liệu, không có code. Chốt bài toán trước khi chọn công nghệ.

| | Nội dung |
|---|---|
| **Input** | Nhu cầu nghiệp vụ; `CLAUDE.md` §0, §3; `data-types.md` |
| **Output** | Mô tả bài toán kỹ thuật + vài segment thực tế đi qua dữ liệu cụ thể + danh sách nguồn dữ liệu và ca biên |

```
Bài toán ──▶ segment ví dụ ──▶ nguồn dữ liệu cần (payment, profile, churn…) ──▶ ca biên (trùng, muộn, xoá/đổi)
```

- Tài liệu mô tả đủ các nguồn (đích cuối), nhưng giai đoạn hiện tại chỉ triển khai S2a `user_profile`; ví dụ đầu tiên là `seg_0001` (`data-flow-examples.md` §0.1).
- Mỗi nguồn ghi rõ: ai sinh ra, dạng **event** hay **snapshot**, khoá, cột thời gian.
- Ví dụ dùng `data-flow-examples.md` làm khung; đây cũng là golden test cho các bước sau.
- Chưa tạo attribute hay catalog ở bước này.

Checklist: xem mục "Bước 1" trong `../checklist.md`.
