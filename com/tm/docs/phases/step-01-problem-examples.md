# Bước 1 — Bài toán + ví dụ (L0)

> Chỉ là tài liệu, không có code. Chốt bài toán trước khi chọn công nghệ.

| | Nội dung |
|---|---|
| **Input** | Nhu cầu nghiệp vụ; `CLAUDE.md` §0, §3; `data-types.md` |
| **Output** | Mô tả bài toán kỹ thuật + vài segment thực tế đi qua dữ liệu cụ thể + danh sách nguồn dữ liệu và ca biên |

```
Bài toán ──▶ segment ví dụ ──▶ nguồn dữ liệu cần (payment, profile, churn…) ──▶ ca biên (trùng, muộn, xoá/đổi)
```

- Mỗi nguồn ghi rõ: ai sinh ra, dạng **event** hay **snapshot**, khoá, cột thời gian.
- Ví dụ dùng `data-flow-examples.md` làm khung; đây cũng là golden test cho các bước sau.
- Chưa tạo attribute hay catalog ở bước này.

## Checklist
**Mục tiêu**: chốt bài toán trước khi đụng công nghệ. Chỉ viết tài liệu, chưa có code, chưa tạo attribute.
- [ ] Bài toán kỹ thuật: phân khúc user để làm gì, vào/ra của hệ thống, ràng buộc (quy mô, SLA)
- [ ] Ví dụ cụ thể: vài segment thực tế, đi qua dữ liệu thật từng bước (dùng `data-flow-examples.md` làm khung)
- [ ] Các nguồn dữ liệu (payment, profile, churn score…): ai sinh ra, dạng event hay snapshot, ca biên (trùng, đến muộn, xoá/đổi giá trị)

**Done khi**: bạn đọc xong và đồng ý đó là bài toán cần giải.
