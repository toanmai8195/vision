---
name: phase-review
description: Xem lại các phase đã làm trong repo vision — đã có gì (code, bảng, service, test, commit), so với checklist và thiết kế. Dùng khi user gõ /phase-review, "review phase", "đã làm được gì rồi". Chỉ đọc, không sửa file.
---

# /phase-review — các phase trước đã có gì

**Chỉ đọc, không sửa file, không commit.** Mục đích: cho user nắm hiện trạng thật, ngắn gọn.

Tham số tuỳ chọn: số bước (vd `/phase-review 3`) → chỉ review bước đó; không có → mọi phase đã bắt đầu.

## Quy trình

1. Đọc `com/tm/docs/checklist.md`. Phase "đã bắt đầu" = có ít nhất một `- [x]`. Không có phase nào → báo "chưa làm gì", dừng.
2. Với mỗi phase đó, đối chiếu **thực tế** (đừng tin checkbox một mình):
   - `git log --oneline --grep "step N"` → các commit của phase.
   - Kiểm tra sản phẩm có thật trên đĩa/hệ thống: file/thư mục theo mô tả ở `phases/step-0N-*.md`, service trong `docker-compose.yml`, bảng/schema, test. Chỉ chạy lệnh đọc (`ls`, `git`, `docker compose ps`, query `SELECT`); test chỉ chạy nếu nhanh và không đổi trạng thái.
   - Lệch: task đã tick nhưng không thấy sản phẩm; hoặc có sản phẩm mà chưa tick.
3. So với thiết kế (`CLAUDE.md`, file step): còn chỗ nào làm khác thiết kế, `TODO(verify)`, hoặc chỉ hỗ trợ thiếu trong 4 loại dữ liệu.

## Báo cáo (ngắn, theo từng phase)

```
Bước N — <tên>   [x/y task] [Done khi: đạt / chưa]
  Đã có: <vài ý chính: thứ gì, ở đâu>
  Lệch/lưu ý: <chỉ nêu nếu có>
```
Cuối cùng: 1–2 dòng tổng kết (phase đang dở, điểm cần chú ý). Không liệt kê dài dòng.
