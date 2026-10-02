---
name: phase-plan
description: Xem trước phase sắp làm (hoặc đang làm dở) trong repo vision — mô tả ra sao, có gì cần điều chỉnh trước khi thực hiện task. Dùng khi user gõ /phase-plan, "plan phase", "trước khi làm bước N". Chỉ đọc và đề xuất, sửa docs chỉ khi user đồng ý.
---

# /phase-plan — xem và chỉnh phase trước khi làm

Tham số tuỳ chọn: số bước (vd `/phase-plan 5`). Không có → phase đầu tiên còn `- [ ]` trong `com/tm/docs/checklist.md` (kể cả đang làm dở).

## Quy trình

1. Xác định phase. Đọc mục của nó trong `checklist.md` và `com/tm/docs/phases/step-0N-*.md`.
2. Đọc phần liên quan của `CLAUDE.md` (layer tương ứng, §13) và kết quả phase trước (chạy nhanh `/phase-review` logic: phase trước đã có gì, output nào phase này dùng làm input).
3. Kiểm tra phase theo các câu hỏi:
   - **Input sẵn sàng?** Output phase trước có đủ cho phase này không.
   - **Task đủ rõ?** Mỗi task có làm được mà không phải đoán; thiếu quyết định nào của user.
   - **Thứ tự hợp lý?** Có task phụ thuộc task sau; thiếu task (vd quên thêm service compose, test, xử lý các loại dữ liệu trong phạm vi hiện tại).
   - **Quá to / thừa?** Task nào nên tách (một commit không gọn) hoặc bỏ.
   - **Done khi đo được?** Tiêu chí kiểm chứng cụ thể, chạy được.
   - **Đụng thiết kế?** Có mâu thuẫn với `CLAUDE.md` (cần sửa thiết kế trước, §13.4).
   - Nếu phase đang dở: task nào đã tick, còn lại gì, có cần điều chỉnh do những gì đã làm lộ ra.
4. **Báo cáo** ngắn:
   ```
   Bước N — <tên>  [x/y task]
   Mô tả: <1–3 dòng: phase làm gì, input → output>
   Task tiếp theo: <dòng - [ ] đầu tiên>
   Cần điều chỉnh: <danh sách đánh số, mỗi ý 1 dòng; "không có" nếu ổn>
   Cần bạn quyết: <câu hỏi cụ thể, nếu có>
   ```
5. **Không tự sửa.** Nếu user đồng ý các điều chỉnh → sửa `checklist.md` / file step / `CLAUDE.md` (theo §13.4 nếu đổi thiết kế) rồi tạo **1 commit** `step N: adjust plan`. Không push (push chỉ khi xong phase, do `/execute`). Không bắt đầu làm task — việc đó của `/execute`.
