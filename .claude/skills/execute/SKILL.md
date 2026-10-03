---
name: execute
description: Thực thi task tiếp theo trong com/tm/docs/checklist.md của repo vision. Mỗi lần gọi làm đúng 1 task chưa tick, commit; mỗi bước một branch `step-NN-<tên>` từ `main`, push khi xong cả phase. Dùng khi user gõ /execute, "làm tiếp", "next task".
---

# /execute — làm task tiếp theo trong checklist

Mỗi lần gọi = **1 task**. Không làm nhiều task một lượt.

## Quy trình

1. **Đọc** `com/tm/docs/checklist.md`. Tìm dòng `- [ ]` đầu tiên (từ trên xuống) — đó là task hiện tại, thuộc phase "Bước N".
   - Không còn `- [ ]` nào → báo "checklist đã xong", dừng.
   - Task thuộc phase sau khi phase trước còn `- [ ]` → không nhảy cóc; làm theo thứ tự.
1b. **Branch**: mỗi bước một branch `step-NN-<tên>` (tên theo `phases/step-NN-<tên>.md`).
   - Đang ở branch của bước đó → tiếp tục.
   - Task đầu của bước N (chưa có branch `step-NN-*`): `git fetch && git checkout main && git pull`, kiểm tra bước N−1 đã merge vào `main` (nội dung bước trước có trong `main`); chưa merge → **dừng, báo user** (không tạo branch từ nhánh cũ). Đã merge → `git checkout -b step-NN-<tên>`.
   - Đang ở branch của bước khác hoặc ở `main` mà bước N đã có branch → chuyển sang branch đó (working tree phải sạch).
2. **Nạp ngữ cảnh**: `CLAUDE.md` (luật §13, các mục liên quan), `com/tm/docs/phases/step-0N-*.md` của phase đó, và docs liên quan (`data-types.md`, `data-flow-examples.md`) nếu task đụng dữ liệu.
3. **Báo user** ngắn: "Task: <nội dung> (Bước N)". Nếu task mơ hồ hoặc cần quyết định của user (vd bước 1, chọn thiết kế) → hỏi, **không tự suy diễn**, không tick.
4. **Thực hiện** đúng phạm vi task đó, không làm lan sang task khác.
   - Đụng dữ liệu → xử lý các loại trong phạm vi hiện tại (xem đầu `CLAUDE.md`, các bước 11–16 của checklist; đích cuối đủ 4 loại), theo §13.
   - Thêm service vào compose chỉ khi phase này cần (xem `phases/step-00-foundation.md`).
5. **Kiểm chứng**: chạy test/lệnh phù hợp (`bazel test` cho package bị ảnh hưởng, `docker compose ... ps`, query kiểm tra…). Chưa xanh/chưa chạy được → sửa, hoặc báo user; **không tick, không commit**.
6. **Tick** `- [x]` cho task trong `checklist.md`.
7. **Commit** (1 task = 1 commit): `git add` đúng file của task + `checklist.md` (không `git add -A`; bỏ qua file untracked không liên quan như `kls-classpath`, `tools/ide/`). Message: `step N: <task>` + dòng attribution theo system-reminder.
8. **Nếu đây là task cuối của phase** — mọi `- [ ]` của phase, kể cả dòng `Done khi:`, đã `[x]`:
   - Với dòng `Done khi:`: chỉ tick khi đã thực sự kiểm chứng tiêu chí đó (chạy kiểm tra, không đoán). Dòng `Done khi:` là một task riêng: nghiệm thu rồi tick + commit.
   - Khi cả phase xong → `git push -u origin step-NN-<tên>` (không force), rồi **báo user mở/merge PR vào `main`** (không tự merge vào `main`). Push lỗi → báo user, không đổi remote.
9. **Báo cáo**: task đã làm, kết quả kiểm chứng, commit hash, task tiếp theo trong checklist (chỉ nêu, không làm), có push hay chưa.

## Luật cứng
- Không push giữa phase. Không force push. Không commit/merge thẳng lên `main`; đang ở `main` → tạo branch `step-NN-<tên>` trước. Branch dùng một lần: sau khi PR merge thì bước sau tạo branch mới từ `main`.
- Không commit khi kiểm chứng chưa xanh.
- Điều chưa xác minh → `TODO(verify)`, không đoán (CLAUDE.md §13).
- Cần đổi semantics → sửa `CLAUDE.md` + docs trước (§13.4), rồi mới code.
