# Vision — AI Coding Pipeline

## Khi bắt đầu session mới

1. Đọc `CHECKLIST.md`
2. Tìm item `[ ]` đầu tiên chưa được tick
3. Báo cho user: đang ở item nào, thuộc phase nào
4. Hỏi "bắt đầu không?" trước khi làm
5. Khi user đồng ý: đọc file `phases/phase{N}.md` tương ứng rồi thực hiện
6. Sau khi xong mỗi item: tick `[x]` trong `CHECKLIST.md`, commit, rồi tiếp item tiếp theo

## Repos

- `vision` (repo này): orchestration system — pipeline, agents, k8s config
- `mark1` (`github.com/toanmai8195/mark1`): target project — agents viết code vào đây

## Workspace

Agents làm việc qua git worktree, không clone lại từ đầu mỗi lần:
```
workspace/mark1/          ← bare clone (tạo 1 lần)
  worktrees/run-{id}/     ← isolated per run
```
