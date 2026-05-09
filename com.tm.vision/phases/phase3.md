# Phase 3 — Multi-Project Config

**Mục tiêu**: Hệ thống reusable — khai báo project bằng YAML, không sửa code.

## Config file per project
```yaml
# projects/mark1.yaml
name: mark1
repo: https://github.com/toanmai8195/mark1.git
branch_prefix: agent
language: [go, python]
build_tool: bazel
max_review_iterations: 3
ollama_model: qwen2.5-coder:7b
```

## API thay đổi
```
POST /run
  body: {
    "project": "mark1",   # load từ projects/mark1.yaml
    "task": "implement X"
  }
```

## Files thêm/sửa
```
pipeline/
  └── config.py           # load + validate projects/*.yaml (dùng pydantic)
pipeline/nodes/
  └── git_ops.py          # cập nhật: dùng git worktree thay vì clone thẳng
api/
  └── main.py             # cập nhật: nhận field "project" trong request
```

## Workspace (git worktree)
```bash
# Lần đầu per project (1 lần):
git clone --bare https://github.com/toanmai8195/mark1.git workspace/mark1

# Mỗi lần chạy:
git -C workspace/mark1 worktree add worktrees/run-{id} main
# ... agents làm việc trong worktrees/run-{id}/ ...
git -C workspace/mark1 worktree remove worktrees/run-{id}
```

## Deliverable
- Thêm project mới = tạo 1 file `projects/{name}.yaml`, không sửa code
- `POST /run {"project": "mark1", "task": "..."}` chạy đúng repo
