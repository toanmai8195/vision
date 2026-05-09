# Phase 2 — Full Pipeline (Reviewer + Git Push)

**Mục tiêu**: Thêm reviewer loop và git push, pipeline hoàn chỉnh không cần SCION/k3s.

## Flow
```
planner → implementer → reviewer (Claude)
               ↑              │ rejected (max 3x)
               └──────────────┘
                               │ approved
                          git_push → branch trên mark1
```

## Files thêm
```
pipeline/nodes/
  ├── reviewer.py   # Claude: đọc code → {"approved": bool, "feedback": str}
  └── git_ops.py    # gitpython: tạo branch agent/{slug}-{ts}, commit, push
pipeline/
  └── graph.py      # cập nhật: thêm reviewer + conditional edges + git_push
```

## AgentState mở rộng
```python
class AgentState(TypedDict):
    task: str
    plan: str
    implementation: dict[str, str]  # {filepath: code}
    review_feedback: str
    review_iterations: int
    approved: bool
    branch_name: str
```

## Conditional edge
```python
def route_after_review(state):
    if state["approved"] or state["review_iterations"] >= 3:
        return "git_push"
    return "implementer"
```

## Deliverable
- `POST /run` → pipeline chạy đủ 4 bước
- Branch `agent/{slug}-{ts}` xuất hiện trên github.com/toanmai8195/mark1
