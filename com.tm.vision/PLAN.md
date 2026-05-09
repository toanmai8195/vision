# Vision: Multi-Project AI Coding Pipeline — Phased Roadmap


## Mục tiêu cuối

Một hệ thống **reusable**: chỉ cần khai báo repo + yêu cầu → pipeline tự động
plan → implement → review → push code → CI/CD.

## Repos

| Repo | URL | Vai trò |
|------|-----|---------|
| `vision` | https://github.com/toanmai8195/vision | Orchestration system (pipeline, agents, k8s config) |
| `mark1` | https://github.com/toanmai8195/mark1 | Target project — nơi agents viết code và push |

Thêm project mới = thêm 1 file `projects/{name}.yaml` trong `vision`.

## Workspace (git worktree)

```
vision/com.tm.vision/
  workspace/
    mark1/              ← bare clone của mark1 (1 lần duy nhất)
      worktrees/
        run-{id}/       ← isolated worktree mỗi lần chạy
```

---

## Phase 1 — LangGraph Basics + API (Hello World)

**Mục tiêu**: Hiểu LangGraph, có API chạy được, test được ngay.

### Stack
- LangGraph `StateGraph`
- Claude (Anthropic SDK) — planner node
- Ollama qwen2.5-coder — implementer node
- FastAPI + OpenAPI docs (`/docs`)

### What to build
```
POST /run
  body: { "task": "write a Go function that adds two numbers" }

Pipeline:
  planner (Claude) → trả về plan text
  implementer (Ollama) → trả về code text

Response:
  { "plan": "...", "code": "..." }
```

### Files
```
com.tm.vision/
├── pipeline/
│   ├── state.py        # AgentState: task, plan, code
│   ├── graph.py        # StateGraph: planner → implementer → END
│   └── nodes/
│       ├── planner.py  # Claude: nhận task → trả plan
│       └── implementer.py  # Ollama: nhận plan → trả code
├── api/
│   └── main.py         # FastAPI: POST /run, GET /health
├── projects/
│   └── mark1.yaml      # config cho repo mark1
├── workspace/          # git worktrees (gitignore)
├── requirements.txt
└── .env.example
```

### Deliverable
- `uvicorn api.main:app` chạy được
- Vào `http://localhost:8000/docs` thấy OpenAPI UI
- POST `/run` trả về plan + code

---

## Phase 2 — Full Pipeline (Reviewer + Git Push)

**Mục tiêu**: Thêm reviewer loop và git push, pipeline hoàn chỉnh không cần SCION/k3s.

### What to add
```
planner → implementer → reviewer (Claude)
               ↑              │ rejected (max 3x)
               └──────────────┘
                               │ approved
                          git_push → branch trên remote
```

### Files thêm
```
pipeline/nodes/
  ├── reviewer.py     # Claude: đọc code → approved/feedback
  └── git_ops.py      # gitpython: tạo branch, commit, push
pipeline/
  └── graph.py        # cập nhật: thêm reviewer + conditional edges
```

### AgentState mở rộng
```python
task, plan, implementation: dict[str,str],
review_feedback, review_iterations, approved, branch_name
```

### Deliverable
- POST `/run` → pipeline chạy đủ 4 bước
- Git branch `agent/{slug}-{ts}` xuất hiện trên remote

---

## Phase 3 — Multi-Project Config

**Mục tiêu**: Hệ thống reusable — khai báo project, không cần sửa code.

### Config file per project
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

### API thay đổi
```
POST /run
  body: {
    "project": "mark1",        # load từ projects/mark1.yaml
    "task": "implement X"
  }
```

### Project Registry
```
com.tm.vision/
├── projects/
│   ├── mark1.yaml             ← project đầu tiên
│   └── another-project.yaml  ← thêm project mới = thêm file này
└── pipeline/
    └── config.py    # load + validate project config
```

### Deliverable
- Thêm project mới = tạo 1 file YAML, không sửa code
- POST `/run` với `project` khác nhau → clone đúng repo, push đúng remote

---

## Phase 4 — Add SCION

**Mục tiêu**: Claude agents chạy trong isolated containers qua SCION thay vì gọi API trực tiếp.

### Thay đổi
- Planner + Reviewer: từ "gọi Anthropic SDK trực tiếp" → "scion start ... (container)"
- Ollama Implementer: giữ nguyên (SCION chưa hỗ trợ Ollama native)
- Workspace chia sẻ qua thư mục local (phase này chưa cần k3s)

### Files thêm
```
.scion/settings.yaml
agents/
  ├── planner-agent.yaml
  └── reviewer-agent.yaml
```

### Nodes thay đổi
```
planner.py:  subprocess "scion start" → poll logs → read plan.md
reviewer.py: subprocess "scion start" → poll logs → read review.json
```

### Deliverable
- `scion list` thấy agents đang chạy trong containers
- Pipeline vẫn hoạt động như Phase 2/3 nhưng agents isolated

---

## Phase 5 — Add k3s

**Mục tiêu**: SCION agents chạy trên k3s (Kubernetes local), workspace qua PVC.

### Thay đổi
- `.scion/settings.yaml`: thêm `runtime: kubernetes`
- Workspace: từ local dir → PersistentVolumeClaim trên k3s
- Manifests: namespace, PVC

### Files thêm
```
k8s/
  ├── namespace.yaml
  └── workspace-pvc.yaml
.scion/settings.yaml   # cập nhật runtime config
```

### Deliverable
- `kubectl get pods -n vision-agents` thấy SCION agent pods
- Pipeline chạy end-to-end trên k3s

---

## Phase 6 — Add GitHub Actions CI/CD

**Mục tiêu**: Sau git push, CI tự chạy test/build, nếu fail feed lỗi về implementer.

### What to add
- `.github/workflows/ci.yaml`: trigger on `agent/**` branches
- Self-hosted runner deploy lên k3s (free, unlimited)
- `nodes/ci_watcher.py`: poll GitHub API đợi CI kết quả
- Webhook: CI gọi về `/ci-callback` → LangGraph tiếp tục

### Full final flow
```
planner → implementer → reviewer → git_push → ci_watcher
               ↑                                   │ fail
               └───────────────────────────────────┘
                                                    │ pass
                                                   END
```

### Files thêm
```
.github/workflows/ci.yaml
k8s/gh-runner-deployment.yaml
pipeline/nodes/ci_watcher.py
api/main.py   # thêm POST /ci-callback endpoint
```

### Deliverable
- Push branch → GitHub Actions tự chạy
- CI fail → implementer nhận lỗi, tự sửa, push lại
- CI pass → pipeline kết thúc, báo kết quả qua API

---

## Summary

| Phase | Focus | Học được |
|-------|-------|---------|
| P1 | LangGraph + FastAPI | StateGraph, nodes, API |
| P2 | Full pipeline | Conditional edges, git ops |
| P3 | Multi-project config | Reusability, project registry |
| P4 | SCION | Container isolation, agent management |
| P5 | k3s | Kubernetes local, PVC, pod scheduling |
| P6 | GitHub Actions | CI/CD, self-hosted runner, feedback loop |

Mỗi phase là một milestone độc lập, có thể demo và test ngay.

