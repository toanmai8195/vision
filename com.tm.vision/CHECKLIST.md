# Checklist

### Phase 1 — LangGraph + FastAPI → [phases/phase1.md](phases/phase1.md)
- [ ] Tạo `requirements.txt` + `.env.example`
- [ ] Tạo `pipeline/state.py` — `AgentState(task, plan, code)`
- [ ] Tạo `pipeline/nodes/planner.py` — gọi Claude, trả plan
- [ ] Tạo `pipeline/nodes/implementer.py` — gọi Ollama, trả code
- [ ] Tạo `pipeline/graph.py` — `StateGraph`: planner → implementer → END
- [ ] Tạo `api/main.py` — FastAPI: `POST /run`, `GET /health`
- [ ] Tạo `projects/mark1.yaml` — config repo mark1
- [ ] Test: `uvicorn api.main:app` chạy không lỗi
- [ ] Test: `http://localhost:8000/docs` hiện OpenAPI UI
- [ ] Test: `POST /run {"task": "..."}` trả về `{plan, code}`

### Phase 2 — Full Pipeline → [phases/phase2.md](phases/phase2.md)
- [ ] Tạo `pipeline/nodes/reviewer.py` — Claude review code
- [ ] Tạo `pipeline/nodes/git_ops.py` — tạo branch, commit, push lên mark1
- [ ] Cập nhật `pipeline/graph.py` — thêm reviewer + conditional edges + git_push
- [ ] Mở rộng `AgentState` — thêm `implementation, review_feedback, review_iterations, approved, branch_name`
- [ ] Test: pipeline chạy đủ 4 bước (plan → implement → review → push)
- [ ] Test: branch `agent/{slug}-{ts}` xuất hiện trên github.com/toanmai8195/mark1

### Phase 3 — Multi-Project Config → [phases/phase3.md](phases/phase3.md)
- [ ] Tạo `pipeline/config.py` — load + validate `projects/*.yaml`
- [ ] Cập nhật `api/main.py` — nhận thêm field `project` trong request body
- [ ] Cập nhật `pipeline/nodes/git_ops.py` — dùng worktree thay vì clone thẳng
- [ ] Setup workspace: `git clone --bare mark1 workspace/mark1`
- [ ] Test: thêm `projects/mark1-v2.yaml` (repo giả), POST `/run {"project": "mark1-v2", ...}` chạy đúng

### Phase 4 — Add SCION → [phases/phase4.md](phases/phase4.md)
- [ ] Cài SCION CLI: `go install github.com/GoogleCloudPlatform/scion/cmd/scion@latest`
- [ ] `scion init` trong `com.tm.vision/`
- [ ] Tạo `.scion/settings.yaml`
- [ ] Tạo `agents/planner-agent.yaml`
- [ ] Tạo `agents/reviewer-agent.yaml`
- [ ] Cập nhật `pipeline/nodes/planner.py` — dùng `scion start` thay Anthropic SDK trực tiếp
- [ ] Cập nhật `pipeline/nodes/reviewer.py` — dùng `scion start`
- [ ] Test: `scion list` thấy agents trong containers khi pipeline chạy
- [ ] Test: pipeline vẫn cho kết quả đúng như Phase 2/3

### Phase 5 — Add k3s → [phases/phase5.md](phases/phase5.md)
- [ ] Cài k3s: `curl -sfL https://get.k3s.io | sh`
- [ ] Tạo `k8s/namespace.yaml` — namespace `vision-agents`
- [ ] Tạo `k8s/workspace-pvc.yaml` — PVC 5Gi (local-path)
- [ ] Cập nhật `.scion/settings.yaml` — thêm `runtime: kubernetes`
- [ ] `kubectl apply -f k8s/`
- [ ] Test: `kubectl get pods -n vision-agents` thấy SCION agent pods
- [ ] Test: pipeline chạy end-to-end trên k3s

### Phase 6 — GitHub Actions CI/CD → [phases/phase6.md](phases/phase6.md)
- [ ] Tạo `k8s/gh-runner-deployment.yaml` — self-hosted runner
- [ ] Tạo secret `gh-runner-secret` trên k3s
- [ ] `kubectl apply -f k8s/gh-runner-deployment.yaml`
- [ ] Tạo `.github/workflows/ci.yaml` trong repo **mark1** — trigger `agent/**`
- [ ] Tạo `pipeline/nodes/ci_watcher.py` — poll GitHub API
- [ ] Cập nhật `api/main.py` — thêm `POST /ci-callback`
- [ ] Cập nhật `pipeline/graph.py` — thêm `ci_watcher` node + conditional edge
- [ ] Mở rộng `AgentState` — thêm `ci_passed, ci_feedback`
- [ ] Test: push branch → Actions tự trigger trên self-hosted runner
- [ ] Test: CI fail → implementer nhận lỗi, retry
- [ ] Test: CI pass → pipeline kết thúc, response trả về branch URL
