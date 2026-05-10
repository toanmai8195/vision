# Phase 6 — GitHub Actions CI/CD

**Mục tiêu**: Sau git push, CI tự chạy test/build trên self-hosted runner trong k3s.
Nếu fail → feed lỗi về implementer (Ollama) retry tự động.

## Full flow

```
planner → implementer → reviewer → git_push → ci_watcher
(Ollama)    (Ollama)     (Ollama)               │ fail (lỗi build/test về implementer)
               ↑                                │
               └────────────────────────────────┘
                                                │ pass
                                               END
```

## Self-hosted runner (free, unlimited)

Runner chạy trên k3s trong namespace `vision-agents` →
không tốn phút GitHub Actions public, không giới hạn.

## Files thêm

```
agent/k8s/
  └── gh-runner-deployment.yaml    # GitHub Actions runner Pod trên k3s
agent/pipeline/nodes/
  └── ci_watcher.py                # poll GitHub API đợi CI kết quả
agent/pipeline/
  └── graph.py                     # thêm ci_watcher node + conditional edge
agent/pipeline/
  └── state.py                     # thêm ci_passed, ci_feedback
agent/api/
  └── main.py                      # thêm POST /ci-callback (optional webhook)
```

## `agent/k8s/gh-runner-deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gh-actions-runner
  namespace: vision-agents
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gh-runner
  template:
    metadata:
      labels:
        app: gh-runner
    spec:
      containers:
        - name: runner
          image: myoung34/github-runner:latest
          env:
            - name: REPO_URL
              value: "https://github.com/toanmai8195/mark1"
            - name: RUNNER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: gh-runner-secret
                  key: token
            - name: LABELS
              value: "self-hosted,k3s,vision"
          resources:
            requests:
              memory: "200Mi"
            limits:
              memory: "500Mi"
```

## Setup runner token

```bash
# Lấy token tại: github.com/toanmai8195/mark1 → Settings → Actions → Runners → New
kubectl create secret generic gh-runner-secret \
  --from-literal=token=<RUNNER_TOKEN> \
  -n vision-agents
kubectl apply -f agent/k8s/gh-runner-deployment.yaml
```

## `.github/workflows/ci.yaml` (trong repo mark1)

```yaml
name: Agent CI
on:
  push:
    branches: ["agent/**"]
jobs:
  build-and-test:
    runs-on: [self-hosted, k3s, vision]
    steps:
      - uses: actions/checkout@v4
      - name: Build
        run: go build ./...          # hoặc bazel build //...
      - name: Test
        run: go test ./...
      - name: Report result
        if: always()
        run: |
          curl -X POST "${{ secrets.PIPELINE_WEBHOOK_URL }}/ci-callback" \
            -H "Content-Type: application/json" \
            -d '{
              "status": "${{ job.status }}",
              "run_id": "${{ github.run_id }}",
              "branch": "${{ github.ref_name }}"
            }'
```

## AgentState mở rộng

```python
ci_passed: bool
ci_feedback: str   # compiler/test error output nếu fail → Ollama implementer đọc và sửa
```

## `nodes/ci_watcher.py`

```
1. Poll GitHub API mỗi 15s, tối đa 10 phút
   GET /repos/toanmai8195/mark1/actions/runs?branch={branch_name}&per_page=1
2. Đợi status == "completed"
3. state["ci_passed"] = (conclusion == "success")
4. Nếu fail: fetch logs → state["ci_feedback"] = error lines (cho Ollama implementer đọc)
```

## Conditional edge sau CI

```python
def route_after_ci(state):
    if state["ci_passed"]:
        return END
    elif state["review_iterations"] >= config.max_review_iterations:
        return END   # give up sau quá nhiều lần retry
    return "implementer"   # Ollama đọc ci_feedback, viết lại code
```

## Graph cập nhật

```
planner → implementer → reviewer ─(ok)→ git_push → ci_watcher ─(pass)→ END
               ↑           │ (fail)                     │ (fail)
               └───────────┘                            │
               ↑                                        │
               └────────────────────────────────────────┘
```

## Deliverable

- Push branch → Actions tự trigger trên self-hosted runner trong k3s
- CI fail → Ollama implementer nhận lỗi build/test, retry tự động
- CI pass → pipeline kết thúc, response trả về branch URL
