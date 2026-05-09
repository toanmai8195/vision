# Phase 4 — Add SCION

**Mục tiêu**: Claude agents chạy trong isolated containers qua SCION.

## Thay đổi so với Phase 3
- Planner + Reviewer: từ gọi Anthropic SDK trực tiếp → `scion start` (container)
- Ollama Implementer: giữ nguyên (SCION chưa hỗ trợ Ollama native)
- Workspace chia sẻ qua thư mục local (chưa cần k3s)

## Cài đặt
```bash
go install github.com/GoogleCloudPlatform/scion/cmd/scion@latest
scion init   # trong com.tm.vision/
```

## Files thêm
```
.scion/
  └── settings.yaml         # grove config, runtime=local (docker)
agent/agents/
  ├── planner-agent.yaml    # claude harness, system prompt planner
  └── reviewer-agent.yaml   # claude harness, system prompt reviewer
```

## `.scion/settings.yaml`
```yaml
schema_version: "1"
grove:
  worktree: ./workspace
```

## `agent/agents/planner-agent.yaml`
```yaml
schema_version: "1"
default_harness_config: "claude"
system_prompt: |
  You are a senior software architect.
  Read workspace/task.txt → write structured plan to workspace/plan.md.
  Do NOT write code. Write ONLY plan.md then exit.
agent_instructions: "Read workspace/task.txt, write workspace/plan.md, then exit."
max_turns: 30
max_duration: "10m"
env:
  ANTHROPIC_API_KEY: "${ANTHROPIC_API_KEY}"
```

## `agent/agents/reviewer-agent.yaml`
```yaml
schema_version: "1"
default_harness_config: "claude"
system_prompt: |
  You are a senior code reviewer.
  Read workspace/implementation/ → write to workspace/review.json:
  {"approved": true/false, "feedback": "..."}
  Do NOT modify code. Write ONLY review.json then exit.
agent_instructions: "Review workspace/implementation/, write workspace/review.json, then exit."
max_turns: 30
max_duration: "10m"
env:
  ANTHROPIC_API_KEY: "${ANTHROPIC_API_KEY}"
```

## Nodes thay đổi
```
planner.py:  subprocess "scion start planner-agent ..." → poll logs → read plan.md
reviewer.py: subprocess "scion start reviewer-agent ..." → poll logs → read review.json
```

## Deliverable
- `scion list` thấy agents đang chạy trong containers khi pipeline chạy
- Pipeline vẫn cho kết quả đúng như Phase 2/3
