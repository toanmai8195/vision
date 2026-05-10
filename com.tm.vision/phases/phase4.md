# Phase 4 — SCION + Ollama Custom Agents

**Mục tiêu**: Planner và Reviewer chạy trong isolated Docker containers qua SCION.
Implementer giữ nguyên (gọi Ollama trực tiếp từ pipeline).

## Stack

```
pipeline (host)
  ├── planner_node  → scion start ollama-planner  → container → Ollama (host.docker.internal)
  ├── implementer   → ollama.chat() trực tiếp (giữ nguyên)
  ├── reviewer_node → scion start ollama-reviewer → container → Ollama (host.docker.internal)
  └── git_push_node → git worktree (giữ nguyên)
```

## Cài đặt SCION

```bash
go install github.com/GoogleCloudPlatform/scion/cmd/scion@latest
# đảm bảo ~/go/bin trong PATH
scion init   # trong com.tm.vision/
```

## Files thêm

```
docker/ollama-agent/
  ├── Dockerfile          # Python + ollama client
  ├── planner.py          # đọc task.txt → gọi Ollama → ghi plan.md
  └── reviewer.py         # đọc plan.md + implementation/ → gọi Ollama → ghi review.json
.scion/
  └── settings.yaml
agent/agents/
  ├── ollama-planner.yaml
  └── ollama-reviewer.yaml
```

## `docker/ollama-agent/Dockerfile`

```dockerfile
FROM python:3.12-slim
RUN pip install ollama
WORKDIR /workspace
COPY planner.py reviewer.py ./
```

## `docker/ollama-agent/planner.py`

```python
import os, ollama

task = open("/workspace/task.txt").read()
client = ollama.Client(host=os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434"))
resp = client.chat(
    model=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
    messages=[
        {"role": "system", "content": "You are a senior software architect. Given a task, produce a concise numbered implementation plan. Do NOT write code."},
        {"role": "user", "content": f"Task: {task}"},
    ],
)
open("/workspace/plan.md", "w").write(resp.message.content)
```

## `docker/ollama-agent/reviewer.py`

```python
import json, os, ollama
from pathlib import Path

plan = open("/workspace/plan.md").read()
impl = "\n\n".join(
    f"### {f.name}\n```\n{f.read_text()}\n```"
    for f in Path("/workspace/implementation").glob("*") if f.is_file()
)
client = ollama.Client(host=os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434"))
resp = client.chat(
    model=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
    messages=[
        {"role": "system", "content": 'You are a code reviewer. Respond ONLY with JSON: {"approved": true/false, "feedback": "..."}'},
        {"role": "user", "content": f"Plan:\n{plan}\n\nImplementation:\n{impl}"},
    ],
)
text = resp.message.content
data = json.loads(text[text.index("{"):text.rindex("}")+1])
open("/workspace/review.json", "w").write(json.dumps(data))
```

## `.scion/settings.yaml`

```yaml
schema_version: "1"
grove:
  worktree: ./workspace
```

## `agent/agents/ollama-planner.yaml`

```yaml
schema_version: "1"
image: ollama-agent:latest
command: ["python", "planner.py"]
env:
  OLLAMA_BASE_URL: "${OLLAMA_BASE_URL}"
  OLLAMA_MODEL: "${OLLAMA_MODEL}"
max_duration: "5m"
```

## `agent/agents/ollama-reviewer.yaml`

```yaml
schema_version: "1"
image: ollama-agent:latest
command: ["python", "reviewer.py"]
env:
  OLLAMA_BASE_URL: "${OLLAMA_BASE_URL}"
  OLLAMA_MODEL: "${OLLAMA_MODEL}"
max_duration: "5m"
```

## Nodes thay đổi

```
planner.py:
  1. ghi task vào workspace/task.txt
  2. scion start ollama-planner (hoặc docker run trực tiếp nếu SCION chưa support custom image)
  3. đọc workspace/plan.md về state["plan"]

reviewer.py:
  1. ghi state["implementation"] vào workspace/implementation/
  2. scion start ollama-reviewer
  3. đọc workspace/review.json → parse approved/feedback
```

## Build image

```bash
docker build -t ollama-agent:latest docker/ollama-agent/
```

## Deliverable

- `docker ps` thấy container `ollama-agent` khi pipeline chạy
- Pipeline cho kết quả đúng như Phase 2/3
- Planner và Reviewer isolated trong container, không chạy trực tiếp trên host
