# Vision Pipeline — Setup Guide

## Yêu cầu

| Tool | Version | Cài đặt |
|------|---------|---------|
| Python | ≥ 3.12 | [python.org](https://python.org) |
| Docker Desktop | ≥ 28 | [docker.com](https://docker.com) |
| Ollama | latest | [ollama.com](https://ollama.com) |
| Go | ≥ 1.21 | `brew install go` |
| Git | any | built-in macOS |

---

## 1. Clone repo

```bash
git clone https://github.com/toanmai8195/vision.git
cd vision/com.tm.vision
```

---

## 2. Python environment

```bash
python3 -m venv ../.venv
source ../.venv/bin/activate
pip install -r requirements.txt
```

---

## 3. Ollama

```bash
# Cài Ollama
brew install ollama        # hoặc curl https://ollama.com/install.sh | sh

# Chạy Ollama server
ollama serve               # chạy ngầm ở localhost:11434

# Pull model
ollama pull qwen2.5-coder:7b
```

---

## 4. Docker — build agent image

```bash
# Đảm bảo Docker Desktop đang chạy
docker build -f docker/ollama-agent/Dockerfile -t ollama-agent:latest docker/ollama-agent/
```

> **macOS/Windows**: container gọi Ollama qua `host.docker.internal:11434` (mặc định).
> **Linux**: đổi `OLLAMA_BASE_URL=http://172.17.0.1:11434` trong `.env`.

---

## 5. SCION CLI

```bash
# Cần Go ≥ 1.21
go install github.com/GoogleCloudPlatform/scion/cmd/scion@latest
export PATH="$PATH:$(go env GOPATH)/bin"   # thêm vào ~/.zshrc để persistent

# Khởi tạo global config (1 lần)
scion init --machine

# Verify
scion version
```

---

## 6. Environment variables

```bash
cp .env.example .env
```

Mở `.env` và điền:

```env
ANTHROPIC_API_KEY=sk-ant-...       # chỉ cần nếu dùng Claude API trực tiếp
OLLAMA_BASE_URL=http://localhost:11434   # hoặc host.docker.internal:11434
OLLAMA_MODEL=qwen2.5-coder:7b
GITHUB_TOKEN=ghp_...               # cần để git push lên mark1
MARK1_REPO_URL=https://github.com/toanmai8195/mark1.git
```

---

## 7. Git workspace (cho git_ops worktree)

```bash
# Chạy 1 lần để clone bare repo mark1
mkdir -p workspace
git clone --bare https://<GITHUB_TOKEN>@github.com/toanmai8195/mark1.git workspace/mark1
```

---

## 8. Chạy server

```bash
source ../.venv/bin/activate   # nếu chưa activate
uvicorn agent.api.main:app --reload
```

Server khởi động tại `http://localhost:8000`.

---

## 9. Test

```bash
# Health check
curl http://localhost:8000/health

# OpenAPI docs
open http://localhost:8000/docs

# Chạy pipeline
curl -s -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d '{"project": "mark1", "task": "write a Go HTTP server with /greeting endpoint returning hello world"}' \
  | python3 -m json.tool
```

Kết quả: branch `agent/{slug}-{ts}` xuất hiện trên github.com/toanmai8195/mark1.

---

## Kiến trúc

```
POST /run
  │
  ├── planner_node     docker run ollama-agent python planner.py
  │                    └── Ollama (qwen2.5-coder:7b) → plan.md
  │
  ├── implementer_node ollama.chat() trực tiếp (không container)
  │                    └── Ollama (qwen2.5-coder:7b) → implementation/
  │
  ├── reviewer_node    docker run ollama-agent python reviewer.py
  │                    └── Ollama (qwen2.5-coder:7b) → review.json
  │   │
  │   ├── approved / đã 3 lần retry → git_push_node
  │   └── rejected → implementer_node (retry)
  │
  └── git_push_node    gitpython worktree → push branch lên GitHub
```

---

## Thêm project mới

Tạo file `app/projects/<name>.yaml`:

```yaml
name: myproject
repo: https://github.com/toanmai8195/myproject.git
branch_prefix: agent
language: [go, python]
max_review_iterations: 3
ollama_model: qwen2.5-coder:7b
```

Sau đó gọi:
```bash
curl -X POST http://localhost:8000/run \
  -d '{"project": "myproject", "task": "..."}'
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Fix |
|-----|------------|-----|
| `model not found` | Chưa pull model | `ollama pull qwen2.5-coder:7b` |
| `Cannot connect to Docker daemon` | Docker Desktop chưa chạy | Mở Docker Desktop |
| `connection refused :11434` | Ollama chưa chạy | `ollama serve` |
| `host.docker.internal not found` | Linux Docker | Set `OLLAMA_BASE_URL=http://172.17.0.1:11434` |
| `GITHUB_TOKEN` push fail | Token hết hạn/thiếu quyền | Tạo token mới với scope `repo` |
| `workspace/mark1` not found | Chưa setup bare clone | Chạy bước 7 |
