# Phase 1 — LangGraph Basics + API (Hello World)

**Mục tiêu**: Hiểu LangGraph, có API chạy được, test được ngay.

## Stack
- LangGraph `StateGraph`
- Claude (Anthropic SDK) — planner node
- Ollama qwen2.5-coder — implementer node
- FastAPI + OpenAPI docs (`/docs`)

## What to build
```
POST /run
  body: { "task": "write a Go function that adds two numbers" }

Pipeline:
  planner (Claude) → trả về plan text
  implementer (Ollama) → trả về code text

Response:
  { "plan": "...", "code": "..." }
```

## Files
```
com.tm.vision/
├── pipeline/
│   ├── state.py            # AgentState: task, plan, code
│   ├── graph.py            # StateGraph: planner → implementer → END
│   └── nodes/
│       ├── planner.py      # Claude: nhận task → trả plan
│       └── implementer.py  # Ollama: nhận plan → trả code
├── api/
│   └── main.py             # FastAPI: POST /run, GET /health
├── projects/
│   └── mark1.yaml          # config cho repo mark1
├── workspace/              # git worktrees (gitignore)
├── requirements.txt
└── .env.example
```

## AgentState
```python
class AgentState(TypedDict):
    task: str
    plan: str
    code: str
```

## Deliverable
- `uvicorn api.main:app` chạy không lỗi
- `http://localhost:8000/docs` hiện OpenAPI UI
- `POST /run {"task": "..."}` trả về `{"plan": "...", "code": "..."}`
