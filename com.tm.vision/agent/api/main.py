from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

from agent.pipeline.graph import build_graph  # noqa: E402 — load_dotenv phải chạy trước

_graph = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _graph
    _graph = build_graph()
    yield


app = FastAPI(
    title="Vision Pipeline",
    description="AI coding pipeline: Claude planner → Ollama implementer",
    version="0.1.0",
    lifespan=lifespan,
)


class RunRequest(BaseModel):
    task: str


class RunResponse(BaseModel):
    plan: str
    code: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run", response_model=RunResponse)
def run(req: RunRequest):
    result = _graph.invoke({"task": req.task, "plan": "", "code": ""})
    return RunResponse(plan=result["plan"], code=result["code"])
