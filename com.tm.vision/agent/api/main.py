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
    description="AI coding pipeline: Claude planner → Ollama implementer → Claude reviewer → git push",
    version="0.2.0",
    lifespan=lifespan,
)


class RunRequest(BaseModel):
    task: str


class RunResponse(BaseModel):
    plan: str
    implementation: dict[str, str]
    review_iterations: int
    approved: bool
    branch_name: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run", response_model=RunResponse)
def run(req: RunRequest):
    result = _graph.invoke({
        "task": req.task,
        "plan": "",
        "implementation": {},
        "review_feedback": "",
        "review_iterations": 0,
        "approved": False,
        "branch_name": "",
    })
    return RunResponse(
        plan=result["plan"],
        implementation=result["implementation"],
        review_iterations=result["review_iterations"],
        approved=result["approved"],
        branch_name=result["branch_name"],
    )
