from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Load .env trước khi import graph — vì graph đọc os.getenv() lúc import
load_dotenv()

from agent.pipeline.config import load_project  # noqa: E402
from agent.pipeline.graph import build_graph    # noqa: E402

# Cache graph theo project — tránh build lại mỗi request
_graphs: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-build graph cho project mặc định khi server khởi động."""
    _graphs["mark1"] = build_graph("mark1")
    yield


app = FastAPI(
    title="Vision Pipeline",
    description="AI coding pipeline: planner → implementer → reviewer → git push",
    version="0.3.0",
    lifespan=lifespan,
)


class RunRequest(BaseModel):
    project: str = "mark1"  # tên project trong app/projects/{project}.yaml
    task: str               # mô tả task muốn agent thực hiện


class RunResponse(BaseModel):
    project: str
    plan: str
    implementation: dict[str, str]
    review_iterations: int
    approved: bool
    branch_name: str


@app.get("/health")
def health():
    """Kiểm tra server còn sống không."""
    return {"status": "ok"}


@app.post("/run", response_model=RunResponse)
def run(req: RunRequest):
    """Chạy toàn bộ pipeline cho một project cụ thể.

    Project được load từ app/projects/{project}.yaml.
    Graph được cache — lần đầu mỗi project sẽ build graph, sau đó tái sử dụng.
    """
    # Validate project tồn tại trước khi chạy
    try:
        load_project(req.project)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Build graph lần đầu nếu chưa có trong cache
    if req.project not in _graphs:
        _graphs[req.project] = build_graph(req.project)

    graph = _graphs[req.project]

    result = graph.invoke({
        "project": req.project,
        "task": req.task,
        "plan": "",
        "implementation": {},
        "review_feedback": "",
        "review_iterations": 0,
        "approved": False,
        "branch_name": "",
    })

    return RunResponse(
        project=result["project"],
        plan=result["plan"],
        implementation=result["implementation"],
        review_iterations=result["review_iterations"],
        approved=result["approved"],
        branch_name=result["branch_name"],
    )
