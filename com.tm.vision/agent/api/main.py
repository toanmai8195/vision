from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

# Load .env trước khi import graph — vì graph đọc os.getenv() lúc import
load_dotenv()

from agent.pipeline.graph import build_graph  # noqa: E402

_graph = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build graph 1 lần khi server khởi động, tái sử dụng cho mọi request.

    Dùng lifespan thay vì global init để đảm bảo .env đã được load trước.
    """
    global _graph
    _graph = build_graph()
    yield  # server chạy ở đây cho đến khi shutdown


app = FastAPI(
    title="Vision Pipeline",
    description="AI coding pipeline: Ollama planner → Ollama implementer → Ollama reviewer → git push",
    version="0.2.0",
    lifespan=lifespan,
)


class RunRequest(BaseModel):
    task: str  # mô tả task muốn agent thực hiện


class RunResponse(BaseModel):
    plan: str                      # kế hoạch implement
    implementation: dict[str, str] # code đã viết {filepath: code}
    review_iterations: int         # số lần review đã chạy
    approved: bool                 # reviewer có approve không
    branch_name: str               # branch đã push lên GitHub


@app.get("/health")
def health():
    """Kiểm tra server còn sống không."""
    return {"status": "ok"}


@app.post("/run", response_model=RunResponse)
def run(req: RunRequest):
    """Chạy toàn bộ pipeline: plan → implement → review → git push.

    Đồng bộ (blocking) — client cần chờ đến khi pipeline hoàn tất.
    """
    # Khởi tạo state với giá trị rỗng — các node sẽ điền dần vào
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
