from langgraph.graph import END, StateGraph

from agent.pipeline.config import load_project
from agent.pipeline.nodes.git_ops import git_push_node
from agent.pipeline.nodes.implementer import implementer_node
from agent.pipeline.nodes.planner import planner_node
from agent.pipeline.nodes.reviewer import reviewer_node
from agent.pipeline.state import AgentState


def _make_router(max_iterations: int):
    """Tạo hàm routing với max_iterations lấy từ project config.

    Dùng closure để truyền max_iterations vào mà không cần đọc lại config trong mỗi lần route.
    """
    def _route_after_review(state: AgentState) -> str:
        """Quyết định bước tiếp theo sau khi reviewer chạy xong.

        - Nếu code được approve → đẩy lên git.
        - Nếu đã retry đủ max_iterations → cũng đẩy lên (tránh loop vô tận).
        - Còn lại → quay lại implementer để sửa theo feedback.
        """
        if state["approved"] or state.get("review_iterations", 0) >= max_iterations:
            return "git_push"
        return "implementer"

    return _route_after_review


def build_graph(project: str = "mark1"):
    """Xây dựng và compile pipeline cho một project cụ thể.

    max_review_iterations được lấy từ project config thay vì hardcode.

    Flow:
        planner → implementer → reviewer ─(ok hoặc hết lượt)→ git_push → END
                       ↑                │
                       └──(chưa ok)─────┘
    """
    config = load_project(project)

    graph = StateGraph(AgentState)

    graph.add_node("planner", planner_node)
    graph.add_node("implementer", implementer_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("git_push", git_push_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "implementer")
    graph.add_edge("implementer", "reviewer")
    graph.add_conditional_edges(
        "reviewer",
        _make_router(config.max_review_iterations),
        {"git_push": "git_push", "implementer": "implementer"},
    )
    graph.add_edge("git_push", END)

    return graph.compile()
