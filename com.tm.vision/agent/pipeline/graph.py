from langgraph.graph import END, StateGraph

from agent.pipeline.nodes.git_ops import git_push_node
from agent.pipeline.nodes.implementer import implementer_node
from agent.pipeline.nodes.planner import planner_node
from agent.pipeline.nodes.reviewer import reviewer_node
from agent.pipeline.state import AgentState


def _route_after_review(state: AgentState) -> str:
    """Quyết định bước tiếp theo sau khi reviewer chạy xong.

    - Nếu code được approve → đẩy lên git.
    - Nếu đã retry đủ 3 lần dù chưa approve → cũng đẩy lên (tránh loop vô tận).
    - Còn lại → quay lại implementer để sửa theo feedback.
    """
    if state["approved"] or state.get("review_iterations", 0) >= 3:
        return "git_push"
    return "implementer"


def build_graph():
    """Xây dựng và compile pipeline dưới dạng LangGraph StateGraph.

    Flow:
        planner → implementer → reviewer ─(ok hoặc hết lượt)→ git_push → END
                       ↑                │
                       └──(chưa ok)─────┘
    """
    # Khởi tạo graph với AgentState làm schema cho shared state
    graph = StateGraph(AgentState)

    # Đăng ký các node — mỗi node là một hàm Python nhận/trả AgentState
    graph.add_node("planner", planner_node)
    graph.add_node("implementer", implementer_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("git_push", git_push_node)

    # Edge cố định: luôn đi theo chiều này, không có điều kiện
    graph.set_entry_point("planner")            # bắt đầu từ planner
    graph.add_edge("planner", "implementer")
    graph.add_edge("implementer", "reviewer")

    # Edge có điều kiện: sau reviewer, gọi _route_after_review để chọn node tiếp theo
    graph.add_conditional_edges(
        "reviewer",
        _route_after_review,
        {"git_push": "git_push", "implementer": "implementer"},
    )

    graph.add_edge("git_push", END)             # sau git_push là kết thúc

    # compile() biến graph thành object có thể invoke/stream được
    return graph.compile()
