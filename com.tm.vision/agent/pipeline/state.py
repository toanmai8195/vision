from typing import TypedDict


class AgentState(TypedDict):
    """Bộ nhớ chung được truyền qua tất cả các node trong pipeline.

    Mỗi node nhận state này, đọc những gì cần, rồi trả về
    dict chứa các field muốn cập nhật — LangGraph tự merge vào state.
    """

    task: str                   # input từ user: "write a function that..."
    plan: str                   # planner điền vào: danh sách bước implement
    implementation: dict[str, str]  # implementer điền vào: {filepath: code}
    review_feedback: str        # reviewer điền vào: feedback nếu chưa approve
    review_iterations: int      # số lần đã review (tối đa 3 lần rồi push dù sao)
    approved: bool              # reviewer có chấp nhận code không
    branch_name: str            # git_push điền vào: tên branch đã push lên GitHub
