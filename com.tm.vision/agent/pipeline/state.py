from typing import TypedDict


class AgentState(TypedDict):
    task: str
    plan: str
    implementation: dict[str, str]  # {filepath: code}
    review_feedback: str
    review_iterations: int
    approved: bool
    branch_name: str
