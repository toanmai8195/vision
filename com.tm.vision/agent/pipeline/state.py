from typing import TypedDict


class AgentState(TypedDict):
    task: str
    plan: str
    code: str
