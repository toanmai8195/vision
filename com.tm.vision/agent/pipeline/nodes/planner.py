import os

import ollama

from agent.pipeline.state import AgentState

# Đọc config từ .env, có giá trị mặc định nếu không set
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

# System prompt định nghĩa "vai trò" của model trong node này
_SYSTEM = """You are a senior software architect.
Given a coding task, produce a concise implementation plan.
Format: numbered steps, each step describes ONE file to create or modify.
Be specific about function signatures, types, and logic — but do not write code."""


def planner_node(state: AgentState) -> AgentState:
    """Node 1: nhận task từ user, trả về plan dạng numbered steps.

    Chỉ lập kế hoạch, không viết code — đó là việc của implementer.
    """
    client = ollama.Client(host=_BASE_URL)
    response = client.chat(
        model=_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": f"Task: {state['task']}"},
        ],
    )
    # Chỉ trả field "plan" — LangGraph merge vào state, các field khác giữ nguyên
    return {"plan": response.message.content}
