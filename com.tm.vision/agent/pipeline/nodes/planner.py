import os

import ollama

from agent.pipeline.state import AgentState

_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

_SYSTEM = """You are a senior software architect.
Given a coding task, produce a concise implementation plan.
Format: numbered steps, each step describes ONE file to create or modify.
Be specific about function signatures, types, and logic — but do not write code."""


def planner_node(state: AgentState) -> AgentState:
    client = ollama.Client(host=_BASE_URL)
    response = client.chat(
        model=_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": f"Task: {state['task']}"},
        ],
    )
    return {"plan": response.message.content}
