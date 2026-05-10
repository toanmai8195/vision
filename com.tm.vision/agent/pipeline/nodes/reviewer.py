import json
import os

import ollama

from agent.pipeline.state import AgentState

_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

_SYSTEM = """You are a senior code reviewer.
Given a plan and its implementation, review for correctness, code quality, and adherence to the plan.
Respond with ONLY a JSON object — no markdown, no prose:
{"approved": true/false, "feedback": "specific actionable feedback, or empty string if approved"}"""


def reviewer_node(state: AgentState) -> AgentState:
    code_sections = "\n\n".join(
        f"### {path}\n```\n{code}\n```"
        for path, code in state["implementation"].items()
    )

    client = ollama.Client(host=_BASE_URL)
    response = client.chat(
        model=_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {
                "role": "user",
                "content": f"Plan:\n{state['plan']}\n\nImplementation:\n{code_sections}",
            },
        ],
    )

    text = response.message.content.strip()
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        data = json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError):
        data = {"approved": False, "feedback": text}

    return {
        "approved": bool(data.get("approved", False)),
        "review_feedback": data.get("feedback", ""),
        "review_iterations": state.get("review_iterations", 0) + 1,
    }
