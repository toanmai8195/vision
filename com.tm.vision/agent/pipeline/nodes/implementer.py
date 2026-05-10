import os

import ollama

from agent.pipeline.state import AgentState

_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

_SYSTEM = """You are an expert software engineer.
Given an implementation plan (and optional reviewer feedback), write the complete code.
Return ONLY the code, no explanations outside of code comments."""


def implementer_node(state: AgentState) -> AgentState:
    prompt = f"Plan:\n{state['plan']}"
    if state.get("review_feedback"):
        prompt += f"\n\nReviewer feedback to address:\n{state['review_feedback']}"

    client = ollama.Client(host=_BASE_URL)
    response = client.chat(
        model=_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": prompt},
        ],
    )
    code = response.message.content
    return {"implementation": {"solution.py": code}}
