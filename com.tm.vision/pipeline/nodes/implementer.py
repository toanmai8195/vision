import os
import ollama
from pipeline.state import AgentState

_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

_SYSTEM = """You are an expert software engineer.
Given an implementation plan, write the complete code.
Return ONLY the code, no explanations outside of code comments."""


def implementer_node(state: AgentState) -> AgentState:
    client = ollama.Client(host=_BASE_URL)
    response = client.chat(
        model=_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": f"Plan:\n{state['plan']}"},
        ],
    )
    return {"code": response.message.content}
