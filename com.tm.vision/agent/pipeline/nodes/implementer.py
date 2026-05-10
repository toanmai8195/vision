import os
import re

import ollama

from agent.pipeline.state import AgentState

_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

_SYSTEM = """You are an expert software engineer.
Given an implementation plan (and optional reviewer feedback), write the complete code.

IMPORTANT: You MUST return each file using this exact format:

--- FILE: path/to/filename.ext ---
<full file content here>
--- END FILE ---

Rules:
- Use the exact filename from the plan (e.g. main.go, openapi.yaml, not solution.py)
- One block per file
- No explanations outside the FILE blocks"""


def _parse_files(text: str) -> dict[str, str]:
    """Parse response dạng --- FILE: name --- ... --- END FILE --- thành dict.

    Nếu model không theo format → fallback về solution.txt để không mất code.
    """
    pattern = r"--- FILE: (.+?) ---\n(.*?)--- END FILE ---"
    matches = re.findall(pattern, text, re.DOTALL)
    if matches:
        return {filename.strip(): code.strip() for filename, code in matches}
    # Fallback: model không theo format, lưu raw output
    return {"solution.txt": text.strip()}


def implementer_node(state: AgentState) -> AgentState:
    """Node 2: nhận plan (và feedback nếu là lần retry), trả về dict các file code."""
    prompt = f"Plan:\n{state['plan']}"

    # Lần retry: thêm feedback để model biết cần sửa gì
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

    # Parse response thành {filepath: code}
    files = _parse_files(response.message.content)
    return {"implementation": files}
