import json
import subprocess

from agent.pipeline.state import AgentState

_SYSTEM = """You are a senior code reviewer.
Given a plan and its implementation, review for correctness, code quality, and adherence to the plan.
Respond with ONLY a JSON object — no markdown, no prose:
{"approved": true/false, "feedback": "specific actionable feedback, or empty string if approved"}"""


def reviewer_node(state: AgentState) -> AgentState:
    code_sections = "\n\n".join(
        f"### {path}\n```\n{code}\n```"
        for path, code in state["implementation"].items()
    )

    prompt = f"Plan:\n{state['plan']}\n\nImplementation:\n{code_sections}"

    result = subprocess.run(
        [
            "claude",
            "-p", prompt,
            "--system-prompt", _SYSTEM,
            "--output-format", "text",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude reviewer failed: {result.stderr}")

    text = result.stdout.strip()
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
