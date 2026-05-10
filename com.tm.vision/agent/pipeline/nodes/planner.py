import shutil
import subprocess

from agent.pipeline.state import AgentState

_CLAUDE = shutil.which("claude") or "/Users/toanmai/.local/bin/claude"

_SYSTEM = """You are a senior software architect.
Given a coding task, produce a concise implementation plan.
Format: numbered steps, each step describes ONE file to create or modify.
Be specific about function signatures, types, and logic — but do not write code."""


def planner_node(state: AgentState) -> AgentState:
    result = subprocess.run(
        [
            _CLAUDE,
            "-p", f"Task: {state['task']}",
            "--system-prompt", _SYSTEM,
            "--output-format", "text",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude planner failed: {result.stderr}")
    return {"plan": result.stdout.strip()}
