import os
import anthropic
from pipeline.state import AgentState

_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

_SYSTEM = """You are a senior software architect.
Given a coding task, produce a concise implementation plan.
Format: numbered steps, each step describes ONE file to create or modify.
Be specific about function signatures, types, and logic — but do not write code."""


def planner_node(state: AgentState) -> AgentState:
    response = _client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=_SYSTEM,
        messages=[{"role": "user", "content": f"Task: {state['task']}"}],
    )
    return {"plan": response.content[0].text}
