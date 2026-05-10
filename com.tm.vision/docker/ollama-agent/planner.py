"""Planner agent — chạy bên trong Docker container.

Input:  /workspace/task.txt    (ghi bởi planner_node trước khi chạy container)
Output: /workspace/plan.md     (đọc bởi planner_node sau khi container kết thúc)

Ollama chạy trên host, truy cập qua host.docker.internal:11434.
"""
import os
import sys

import ollama

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

SYSTEM = """You are a senior software architect.
Given a coding task, produce a concise implementation plan.
Format: numbered steps, each step describes ONE file to create or modify.
Be specific about function signatures, types, and logic — but do not write code."""


def main():
    task_path = "/workspace/task.txt"
    plan_path = "/workspace/plan.md"

    if not os.path.exists(task_path):
        print(f"ERROR: {task_path} not found", file=sys.stderr)
        sys.exit(1)

    task = open(task_path).read().strip()
    print(f"[planner] task: {task[:80]}...")

    client = ollama.Client(host=OLLAMA_BASE_URL)
    response = client.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": f"Task: {task}"},
        ],
    )

    plan = response.message.content
    open(plan_path, "w").write(plan)
    print(f"[planner] plan written to {plan_path} ({len(plan)} chars)")


if __name__ == "__main__":
    main()
