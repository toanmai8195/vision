"""Reviewer agent — chạy bên trong Docker container.

Input:  /workspace/plan.md              (kế hoạch từ planner)
        /workspace/implementation/      (các file code từ implementer)
Output: /workspace/review.json          ({"approved": bool, "feedback": str})

Ollama chạy trên host, truy cập qua host.docker.internal:11434.
"""
import json
import os
import sys
from pathlib import Path

import ollama

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

SYSTEM = """You are a senior code reviewer.
Given a plan and its implementation, review for correctness, code quality, and adherence to the plan.
Respond with ONLY a JSON object — no markdown, no prose:
{"approved": true/false, "feedback": "specific actionable feedback, or empty string if approved"}"""


def main():
    plan_path = "/workspace/plan.md"
    impl_dir = Path("/workspace/implementation")
    review_path = "/workspace/review.json"

    if not os.path.exists(plan_path):
        print(f"ERROR: {plan_path} not found", file=sys.stderr)
        sys.exit(1)

    plan = open(plan_path).read().strip()

    # Đọc tất cả file trong /workspace/implementation/
    code_sections = ""
    if impl_dir.exists():
        for f in sorted(impl_dir.glob("*")):
            if f.is_file():
                code_sections += f"\n\n### {f.name}\n```\n{f.read_text()}\n```"
    else:
        print(f"WARNING: {impl_dir} not found, reviewing plan only", file=sys.stderr)

    print(f"[reviewer] reviewing {len(list(impl_dir.glob('*')) if impl_dir.exists() else [])} files...")

    client = ollama.Client(host=OLLAMA_BASE_URL)
    response = client.chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": f"Plan:\n{plan}\n\nImplementation:{code_sections}"},
        ],
    )

    text = response.message.content.strip()

    # Parse JSON — tìm { } phòng model thêm text thừa
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        data = json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError):
        data = {"approved": False, "feedback": text}

    open(review_path, "w").write(json.dumps(data, ensure_ascii=False))
    print(f"[reviewer] approved={data.get('approved')} → written to {review_path}")


if __name__ == "__main__":
    main()
