"""Reviewer node — chạy reviewer agent trong Docker container (ollama-agent:latest).

Flow:
  1. Tạo thư mục workspace tạm
  2. Ghi plan.md + implementation/<files> vào workspace
  3. docker run ollama-agent python reviewer.py
  4. Đọc workspace/review.json → parse approved/feedback
  5. Dọn workspace tạm

SCION (phase 5): thay docker run bằng scion start --no-hub ollama-reviewer --workspace <path>
"""
import json
import os
import subprocess
import tempfile
from pathlib import Path

from agent.pipeline.state import AgentState

_OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_IMAGE = os.getenv("REVIEWER_IMAGE", "ollama-agent:latest")


def reviewer_node(state: AgentState) -> AgentState:
    """Node 3: chạy reviewer agent trong container, đọc kết quả từ review.json."""

    with tempfile.TemporaryDirectory(prefix="vision-reviewer-") as tmpdir:
        ws = Path(tmpdir)

        # 1. Ghi plan và implementation vào workspace
        (ws / "plan.md").write_text(state["plan"], encoding="utf-8")

        impl_dir = ws / "implementation"
        impl_dir.mkdir()
        for filepath, code in state["implementation"].items():
            dest = impl_dir / Path(filepath).name  # flatten path để đơn giản
            dest.write_text(code, encoding="utf-8")

        # 2. Chạy container
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{tmpdir}:/workspace",
                "-e", f"OLLAMA_BASE_URL={_OLLAMA_BASE_URL}",
                "-e", f"OLLAMA_MODEL={_OLLAMA_MODEL}",
                _IMAGE,
                "python", "reviewer.py",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"reviewer container failed (rc={result.returncode})\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )

        # 3. Đọc và parse review.json
        review_path = ws / "review.json"
        if not review_path.exists():
            raise RuntimeError("reviewer container did not produce review.json")

        data = json.loads(review_path.read_text(encoding="utf-8"))

    return {
        "approved": bool(data.get("approved", False)),
        "review_feedback": data.get("feedback", ""),
        "review_iterations": state.get("review_iterations", 0) + 1,
    }
