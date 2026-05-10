"""Planner node — chạy planner agent trong Docker container (ollama-agent:latest).

Flow:
  1. Tạo thư mục workspace tạm
  2. Ghi task vào workspace/task.txt
  3. docker run ollama-agent python planner.py  (với workspace mounted)
  4. Đọc workspace/plan.md về state["plan"]
  5. Dọn workspace tạm

Container gọi Ollama qua host.docker.internal:11434 (trên macOS/Windows Docker Desktop).
Trên Linux thay bằng 172.17.0.1:11434.

SCION (phase 5): thay docker run bằng scion start --no-hub ollama-planner --workspace <path>
"""
import os
import subprocess
import tempfile
from pathlib import Path

from agent.pipeline.state import AgentState

_OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_IMAGE = os.getenv("PLANNER_IMAGE", "ollama-agent:latest")


def planner_node(state: AgentState) -> AgentState:
    """Node 1: chạy planner agent trong container, đọc plan từ workspace."""

    with tempfile.TemporaryDirectory(prefix="vision-planner-") as tmpdir:
        ws = Path(tmpdir)

        # 1. Ghi task vào file để container đọc
        (ws / "task.txt").write_text(state["task"], encoding="utf-8")

        # 2. Chạy container — mount workspace, truyền Ollama config qua env
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                # mount workspace vào /workspace trong container
                "-v", f"{tmpdir}:/workspace",
                # Ollama config — host.docker.internal trỏ về máy host
                "-e", f"OLLAMA_BASE_URL={_OLLAMA_BASE_URL}",
                "-e", f"OLLAMA_MODEL={_OLLAMA_MODEL}",
                _IMAGE,
                "python", "planner.py",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"planner container failed (rc={result.returncode})\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )

        # 3. Đọc plan từ file output của container
        plan_path = ws / "plan.md"
        if not plan_path.exists():
            raise RuntimeError("planner container did not produce plan.md")

        plan = plan_path.read_text(encoding="utf-8").strip()

    return {"plan": plan}
