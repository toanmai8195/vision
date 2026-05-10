import os
import re
import tempfile
from datetime import datetime, timezone

import git

from agent.pipeline.state import AgentState

_REPO_URL = os.getenv("MARK1_REPO_URL", "https://github.com/toanmai8195/mark1.git")
_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
_GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "Vision Agent")
_GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "agent@vision.local")


def _authenticated_url(url: str) -> str:
    """Nhúng GITHUB_TOKEN vào URL để push không cần nhập password.

    https://github.com/... → https://<token>@github.com/...
    """
    if _GITHUB_TOKEN and url.startswith("https://"):
        return url.replace("https://", f"https://{_GITHUB_TOKEN}@", 1)
    return url


def _branch_name(task: str) -> str:
    """Tạo tên branch dạng agent/{slug}-{timestamp}.

    Ví dụ: "Write a Python function" → "agent/write-a-python-function-20260510032441"
    - slug: chuyển task thành chữ thường, thay ký tự đặc biệt bằng "-", cắt 40 ký tự
    - timestamp: đảm bảo mỗi lần chạy có branch riêng, không bị trùng
    """
    slug = re.sub(r"[^a-z0-9]+", "-", task.lower())[:40].strip("-")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"agent/{slug}-{ts}"


def git_push_node(state: AgentState) -> AgentState:
    """Node 4: tạo branch mới trên mark1, commit code, push lên GitHub.

    Dùng thư mục tạm (TemporaryDirectory) để clone — tự xóa sau khi push xong,
    không để lại file rác trên máy.
    """
    branch = _branch_name(state["task"])
    url = _authenticated_url(_REPO_URL)

    # TemporaryDirectory tự dọn khi ra khỏi block `with`
    with tempfile.TemporaryDirectory() as tmpdir:
        # Clone repo mark1 về thư mục tạm
        repo = git.Repo.clone_from(url, tmpdir)

        # Tạo branch mới từ HEAD của main
        repo.git.checkout("-b", branch)

        # Ghi từng file trong implementation xuống disk
        for filepath, code in state["implementation"].items():
            full = os.path.join(tmpdir, filepath)
            os.makedirs(os.path.dirname(full), exist_ok=True)  # tạo thư mục cha nếu chưa có
            with open(full, "w", encoding="utf-8") as f:
                f.write(code)

        # Stage tất cả file mới
        repo.git.add(".")

        # Commit với author là "Vision Agent" thay vì git config local
        actor = git.Actor(_GIT_AUTHOR_NAME, _GIT_AUTHOR_EMAIL)
        repo.index.commit(
            f"agent: {state['task'][:72]}",  # commit message lấy từ task, cắt 72 ký tự
            author=actor,
            committer=actor,
        )

        # Push branch lên GitHub
        repo.remote("origin").push(branch)

    # Trả branch_name để API response biết link branch ở đâu
    return {"branch_name": branch}
