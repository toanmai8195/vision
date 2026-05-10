import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import git

from agent.pipeline.config import load_project
from agent.pipeline.state import AgentState

_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
_GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "Vision Agent")
_GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "agent@vision.local")

# workspace/ nằm cùng cấp với agent/, app/ (tức là trong com.tm.vision/)
_WORKSPACE = Path(__file__).resolve().parents[3] / "workspace"


def _authenticated_url(url: str) -> str:
    """Nhúng GITHUB_TOKEN vào URL để push không cần nhập password."""
    if _GITHUB_TOKEN and url.startswith("https://"):
        return url.replace("https://", f"https://{_GITHUB_TOKEN}@", 1)
    return url


def _branch_name(prefix: str, task: str) -> str:
    """Tạo tên branch dạng {prefix}/{slug}-{timestamp}."""
    slug = re.sub(r"[^a-z0-9]+", "-", task.lower())[:40].strip("-")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{prefix}/{slug}-{ts}"


def _ensure_bare_clone(bare_dir: Path, url: str) -> git.Repo:
    """Đảm bảo bare clone tồn tại và up-to-date.

    - Lần đầu: clone --bare về workspace/{project_name}
    - Các lần sau: chỉ fetch để lấy commit mới nhất
    Bare clone không có working tree — nhẹ hơn clone thường, dùng để tạo worktree.
    """
    if not bare_dir.exists():
        bare_dir.parent.mkdir(parents=True, exist_ok=True)
        print(f"[git_ops] bare clone {url} → {bare_dir}")
        git.Repo.clone_from(url, str(bare_dir), bare=True)
    repo = git.Repo(str(bare_dir))
    print(f"[git_ops] fetching latest from origin...")
    repo.remote("origin").fetch()
    return repo


def git_push_node(state: AgentState) -> AgentState:
    """Node 4: tạo worktree mới, commit code, push lên GitHub, dọn worktree.

    Dùng git worktree thay vì clone tạm:
    - Bare clone tồn tại lâu dài trong workspace/ (tạo 1 lần per project)
    - Mỗi request tạo worktree riêng biệt → nhanh hơn, không tải lại toàn bộ repo
    - Worktree bị xóa ngay sau khi push xong (kể cả khi lỗi)
    """
    config = load_project(state["project"])
    auth_url = _authenticated_url(config.repo)
    branch = _branch_name(config.branch_prefix, state["task"])

    bare_dir = _WORKSPACE / config.name
    # Mỗi run có worktree riêng, tránh conflict khi chạy song song
    worktree_dir = bare_dir / "worktrees" / f"run-{uuid.uuid4().hex[:8]}"

    bare_repo = _ensure_bare_clone(bare_dir, auth_url)

    # Tạo worktree + checkout branch mới từ origin/main trong 1 lệnh
    print(f"[git_ops] creating worktree at {worktree_dir}")
    bare_repo.git.worktree("add", "-b", branch, str(worktree_dir), "origin/main")

    try:
        wt_repo = git.Repo(str(worktree_dir))

        # Ghi từng file trong implementation xuống disk
        for filepath, code in state["implementation"].items():
            full = worktree_dir / filepath
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(code, encoding="utf-8")

        # Stage và commit
        wt_repo.git.add(".")
        actor = git.Actor(_GIT_AUTHOR_NAME, _GIT_AUTHOR_EMAIL)
        wt_repo.index.commit(
            f"agent: {state['task'][:72]}",
            author=actor,
            committer=actor,
        )

        # Push trực tiếp bằng URL có token (không dùng remote để tránh lưu token)
        print(f"[git_ops] pushing branch {branch}...")
        wt_repo.git.push(auth_url, branch)

    finally:
        # Dọn worktree dù thành công hay lỗi — tránh để lại rác
        print(f"[git_ops] removing worktree {worktree_dir}")
        bare_repo.git.worktree("remove", str(worktree_dir), "--force")

    return {"branch_name": branch}
