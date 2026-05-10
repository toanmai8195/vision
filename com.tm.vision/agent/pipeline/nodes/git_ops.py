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
    if _GITHUB_TOKEN and url.startswith("https://"):
        return url.replace("https://", f"https://{_GITHUB_TOKEN}@", 1)
    return url


def _branch_name(task: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", task.lower())[:40].strip("-")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"agent/{slug}-{ts}"


def git_push_node(state: AgentState) -> AgentState:
    branch = _branch_name(state["task"])
    url = _authenticated_url(_REPO_URL)

    with tempfile.TemporaryDirectory() as tmpdir:
        repo = git.Repo.clone_from(url, tmpdir)
        repo.git.checkout("-b", branch)

        for filepath, code in state["implementation"].items():
            full = os.path.join(tmpdir, filepath)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write(code)

        repo.git.add(".")
        actor = git.Actor(_GIT_AUTHOR_NAME, _GIT_AUTHOR_EMAIL)
        repo.index.commit(
            f"agent: {state['task'][:72]}",
            author=actor,
            committer=actor,
        )
        repo.remote("origin").push(branch)

    return {"branch_name": branch}
