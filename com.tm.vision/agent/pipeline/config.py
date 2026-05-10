from pathlib import Path

import yaml
from pydantic import BaseModel

# app/projects/ nằm 2 cấp trên so với file này (agent/pipeline/config.py)
_PROJECTS_DIR = Path(__file__).resolve().parents[2] / "app" / "projects"


class ProjectConfig(BaseModel):
    """Schema cho một file YAML trong app/projects/.

    Thêm project mới = tạo file YAML mới, không cần sửa code.
    """

    name: str
    repo: str                           # URL repo git đầy đủ
    branch_prefix: str = "agent"        # prefix cho tên branch: agent/{slug}-{ts}
    language: list[str] = []            # danh sách ngôn ngữ (dùng để hint cho model)
    build_tool: str = ""
    max_review_iterations: int = 3      # số lần reviewer reject tối đa trước khi push
    ollama_model: str = "qwen2.5-coder:7b"  # model Ollama dùng cho project này


def load_project(name: str) -> ProjectConfig:
    """Load và validate config của project từ app/projects/{name}.yaml.

    Raises FileNotFoundError nếu project chưa được khai báo.
    """
    path = _PROJECTS_DIR / f"{name}.yaml"
    if not path.exists():
        available = [p.stem for p in _PROJECTS_DIR.glob("*.yaml")]
        raise FileNotFoundError(
            f"Project '{name}' not found. Available: {available}"
        )
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return ProjectConfig(**data)
