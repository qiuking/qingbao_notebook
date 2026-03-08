"""
API Key 认证

Key 存储在项目根目录 .env 文件的 PROCESSOR_API_KEY 字段中。
首次启动时若不存在则自动生成。

写操作（POST/PATCH/DELETE）需要认证，读操作（GET）保持开放。
"""

import secrets
from pathlib import Path

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"
_KEY_NAME = "PROCESSOR_API_KEY"

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _load_or_generate_key() -> str:
    """从 .env 读取 API Key，不存在则生成并写入。"""
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{_KEY_NAME}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")

    key = secrets.token_urlsafe(32)
    existing = ""
    if _ENV_FILE.exists():
        existing = _ENV_FILE.read_text(encoding="utf-8")
        if not existing.endswith("\n"):
            existing += "\n"
    _ENV_FILE.write_text(existing + f"{_KEY_NAME}={key}\n", encoding="utf-8")
    return key


_API_KEY = _load_or_generate_key()


def get_api_key() -> str:
    """返回当前有效的 API Key（供 source 脚本读取）。"""
    return _API_KEY


async def require_api_key(
    api_key: str | None = Security(_api_key_header),
) -> str:
    """FastAPI 依赖：验证写操作的 API Key。"""
    if api_key is None or api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="无效的 API Key")
    return api_key
