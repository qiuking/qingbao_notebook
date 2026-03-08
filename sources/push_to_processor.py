"""
将抓取结果推送到 processor 服务

被 kr36_base.py / autohome_base.py 等引擎调用。
从项目根目录 .env 读取 API Key 和服务地址。
推送失败不影响主抓取流程（仅记录日志警告）。

推送结果:
  "ok"       — 新增入库成功
  "exists"   — 服务端已有该记录（视为成功）
  "failed"   — 推送失败（网络异常/服务端错误等）
  "skipped"  — 未配置 API Key，跳过推送
"""

import json
import logging
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"

_PROCESSOR_URL = "http://127.0.0.1:8000"

PUSH_OK = "ok"
PUSH_EXISTS = "exists"
PUSH_FAILED = "failed"
PUSH_SKIPPED = "skipped"


def push_status_done(status: str) -> bool:
    """推送状态为 ok 或 exists 时视为已成功，无需再次推送。"""
    return status in (PUSH_OK, PUSH_EXISTS)


def _read_env(key: str, default: str = "") -> str:
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


def _get_api_key() -> str:
    return _read_env("PROCESSOR_API_KEY")


def _get_processor_url() -> str:
    return _read_env("PROCESSOR_URL", _PROCESSOR_URL)


# ---------------------------------------------------------------------------
# 限速器
# ---------------------------------------------------------------------------

class RateLimiter:
    """简单令牌桶限速器，保证调用频率不超过 max_per_sec。"""

    def __init__(self, max_per_sec: float = 3.0):
        self._interval = 1.0 / max_per_sec if max_per_sec > 0 else 0
        self._last_call = 0.0

    def wait(self):
        if self._interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# 推送
# ---------------------------------------------------------------------------

def push_article(
    *,
    source_id: str,
    source_name: str,
    origin_id: str,
    title: str,
    summary: str = "",
    content_text: str = "",
    content_html: str = "",
    author: str = "",
    source_url: str = "",
    publish_time: str = "",
    fetch_time: str = "",
    log: logging.Logger,
    rate_limiter: RateLimiter | None = None,
) -> str:
    """推送单条文章到 processor 服务，返回推送状态。"""
    api_key = _get_api_key()
    if not api_key:
        log.warning("推送跳过: .env 中未找到 PROCESSOR_API_KEY")
        return PUSH_SKIPPED

    if rate_limiter:
        rate_limiter.wait()

    base_url = _get_processor_url()
    payload = {
        "source_id": source_id,
        "source_name": source_name,
        "origin_id": origin_id,
        "title": title,
        "summary": summary,
        "content_text": content_text,
        "content_html": content_html,
        "author": author,
        "source_url": source_url,
        "publish_time": publish_time,
        "fetch_time": fetch_time,
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        f"{base_url}/articles",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        },
    )

    try:
        with urlopen(req, timeout=10) as resp:
            resp.read()
        log.debug("推送成功 origin_id=%s title=%s", origin_id, title[:30])
        return PUSH_OK
    except HTTPError as exc:
        if exc.code == 409:
            log.debug("推送跳过(已存在) origin_id=%s", origin_id)
            return PUSH_EXISTS
        body = exc.read().decode("utf-8", errors="replace")
        log.warning("推送失败 origin_id=%s http=%d body=%s",
                     origin_id, exc.code, body[:200])
        return PUSH_FAILED
    except (URLError, TimeoutError, OSError) as exc:
        log.warning("推送失败(连接异常) origin_id=%s error=%s", origin_id, exc)
        return PUSH_FAILED
