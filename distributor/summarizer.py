"""
文章智能总结模块 — 使用 Claude API

从 .env 读取 CLAUDE_API_KEY
"""

import logging
import os
from pathlib import Path

from anthropic import Anthropic

logger = logging.getLogger("distributor.summarizer")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"

# 总结提示词模板
SUMMARY_PROMPT = """请对以下文章进行总结，要求：
1. 一句话概括文章核心内容（30字以内）
2. 列出3-5个关键要点
3. 判断文章类别（AI/汽车/其他）

文章标题：{title}
文章内容：
{content}

请按以下格式输出：
【一句话总结】...
【关键要点】
- ...
- ...
【类别】...
"""


def _read_env(key: str, default: str = "") -> str:
    """从 .env 文件读取配置"""
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


def get_claude_api_key() -> str:
    """获取 Claude API Key"""
    return _read_env("CLAUDE_API_KEY", os.environ.get("CLAUDE_API_KEY", ""))


def summarize_article(title: str, content: str, max_content_length: int = 8000) -> dict:
    """
    使用 Claude API 总结文章

    Args:
        title: 文章标题
        content: 文章内容（纯文本）
        max_content_length: 截断长度，防止过长内容

    Returns:
        {
            "summary": "一句话总结",
            "key_points": ["要点1", "要点2", ...],
            "category": "类别",
            "full_text": "完整总结文本",
            "success": True/False,
            "error": "错误信息（如果有）"
        }
    """
    api_key = get_claude_api_key()
    if not api_key:
        logger.warning("CLAUDE_API_KEY 未配置，跳过总结")
        return {
            "summary": "",
            "key_points": [],
            "category": "",
            "full_text": "",
            "success": False,
            "error": "CLAUDE_API_KEY 未配置"
        }

    # 截断过长内容
    truncated_content = content[:max_content_length]
    if len(content) > max_content_length:
        truncated_content += "\n...(内容已截断)"

    prompt = SUMMARY_PROMPT.format(title=title, content=truncated_content)

    try:
        client = Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )

        summary_text = response.content[0].text if response.content else ""

        # 解析总结结果
        result = _parse_summary(summary_text)
        result["full_text"] = summary_text
        result["success"] = True

        logger.info("文章总结完成: %s", title[:30])
        return result

    except Exception as exc:
        logger.error("Claude API 调用失败: %s", exc)
        return {
            "summary": "",
            "key_points": [],
            "category": "",
            "full_text": "",
            "success": False,
            "error": str(exc)
        }


def _parse_summary(text: str) -> dict:
    """解析 Claude 返回的总结文本"""
    summary = ""
    key_points = []
    category = ""

    lines = text.strip().split("\n")
    current_section = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("【一句话总结】"):
            current_section = "summary"
            summary = line.replace("【一句话总结】", "").strip()
        elif line.startswith("【关键要点】"):
            current_section = "points"
        elif line.startswith("【类别】"):
            current_section = "category"
            category = line.replace("【类别】", "").strip()
        elif current_section == "points" and line.startswith("-"):
            key_points.append(line.lstrip("-").strip())
        elif current_section == "summary" and not summary:
            summary = line

    return {
        "summary": summary,
        "key_points": key_points,
        "category": category
    }
