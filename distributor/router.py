"""
分发路由 — 协调文章总结与多平台分发

支持:
  - 文章入库后自动触发总结和分发
  - 手动触发指定文章的分发
  - 配置控制是否启用自动分发
"""

import json
import logging
import os
from pathlib import Path

from .feishu import push_article_to_feishu
from .summarizer import summarize_article

logger = logging.getLogger("distributor.router")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


def _read_env(key: str, default: str = "") -> str:
    """从 .env 文件读取配置"""
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


def is_auto_distribute_enabled() -> bool:
    """检查是否启用自动分发"""
    value = _read_env("AUTO_DISTRIBUTE", os.environ.get("AUTO_DISTRIBUTE", "true"))
    return value.lower() in ("true", "1", "yes", "on")


def distribute_article(
    article_id: int,
    title: str,
    content_text: str,
    source_name: str,
    source_url: str,
    publish_time: str,
    summary: str = "",
    author: str = "",
    skip_if_no_content: bool = True
) -> dict:
    """
    完整的分发流程：总结 -> 推送到飞书

    Args:
        article_id: 文章 ID
        title: 标题
        content_text: 正文内容
        source_name: 来源名称
        source_url: 原文链接
        publish_time: 发布时间
        summary: 原始摘要（可选）
        author: 作者（可选）
        skip_if_no_content: 如果内容为空是否跳过

    Returns:
        {
            "article_id": int,
            "summarized": True/False,
            "summary_result": {...},
            "feishu_pushed": True/False,
            "feishu_error": "...",
            "success": True/False
        }
    """
    result = {
        "article_id": article_id,
        "summarized": False,
        "summary_result": {},
        "feishu_pushed": False,
        "feishu_error": "",
        "success": False
    }

    # 检查内容
    if skip_if_no_content and not content_text.strip():
        logger.warning("文章内容为空，跳过分发: article_id=%s", article_id)
        result["feishu_error"] = "文章内容为空"
        return result

    # 1. 总结文章
    try:
        summary_result = summarize_article(title, content_text)
        result["summarized"] = summary_result["success"]
        result["summary_result"] = summary_result

        if not summary_result["success"]:
            logger.warning("文章总结失败: %s", summary_result.get("error", "未知错误"))
            # 继续尝试推送，使用原始摘要
    except Exception as exc:
        logger.error("总结过程异常: %s", exc)
        summary_result = {"success": False, "error": str(exc)}

    # 2. 推送到飞书
    try:
        # 使用 AI 总结或原始摘要
        final_summary = summary_result.get("summary", "") or summary
        category = summary_result.get("category", "")
        key_points = summary_result.get("key_points", [])

        feishu_ok = push_article_to_feishu(
            title=title,
            summary=final_summary,
            category=category,
            source_name=source_name,
            source_url=source_url,
            publish_time=publish_time,
            key_points=key_points
        )

        result["feishu_pushed"] = feishu_ok
        if not feishu_ok:
            result["feishu_error"] = "飞书推送失败，请检查配置"

    except Exception as exc:
        logger.error("飞书推送异常: %s", exc)
        result["feishu_error"] = str(exc)

    # 判定整体成功
    result["success"] = result["summarized"] and result["feishu_pushed"]

    if result["success"]:
        logger.info("文章分发成功: article_id=%s, title=%s", article_id, title[:30])
    else:
        logger.warning(
            "文章分发部分失败: article_id=%s, summarized=%s, feishu=%s",
            article_id, result["summarized"], result["feishu_pushed"]
        )

    return result


def auto_distribute_if_enabled(article: dict) -> dict | None:
    """
    如果启用了自动分发，执行分发流程

    Args:
        article: 文章字典，包含 id, title, content_text, source_name, source_url, publish_time 等

    Returns:
        分发结果字典，如果未启用自动分发返回 None
    """
    if not is_auto_distribute_enabled():
        logger.debug("自动分发未启用，跳过")
        return None

    return distribute_article(
        article_id=article["id"],
        title=article.get("title", ""),
        content_text=article.get("content_text", ""),
        source_name=article.get("source_name", ""),
        source_url=article.get("source_url", ""),
        publish_time=article.get("publish_time", ""),
        summary=article.get("summary", ""),
        author=article.get("author", "")
    )


# 用于后台异步执行的分发函数（不依赖返回值的简化版）
def distribute_article_async(article: dict):
    """异步执行分发（用于后台任务）"""
    try:
        auto_distribute_if_enabled(article)
    except Exception as exc:
        logger.error("异步分发异常: %s", exc)
