"""
临时脚本：检查 sources 抓取到的数据（history.json）是否存在于数据库中
如果没有则重新 push 插入

用法:
    uv run python check_and_repush.py [source_id]

示例:
    uv run python check_and_repush.py aibase_news
    uv run python check_and_repush.py kr36_ai
    uv run python check_and_repush.py all  # 检查所有数据源
"""

import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

from sources.push_to_processor import (
    RateLimiter,
    push_article,
    push_status_done,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("check_and_repush")

# 项目根目录
_PROJECT_ROOT = Path(__file__).resolve().parent
_DB_PATH = _PROJECT_ROOT / "data_server" / "qingbao_zx.db"
_DATA_DIR = _PROJECT_ROOT / "data"

# 数据源配置映射
_SOURCE_CONFIGS = {
    "aibase_news": {"source_name": "AIbase资讯", "source_url": "https://news.aibase.cn/zh/news"},
    "kr36_ai": {"source_name": "36氪AI", "source_url": "https://36kr.com/information/AI/"},
    "kr36_travel": {"source_name": "36氪汽车", "source_url": "https://36kr.com/information/travel/"},
    "autohome_all": {"source_name": "汽车之家", "source_url": "https://www.autohome.com.cn/all/"},
}


def get_db_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def check_article_exists(conn: sqlite3.Connection, source_id: str, origin_id: str) -> bool:
    """检查文章是否已存在于数据库中"""
    cursor = conn.execute(
        "SELECT 1 FROM articles WHERE source_id = ? AND origin_id = ? LIMIT 1",
        (source_id, origin_id)
    )
    return cursor.fetchone() is not None


def load_history(source_id: str) -> dict[str, Any]:
    """加载 history.json 文件"""
    history_path = _DATA_DIR / source_id / "history.json"
    if not history_path.exists():
        logger.warning("历史文件不存在: %s", history_path)
        return {}

    try:
        data = json.loads(history_path.read_text(encoding="utf-8"))
        return data
    except (json.JSONDecodeError, KeyError) as exc:
        logger.error("历史文件解析失败: %s error=%s", history_path, exc)
        return {}


def load_article_content(source_id: str, article_id: str) -> dict | None:
    """加载文章全文内容"""
    content_path = _DATA_DIR / source_id / "articles" / f"{article_id}.json"
    if not content_path.exists():
        return None
    try:
        return json.loads(content_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def repush_article(
    source_id: str,
    source_name: str,
    article: dict,
    content: dict,
    rate_limiter: RateLimiter | None = None,
) -> str:
    """重新推送文章到数据库"""
    status = push_article(
        source_id=source_id,
        source_name=source_name,
        origin_id=article["id"],
        title=content.get("title", article["title"]),
        summary=content.get("summary", article.get("summary", "")),
        content_text=content.get("content_text", ""),
        content_html=content.get("content_html", ""),
        author=content.get("author", article.get("author", "")),
        source_url=content.get("url", article.get("url", "")),
        publish_time=content.get("publish_time", article.get("publish_time", "")),
        fetch_time=content.get("fetch_time", ""),
        log=logger,
        rate_limiter=rate_limiter,
    )
    return status


def update_history_push_status(source_id: str, article_id: str, status: str) -> bool:
    """更新 history.json 中的 push_status"""
    history_path = _DATA_DIR / source_id / "history.json"
    if not history_path.exists():
        return False

    try:
        data = json.loads(history_path.read_text(encoding="utf-8"))
        for article in data.get("articles", []):
            if article["id"] == article_id:
                article["push_status"] = status
                break

        history_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return True
    except Exception as exc:
        logger.error("更新 history.json 失败: %s", exc)
        return False


def check_and_repush_source(source_id: str, dry_run: bool = False) -> dict:
    """
    检查指定数据源的历史记录，对缺失的文章重新推送

    Args:
        source_id: 数据源ID
        dry_run: 如果为True，只检查不推送

    Returns:
        统计结果字典
    """
    if source_id not in _SOURCE_CONFIGS:
        logger.error("未知的数据源: %s", source_id)
        return {"error": f"Unknown source: {source_id}"}

    config = _SOURCE_CONFIGS[source_id]
    source_name = config["source_name"]

    logger.info("=" * 60)
    logger.info("开始检查数据源: %s (%s)", source_id, source_name)
    logger.info("=" * 60)

    # 加载历史记录
    history_data = load_history(source_id)
    if not history_data:
        logger.warning("历史记录为空，跳过")
        return {"skipped": True}

    articles = history_data.get("articles", [])
    logger.info("历史记录共 %d 条文章", len(articles))

    # 获取数据库连接
    conn = get_db_connection()

    # 统计
    stats = {
        "total": len(articles),
        "exists_in_db": 0,
        "missing_in_db": 0,
        "content_not_fetched": 0,
        "content_file_missing": 0,
        "repush_success": 0,
        "repush_failed": 0,
        "repush_skipped": 0,
    }

    # 限速器
    rate_limiter = RateLimiter(max_per_sec=3.0)

    # 检查每篇文章
    for article in articles:
        article_id = article["id"]

        # 检查是否已在数据库中
        if check_article_exists(conn, source_id, article_id):
            stats["exists_in_db"] += 1
            continue

        stats["missing_in_db"] += 1
        logger.info("数据库中不存在: id=%s title=%s", article_id, article["title"][:50])

        # 检查是否有 content_fetched 标记
        if not article.get("content_fetched"):
            logger.warning("  -> 跳过: 文章内容未抓取 id=%s", article_id)
            stats["content_not_fetched"] += 1
            continue

        # 加载文章内容
        content = load_article_content(source_id, article_id)
        if not content:
            logger.warning("  -> 跳过: 内容文件不存在 id=%s", article_id)
            stats["content_file_missing"] += 1
            continue

        if dry_run:
            logger.info("  -> [DRY RUN] 将重新推送 id=%s", article_id)
            continue

        # 重新推送
        status = repush_article(source_id, source_name, article, content, rate_limiter)

        # 更新历史记录中的 push_status
        update_history_push_status(source_id, article_id, status)

        if push_status_done(status):
            logger.info("  -> 推送成功 status=%s id=%s", status, article_id)
            stats["repush_success"] += 1
        elif status == "skipped":
            logger.warning("  -> 推送跳过 (未配置 API Key) id=%s", article_id)
            stats["repush_skipped"] += 1
        else:
            logger.error("  -> 推送失败 status=%s id=%s", status, article_id)
            stats["repush_failed"] += 1

    conn.close()

    logger.info("=" * 60)
    logger.info("检查完成: %s", source_id)
    logger.info("  总计: %d", stats["total"])
    logger.info("  数据库中已存在: %d", stats["exists_in_db"])
    logger.info("  数据库中缺失: %d", stats["missing_in_db"])
    logger.info("  内容未抓取: %d", stats["content_not_fetched"])
    logger.info("  内容文件缺失: %d", stats["content_file_missing"])
    if not dry_run:
        logger.info("  重新推送成功: %d", stats["repush_success"])
        logger.info("  重新推送失败: %d", stats["repush_failed"])
        logger.info("  重新推送跳过: %d", stats["repush_skipped"])
    logger.info("=" * 60)

    return stats


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="检查并重新推送缺失的文章到数据库")
    parser.add_argument(
        "source",
        nargs="?",
        default="all",
        help="数据源ID (aibase_news, kr36_ai, kr36_travel, autohome_all) 或 'all' 检查所有 (默认: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅检查，不实际推送",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="列出所有可用的数据源",
    )

    args = parser.parse_args()

    if args.list_sources:
        print("可用的数据源:")
        for sid, cfg in _SOURCE_CONFIGS.items():
            print(f"  {sid}: {cfg['source_name']}")
        return

    # 检查数据库是否存在
    if not _DB_PATH.exists():
        logger.error("数据库文件不存在: %s", _DB_PATH)
        sys.exit(1)

    if args.source == "all":
        # 检查所有数据源
        all_stats = {}
        for source_id in _SOURCE_CONFIGS:
            stats = check_and_repush_source(source_id, dry_run=args.dry_run)
            all_stats[source_id] = stats

        # 汇总
        logger.info("\n" + "=" * 60)
        logger.info("全部数据源检查完成")
        logger.info("=" * 60)
        total_exists = sum(s.get("exists_in_db", 0) for s in all_stats.values())
        total_missing = sum(s.get("missing_in_db", 0) for s in all_stats.values())
        total_success = sum(s.get("repush_success", 0) for s in all_stats.values())
        total_failed = sum(s.get("repush_failed", 0) for s in all_stats.values())
        logger.info("总计已存在: %d", total_exists)
        logger.info("总计缺失: %d", total_missing)
        if not args.dry_run:
            logger.info("总计重新推送成功: %d", total_success)
            logger.info("总计重新推送失败: %d", total_failed)
    else:
        check_and_repush_source(args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
