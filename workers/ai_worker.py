"""
AI 处理 Worker

轮询数据库中待处理的文章，调用 LLM 进行摘要和分类处理。

启动方式:
    uv run python -m workers.ai_worker

后台运行:
    nohup uv run python -m workers.ai_worker > logs/ai_worker_stdout.log 2>&1 &

停止:
    kill $(cat logs/ai_worker.pid)
"""

import json
import logging
import signal
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from processor.database import DB_PATH, get_connection
from processor.llm_client import get_llm_client

# 日志配置
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "ai_worker.log"
_PID_FILE = _LOG_DIR / "ai_worker.pid"

# 配置
POLL_INTERVAL = 10  # 轮询间隔（秒）
BATCH_SIZE = 5      # 每次处理的最大数量
MAX_RETRIES = 3     # 最大重试次数


def _init_logging() -> logging.Logger:
    """初始化日志"""
    logger = logging.getLogger("ai_worker")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(
        _LOG_FILE.as_posix(), encoding="utf-8", mode="a"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = _init_logging()
_running = True


def _signal_handler(signum, frame):
    """信号处理器"""
    global _running
    logger.info("收到停止信号，正在退出...")
    _running = False
    # 清理 PID 文件
    if _PID_FILE.exists():
        _PID_FILE.unlink()


def _write_pid():
    """写入 PID 文件"""
    _PID_FILE.write_text(str(os.getpid()))


import os


def _get_pending_articles(conn: sqlite3.Connection, limit: int = BATCH_SIZE) -> list[sqlite3.Row]:
    """获取待处理的文章"""
    rows = conn.execute(
        """SELECT id, title, content_text, summary
           FROM articles
           WHERE ai_status = 'pending' AND content_text != ''
           ORDER BY created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()
    return rows


def _update_article_ai_status(
    conn: sqlite3.Connection,
    article_id: int,
    status: str,
    summary: str = "",
    key_points: list = None,
    category: str = "",
    error: str = ""
):
    """更新文章的 AI 处理状态"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """UPDATE articles SET
           ai_status = ?,
           ai_summary = ?,
           ai_key_points = ?,
           ai_category = ?,
           ai_processed_at = ?,
           ai_error = ?
           WHERE id = ?""",
        (status, summary, json.dumps(key_points or [], ensure_ascii=False),
         category, now, error, article_id)
    )
    conn.commit()


def _process_article(conn: sqlite3.Connection, article: sqlite3.Row) -> bool:
    """处理单篇文章"""
    article_id = article["id"]
    title = article["title"]
    content = article["content_text"] or article["summary"]

    if not content:
        logger.warning("文章 %d 无内容，跳过", article_id)
        _update_article_ai_status(conn, article_id, "failed", error="无内容")
        return False

    logger.info("处理文章: id=%d title=%s", article_id, title[:50])

    # 标记为处理中
    _update_article_ai_status(conn, article_id, "processing")

    try:
        client = get_llm_client()
        if not client.is_available:
            raise RuntimeError("LLM 客户端不可用")

        result = client.summarize(title=title, content=content)

        if not result.get("success"):
            raise RuntimeError(result.get("error", "未知错误"))

        # 更新处理结果
        _update_article_ai_status(
            conn, article_id, "completed",
            summary=result.get("summary", ""),
            key_points=result.get("key_points", []),
            category=result.get("category", "")
        )

        logger.info("文章处理完成: id=%d category=%s", article_id, result.get("category"))
        return True

    except Exception as e:
        logger.error("文章处理失败: id=%d error=%s", article_id, e)
        _update_article_ai_status(conn, article_id, "failed", error=str(e)[:500])
        return False


def run_ai_worker():
    """运行 AI 处理 Worker"""
    global _running

    logger.info("=" * 60)
    logger.info("AI 处理 Worker 启动")
    logger.info("数据库: %s", DB_PATH)
    logger.info("轮询间隔: %d 秒", POLL_INTERVAL)
    logger.info("批量大小: %d", BATCH_SIZE)
    logger.info("=" * 60)

    # 注册信号处理
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    _write_pid()

    conn = get_connection()

    try:
        while _running:
            try:
                # 获取待处理文章
                articles = _get_pending_articles(conn)

                if not articles:
                    logger.debug("无待处理文章")
                else:
                    logger.info("发现 %d 篇待处理文章", len(articles))

                    for article in articles:
                        if not _running:
                            break
                        _process_article(conn, article)

                # 等待下一次轮询
                for _ in range(POLL_INTERVAL):
                    if not _running:
                        break
                    time.sleep(1)

            except sqlite3.Error as e:
                logger.error("数据库错误: %s", e)
                time.sleep(5)

    except Exception as e:
        logger.exception("Worker 异常退出: %s", e)

    finally:
        conn.close()
        if _PID_FILE.exists():
            _PID_FILE.unlink()
        logger.info("AI 处理 Worker 已停止")


if __name__ == "__main__":
    run_ai_worker()