"""
分发 Worker: 将文章信息和摘要分发到飞书的多维表格中。注意不是分发给每个人。按机器人分发只能做日报和周报分发，那个在之后实现。

轮询数据库中已完成 AI 处理但未分发的文章，根据配置推送到飞书 Webhook。

启动方式:
    uv run python -m workers.distribute_worker

后台运行:
    nohup uv run python -m workers.distribute_worker > logs/distribute_worker_stdout.log 2>&1 &

停止:
    kill $(cat logs/distribute_worker.pid)

配置文件: task_scheduler/distribute_config.json
"""

import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

# 添加项目根目录到路径
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from processor.database import DB_PATH, get_connection


# 日志配置
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "distribute_worker.log"
_PID_FILE = _LOG_DIR / "distribute_worker.pid"
_CONFIG_FILE = _PROJECT_ROOT / "task_scheduler" / "distribute_config.json"

# .env 读取（与其他模块保持一致）
_ENV_FILE = _PROJECT_ROOT / ".env"


def _read_env(key: str, default: str = "") -> str:
    """从项目根目录 `.env` 读取配置（优先匹配 `KEY=` 行）。"""
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            # 兼容 `KEY="value"` / `KEY='value'`
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default

# 配置
POLL_INTERVAL = 10  # 轮询间隔（秒）
BATCH_SIZE = 10     # 每次处理的最大数量
RECENT_DAYS = 7     # 最多只分发最近 N 天的内容，避免干扰
# 默认非特别申明的信息保存表
FEISHU_WEBHOOK_URL_default = _read_env("FEISHU_WEBHOOK_URL_default")
# 政府和法规政策的权力部门公布的法规政策信息保存表
DUOWEIBIAOGE_gov_WEBHOOK_URL = _read_env("DUOWEIBIAOGE_gov_WEBHOOK_URL")
DUOWEIBIAOGE_gov_source_id = _read_env("DUOWEIBIAOGE_gov_source_id")




def _init_logging() -> logging.Logger:
    """初始化日志"""
    logger = logging.getLogger("distribute_worker")
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
    if _PID_FILE.exists():
        _PID_FILE.unlink()


def _write_pid():
    """写入 PID 文件"""
    _PID_FILE.write_text(str(os.getpid()))


def _load_config() -> dict:
    """加载分发配置"""
    if not _CONFIG_FILE.exists():
        logger.warning("配置文件不存在: %s", _CONFIG_FILE)
        return {"enabled": False, "rules": []}

    try:
        return json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error("配置文件解析失败: %s", e)
        return {"enabled": False, "rules": []}


def _get_pending_articles(conn: sqlite3.Connection, limit: int = BATCH_SIZE) -> list[sqlite3.Row]:
    """获取待分发的文章（AI 处理已完成，仅最近一周抓取的内容）"""
    tz_cn = timezone(timedelta(hours=8))
    cutoff = (
        datetime.now(tz=tz_cn) - timedelta(days=RECENT_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        """SELECT id, title, ai_summary, ai_key_points, ai_category,
                  source_id, source_name, publish_time, fetch_time, source_url
           FROM articles
           WHERE ai_status = 'completed' AND distribute_status = 'pending'
             AND fetch_time != '' AND fetch_time >= ?
           ORDER BY ai_processed_at ASC
           LIMIT ?""",
        (cutoff, limit)
    ).fetchall()
    return rows


def _update_distribute_status(
    conn: sqlite3.Connection,
    article_id: int,
    status: str,
    webhook: str = "",
    error: str = ""
):
    """更新分发状态"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """UPDATE articles SET
           distribute_status = ?,
           distribute_at = ?,
           distribute_webhook = ?,
           distribute_error = ?
           WHERE id = ?""",
        (status, now, webhook, error, article_id)
    )
    conn.commit()


# --------------------
def _distribute_article_duoweibiaoge(conn: sqlite3.Connection, article: sqlite3.Row, config: dict) -> bool:
    """分发单篇文章 到多维表格中：
    id, title, ai_summary, ai_key_points, ai_category, source_id, source_name, publish_time, fetch_time, source_url"""
    
    """"id": 1800,
      "source_id": "kr36_ai",
      "source_name": "36氪AI资讯",
      "origin_id": "3737905849958402",
      "title": 
      """
    
    article_id = article["id"]
    title = article["title"]
    source_id = article["source_id"]
    source_name = article["source_name"]
    source_url = article["source_url"]
    category = article["ai_category"] or "其他"

    logger.info("分发文章到多维表格: id=%d category=%s title=%s", article_id, category, title[:50])

    # 标记为处理中，开始处理分发
    _update_distribute_status(conn, article_id, "processing")

    # 查找匹配的 webhook：根据预先设定的规则，进行精确匹配：此处使用
    if source_id in DUOWEIBIAOGE_gov_source_id or '.gov.' in source_url:
        webhook_url = DUOWEIBIAOGE_gov_WEBHOOK_URL
    else:
        webhook_url = FEISHU_WEBHOOK_URL_default

    if not webhook_url:
        logger.warning("未找到分类 %s 的 webhook，跳过", category)
        _update_distribute_status(conn, article_id, "skipped", error="未匹配 webhook")
        return False

    # 解析关键要点
    try:
        key_points = json.loads(article["ai_key_points"]) if article["ai_key_points"] else []
    except json.JSONDecodeError:
        key_points = []

    # 发送到飞书多维表格
    summary = article["ai_summary"] + '\n ►' +  ' ►'.join(key_points)
    feishu_webhook_data = {
        "title": title,
        "summary": summary,
        "category": category,
        "source_url": source_url,
        "source_name": source_name,
        "publish_time": article["publish_time"],
        "fetch_time": article["fetch_time"]
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(feishu_webhook_data), headers=headers)
    if response.status_code == 200:
        logger.info("-成功- 分发到多维表格成功: id=%d webhook=%s", article_id, webhook_url[:50])
        _update_distribute_status(conn, article_id, "completed", webhook=webhook_url)
        return True
    else:
        logger.error("-失败- 分发到多维表格失败: id=%d title=%s error=%s", article_id, title, response.text)
        _update_distribute_status(conn, article_id, "failed", webhook=webhook_url, error=response.text)
        return False


# ----------------------------------------------------
# main distribute function
# ----------------------------------------------------


def _distribute_article(conn: sqlite3.Connection, article: sqlite3.Row, config: dict) -> bool:
    """统一入口：保留现有实现命名，供 run_distribute_worker() 调用。"""
    return _distribute_article_duoweibiaoge(conn, article, config)


def run_distribute_worker():
    """运行分发 Worker"""
    global _running

    logger.info("=" * 60)
    logger.info("分发 Worker 启动")
    logger.info("数据库: %s", DB_PATH)
    logger.info("配置文件: %s", _CONFIG_FILE)
    logger.info("轮询间隔: %d 秒", POLL_INTERVAL)
    logger.info("=" * 60)

    # 注册信号处理
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    _write_pid()

    conn = get_connection()

    try:
        while _running:
            try:
                # 加载配置: 每次重新加载，可保证信息最新
                config = _load_config()

                if not config.get("enabled", True):
                    logger.debug("分发功能已禁用")
                    time.sleep(POLL_INTERVAL)
                    continue

                # 获取待分发文章
                articles = _get_pending_articles(conn)

                if not articles:
                    logger.debug("无待分发文章")
                else:
                    logger.info("发现 %d 篇待分发文章", len(articles))

                    for article in articles:
                        if not _running:
                            break
                        status = _distribute_article(conn, article, config)
                        if not status:
                            exit(1) # 测试时候，用于结束测试
                            
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
        logger.info("分发 Worker 已停止")


if __name__ == "__main__":
    run_distribute_worker()