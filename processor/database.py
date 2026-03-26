"""
SQLite 数据库管理

使用 Python 标准库 sqlite3，零额外依赖。
数据库文件位于 data/processor/qingbao_zx.db
"""

import sqlite3
from collections.abc import Generator
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = _PROJECT_ROOT / "data_server"
DB_PATH = DB_DIR / "qingbao_zx.db"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT    NOT NULL,
    source_name     TEXT    NOT NULL DEFAULT '',
    origin_id       TEXT    NOT NULL DEFAULT '',
    title           TEXT    NOT NULL,
    summary         TEXT    NOT NULL DEFAULT '',
    content_text    TEXT    NOT NULL DEFAULT '',
    content_html    TEXT    NOT NULL DEFAULT '',
    author          TEXT    NOT NULL DEFAULT '',
    source_url      TEXT    NOT NULL DEFAULT '',
    publish_time    TEXT    NOT NULL DEFAULT '',
    fetch_time      TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime')),

    -- AI 处理相关字段
    ai_status       TEXT    NOT NULL DEFAULT 'pending',
    ai_summary      TEXT    NOT NULL DEFAULT '',
    ai_key_points   TEXT    NOT NULL DEFAULT '[]',
    ai_category     TEXT    NOT NULL DEFAULT '',
    ai_processed_at TEXT    NOT NULL DEFAULT '',
    ai_error        TEXT    NOT NULL DEFAULT '',

    -- 分发相关字段
    distribute_status   TEXT    NOT NULL DEFAULT 'pending',
    distribute_at       TEXT    NOT NULL DEFAULT '',
    distribute_webhook  TEXT    NOT NULL DEFAULT '',
    distribute_error    TEXT    NOT NULL DEFAULT '',

    UNIQUE(source_id, origin_id)
);

CREATE INDEX IF NOT EXISTS idx_articles_source_id  ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_origin_id  ON articles(origin_id);
CREATE INDEX IF NOT EXISTS idx_articles_title      ON articles(title);
CREATE INDEX IF NOT EXISTS idx_articles_publish    ON articles(publish_time);

CREATE TRIGGER IF NOT EXISTS trg_articles_updated
AFTER UPDATE ON articles
BEGIN
    UPDATE articles SET updated_at = datetime('now', 'localtime') WHERE id = NEW.id;
END;
"""


def init_db() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
        # 先尝试迁移旧表结构（在执行 schema 之前）
        _migrate_db(conn)
        # 再执行 schema（新表会包含所有字段，旧表会跳过创建）
        conn.executescript(_SCHEMA_SQL)
        # 最后创建可能缺失的索引
        _create_indexes(conn)
        conn.commit()


def _migrate_db(conn: sqlite3.Connection) -> None:
    """迁移旧数据库结构，添加新字段"""
    # 检查表是否存在
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='articles'"
    )
    if not cursor.fetchone():
        return  # 表不存在，无需迁移

    cursor = conn.execute("PRAGMA table_info(articles)")
    columns = {row[1] for row in cursor.fetchall()}

    migrations = [
        ("ai_status", "ALTER TABLE articles ADD COLUMN ai_status TEXT NOT NULL DEFAULT 'pending'"),
        ("ai_summary", "ALTER TABLE articles ADD COLUMN ai_summary TEXT NOT NULL DEFAULT ''"),
        ("ai_key_points", "ALTER TABLE articles ADD COLUMN ai_key_points TEXT NOT NULL DEFAULT '[]'"),
        ("ai_category", "ALTER TABLE articles ADD COLUMN ai_category TEXT NOT NULL DEFAULT ''"),
        ("ai_processed_at", "ALTER TABLE articles ADD COLUMN ai_processed_at TEXT NOT NULL DEFAULT ''"),
        ("ai_error", "ALTER TABLE articles ADD COLUMN ai_error TEXT NOT NULL DEFAULT ''"),
        ("distribute_status", "ALTER TABLE articles ADD COLUMN distribute_status TEXT NOT NULL DEFAULT 'pending'"),
        ("distribute_at", "ALTER TABLE articles ADD COLUMN distribute_at TEXT NOT NULL DEFAULT ''"),
        ("distribute_webhook", "ALTER TABLE articles ADD COLUMN distribute_webhook TEXT NOT NULL DEFAULT ''"),
        ("distribute_error", "ALTER TABLE articles ADD COLUMN distribute_error TEXT NOT NULL DEFAULT ''"),
    ]

    for col_name, sql in migrations:
        if col_name not in columns:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # 列已存在，忽略


def _create_indexes(conn: sqlite3.Connection) -> None:
    """创建 AI 和分发相关的索引"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_articles_ai_status ON articles(ai_status)",
        "CREATE INDEX IF NOT EXISTS idx_articles_dist_status ON articles(distribute_status)",
    ]
    for sql in indexes:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """FastAPI 依赖注入用的数据库连接生成器。"""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
