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

    UNIQUE(source_id, origin_id)
);

CREATE INDEX IF NOT EXISTS idx_articles_source_id  ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_origin_id  ON articles(origin_id);
CREATE INDEX IF NOT EXISTS idx_articles_title      ON articles(title);
CREATE INDEX IF NOT EXISTS idx_articles_publish     ON articles(publish_time);

CREATE TRIGGER IF NOT EXISTS trg_articles_updated
AFTER UPDATE ON articles
BEGIN
    UPDATE articles SET updated_at = datetime('now', 'localtime') WHERE id = NEW.id;
END;
"""


def init_db() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.executescript(_SCHEMA_SQL)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
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
