"""
情报处理 Web 服务

提供 RESTful API 接收各 source 抓取结果并进行结构化存储。
支持：入库、批量入库、查重、查询、分页、搜索、修改、删除。
同时提供 Web 前端界面，支持数据可视化与趋势分析。

启动方式:
    uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000 --reload
"""

import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .auth import require_api_key
from .database import get_db, init_db
from .schemas import (
    ArticleBatchCreate,
    ArticleCreate,
    ArticleListOut,
    ArticleOut,
    ArticleUpdate,
    BatchCreateResult,
    DuplicateCheckResult,
)

# 前端静态文件目录
_STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="qbNoteBook 情报处理服务",
    description="接收情报源抓取结果，提供结构化存储与查询 API",
    version="0.1.0",
    lifespan=lifespan,
)


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


# ---------------------------------------------------------------------------
# 入库
# ---------------------------------------------------------------------------

def _insert_article(conn: sqlite3.Connection, art: ArticleCreate) -> tuple[int | None, bool]:
    """插入单条，返回 (article_id, is_duplicate)。"""
    try:
        cur = conn.execute(
            """INSERT INTO articles
               (source_id, source_name, origin_id, title, summary,
                content_text, content_html, author, source_url,
                publish_time, fetch_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (art.source_id, art.source_name, art.origin_id, art.title,
             art.summary, art.content_text, art.content_html,
             art.author, art.source_url, art.publish_time, art.fetch_time),
        )
        conn.commit()
        return cur.lastrowid, False
    except sqlite3.IntegrityError:
        row = conn.execute(
            "SELECT id FROM articles WHERE source_id=? AND origin_id=?",
            (art.source_id, art.origin_id),
        ).fetchone()
        return (row["id"] if row else None), True


@app.post("/articles", response_model=ArticleOut, status_code=201)
def create_article(art: ArticleCreate, _key: str = Depends(require_api_key), conn: sqlite3.Connection = Depends(get_db)):
    article_id, is_dup = _insert_article(conn, art)
    if is_dup:
        raise HTTPException(
            status_code=409,
            detail=f"重复条目: source_id={art.source_id}, origin_id={art.origin_id}, existing_id={article_id}",
        )
    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    return _row_to_dict(row)


@app.post("/articles/batch", response_model=BatchCreateResult, status_code=201)
def create_articles_batch(
    payload: ArticleBatchCreate,
    _key: str = Depends(require_api_key),
    conn: sqlite3.Connection = Depends(get_db),
):
    inserted = 0
    duplicates = 0
    details: list[DuplicateCheckResult] = []

    for art in payload.articles:
        article_id, is_dup = _insert_article(conn, art)
        if is_dup:
            duplicates += 1
        else:
            inserted += 1
        details.append(DuplicateCheckResult(
            origin_id=art.origin_id,
            exists=is_dup,
            article_id=article_id,
        ))

    return BatchCreateResult(inserted=inserted, duplicates=duplicates, details=details)


# ---------------------------------------------------------------------------
# 查重
# ---------------------------------------------------------------------------

@app.post("/articles/check-duplicates", response_model=list[DuplicateCheckResult])
def check_duplicates(
    source_id: str,
    origin_ids: list[str],
    _key: str = Depends(require_api_key),
    conn: sqlite3.Connection = Depends(get_db),
):
    results = []
    for oid in origin_ids:
        row = conn.execute(
            "SELECT id FROM articles WHERE source_id=? AND origin_id=?",
            (source_id, oid),
        ).fetchone()
        results.append(DuplicateCheckResult(
            origin_id=oid,
            exists=row is not None,
            article_id=row["id"] if row else None,
        ))
    return results


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

@app.get("/articles", response_model=ArticleListOut)
def list_articles(
    source_id: str | None = Query(None, description="按情报源过滤"),
    keyword: str | None = Query(None, description="按标题/摘要关键词搜索"),
    author: str | None = Query(None, description="按作者过滤"),
    start_time: str | None = Query(None, description="发布时间起始 (含)"),
    end_time: str | None = Query(None, description="发布时间截止 (含)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    conn: sqlite3.Connection = Depends(get_db),
):
    conditions: list[str] = []
    params: list = []

    if source_id:
        conditions.append("source_id = ?")
        params.append(source_id)
    if keyword:
        conditions.append("(title LIKE ? OR summary LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    if author:
        conditions.append("author LIKE ?")
        params.append(f"%{author}%")
    if start_time:
        conditions.append("publish_time >= ?")
        params.append(start_time)
    if end_time:
        conditions.append("publish_time <= ?")
        params.append(end_time)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    total = conn.execute(f"SELECT COUNT(*) FROM articles{where}", params).fetchone()[0]

    offset = (page - 1) * page_size
    rows = conn.execute(
        f"SELECT * FROM articles{where} ORDER BY publish_time DESC LIMIT ? OFFSET ?",
        params + [page_size, offset],
    ).fetchall()

    return ArticleListOut(
        total=total,
        page=page,
        page_size=page_size,
        articles=[_row_to_dict(r) for r in rows],
    )


@app.get("/articles/{article_id}", response_model=ArticleOut)
def get_article(article_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="条目不存在")
    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# 修改
# ---------------------------------------------------------------------------

@app.patch("/articles/{article_id}", response_model=ArticleOut)
def update_article(
    article_id: int,
    payload: ArticleUpdate,
    _key: str = Depends(require_api_key),
    conn: sqlite3.Connection = Depends(get_db),
):
    existing = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="条目不存在")

    updates = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not updates:
        return _row_to_dict(existing)

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [article_id]
    conn.execute(f"UPDATE articles SET {set_clause} WHERE id=?", values)
    conn.commit()

    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# 删除
# ---------------------------------------------------------------------------

@app.delete("/articles/{article_id}", status_code=204)
def delete_article(article_id: int, _key: str = Depends(require_api_key), conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute("SELECT id FROM articles WHERE id=?", (article_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="条目不存在")
    conn.execute("DELETE FROM articles WHERE id=?", (article_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# 统计
# ---------------------------------------------------------------------------

@app.get("/stats")
def get_stats(conn: sqlite3.Connection = Depends(get_db)):
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    sources = conn.execute(
        "SELECT source_id, source_name, COUNT(*) as count FROM articles GROUP BY source_id"
    ).fetchall()
    return {
        "total_articles": total,
        "sources": [dict(r) for r in sources],
    }


# ---------------------------------------------------------------------------
# 趋势分析
# ---------------------------------------------------------------------------

@app.get("/api/trend/daily")
def get_daily_trend(
    days: int = Query(30, ge=7, le=90, description="查询天数"),
    source_id: str | None = Query(None, description="按情报源过滤"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """获取每日文章数量趋势"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    where = "WHERE publish_time >= ?"
    params = [start_date.strftime("%Y-%m-%d")]
    if source_id:
        where += " AND source_id = ?"
        params.append(source_id)

    sql = f"""
        SELECT DATE(publish_time) as date, COUNT(*) as count
        FROM articles {where}
        GROUP BY DATE(publish_time)
        ORDER BY date
    """
    rows = conn.execute(sql, params).fetchall()

    # 补齐缺失的日期
    date_counts = {dict(r)["date"]: dict(r)["count"] for r in rows}
    result = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        result.append({"date": date_str, "count": date_counts.get(date_str, 0)})
        current += timedelta(days=1)

    return result


@app.get("/api/trend/by-source")
def get_source_trend(
    days: int = Query(30, ge=7, le=90),
    conn: sqlite3.Connection = Depends(get_db),
):
    """获取各数据源的每日趋势"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    sql = """
        SELECT DATE(publish_time) as date, source_id, source_name, COUNT(*) as count
        FROM articles
        WHERE publish_time >= ?
        GROUP BY DATE(publish_time), source_id
        ORDER BY date, source_id
    """
    rows = conn.execute(sql, [start_date.strftime("%Y-%m-%d")]).fetchall()

    # 按日期分组
    trend: dict = {}
    for r in rows:
        d = dict(r)
        date = d["date"]
        if date not in trend:
            trend[date] = []
        trend[date].append({
            "source_id": d["source_id"],
            "source_name": d["source_name"],
            "count": d["count"],
        })

    return {"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d"), "trend": trend}


@app.get("/api/groups")
def get_groups(conn: sqlite3.Connection = Depends(get_db)):
    """获取所有分组（数据源分组信息）"""
    rows = conn.execute("""
        SELECT source_id, source_name, COUNT(*) as count,
               MIN(publish_time) as earliest,
               MAX(publish_time) as latest
        FROM articles
        GROUP BY source_id
        ORDER BY count DESC
    """).fetchall()

    groups = []
    for r in rows:
        d = dict(r)
        groups.append({
            "source_id": d["source_id"],
            "source_name": d["source_name"] or d["source_id"],
            "count": d["count"],
            "earliest": d["earliest"],
            "latest": d["latest"],
        })
    return groups


# ---------------------------------------------------------------------------
# 前端页面
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    """主页面"""
    static_index = _STATIC_DIR / "index.html"
    if static_index.exists():
        return static_index.read_text(encoding="utf-8")
    return """
    <!DOCTYPE html>
    <html>
    <head><title>qbNoteBook 数据中心</title></head>
    <body>
        <h1>qbNoteBook 数据中心</h1>
        <p>静态文件未找到，请确保 processor/static/index.html 存在</p>
    </body>
    </html>
    """


# 挂载静态文件目录
if _STATIC_DIR.exists():
    # 使用 as_posix() 确保路径为正向斜杠，避免 Win/Ubuntu 混用时的乱码
    app.mount("/static", StaticFiles(directory=_STATIC_DIR.as_posix()), name="static")
