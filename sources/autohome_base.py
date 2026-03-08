"""
汽车之家资讯通用抓取引擎

所有汽车之家频道共用的抓取、解析、存储、日志逻辑。
各频道脚本（autohome_all.py 等）只需提供配置参数即可。

页面特征:
  - 列表页: GBK 编码, SSR HTML 中直接包含文章列表
  - 详情页: UTF-8 编码, Next.js __NEXT_DATA__ JSON 中包含 articleContent

每个 source 的数据完全隔离在各自目录中:
  data/{source_id}/
    history.json       — 累积历史记录
    latest.json        — 本轮运行结果
    articles/          — 全文存档
    logs/
      {source_id}.log  — 运行日志（按天轮转，保留30天）
"""

import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TZ_CN = timezone(timedelta(hours=8))

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
]


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    source_name: str
    source_url: str
    max_content_per_run: int = 8
    delay_range: tuple[float, float] = (5.0, 10.0)


# ---------------------------------------------------------------------------
# 路径管理
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _get_paths(cfg: SourceConfig) -> dict[str, Path]:
    base = _PROJECT_ROOT / "data" / cfg.source_id
    return {
        "base": base,
        "history": base / "history.json",
        "latest": base / "latest.json",
        "articles": base / "articles",
        "logs": base / "logs",
    }


# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

_initialized_loggers: set[str] = set()


def _get_logger(cfg: SourceConfig, paths: dict[str, Path]) -> logging.Logger:
    if cfg.source_id in _initialized_loggers:
        return logging.getLogger(cfg.source_id)

    paths["logs"].mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(cfg.source_id)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = TimedRotatingFileHandler(
        paths["logs"] / f"{cfg.source_id}.log",
        when="midnight", interval=1, backupCount=30, encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    fh.suffix = "%Y-%m-%d"

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    _initialized_loggers.add(cfg.source_id)
    return logger


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _build_headers(*, referer: str | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _fetch(url: str, log: logging.Logger, *,
           referer: str | None = None, retries: int = 2,
           encoding: str = "utf-8") -> str | None:
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_headers(referer=referer))
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                raw = resp.read()
            elapsed = time.monotonic() - t0

            html = raw.decode(encoding, errors="replace")
            log.debug("HTTP GET %s -> %d bytes, %.2fs (enc=%s)",
                      url, len(raw), elapsed, encoding)
            return html
        except (URLError, HTTPError, TimeoutError) as exc:
            if attempt <= retries:
                wait = 4 * attempt + random.uniform(1, 3)
                log.warning("请求失败 attempt=%d/%d url=%s error=%s 等待%.1fs",
                            attempt, retries, url, exc, wait)
                time.sleep(wait)
            else:
                log.error("请求彻底失败 url=%s error=%s", url, exc)
                raise
    return None


# ---------------------------------------------------------------------------
# 列表页解析
# ---------------------------------------------------------------------------

_RE_ARTICLE = re.compile(
    r'<li\s+data-artidanchor="(\d+)">\s*'
    r'<a\s+href="(//www\.autohome\.com\.cn/news/\d+/\d+\.html[^"]*)">\s*'
    r'<div class="article-pic">\s*'
    r'<img\s+src="([^"]*)"/?>\s*'
    r'</div>\s*'
    r'<h3>(.*?)</h3>\s*'
    r'<div class="article-bar">\s*'
    r'<span class="fn-left">(.*?)</span>\s*'
    r'<span class="fn-right">\s*'
    r'<em>.*?</i>(\d+)</em>\s*'
    r'<em[^>]*>.*?</i>(\d+)</em>\s*'
    r'</span>\s*'
    r'</div>\s*'
    r'<p>(.*?)</p>',
    re.DOTALL,
)

_TIME_UNIT_MAP = {
    "分钟前": "minutes",
    "小时前": "hours",
    "天前": "days",
}


def _relative_time_to_dt(time_str: str, now: datetime) -> datetime | None:
    """将 "3小时前" / "2天前" 这类相对时间转为 datetime。"""
    for suffix, unit in _TIME_UNIT_MAP.items():
        if time_str.endswith(suffix):
            try:
                n = int(time_str.replace(suffix, "").strip())
            except ValueError:
                return None
            return now - timedelta(**{unit: n})
    return None


def _parse_list_page(html: str, now: datetime) -> list[dict]:
    articles: list[dict] = []
    for m in _RE_ARTICLE.finditer(html):
        aid, url, img, title, time_str, views, comments, summary = m.groups()
        url_clean = "https:" + url.split("#")[0]

        dt = _relative_time_to_dt(time_str.strip(), now)
        if dt:
            publish_time = dt.strftime("%Y-%m-%d %H:%M")
            timestamp = int(dt.timestamp() * 1000)
        else:
            publish_time = time_str.strip()
            timestamp = 0

        source = ""
        src_m = re.match(r'\[([^\]]+)\]', summary.strip())
        if src_m:
            source = src_m.group(1)

        articles.append({
            "id": aid,
            "title": title.strip(),
            "summary": summary.strip(),
            "url": url_clean,
            "source": source or "汽车之家",
            "publish_time": publish_time,
            "timestamp": timestamp,
            "views": int(views),
            "comments": int(comments),
            "img": img,
        })

    return articles


# ---------------------------------------------------------------------------
# 详情页解析
# ---------------------------------------------------------------------------

def _html_to_text(html_content: str) -> str:
    text = re.sub(r'<br\s*/?>', '\n', html_content)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</h[1-6]>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    for entity, char in [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"),
                          ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'")]:
        text = text.replace(entity, char)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class CaptchaTriggered(Exception):
    pass


def _fetch_article_content(article: dict, cfg: SourceConfig,
                           log: logging.Logger) -> dict | None:
    try:
        html = _fetch(article["url"], log,
                      referer=cfg.source_url, encoding="utf-8")
    except Exception as exc:
        log.error("全文获取网络异常 id=%s title=%s error=%s",
                  article["id"], article["title"], exc)
        return None

    if html is None:
        log.warning("全文获取失败(无响应) id=%s url=%s",
                    article["id"], article["url"])
        return None

    # 汽车之家详情页数据在 __NEXT_DATA__ JSON 中
    marker = '"articleContent":'
    idx = html.find(marker)
    if idx < 0:
        log.warning("全文提取失败(未找到articleContent) id=%s", article["id"])
        return None

    obj_start = html.find("{", idx + len(marker))
    if obj_start < 0:
        log.warning("全文提取失败(JSON起始未找到) id=%s", article["id"])
        return None

    depth = 0
    data = None
    for i, ch in enumerate(html[obj_start:obj_start + 500_000]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    data = json.loads(html[obj_start:obj_start + i + 1])
                except json.JSONDecodeError as exc:
                    log.warning("全文JSON解析失败 id=%s error=%s",
                                article["id"], exc)
                    return None
                break

    if data is None:
        log.warning("全文提取失败(JSON未闭合) id=%s", article["id"])
        return None

    content_html = data.get("content", "")
    if not content_html:
        log.warning("全文提取失败(正文为空) id=%s title=%s",
                    article["id"], article["title"])
        return None

    return {
        "id": article["id"],
        "title": data.get("title", article["title"]),
        "author": data.get("authorName", article.get("source", "汽车之家")),
        "publish_time": data.get("publishDate", article["publish_time"]),
        "url": article["url"],
        "summary": data.get("summary", article["summary"]),
        "content_html": content_html,
        "content_text": _html_to_text(content_html),
        "views": article.get("views", 0),
        "comments": article.get("comments", 0),
        "fetch_time": datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# 历史记录
# ---------------------------------------------------------------------------

def _load_history(paths: dict[str, Path], log: logging.Logger) -> dict[str, dict]:
    hf = paths["history"]
    if not hf.exists():
        log.debug("历史文件不存在，首次运行")
        return {}
    try:
        data = json.loads(hf.read_text(encoding="utf-8"))
        history = {a["id"]: a for a in data.get("articles", [])}
        log.debug("加载历史记录 %d 条", len(history))
        return history
    except (json.JSONDecodeError, KeyError) as exc:
        log.error("历史文件解析失败 path=%s error=%s", hf, exc)
        return {}


def _save_history(history: dict[str, dict], cfg: SourceConfig,
                  paths: dict[str, Path], log: logging.Logger):
    sorted_articles = sorted(
        history.values(), key=lambda a: a.get("timestamp", 0), reverse=True,
    )
    payload = {
        "meta": {
            "source_name": cfg.source_name,
            "last_update": datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
            "total_articles": len(sorted_articles),
        },
        "articles": sorted_articles,
    }
    paths["history"].write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    log.debug("历史记录已保存 count=%d path=%s", len(sorted_articles), paths["history"])


def _save_article_file(article_data: dict, paths: dict[str, Path],
                       log: logging.Logger) -> Path:
    paths["articles"].mkdir(parents=True, exist_ok=True)
    filepath = paths["articles"] / f"{article_data['id']}.json"
    filepath.write_text(
        json.dumps(article_data, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    log.debug("全文已保存 id=%s path=%s", article_data["id"], filepath)
    return filepath


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def _is_within_24h(timestamp_ms: int, now: datetime) -> bool:
    if timestamp_ms <= 0:
        return False
    return (now - datetime.fromtimestamp(timestamp_ms / 1000, tz=TZ_CN)) <= timedelta(hours=24)


def run(cfg: SourceConfig) -> dict:
    """通用入口：基于配置执行一轮完整的抓取流程。"""
    paths = _get_paths(cfg)
    paths["base"].mkdir(parents=True, exist_ok=True)

    log = _get_logger(cfg, paths)
    now = datetime.now(tz=TZ_CN)
    run_id = now.strftime("%Y%m%d_%H%M%S")

    log.info("========== 运行开始 run=%s ==========", run_id)
    log.info("情报源=%s 目标=%s", cfg.source_name, cfg.source_url)

    # ---- 步骤1: 获取列表页 (GBK 编码) ----
    log.info("[步骤1] 获取列表页")
    try:
        html = _fetch(cfg.source_url, log, encoding="gbk")
    except Exception as exc:
        log.critical("列表页请求异常 error=%s", exc, exc_info=True)
        sys.exit(1)
    if html is None:
        log.critical("列表页获取失败，本轮放弃")
        sys.exit(1)
    log.info("列表页获取成功 size=%d chars", len(html))

    # ---- 步骤2: 解析文章索引 ----
    log.info("[步骤2] 解析文章索引")
    articles = _parse_list_page(html, now)
    if not articles:
        log.critical("列表页解析结果为空，页面结构可能已变更")
        sys.exit(1)

    articles_24h = [a for a in articles if _is_within_24h(a["timestamp"], now)]
    log.info("列表页解析完成 total=%d within_24h=%d", len(articles), len(articles_24h))

    # ---- 步骤3: 增量比对 ----
    log.info("[步骤3] 增量比对")
    history = _load_history(paths, log)

    new_articles = [a for a in articles if a["id"] not in history]
    log.info("增量比对完成 history=%d new=%d", len(history), len(new_articles))

    for a in new_articles:
        log.info("新增条目 id=%s source=%s title=%s", a["id"], a["source"], a["title"])

    # ---- 步骤4: 全文获取 ----
    pending_content: list[dict] = list(new_articles)
    for a in articles:
        if a["id"] in history and not history[a["id"]].get("content_fetched"):
            if a["id"] not in {p["id"] for p in pending_content}:
                pending_content.append(a)

    pending_content.sort(key=lambda a: (
        0 if _is_within_24h(a["timestamp"], now) else 1,
        -a["timestamp"],
    ))

    batch = pending_content[:cfg.max_content_per_run]
    skipped = len(pending_content) - len(batch)

    success_count = 0
    fail_count = 0

    if batch:
        log.info("[步骤4] 全文获取 batch=%d pending_total=%d deferred=%d",
                 len(batch), len(pending_content), skipped)

        for i, article in enumerate(batch, 1):
            is_retry = article["id"] in history
            label = "补获" if is_retry else "新增"
            log.info("全文获取 [%d/%d] type=%s id=%s title=%s",
                     i, len(batch), label, article["id"], article["title"][:40])

            if i > 1:
                delay = random.uniform(*cfg.delay_range)
                log.debug("请求间隔等待 %.1fs", delay)
                time.sleep(delay)

            content = _fetch_article_content(article, cfg, log)

            if content:
                fp = _save_article_file(content, paths, log)
                article["content_fetched"] = True
                article["content_file"] = fp.name
                success_count += 1
                log.info("全文获取成功 id=%s chars=%d file=%s",
                         article["id"], len(content["content_text"]), fp.name)
            else:
                article["content_fetched"] = False
                article["content_file"] = None
                fail_count += 1
                log.warning("全文获取失败 id=%s title=%s",
                            article["id"], article["title"])
    else:
        log.info("[步骤4] 无待获取全文的条目，跳过")

    # ---- 步骤5: 更新历史记录 ----
    log.info("[步骤5] 更新历史记录")
    new_ids = {a["id"] for a in new_articles}
    for a in articles:
        old = history.get(a["id"], {})
        history[a["id"]] = {
            "id": a["id"],
            "title": a["title"],
            "summary": a["summary"],
            "url": a["url"],
            "source": a["source"],
            "publish_time": a["publish_time"],
            "timestamp": a["timestamp"],
            "views": a.get("views", old.get("views", 0)),
            "comments": a.get("comments", old.get("comments", 0)),
            "first_seen": old.get("first_seen", now.strftime("%Y-%m-%d %H:%M:%S")),
            "content_fetched": a.get("content_fetched", old.get("content_fetched", False)),
            "content_file": a.get("content_file", old.get("content_file")),
        }

    _save_history(history, cfg, paths, log)

    # ---- 步骤6: 输出本轮结果 ----
    seen: set[str] = set()
    merged: list[dict] = []
    for a in articles_24h + articles[:10]:
        if a["id"] not in seen:
            seen.add(a["id"])
            h = history.get(a["id"], {})
            merged.append({
                "id": a["id"],
                "title": a["title"],
                "summary": a["summary"],
                "url": a["url"],
                "source": a["source"],
                "publish_time": a["publish_time"],
                "views": a.get("views", 0),
                "comments": a.get("comments", 0),
                "is_new": a["id"] in new_ids,
                "content_fetched": h.get("content_fetched", False),
                "content_file": h.get("content_file"),
            })

    unfetched_in_history = sum(
        1 for h in history.values() if not h.get("content_fetched")
    )

    result = {
        "meta": {
            "source_name": cfg.source_name,
            "source_url": cfg.source_url,
            "fetch_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "total_on_page": len(articles),
            "within_24h": len(articles_24h),
            "new_articles": len(new_articles),
            "content_fetch_success": success_count,
            "content_fetch_fail": fail_count,
            "content_pending": unfetched_in_history,
            "output_count": len(merged),
            "history_total": len(history),
        },
        "articles": merged,
    }

    paths["latest"].write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    # ---- 运行汇总 ----
    log.info("========== 运行汇总 run=%s ==========", run_id)
    log.info("页面文章=%d 24h内=%d 新增=%d",
             len(articles), len(articles_24h), len(new_articles))
    log.info("全文: 成功=%d 失败=%d 待补获=%d",
             success_count, fail_count, unfetched_in_history)
    log.info("历史累计=%d 本轮输出=%d", len(history), len(merged))
    log.info("数据目录: %s", paths["base"])
    log.info("========== 运行结束 run=%s ==========", run_id)

    return result
