"""
懂车帝汽车资讯通用抓取引擎

抓取懂车帝汽车新闻列表及全文。
数据来源：移动端页面的 __NEXT_DATA__ JSON。

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

from push_to_processor import (
    RateLimiter,
    push_article,
    push_status_done,
    push_status_retryable,
)

TZ_CN = timezone(timedelta(hours=8))

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
]

_UA_MOBILE = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    source_name: str
    source_url: str
    max_content_per_run: int = 8
    delay_range: tuple[float, float] = (3.0, 6.0)
    push_max_per_sec: float = 2.0


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

    log_path = (paths["logs"] / f"{cfg.source_id}.log").as_posix()
    fh = TimedRotatingFileHandler(
        log_path,
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

def _build_headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }


def _build_mobile_headers() -> dict[str, str]:
    return {
        "User-Agent": _UA_MOBILE,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }


def _fetch(url: str, log: logging.Logger, retries: int = 2) -> str | None:
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_headers())
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8")
            elapsed = time.monotonic() - t0
            log.debug("HTTP GET %s -> %d bytes, %.2fs", url[:80], len(html), elapsed)
            return html
        except (URLError, HTTPError, TimeoutError) as exc:
            if attempt <= retries:
                wait = 4 * attempt + random.uniform(1, 3)
                log.warning("请求失败 attempt=%d/%d url=%s error=%s 等待%.1fs",
                            attempt, retries, url[:60], exc, wait)
                time.sleep(wait)
            else:
                log.error("请求彻底失败 url=%s error=%s", url[:60], exc)
                raise
    return None


def _fetch_mobile(url: str, log: logging.Logger, retries: int = 2) -> str | None:
    """使用移动端 UA 获取页面"""
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_mobile_headers())
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8")
            elapsed = time.monotonic() - t0
            log.debug("HTTP GET (mobile) %s -> %d bytes, %.2fs", url[:80], len(html), elapsed)
            return html
        except (URLError, HTTPError, TimeoutError) as exc:
            if attempt <= retries:
                wait = 4 * attempt + random.uniform(1, 3)
                log.warning("请求失败 attempt=%d/%d url=%s error=%s 等待%.1fs",
                            attempt, retries, url[:60], exc, wait)
                time.sleep(wait)
            else:
                log.error("请求彻底失败 url=%s error=%s", url[:60], exc)
                raise
    return None


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def _extract_next_data(html: str) -> dict | None:
    """从页面中提取 __NEXT_DATA__ JSON"""
    match = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _ts_to_str(ts: int) -> str:
    """时间戳转字符串"""
    try:
        dt = datetime.fromtimestamp(ts, tz=TZ_CN)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, OSError):
        return ""


def _parse_list_page(html: str) -> list[dict]:
    """解析列表页，返回文章索引"""
    data = _extract_next_data(html)
    if not data:
        return []

    props = data.get("props", {}).get("pageProps", {})
    news_list = props.get("staticData", {}).get("news", [])

    articles = []
    for item in news_list:
        article_id = str(item.get("unique_id_str") or item.get("unique_id", ""))
        title = item.get("title", "").strip()
        if not article_id or not title:
            continue

        ts = item.get("publish_time", 0)
        user_info = item.get("user_info", {})

        articles.append({
            "id": article_id,
            "title": title,
            "summary": "",
            "url": f"https://www.dongchedi.com/article/{article_id}",
            "source": user_info.get("name", "懂车帝"),
            "publish_time": _ts_to_str(ts),
            "timestamp": ts * 1000 if ts else 0,
        })

    return articles


def _html_to_text(html_content: str) -> str:
    """HTML 转纯文本"""
    text = re.sub(r'<br\s*/?>', '\n', html_content)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</h[1-6]>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    # 移除 span 标签但保留内容
    text = re.sub(r'</?span[^>]*>', '', text)
    # 移除其他标签
    text = re.sub(r'<[^>]+>', '', text)
    for entity, char in [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"),
                          ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"),
                          ("\u00a0", " ")]:
        text = text.replace(entity, char)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _fetch_article_content(article: dict, cfg: SourceConfig,
                           log: logging.Logger) -> dict | None:
    """抓取文章详情页，返回含全文的字典"""
    # 使用移动端 URL 获取更简洁的页面
    mobile_url = f"https://m.dongchedi.com/article/{article['id']}"

    try:
        html = _fetch_mobile(mobile_url, log)
    except Exception as exc:
        log.error("全文获取网络异常 id=%s title=%s error=%s",
                  article["id"], article["title"], exc)
        return None

    if html is None:
        log.warning("全文获取失败(空响应) id=%s url=%s", article["id"], mobile_url)
        return None

    data = _extract_next_data(html)
    if not data:
        log.warning("全文页 NEXT_DATA 提取失败 id=%s", article["id"])
        return None

    detail = data.get("props", {}).get("pageProps", {}).get("articleDetail", {})
    if not detail:
        log.warning("全文页 articleDetail 为空 id=%s", article["id"])
        return None

    content_html = detail.get("content", "")
    if not content_html:
        log.warning("全文提取失败(正文为空) id=%s title=%s", article["id"], article["title"])
        return None

    title = detail.get("title", article["title"])
    media_user = detail.get("media_user", {})
    author = media_user.get("screen_name", article.get("source", "懂车帝"))
    publish_time = _ts_to_str(detail.get("publish_time", 0)) or article["publish_time"]

    return {
        "id": article["id"],
        "title": title,
        "author": author,
        "publish_time": publish_time,
        "url": article["url"],
        "summary": detail.get("abstract", ""),
        "content_html": content_html,
        "content_text": _html_to_text(content_html),
        "source": author,
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
        log.error("历史文件解析失败 path=%s error=%s", hf.as_posix(), exc)
        return {}


REPAIR_WINDOW_DAYS = 7
MAX_RETRY_PUSH_PER_RUN = 20


def _repair_history_missing_files(
    history: dict[str, dict], paths: dict[str, Path], log: logging.Logger
) -> int:
    """将 content_fetched=true 且全文文件不存在的记录置为未获取"""
    now = datetime.now(tz=TZ_CN)
    cutoff_ms = int((now - timedelta(days=REPAIR_WINDOW_DAYS)).timestamp() * 1000)
    repaired = 0
    for aid, h in list(history.items()):
        if h.get("timestamp", 0) < cutoff_ms:
            continue
        if not h.get("content_fetched") or not h.get("content_file"):
            continue
        fp = paths["articles"] / h["content_file"]
        if fp.exists():
            continue
        log.info("修复孤儿记录: 全文文件不存在 id=%s content_file=%s，置为未获取",
                 aid, h["content_file"])
        h["content_fetched"] = False
        h["content_file"] = None
        repaired += 1
    return repaired


def _save_history(history: dict[str, dict], cfg: SourceConfig,
                  paths: dict[str, Path], log: logging.Logger):
    sorted_articles = sorted(
        history.values(), key=lambda a: a.get("timestamp", 0), reverse=True
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
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.debug("历史记录已保存 count=%d path=%s", len(sorted_articles), paths["history"].as_posix())


def _save_article_file(article_data: dict, paths: dict[str, Path],
                       log: logging.Logger) -> Path:
    paths["articles"].mkdir(parents=True, exist_ok=True)
    filepath = paths["articles"] / f"{article_data['id']}.json"
    filepath.write_text(
        json.dumps(article_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.debug("全文已保存 id=%s path=%s", article_data["id"], filepath.as_posix())
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

    # ---- 步骤1: 获取列表页 ----
    log.info("[步骤1] 获取列表页")
    try:
        html = _fetch(cfg.source_url, log)
    except Exception as exc:
        log.critical("列表页请求异常 error=%s", exc, exc_info=True)
        sys.exit(1)
    if html is None:
        log.critical("列表页获取失败，本轮放弃")
        sys.exit(1)

    # ---- 步骤2: 解析文章索引 ----
    log.info("[步骤2] 解析文章索引")
    articles = _parse_list_page(html)
    if not articles:
        log.critical("列表页解析结果为空，页面结构可能已变更")
        sys.exit(1)

    articles_24h = [a for a in articles if _is_within_24h(a["timestamp"], now)]
    log.info("列表页解析完成 total=%d within_24h=%d", len(articles), len(articles_24h))

    # ---- 步骤3: 增量比对 ----
    log.info("[步骤3] 增量比对")
    history = _load_history(paths, log)

    repaired = _repair_history_missing_files(history, paths, log)
    if repaired:
        log.info("修复孤儿记录 %d 条，已写回 history", repaired)
        _save_history(history, cfg, paths, log)

    new_articles = [a for a in articles if a["id"] not in history]
    log.info("增量比对完成 history=%d new=%d", len(history), len(new_articles))

    for a in new_articles:
        log.info("新增条目 id=%s source=%s title=%s", a["id"], a["source"], a["title"][:40])

    # ---- 步骤4: 全文获取 ----
    pending_content = list(new_articles)
    for a in articles:
        if a["id"] not in history:
            continue
        h = history[a["id"]]
        if h.get("content_fetched"):
            continue
        if push_status_done(h.get("push_status", "")):
            continue
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
    push_limiter = RateLimiter(cfg.push_max_per_sec)

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
                article["_content"] = content
                success_count += 1
                log.info("全文获取成功 id=%s chars=%d file=%s",
                         article["id"], len(content["content_text"]), fp.name)

                status = push_article(
                    source_id=cfg.source_id,
                    source_name=cfg.source_name,
                    origin_id=article["id"],
                    title=content["title"],
                    summary=content.get("summary", ""),
                    content_text=content["content_text"],
                    content_html=content["content_html"],
                    author=content.get("author", ""),
                    source_url=content["url"],
                    publish_time=content.get("publish_time", ""),
                    fetch_time=content.get("fetch_time", ""),
                    log=log,
                    rate_limiter=push_limiter,
                )
                article["push_status"] = status
                if not push_status_done(status):
                    log.warning("推送未成功 id=%s status=%s 将在补推阶段重试", article["id"], status)
                else:
                    log.info("推送成功 id=%s status=%s", article["id"], status)
            else:
                article["content_fetched"] = False
                article["content_file"] = None
                fail_count += 1
                log.warning("全文获取失败 id=%s title=%s", article["id"], article["title"])
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
            "first_seen": old.get("first_seen", now.strftime("%Y-%m-%d %H:%M:%S")),
            "content_fetched": a.get("content_fetched", old.get("content_fetched", False)),
            "content_file": a.get("content_file", old.get("content_file")),
            "push_status": a.get("push_status", old.get("push_status", "")),
        }

    _save_history(history, cfg, paths, log)

    # ---- 步骤5.5: 补推未推送成功的条目 ----
    unpushed = [
        h for h in history.values()
        if h.get("content_fetched") and push_status_retryable(h.get("push_status", ""))
    ]
    unpushed.sort(key=lambda h: h.get("timestamp", 0), reverse=True)
    retry_batch = unpushed[:MAX_RETRY_PUSH_PER_RUN]
    push_ok_count = 0
    push_fail_count = 0
    if retry_batch:
        log.info("[步骤5.5] 补推失败条目 batch=%d total_failed=%d",
                 len(retry_batch), len(unpushed))
        for item in retry_batch:
            content_file = paths["articles"] / f"{item['id']}.json"
            if not content_file.exists():
                log.warning("补推跳过: 全文文件不存在 id=%s", item["id"])
                push_fail_count += 1
                continue

            content = json.loads(content_file.read_text(encoding="utf-8"))
            status = push_article(
                source_id=cfg.source_id,
                source_name=cfg.source_name,
                origin_id=item["id"],
                title=content.get("title", item["title"]),
                summary=content.get("summary", item.get("summary", "")),
                content_text=content.get("content_text", ""),
                content_html=content.get("content_html", ""),
                author=content.get("author", ""),
                source_url=content.get("url", item.get("url", "")),
                publish_time=content.get("publish_time", item.get("publish_time", "")),
                fetch_time=content.get("fetch_time", ""),
                log=log,
                rate_limiter=push_limiter,
            )
            item["push_status"] = status
            if push_status_done(status):
                push_ok_count += 1
                log.info("补推成功 id=%s status=%s", item["id"], status)
            else:
                push_fail_count += 1
                log.warning("补推失败 id=%s status=%s", item["id"], status)

        _save_history(history, cfg, paths, log)
        log.info("补推完成 成功=%d 失败=%d", push_ok_count, push_fail_count)
    else:
        log.info("[步骤5.5] 无需补推")

    # ---- 步骤6: 输出本轮结果 ----
    seen = set()
    merged = []
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
                "is_new": a["id"] in new_ids,
                "content_fetched": h.get("content_fetched", False),
                "content_file": h.get("content_file"),
                "push_status": h.get("push_status", ""),
            })

    unfetched_in_history = sum(1 for h in history.values() if not h.get("content_fetched"))
    unpushed_in_history = sum(
        1 for h in history.values()
        if h.get("content_fetched") and push_status_retryable(h.get("push_status", ""))
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
            "push_retry_ok": push_ok_count,
            "push_retry_fail": push_fail_count,
            "push_pending": unpushed_in_history,
            "output_count": len(merged),
            "history_total": len(history),
        },
        "articles": merged,
    }

    paths["latest"].write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    log.info("========== 运行汇总 run=%s ==========", run_id)
    log.info("页面文章=%d 24h内=%d 新增=%d", len(articles), len(articles_24h), len(new_articles))
    log.info("全文: 成功=%d 失败=%d 待补获=%d", success_count, fail_count, unfetched_in_history)
    log.info("推送: 补推成功=%d 补推失败=%d 待推送=%d",
             push_ok_count, push_fail_count, unpushed_in_history)
    log.info("历史累计=%d 本轮输出=%d", len(history), len(merged))
    log.info("数据目录: %s", paths["base"].as_posix())
    log.info("========== 运行结束 run=%s ==========", run_id)

    return result