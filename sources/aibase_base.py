"""
AIBase 资讯通用抓取引擎

抓取 https://news.aibase.cn/zh/news 的 AI 资讯列表及全文。
数据嵌入在页面 __NUXT_DATA__（Nuxt devalue 格式）中，无需调用 API。

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
)

TZ_CN = timezone(timedelta(hours=8))

ARTICLE_URL_TPL = "https://news.aibase.cn/zh/news/{oid}"
LIST_API_TPL = "https://mcpapi.aibase.cn/api/aiInfo/aiNews?t={ts}&langType=zh_cn&pageNo={page}"

# 列表翻页：通过 API 获取，每页 8 条，支持 pageNo 参数
MAX_LIST_PAGES = 10

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
    source_id: str          # 唯一标识，如 "aibase_news"，决定数据目录名
    source_name: str        # 显示名，如 "AIbase资讯"
    source_url: str         # 列表页URL
    max_content_per_run: int = 8
    delay_range: tuple[float, float] = (5.0, 10.0)
    push_max_per_sec: float = 3.0


# ---------------------------------------------------------------------------
# 路径管理
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _get_paths(cfg: SourceConfig) -> dict[str, Path]:
    base = _PROJECT_ROOT / "data" / cfg.source_id
    logs = base / "logs"
    articles = base / "articles"
    return {
        "base": base,
        "history": base / "history.json",
        "latest": base / "latest.json",
        "articles": articles,
        "logs": logs,
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
           referer: str | None = None, retries: int = 2) -> str | None:
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_headers(referer=referer))
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8")
            elapsed = time.monotonic() - t0
            log.debug("HTTP GET %s -> %d bytes, %.2fs", url, len(html), elapsed)
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
# Nuxt devalue 解析
# ---------------------------------------------------------------------------

def _extract_nuxt_data(html: str) -> list | None:
    """从页面中提取 __NUXT_DATA__ JSON 数组。

    Nuxt SSR 将数据以 devalue 格式内嵌在:
      <script ... data-nuxt-data="nuxt-app" ...>[...]</script>
    """
    marker = 'data-nuxt-data="nuxt-app"'
    idx = html.find(marker)
    if idx < 0:
        return None
    arr_start = html.find("[", idx)
    script_end = html.find("</script>", arr_start)
    if arr_start < 0 or script_end < 0:
        return None
    try:
        return json.loads(html[arr_start:script_end])
    except json.JSONDecodeError:
        return None


def _devalue_resolve(arr: list, idx: int, visited: frozenset = frozenset()) -> object:
    """递归解析 Nuxt devalue 格式的索引引用数组。

    devalue 将对象图序列化为一个扁平数组，其中整数值表示对该数组其他位置的引用。
    特殊包装类型：
      ["ShallowReactive", N]  — 浅响应式代理，解包为 N
      ["Reactive", N]         — 深响应式代理，解包为 N
      ["Set"]                 — Set 对象，返回空列表
    """
    if not isinstance(idx, int) or idx < 0 or idx >= len(arr):
        return idx
    if idx in visited:
        return None
    visited = visited | {idx}
    val = arr[idx]

    if isinstance(val, list):
        if (len(val) == 2
                and isinstance(val[0], str)
                and val[0] in ("ShallowReactive", "Reactive")):
            return _devalue_resolve(arr, val[1], visited)
        if val and isinstance(val[0], str) and val[0] == "Set":
            return []
        return [
            _devalue_resolve(arr, v, visited) if isinstance(v, int) else v
            for v in val
        ]
    if isinstance(val, dict):
        return {
            k: _devalue_resolve(arr, v, visited) if isinstance(v, int) else v
            for k, v in val.items()
        }
    return val


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def _parse_create_time(create_time_str: str) -> tuple[int, str]:
    """解析 createTime 字符串，返回 (timestamp_ms, formatted_str)。"""
    try:
        dt = datetime.strptime(str(create_time_str), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_CN)
        return int(dt.timestamp() * 1000), dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, AttributeError):
        return 0, str(create_time_str or "")


def _is_within_24h(ts_ms: int, now: datetime) -> bool:
    if ts_ms <= 0:
        return False
    return (now - datetime.fromtimestamp(ts_ms / 1000, tz=TZ_CN)) <= timedelta(hours=24)


def _parse_list_api(json_str: str) -> list[dict]:
    """从 API 返回的 JSON 中解析文章索引列表。"""
    try:
        data = json.loads(json_str)
        if data.get("code") != 200:
            return []
        items = data.get("data", {}).get("list", [])
    except (json.JSONDecodeError, AttributeError):
        return []

    if not isinstance(items, list):
        return []

    articles = []
    for item in items:
        if not isinstance(item, dict):
            continue
        oid = item.get("oid")
        title = str(item.get("title") or "").strip()
        if not oid or not title:
            continue
        ts_ms, formatted = _parse_create_time(item.get("createTime", ""))
        articles.append({
            "id": str(oid),
            "title": title,
            "summary": str(item.get("description") or "").strip(),
            "url": ARTICLE_URL_TPL.format(oid=oid),
            "source": str(item.get("sourceName") or "AIbase").strip(),
            "author": str(item.get("author") or "").strip(),
            "publish_time": formatted,
            "timestamp": ts_ms,
        })
    return articles


def _html_to_text(html_content: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html_content)
    text = re.sub(r"</p>", "\n\n", text)
    text = re.sub(r"</h[1-6]>", "\n\n", text)
    text = re.sub(r"</li>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    for entity, char in [
        ("&nbsp;", " "), ("\u00a0", " "),
        ("&lt;", "<"), ("&gt;", ">"),
        ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"),
    ]:
        text = text.replace(entity, char)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _fetch_article_content(article: dict, cfg: SourceConfig,
                           log: logging.Logger) -> dict | None:
    """抓取文章详情页，返回含全文的字典。"""
    try:
        html = _fetch(article["url"], log, referer=cfg.source_url)
    except Exception as exc:
        log.error("全文获取网络异常 id=%s title=%s error=%s",
                  article["id"], article["title"], exc)
        return None

    if html is None:
        log.warning("全文获取失败(空响应) id=%s url=%s", article["id"], article["url"])
        return None

    arr = _extract_nuxt_data(html)
    if not arr:
        log.warning("全文页 NUXT_DATA 提取失败 id=%s", article["id"])
        return None

    try:
        root = _devalue_resolve(arr, 0)
        detail = root["data"]["getAIDetail"]["data"]
    except (KeyError, TypeError, AttributeError) as exc:
        log.warning("全文页数据解析失败 id=%s error=%s", article["id"], exc)
        return None

    if not isinstance(detail, dict):
        log.warning("全文页 detail 不是 dict id=%s", article["id"])
        return None

    content_html = str(detail.get("summary") or "").strip()
    if not content_html:
        log.warning("全文提取失败(正文为空) id=%s title=%s", article["id"], article["title"])
        return None

    title = str(detail.get("title") or article["title"]).strip()
    author = str(detail.get("author") or article.get("author") or "").strip()
    _, formatted = _parse_create_time(str(detail.get("createTime") or article.get("publish_time", "")))

    return {
        "id": article["id"],
        "title": title,
        "author": author,
        "publish_time": formatted or article["publish_time"],
        "url": article["url"],
        "summary": article.get("summary", ""),
        "content_html": content_html,
        "content_text": _html_to_text(content_html),
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


def _repair_history_missing_files(
    history: dict[str, dict], paths: dict[str, Path], log: logging.Logger
) -> int:
    """将 content_fetched=true 且全文文件不存在的记录置为未获取，返回修复条数。"""
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

def run(cfg: SourceConfig) -> dict:
    """通用入口：基于配置执行一轮完整的抓取流程。"""
    paths = _get_paths(cfg)
    paths["base"].mkdir(parents=True, exist_ok=True)

    log = _get_logger(cfg, paths)
    now = datetime.now(tz=TZ_CN)
    run_id = now.strftime("%Y%m%d_%H%M%S")

    log.info("========== 运行开始 run=%s ==========", run_id)
    log.info("情报源=%s 目标=%s", cfg.source_name, cfg.source_url)

    # ---- 步骤1: 获取列表页（API 翻页，获取当天所有文章） ----
    log.info("[步骤1] 获取列表页（API 翻页）")
    merged_by_id: dict[str, dict] = {}
    found_old_article = False
    for page in range(1, MAX_LIST_PAGES + 1):
        list_url = LIST_API_TPL.format(ts=int(time.time() * 1000), page=page)
        try:
            resp = _fetch(list_url, log)
        except Exception as exc:
            log.critical("列表页请求异常 page=%d error=%s", page, exc, exc_info=True)
            sys.exit(1)
        if resp is None:
            log.warning("列表页获取失败 page=%d，停止翻页", page)
            break

        page_articles = _parse_list_api(resp)
        new_in_page = 0
        for a in page_articles:
            if a["id"] not in merged_by_id:
                merged_by_id[a["id"]] = a
                new_in_page += 1
        log.debug("page=%d 解析 %d 条，其中新增 %d 条（去重后累计 %d）",
                  page, len(page_articles), new_in_page, len(merged_by_id))

        if not page_articles:
            log.info("page=%d 无文章，停止翻页", page)
            break
        # 检查是否遇到了超过24小时的旧文章
        for a in page_articles:
            if not _is_within_24h(a["timestamp"], now):
                found_old_article = True
                break
        if found_old_article:
            log.info("page=%d 遇到超过24小时的旧文章，停止翻页", page)
            break
        if page < MAX_LIST_PAGES:
            time.sleep(random.uniform(*cfg.delay_range))

    articles = list(merged_by_id.values())
    articles.sort(key=lambda a: int(a["id"]), reverse=True)
    if not articles:
        log.critical("列表页解析结果为空，页面结构可能已变更")
        sys.exit(1)
    log.info("[步骤2] 解析文章索引 共 %d 条（多页去重后）", len(articles))

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
        log.info("新增条目 id=%s source=%s title=%s", a["id"], a["source"], a["title"])

    # ---- 步骤4: 全文获取（仅针对尚未入库的条目）----
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

    batch = pending_content[: cfg.max_content_per_run]
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
                    log.warning("推送未成功 id=%s status=%s 将在补推阶段重试",
                                article["id"], status)
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
            "author": a.get("author", ""),
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
        if h.get("content_fetched") and not push_status_done(h.get("push_status", ""))
    ]
    push_ok_count = 0
    push_fail_count = 0
    if unpushed:
        log.info("[步骤5.5] 补推未推送条目 count=%d", len(unpushed))
        for item in unpushed:
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
        if h.get("content_fetched") and not push_status_done(h.get("push_status", ""))
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
