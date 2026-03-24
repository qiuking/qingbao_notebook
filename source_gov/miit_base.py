"""
工信部网站通用抓取引擎

抓取工信部各栏目的政府文件。
数据来源：通过 API 获取列表，解析 HTML 获取全文。

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
from urllib.parse import urlencode

import sys
from pathlib import Path

# 添加 sources 目录到路径，以便导入 push_to_processor
_sources_dir = Path(__file__).resolve().parent.parent / "sources"
if str(_sources_dir) not in sys.path:
    sys.path.insert(0, str(_sources_dir))

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
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceConfig:
    source_id: str          # 唯一标识，如 "miit_wjfb"
    source_name: str        # 显示名，如 "工信部文件发布"
    source_url: str         # 列表页URL（用于显示）
    # API 参数
    web_id: str             # 网站ID
    tpl_set_id: str         # 模板集ID
    page_id: str            # 页面ID
    # 抓取参数
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

_API_BASE = "https://www.miit.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit"


def _build_headers(*, referer: str | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _fetch_api(params: dict, log: logging.Logger, retries: int = 2) -> dict | None:
    """调用工信部 API 获取数据"""
    url = f"{_API_BASE}?{urlencode(params)}"
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_headers())
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            elapsed = time.monotonic() - t0
            log.debug("API GET %s -> %.2fs", url[:80], elapsed)
            return data
        except (URLError, HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt <= retries:
                wait = 4 * attempt + random.uniform(1, 3)
                log.warning("请求失败 attempt=%d/%d error=%s 等待%.1fs",
                            attempt, retries, exc, wait)
                time.sleep(wait)
            else:
                log.error("请求彻底失败 error=%s", exc)
                raise
    return None


def _fetch_html(url: str, log: logging.Logger, *,
                referer: str | None = None, retries: int = 2) -> str | None:
    """获取 HTML 页面"""
    full_url = f"https://www.miit.gov.cn{url}" if url.startswith("/") else url
    for attempt in range(1, retries + 2):
        try:
            req = Request(full_url, headers=_build_headers(referer=referer))
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8")
            elapsed = time.monotonic() - t0
            log.debug("HTTP GET %s -> %d bytes, %.2fs", full_url, len(html), elapsed)
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
    r'<li[^>]*class="cf"[^>]*>\s*'
    r'<a[^>]+href="([^"]+)"[^>]+title="([^"]+)"[^>]*>\s*<i></i>[^<]*</a>\s*'
    r'<span[^>]*>(\d{4}-\d{2}-\d{2})</span>',
    re.DOTALL,
)


def _parse_list_html(html: str) -> list[dict]:
    """从 API 返回的 HTML 中解析文章列表"""
    articles = []
    for m in _RE_ARTICLE.finditer(html):
        url, title, date = m.groups()
        # 从 URL 提取文章 ID
        # URL 格式: /jgsj/zbys/wjfb/art/2026/art_xxx.html
        art_id_match = re.search(r'/art_([a-f0-9]+)\.html', url)
        if not art_id_match:
            continue
        art_id = art_id_match.group(1)

        # 解析日期
        try:
            dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=TZ_CN)
            timestamp = int(dt.timestamp() * 1000)
            publish_time = dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            timestamp = 0
            publish_time = date

        articles.append({
            "id": art_id,
            "title": title.strip(),
            "summary": "",
            "url": f"https://www.miit.gov.cn{url}",
            "source": "工业和信息化部",
            "publish_time": publish_time,
            "timestamp": timestamp,
        })
    return articles


def _fetch_list_page(cfg: SourceConfig, log: logging.Logger, page: int = 1) -> list[dict]:
    """获取列表页文章"""
    params = {
        "parseType": "buildstatic",
        "webId": cfg.web_id,
        "tplSetId": cfg.tpl_set_id,
        "pageType": "column",
        "tagId": "当前栏目_list",
        "editType": "null",
        "pageId": cfg.page_id,
        "pageNo": str(page),
    }

    data = _fetch_api(params, log)
    if not data or not data.get("success"):
        log.warning("API 返回失败: %s", data.get("message", "unknown"))
        return []

    html = data.get("data", {}).get("html", "")
    if not html:
        log.warning("API 返回的 HTML 为空")
        return []

    return _parse_list_html(html)


# ---------------------------------------------------------------------------
# 详情页解析
# ---------------------------------------------------------------------------

def _html_to_text(html_content: str) -> str:
    """HTML 转纯文本"""
    text = re.sub(r'<br\s*/?>', '\n', html_content)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</h[1-6]>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    for entity, char in [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"),
                          ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"),
                          ("\u00a0", " ")]:
        text = text.replace(entity, char)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_meta(html: str, name: str) -> str:
    """提取 meta 标签内容"""
    m = re.search(rf'<meta\s+name="{name}"\s+content="([^"]*)"', html, re.IGNORECASE)
    if m:
        return m.group(1)
    return ""


def _fetch_article_content(article: dict, cfg: SourceConfig,
                           log: logging.Logger) -> dict | None:
    """抓取文章详情页，返回含全文的字典"""
    try:
        html = _fetch_html(article["url"], log)
    except Exception as exc:
        log.error("全文获取网络异常 id=%s title=%s error=%s",
                  article["id"], article["title"], exc)
        return None

    if html is None:
        log.warning("全文获取失败(空响应) id=%s url=%s", article["id"], article["url"])
        return None

    # 提取标题
    title = _extract_meta(html, "ArticleTitle") or article["title"]

    # 提取发布时间
    pub_date = _extract_meta(html, "PubDate")
    if pub_date:
        try:
            dt = datetime.strptime(pub_date, "%Y-%m-%d %H:%M").replace(tzinfo=TZ_CN)
            publish_time = dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            publish_time = pub_date
    else:
        publish_time = article["publish_time"]

    # 提取来源
    source = _extract_meta(html, "ContentSource") or article["source"]

    # 提取正文 - 在 id="con_con" 的 div 中
    content_match = re.search(
        r'<div[^>]+id="con_con"[^>]*>(.*?)</div>\s*(?:<div|<!--二维码|<script)',
        html, re.DOTALL
    )
    if not content_match:
        # 尝试另一种方式
        content_match = re.search(
            r'<div[^>]+class="ccontent[^"]*"[^>]*>(.*?)</div>\s*(?:<div id="ewm|<div class="article_fd)',
            html, re.DOTALL
        )

    if not content_match:
        log.warning("全文提取失败(未找到正文) id=%s title=%s", article["id"], article["title"])
        return None

    content_html = content_match.group(1)
    content_text = _html_to_text(content_html)

    if not content_text.strip():
        log.warning("全文提取失败(正文为空) id=%s title=%s", article["id"], article["title"])
        return None

    # 提取发文字号（如果有）
    doc_no = ""
    doc_no_match = re.search(r'<span[^>]*class="xxgk-fwzh[^"]*"[^>]*>([^<]+)</span>', html)
    if doc_no_match:
        doc_no = doc_no_match.group(1).strip()

    return {
        "id": article["id"],
        "title": title.strip(),
        "author": "",
        "publish_time": publish_time,
        "url": article["url"],
        "summary": f"发文字号：{doc_no}" if doc_no else "",
        "content_html": content_html.strip(),
        "content_text": content_text,
        "source": source,
        "doc_no": doc_no,
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
        articles = _fetch_list_page(cfg, log)
    except Exception as exc:
        log.critical("列表页请求异常 error=%s", exc, exc_info=True)
        sys.exit(1)

    if not articles:
        log.critical("列表页解析结果为空，页面结构可能已变更")
        sys.exit(1)

    log.info("[步骤2] 解析文章索引 共 %d 条", len(articles))

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
        log.info("新增条目 id=%s title=%s", a["id"], a["title"][:50])

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

                # 构建推送的 summary（包含发文字号）
                push_summary = content.get("summary", "")
                if not push_summary and content.get("doc_no"):
                    push_summary = f"发文字号：{content['doc_no']}"

                status = push_article(
                    source_id=cfg.source_id,
                    source_name=cfg.source_name,
                    origin_id=article["id"],
                    title=content["title"],
                    summary=push_summary,
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
            push_summary = content.get("summary", "")
            if not push_summary and content.get("doc_no"):
                push_summary = f"发文字号：{content['doc_no']}"

            status = push_article(
                source_id=cfg.source_id,
                source_name=cfg.source_name,
                origin_id=item["id"],
                title=content.get("title", item["title"]),
                summary=push_summary,
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