"""
36氪AI资讯情报源 —— 增量抓取模块

设计为每30分钟被平台调度一次，核心特性:
  - 增量采集：基于历史记录比对，仅处理新增条目
  - 反爬对抗：UA轮换、随机延迟、Referer伪装、验证码检测
  - 渐进全文：每轮限量获取全文，遇反爬立即停止，下轮继续
  - 全文补获：历史中未获取全文的条目，后续轮次自动补取

文件结构:
  output/
    kr36_ai_history.json   — 累积历史记录（去重基准 + 全文获取状态）
    kr36_ai_latest.json    — 本轮运行结果（新增标记 + 内容摘要）
    kr36_articles/         — 全文存档，每篇一个 {article_id}.json
  logs/
    kr36_ai.log            — 运行日志（按天轮转，保留30天）
"""

import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# 路径 & 常量
# ---------------------------------------------------------------------------
SOURCE_URL = "https://36kr.com/information/AI/"
ARTICLE_URL_TPL = "https://36kr.com/p/{item_id}"

_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = _ROOT / "output"
HISTORY_FILE = OUTPUT_DIR / "kr36_ai_history.json"
LATEST_FILE = OUTPUT_DIR / "kr36_ai_latest.json"
ARTICLES_DIR = OUTPUT_DIR / "kr36_articles"
LOG_DIR = _ROOT / "logs"

TZ_CN = timezone(timedelta(hours=8))

MAX_CONTENT_FETCH_PER_RUN = 8
DELAY_RANGE = (5.0, 10.0)

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("kr36_ai")
log.setLevel(logging.DEBUG)

_log_fmt = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = TimedRotatingFileHandler(
    LOG_DIR / "kr36_ai.log",
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_log_fmt)
_file_handler.suffix = "%Y-%m-%d"

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_log_fmt)

log.addHandler(_file_handler)
log.addHandler(_console_handler)

# ---------------------------------------------------------------------------
# 反爬
# ---------------------------------------------------------------------------
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
]


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


def _polite_delay():
    delay = random.uniform(*DELAY_RANGE)
    log.debug("请求间隔等待 %.1fs", delay)
    time.sleep(delay)


def _is_captcha_page(html: str) -> bool:
    if len(html) < 3000 and ("captcha" in html.lower() or "TTGCaptcha" in html):
        return True
    if "verify_event" in html and "sec_sdk" in html:
        return True
    return False


def _fetch(url: str, *, referer: str | None = None, retries: int = 2) -> str | None:
    """带重试的HTTP GET。返回HTML字符串，遇到验证码返回None。"""
    for attempt in range(1, retries + 2):
        try:
            req = Request(url, headers=_build_headers(referer=referer))
            t0 = time.monotonic()
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8")
            elapsed = time.monotonic() - t0
            log.debug("HTTP GET %s -> %d bytes, %.2fs", url, len(html), elapsed)

            if _is_captcha_page(html):
                log.warning("验证码拦截 url=%s html_len=%d", url, len(html))
                return None
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

def _extract_json_array(html: str, marker: str) -> list[dict]:
    start = html.find(marker)
    if start < 0:
        return []
    arr_start = html.index("[", start)
    depth = 0
    for i, ch in enumerate(html[arr_start:arr_start + 200_000]):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return json.loads(html[arr_start:arr_start + i + 1])
    return []


def _ts_to_str(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=TZ_CN).strftime("%Y-%m-%d %H:%M")


def _is_within_24h(ts_ms: int, now: datetime) -> bool:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=TZ_CN)
    return (now - dt) <= timedelta(hours=24)


def parse_list_page(html: str) -> list[dict]:
    items = _extract_json_array(html, '"itemList":[')
    articles = []
    for item in items:
        tm = item.get("templateMaterial", {})
        item_id = str(tm.get("itemId") or item.get("itemId", ""))
        title = tm.get("widgetTitle", "").strip()
        if not item_id or not title:
            continue
        articles.append({
            "id": item_id,
            "title": title,
            "summary": tm.get("summary", "").strip(),
            "url": ARTICLE_URL_TPL.format(item_id=item_id),
            "source": tm.get("authorName", "").strip() or "36氪",
            "publish_time": _ts_to_str(tm.get("publishTime", 0)),
            "timestamp": tm.get("publishTime", 0),
        })
    return articles

# ---------------------------------------------------------------------------
# 详情页全文提取
# ---------------------------------------------------------------------------

def _extract_json_object(html: str, marker: str) -> dict:
    idx = html.find(marker)
    if idx < 0:
        return {}
    obj_start = html.index("{", idx + len(marker))
    depth = 0
    for i, ch in enumerate(html[obj_start:obj_start + 200_000]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(html[obj_start:obj_start + i + 1])
    return {}


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
    """触发验证码，应立即停止后续请求"""


def fetch_article_content(article: dict) -> dict | None:
    """获取单篇全文。触发验证码时抛出 CaptchaTriggered。"""
    try:
        html = _fetch(article["url"], referer=SOURCE_URL)
    except Exception as exc:
        log.error("全文获取网络异常 id=%s title=%s error=%s",
                  article["id"], article["title"], exc)
        return None

    if html is None:
        log.warning("全文获取触发验证码 id=%s url=%s", article["id"], article["url"])
        raise CaptchaTriggered(article["url"])

    detail = _extract_json_object(html, '"articleDetailData":')
    data = detail.get("data", {})
    content_html = data.get("widgetContent", "")
    if not content_html:
        log.warning("全文提取失败(正文为空) id=%s title=%s", article["id"], article["title"])
        return None

    return {
        "id": article["id"],
        "title": data.get("widgetTitle", article["title"]),
        "author": data.get("author", article["source"]),
        "publish_time": article["publish_time"],
        "url": article["url"],
        "summary": data.get("summary", article["summary"]),
        "content_html": content_html,
        "content_text": _html_to_text(content_html),
        "fetch_time": datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
    }

# ---------------------------------------------------------------------------
# 历史记录
# ---------------------------------------------------------------------------

def load_history() -> dict[str, dict]:
    if not HISTORY_FILE.exists():
        log.debug("历史文件不存在，首次运行")
        return {}
    try:
        data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        history = {a["id"]: a for a in data.get("articles", [])}
        log.debug("加载历史记录 %d 条", len(history))
        return history
    except (json.JSONDecodeError, KeyError) as exc:
        log.error("历史文件解析失败 path=%s error=%s", HISTORY_FILE, exc)
        return {}


def save_history(history: dict[str, dict]):
    sorted_articles = sorted(
        history.values(), key=lambda a: a.get("timestamp", 0), reverse=True
    )
    payload = {
        "meta": {
            "source_name": "36氪AI资讯",
            "last_update": datetime.now(tz=TZ_CN).strftime("%Y-%m-%d %H:%M:%S"),
            "total_articles": len(sorted_articles),
        },
        "articles": sorted_articles,
    }
    HISTORY_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.debug("历史记录已保存 count=%d path=%s", len(sorted_articles), HISTORY_FILE)


def save_article_file(article_data: dict) -> Path:
    ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
    filepath = ARTICLES_DIR / f"{article_data['id']}.json"
    filepath.write_text(
        json.dumps(article_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.debug("全文已保存 id=%s path=%s", article_data["id"], filepath)
    return filepath

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def fetch_and_save() -> dict:
    now = datetime.now(tz=TZ_CN)
    run_id = now.strftime("%Y%m%d_%H%M%S")

    log.info("========== 运行开始 run=%s ==========", run_id)
    log.info("情报源=36氪AI资讯 目标=%s", SOURCE_URL)

    # ---- 步骤1: 获取列表页 ----
    log.info("[步骤1] 获取列表页")
    try:
        html = _fetch(SOURCE_URL)
    except Exception as exc:
        log.critical("列表页请求异常 error=%s", exc, exc_info=True)
        sys.exit(1)
    if html is None:
        log.critical("列表页触发验证码，本轮放弃")
        sys.exit(1)
    log.info("列表页获取成功 size=%d bytes", len(html))

    # ---- 步骤2: 解析文章索引 ----
    log.info("[步骤2] 解析文章索引")
    articles = parse_list_page(html)
    if not articles:
        log.critical("列表页解析结果为空，页面结构可能已变更")
        sys.exit(1)

    articles_24h = [a for a in articles if _is_within_24h(a["timestamp"], now)]
    log.info("列表页解析完成 total=%d within_24h=%d", len(articles), len(articles_24h))

    # ---- 步骤3: 增量比对 ----
    log.info("[步骤3] 增量比对")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    history = load_history()

    new_articles = [a for a in articles if a["id"] not in history]
    log.info("增量比对完成 history=%d new=%d", len(history), len(new_articles))

    for a in new_articles:
        log.info("新增条目 id=%s source=%s title=%s", a["id"], a["source"], a["title"])

    # ---- 步骤4: 全文获取 ----
    pending_content = []
    for a in new_articles:
        pending_content.append(a)
    for a in articles:
        if a["id"] in history and not history[a["id"]].get("content_fetched"):
            if a["id"] not in {p["id"] for p in pending_content}:
                pending_content.append(a)

    pending_content.sort(key=lambda a: (
        0 if _is_within_24h(a["timestamp"], now) else 1,
        -a["timestamp"],
    ))

    batch = pending_content[:MAX_CONTENT_FETCH_PER_RUN]
    skipped = len(pending_content) - len(batch)

    success_count = 0
    fail_count = 0
    captcha_hit = False

    if batch:
        log.info("[步骤4] 全文获取 batch=%d pending_total=%d deferred=%d",
                 len(batch), len(pending_content), skipped)

        for i, article in enumerate(batch, 1):
            is_retry = article["id"] in history
            label = "补获" if is_retry else "新增"
            log.info("全文获取 [%d/%d] type=%s id=%s title=%s",
                     i, len(batch), label, article["id"], article["title"][:40])

            if i > 1:
                _polite_delay()

            try:
                content = fetch_article_content(article)
            except CaptchaTriggered:
                log.warning("全文获取中断: 触发验证码 已完成=%d/%d", i - 1, len(batch))
                captcha_hit = True
                article["content_fetched"] = False
                article["content_file"] = None
                fail_count += 1
                break

            if content:
                fp = save_article_file(content)
                article["content_fetched"] = True
                article["content_file"] = fp.name
                success_count += 1
                log.info("全文获取成功 id=%s chars=%d file=%s",
                         article["id"], len(content["content_text"]), fp.name)
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
        }

    save_history(history)

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
            })

    unfetched_in_history = sum(1 for h in history.values() if not h.get("content_fetched"))

    result = {
        "meta": {
            "source_name": "36氪AI资讯",
            "source_url": SOURCE_URL,
            "fetch_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "total_on_page": len(articles),
            "within_24h": len(articles_24h),
            "new_articles": len(new_articles),
            "content_fetch_success": success_count,
            "content_fetch_fail": fail_count,
            "captcha_triggered": captcha_hit,
            "content_pending": unfetched_in_history,
            "output_count": len(merged),
            "history_total": len(history),
        },
        "articles": merged,
    }

    LATEST_FILE.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ---- 运行汇总 ----
    log.info("========== 运行汇总 run=%s ==========", run_id)
    log.info("页面文章=%d 24h内=%d 新增=%d", len(articles), len(articles_24h), len(new_articles))
    log.info("全文: 成功=%d 失败=%d 验证码=%s 待补获=%d",
             success_count, fail_count, captcha_hit, unfetched_in_history)
    log.info("历史累计=%d 本轮输出=%d", len(history), len(merged))
    log.info("输出文件: latest=%s history=%s", LATEST_FILE.name, HISTORY_FILE.name)
    log.info("========== 运行结束 run=%s ==========", run_id)

    return result


if __name__ == "__main__":
    fetch_and_save()
