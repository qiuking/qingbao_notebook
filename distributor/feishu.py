"""
飞书多维表格集成模块

支持:
  - 获取 tenant_access_token
  - 向指定表格追加记录

配置 (.env):
  FEISHU_APP_ID=cli_xxx
  FEISHU_APP_SECRET=xxx
  FEISHU_SPREADSHEET_TOKEN=shtxxx
  FEISHU_SHEET_ID=0
"""

import json
import logging
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

logger = logging.getLogger("distributor.feishu")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"


def _read_env(key: str, default: str = "") -> str:
    """从 .env 文件读取配置"""
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


def get_feishu_config() -> dict:
    """获取飞书配置"""
    return {
        "app_id": _read_env("FEISHU_APP_ID", os.environ.get("FEISHU_APP_ID", "")),
        "app_secret": _read_env("FEISHU_APP_SECRET", os.environ.get("FEISHU_APP_SECRET", "")),
        "spreadsheet_token": _read_env("FEISHU_SPREADSHEET_TOKEN", os.environ.get("FEISHU_SPREADSHEET_TOKEN", "")),
        "sheet_id": _read_env("FEISHU_SHEET_ID", os.environ.get("FEISHU_SHEET_ID", "0")),
    }


def get_tenant_access_token(app_id: str, app_secret: str) -> str | None:
    """
    获取飞书 tenant_access_token

    Returns:
        token 字符串，失败返回 None
    """
    url = f"{FEISHU_API_BASE}/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": app_id,
        "app_secret": app_secret
    }).encode("utf-8")

    req = Request(
        url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"}
    )

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("code") == 0:
                token = data.get("tenant_access_token")
                logger.debug("获取 tenant_access_token 成功")
                return token
            else:
                logger.error("获取 token 失败: %s - %s", data.get("code"), data.get("msg"))
                return None
    except HTTPError as exc:
        logger.error("获取 token HTTP 错误: %s", exc.code)
        return None
    except Exception as exc:
        logger.error("获取 token 异常: %s", exc)
        return None


def append_record_to_sheet(
    token: str,
    spreadsheet_token: str,
    sheet_id: str,
    fields: dict
) -> bool:
    """
    向飞书多维表格追加一条记录

    Args:
        token: tenant_access_token
        spreadsheet_token: 表格 token
        sheet_id: 工作表 ID（默认为 "0"）
        fields: 字段值字典，key 是字段名，value 是字段值

    Returns:
        是否成功
    """
    url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values_append"

    # 构建请求体
    payload = {
        "valueRange": {
            "range": f"{sheet_id}",  # 自动追加到最后一行
            "values": [list(fields.values())]
        }
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    )

    try:
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if result.get("code") == 0:
                logger.info("飞书表格记录追加成功")
                return True
            else:
                logger.error("飞书表格追加失败: %s - %s", result.get("code"), result.get("msg"))
                return False
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.error("飞书表格 HTTP 错误: %s - %s", exc.code, body[:200])
        return False
    except Exception as exc:
        logger.error("飞书表格追加异常: %s", exc)
        return False


def add_record_v2(
    spreadsheet_token: str,
    sheet_id: str,
    fields: dict
) -> bool:
    """
    完整流程：获取 token 并添加记录（新版多维表格 API）

    Args:
        spreadsheet_token: 表格 token
        sheet_id: 工作表 ID
        fields: 字段值字典

    Returns:
        是否成功
    """
    config = get_feishu_config()

    if not config["app_id"] or not config["app_secret"]:
        logger.warning("飞书配置不完整，跳过推送")
        return False

    token = get_tenant_access_token(config["app_id"], config["app_secret"])
    if not token:
        return False

    return append_record_to_sheet(token, spreadsheet_token, sheet_id, fields)


def push_article_to_feishu(
    title: str,
    summary: str,
    category: str,
    source_name: str,
    source_url: str,
    publish_time: str,
    key_points: list[str] = None
) -> bool:
    """
    推送单篇文章到飞书多维表格的便捷函数

    Args:
        title: 标题
        summary: 一句话总结
        category: 类别
        source_name: 来源名称
        source_url: 原文链接
        publish_time: 发布时间
        key_points: 关键要点列表

    Returns:
        是否成功
    """
    config = get_feishu_config()

    if not config["spreadsheet_token"]:
        logger.warning("FEISHU_SPREADSHEET_TOKEN 未配置")
        return False

    # 构建字段（根据常见的多维表格列名）
    key_points_str = "\n".join(f"- {p}" for p in (key_points or []))

    fields = {
        "标题": title,
        "一句话总结": summary,
        "类别": category,
        "来源": source_name,
        "原文链接": source_url,
        "发布时间": publish_time,
        "关键要点": key_points_str,
        "入库时间": __import__('datetime').datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    return add_record_v2(
        config["spreadsheet_token"],
        config["sheet_id"],
        fields
    )
