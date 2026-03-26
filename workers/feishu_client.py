"""
飞书 Webhook 客户端

支持通过飞书机器人 Webhook 发送消息。

配置示例 (task_scheduler/distribute_config.json):
{
    "rules": [
        {
            "name": "AI资讯",
            "categories": ["AI", "智能驾驶"],
            "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
        }
    ]
}
"""

import json
import logging
import time
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger("feishu_client")


class FeishuWebhookClient:
    """飞书机器人 Webhook 客户端"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    

    def send_message(self, content: dict, msg_type: str = "interactive") -> dict:
        """
        发送消息到飞书

        Args:
            content: 消息内容
            msg_type: 消息类型 (interactive/text/post)

        Returns:
            {"success": bool, "error": str}
        """
        payload = {
            "msg_type": msg_type,
            "content": content
        }

        try:
            req = Request(
                self.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )

            with urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))

            if result.get("code", 0) != 0:
                return {"success": False, "error": result.get("msg", "未知错误")}

            logger.debug("飞书消息发送成功: %s", self.webhook_url)
            return {"success": True, "error": None}

        except HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            logger.error("飞书 HTTP 错误: %d %s", e.code, error_body[:200])
            return {"success": False, "error": f"HTTP {e.code}"}

        except (URLError, TimeoutError) as e:
            logger.error("飞书网络错误: %s", e)
            return {"success": False, "error": f"网络错误: {e}"}

        except Exception as e:
            logger.exception("飞书发送异常: %s", e)
            return {"success": False, "error": str(e)}

    def send_article_card(
        self,
        title: str,
        summary: str,
        category: str,
        source_url: str,
        source_name: str,
        publish_time: str,
        fetch_time: str
    ) -> dict:
        """
        发送文章卡片消息
        title, ai_summary, ai_key_points, ai_category, source_id, source_name, publish_time, fetch_time, source_url

        Args:
            title: 文章标题
            summary: 摘要
            category: 文章分类
            source_url: 原文链接
            source_name: 来源名称
            publish_time: 发布时间
            fetch_time: 抓取时间

        Returns:
            {"success": bool, "error": str}
        """

        # 构建飞书卡片
        card_content = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": _get_color_for_category(category)
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**摘要**\n{summary}"
                    }
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**关键要点**\n{points_text}"
                    }
                },
                {
                    "tag": "div",
                    "fields": [
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**分类**\n{category}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**来源**\n{source_name}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**时间**\n{publish_time}"}}
                    ]
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "查看原文"},
                            "url": source_url,
                            "type": "primary"
                        }
                    ]
                },
                {"tag": "hr"},
                {
                    "tag": "note",
                    "elements": [
                        {"tag": "plain_text", "content": "由 qbNoteBook 自动推送"}
                    ]
                }
            ]
        }

        return self.send_message(card_content)


def _get_color_for_category(category: str) -> str:
    """根据分类返回卡片颜色"""
    category_colors = {
        "AI": "blue",
        "智能驾驶": "purple",
        "智能座舱": "purple",
        "汽车资讯": "green",
        "新车发布": "orange",
        "OTA资讯": "cyan",
        "政策法规": "red",
    }

    for key, color in category_colors.items():
        if key in category:
            return color

    return "grey"


def format_simple_message(
    title: str,
    summary: str,
    category: str,
    source_url: str,
    source_name: str
) -> dict:
    """格式化简单文本消息（备用）"""
    return {
        "text": f"【{category}】{title}\n\n{summary}\n\n来源: {source_name}\n链接: {source_url}"
    }