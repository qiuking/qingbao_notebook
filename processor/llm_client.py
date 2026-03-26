"""
大模型客户端模块 — 支持 OpenAI 兼容 API

从 .env 读取配置：
  LLM_BASE_URL       - API 基础地址 (如 https://api.openai.com/v1, 默认 空)
  LLM_API_KEY        - API 密钥
  LLM_THINKING       - 是否启用思考 (默认 False)
  LLM_MODEL          - 模型名称 (默认 doubao-seed-2-0-mini-260215)

使用示例：
    # 方式1：重新定义client
    from processor.llm_client import LLMClient

    client = LLMClient()
    response = client.chat("请总结以下文章", "文章内容...")

    # 方式2： 获取默认的client
    client = get_llm_client()
    response = client.chat("请总结以下文章", "文章内容...")
    print(response)
"""

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from openai import OpenAI


def _init_logging() -> logging.Logger:
    """初始化日志记录器"""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    _LOG_DIR = _PROJECT_ROOT / "logs"
    _LOG_FILE = _LOG_DIR / "llm_client.log"
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("processor.llm_client")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(
        _LOG_FILE.as_posix(), encoding="utf-8", mode="a",
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = _init_logging()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


def _read_env(key: str, default: str = "") -> str:
    """从 .env 文件读取配置"""
    if not _ENV_FILE.exists():
        return default
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


@dataclass
class LLMConfig:
    """大模型配置"""
    base_url: str
    api_key: str
    model: str
    thinking: bool


def get_llm_config() -> LLMConfig:
    """获取大模型配置"""
    logger.info("获取大模型配置...（get_llm_config）")
    return LLMConfig(
        base_url=_read_env("LLM_BASE_URL", os.environ.get("LLM_BASE_URL", "")),
        api_key=_read_env("LLM_API_KEY", os.environ.get("LLM_API_KEY", "")),
        model=_read_env("LLM_MODEL", os.environ.get("LLM_MODEL", "doubao-seed-2-0-mini-260215")),
        thinking=bool(_read_env("LLM_THINKING", "False"))
    )


class LLMClient:
    """大模型客户端 — 支持 OpenAI 兼容 API（异步版本）"""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or get_llm_config()
        self._initialized = bool(self.config.api_key and self.config.base_url)

        if not self._initialized:
            logger.error("LLM_API_KEY 或 LLM_BASE_URL 未配置，大模型功能不可用")
            self.client = None
        else:
            self.client = OpenAI(
                base_url=self.config.base_url.rstrip('/'),
                api_key=self.config.api_key,
            )

    @property
    def is_available(self) -> bool:
        """检查客户端是否可用"""
        return self.client is not None

    def chat(
        self,
        prompt: str,
        content: list[dict] = None,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        thinking: Optional[bool] = None,
    ) -> dict:
        """
        发送对话请求

        Args:
            prompt: 用户提示词，用于用户的文字信息输入
            content: 要处理的内容（会追加到 prompt 后面,dic内容，用于通用数据输入,需要满足openAI格式）
            system_prompt: 系统提示词（可选,用于设置模型行为）
            model: 模型名称（可选,用于指定模型）
            thinking: 是否启用思考（可选,用于设置模型思考）

        Returns:
            {
                "success": True/False,
                "output_text": "模型返回的文本",
                "response": "模型返回的response原始数据",
                "error": "错误信息（如果有）",
                "usage": {"prompt_tokens": N, "completion_tokens": N, "total_tokens": N}
            }
        """
        if not self.client:
            logger.error("LLM client 获取失败: model=%s url=%s", self.config.model, self.config.base_url)
            return {
                "success": False,
                "output_text": "",
                "usage": None,
                "error": "LLM client 未配置"
            }
        #
        input_content = [
            {
                "type": "input_text",
                "text": f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
            }
        ]
        if content:
            if isinstance(content, list):
                for item in content:
                    input_content.append(item)
            elif isinstance(content, dict):
                input_content.append(content)
            else:
                logger.error("LLM chat 输入的content类型错误: content=%s", content)
                return {
                    "success": False,
                    "content": "",
                    "usage": None,
                    "error": "chat 输入的content类型错误"
                }
        
        response = self.client.responses.create(
            model=model if model else self.config.model,
            extra_body={"thinking": {"type": "enabled" if thinking else "disabled"}},
            input=[
                {
                    "role": "user",
                    "content": input_content
                }
            ],
        )

        logger.info("LLM 调用成功: model=%s  tokens_usage: in-%d  out-%d", self.config.model, response.usage.input_tokens, response.usage.output_tokens)

        return {
            "success": True,
            "output_text": response.output_text,
            "response": response,
            "usage": response.usage,
            "error": None,
        }


    def summarize(self, title: str, content: str, system_prompt: str=None) -> dict:
        """
        总结文章

        Args:
            title: 文章标题
            content: 文章内容
            system_prompt: 系统提示词
        Returns:
            {
                "summary": "一句话总结",
                "key_points": ["要点1", "要点2", ...],
                "category": "类别",
                "full_text": "完整总结文本",
                "success": True/False,
                "error": "错误信息（如果有）"
            }
        """
        default_prompt = """你是一个专业汽车及AI智能化的产品和技术专家，智商和眼光远超常人，且尤其擅长总结文章内容和对文章进行领域分类。请总结提供的文章的摘要，并对文章分类。
回复格式要求：
【一句话总结】总结文章的核心内容，要求字数少于100字。
【关键要点】根据需要列出必要的2-6个关键要点，每条以"- "开头，每条单独一行,且每条字数少于80字,注意带上必要的关键数据。
【类别】判断文章类别,以(AI/汽车资讯/新车发布/智能驾驶/智能座舱/政策法规/OTA资讯/其他)中的一个或多个分类，用/分隔""" 
        
        system_prompt = system_prompt if system_prompt else default_prompt
        prompt = f"请总结和分类以下文章：\n\n标题：{title}\n\n内容：{content}"

        res = self.chat(prompt, system_prompt=system_prompt)

        if not res["success"]:
            logger.error("LLM 总结文章失败: title=%s error=%s", title, res["error"])
            return {
                "summary": "",
                "key_points": [],
                "category": "",
                "full_text": "",
                "success": False,
                "error": res["error"],
            }

        # 解析总结结果
        full_text = res["output_text"]
        result = self._parse_summary(full_text)
        result["full_text"] = full_text
        result["success"] = True
        return result

    def _parse_summary(self, text: str) -> dict:
        """解析模型返回的总结文本"""
        summary = ""
        key_points = []
        category = ""

        lines = text.strip().split("\n")
        current_section = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("【一句话总结】"):
                current_section = "summary"
                summary = line.replace("【一句话总结】", "").strip()
            elif line.startswith("【关键要点】"):
                current_section = "points"
            elif line.startswith("【类别】"):
                current_section = "category"
                category = line.replace("【类别】", "").strip()
            elif current_section == "points" and line.startswith("-"):
                key_points.append(line.lstrip("-").strip())
            elif current_section == "summary" and not summary:
                summary = line

        return {
            "summary": summary,
            "key_points": key_points,
            "category": category,
            "full_text": text,
            "success": True,
            "error": None,
        }


# 全局单例
_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """获取全局 LLM 客户端实例"""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client

