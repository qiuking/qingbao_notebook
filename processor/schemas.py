"""
Pydantic 模型 — 请求与响应的数据契约
"""

import json
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# 入库请求
# ---------------------------------------------------------------------------

class ArticleCreate(BaseModel):
    """source 脚本提交情报条目时使用。"""
    source_id: str = Field(..., description="情报源标识，如 kr36_ai / autohome_all")
    source_name: str = Field("", description="情报源显示名")
    origin_id: str = Field("", description="源站文章ID，用于去重")
    title: str = Field(..., description="标题")
    summary: str = Field("", description="摘要")
    content_text: str = Field("", description="正文纯文本")
    content_html: str = Field("", description="正文原始HTML")
    author: str = Field("", description="作者")
    source_url: str = Field("", description="原文网址")
    publish_time: str = Field("", description="消息发布时间")
    fetch_time: str = Field("", description="抓取时间")


class ArticleBatchCreate(BaseModel):
    """批量入库。"""
    articles: list[ArticleCreate]


class ArticleUpdate(BaseModel):
    """部分更新（所有字段可选）。"""
    title: str | None = None
    summary: str | None = None
    content_text: str | None = None
    content_html: str | None = None
    author: str | None = None
    source_url: str | None = None
    publish_time: str | None = None


# ---------------------------------------------------------------------------
# 响应
# ---------------------------------------------------------------------------

class ArticleOut(BaseModel):
    """单条情报返回。"""
    id: int
    source_id: str
    source_name: str
    origin_id: str
    title: str
    summary: str
    content_text: str
    content_html: str
    author: str
    source_url: str
    publish_time: str
    fetch_time: str
    created_at: str
    updated_at: str
    # AI 处理字段
    ai_status: str = "pending"
    ai_summary: str = ""
    ai_key_points: list[str] = []
    ai_category: str = ""
    ai_processed_at: str = ""
    ai_error: str = ""
    # 分发字段
    distribute_status: str = "pending"
    distribute_at: str = ""
    distribute_webhook: str = ""
    distribute_error: str = ""


class ArticleListOut(BaseModel):
    total: int
    page: int
    page_size: int
    articles: list[ArticleOut]


class DuplicateCheckResult(BaseModel):
    """查重结果。"""
    origin_id: str
    exists: bool
    article_id: int | None = None


class BatchCreateResult(BaseModel):
    inserted: int
    duplicates: int
    details: list[DuplicateCheckResult]


# ---------------------------------------------------------------------------
# AI 处理状态
# ---------------------------------------------------------------------------

class AIStatusStats(BaseModel):
    """AI 处理状态统计"""
    pending: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0
    total: int = 0


class DistributeStatusStats(BaseModel):
    """分发状态统计"""
    pending: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    total: int = 0


class WorkerStatusResponse(BaseModel):
    """Worker 状态响应"""
    ai_stats: AIStatusStats
    distribute_stats: DistributeStatusStats
    ai_worker_running: bool = False
    distribute_worker_running: bool = False


# ---------------------------------------------------------------------------
# LLM 相关
# ---------------------------------------------------------------------------

class LLMChatRequest(BaseModel):
    """LLM 对话请求"""
    prompt: str = Field(..., description="用户提示词")
    content: list[dict] | None = Field(None, description="要处理的内容（OpenAI 格式）")
    system_prompt: str | None = Field(None, description="系统提示词")
    model: str | None = Field(None, description="模型名称")
    thinking: bool | None = Field(None, description="是否启用思考")


class LLMChatResponse(BaseModel):
    """LLM 对话响应"""
    success: bool
    output_text: str = Field(..., description="模型返回的文本")
    response: dict | None = Field(None, description="模型返回的response原始数据")
    usage: dict | None = Field(None, description="模型返回的usage原始数据")
    error: str | None = Field(None, description="错误信息")


class LLMSummarizeRequest(BaseModel):
    """LLM 摘要请求"""
    title: str = Field(..., description="文章标题")
    content: str = Field(..., description="文章内容")
    system_prompt: str | None = Field(None, description="系统提示词")


class LLMSummarizeResponse(BaseModel):
    """LLM 摘要响应"""
    summary: str
    key_points: list[str]
    category: str
    full_text: str
    success: bool
    error: str | None
