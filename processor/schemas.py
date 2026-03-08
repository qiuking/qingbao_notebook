"""
Pydantic 模型 — 请求与响应的数据契约
"""

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
