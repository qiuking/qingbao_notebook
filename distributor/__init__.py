"""
分发模块 — 文章总结与多平台分发

支持:
  - 使用 Claude API 智能总结文章内容
  - 推送到飞书多维表格

配置 (.env):
  CLAUDE_API_KEY=sk-xxx
  FEISHU_APP_ID=cli_xxx
  FEISHU_APP_SECRET=xxx
  FEISHU_SPREADSHEET_TOKEN=shtxxx
  FEISHU_SHEET_ID=0  # 可选，默认为 0
  AUTO_DISTRIBUTE=true  # 是否入库后自动分发
"""

__version__ = "0.1.0"
