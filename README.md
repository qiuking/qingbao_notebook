# qbNoteBook — 情报获取与分析平台

情报的自动化采集、处理与分发平台。

## 平台架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  情报获取    │────▶│  情报处理    │────▶│  数据库     │
│  sources/    │     │  processor/  │     │  SQLite     │
│  source_gov/ │     │             │     └──────┬──────┘
└─────────────┘     └─────────────┘            │
       ▲                                       │
       │                   ┌───────────────────┼───────────────────┐
       │                   ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  统一调度器   │     │  大模型服务   │    │  AI Worker  │    │  分发Worker  │
│  scheduler   │     │  llm_client  │    │  摘要+分类   │    │  飞书推送   │
└─────────────┘     └─────────────┘    └─────────────┘    └─────────────┘
```

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 配置环境变量

创建 `.env` 文件（首次启动 processor 会自动生成 `PROCESSOR_API_KEY`）：

```env
# 大模型配置（必需，用于 AI 摘要）
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=sk-xxx
LLM_MODEL=gpt-4o-mini
```

### 3. 启动所有服务

**前台启动（开发调试）：**

```bash
# 终端 1：启动 API 服务
uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000

# 终端 2：启动调度器
uv run python scheduler.py

# 终端 3：启动 AI 处理 Worker
uv run python -m workers.ai_worker

# 终端 4：启动分发 Worker
uv run python -m workers.distribute_worker
```

**后台启动（生产环境）：**

```bash
# 一键启动所有服务
nohup uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000 > logs/processor_stdout.log 2>&1 &
echo $! > logs/processor.pid

nohup uv run python scheduler.py > logs/scheduler_stdout.log 2>&1 &
echo $! > logs/scheduler.pid

nohup uv run python -m workers.ai_worker > logs/ai_worker_stdout.log 2>&1 &
echo $! > logs/ai_worker.pid

nohup uv run python -m workers.distribute_worker > logs/distribute_worker_stdout.log 2>&1 &
echo $! > logs/distribute_worker.pid
```

### 4. 停止所有服务

```bash
# 方式一：通过 PID 文件停止
kill $(cat logs/processor.pid) 2>/dev/null
kill $(cat logs/scheduler.pid) 2>/dev/null
kill $(cat logs/ai_worker.pid) 2>/dev/null
kill $(cat logs/distribute_worker.pid) 2>/dev/null

# 方式二：查找并停止进程
pkill -f "uvicorn processor.app"
pkill -f "scheduler.py"
pkill -f "workers.ai_worker"
pkill -f "workers.distribute_worker"
```

### 5. 查看服务状态

```bash
# 查看进程
ps aux | grep -E "uvicorn|scheduler|workers"

# 查看日志
tail -f logs/scheduler.log
tail -f logs/ai_worker.log
tail -f logs/distribute_worker.log

# 通过 API 查看状态
curl http://localhost:8000/api/workers/status
```

访问 http://localhost:8000 查看 Web 界面，http://localhost:8000/docs 查看 API 文档。

---

## 模块说明

### 模块一：情报获取 (`sources/`, `source_gov/`)

负责从各网站抓取情报数据，推送到 processor 入库。

| 目录 | 用途 | 数据源 |
|------|------|--------|
| `sources/` | 普通新闻资讯 | 36氪、汽车之家、AIbase、懂车帝 |
| `source_gov/` | 政府文件 | 工信部文件发布 |

**运行单个数据源：**

```bash
uv run python sources/kr36_ai.py
uv run python source_gov/miit_wjfb.py
```

### 模块二：情报处理 (`processor/`)

FastAPI Web 服务，提供 REST API 接收数据、查询数据、Web 界面。

**核心 API：**

| 方法 | 路径 | 功能 |
|------|------|------|
| `POST` | `/articles` | 单条入库 |
| `POST` | `/articles/batch` | 批量入库 |
| `GET` | `/articles` | 分页查询 |
| `GET` | `/stats` | 统计概览 |
| `GET` | `/api/workers/status` | Worker 状态 |

### 模块三：统一调度器 (`scheduler.py`)

按配置定时运行各数据源抓取任务。

**调度模式：**

| 模式 | 配置 | 说明 |
|------|------|------|
| 周期调度 | `interval: 20` | 每 20 分钟运行一次 |
| 定点调度 | `times: [8, 12, 18, 23]` | 在指定时间点附近运行 |

**配置文件：** `task_scheduler/sources.json`

### 模块四：大模型服务 (`processor/llm_client.py`)

支持 OpenAI 兼容 API 的 LLM 客户端。

**配置（`.env`）：**

```env
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=sk-xxx
LLM_MODEL=gpt-4o-mini
```

### 模块五：AI 处理 Worker (`workers/ai_worker.py`)

轮询数据库，对未处理的文章调用 LLM 进行摘要和分类。

**启动：** `uv run python -m workers.ai_worker`

**状态流转：** `pending` → `processing` → `completed` / `failed`

### 模块六：分发 Worker (`workers/distribute_worker.py`)

将已完成 AI 处理的文章推送到飞书 Webhook。

**启动：** `uv run python -m workers.distribute_worker`

**配置文件：** `task_scheduler/distribute_config.json`

```json
{
  "enabled": true,
  "rules": [
    {
      "name": "AI资讯",
      "categories": ["AI", "智能驾驶"],
      "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
    }
  ]
}
```

---

## 数据流程

```
1. 数据源抓取文章
   ↓
2. 推送到 processor 入库（ai_status=pending）
   ↓
3. AI Worker 调用 LLM 摘要+分类（ai_status=completed）
   ↓
4. 分发 Worker 推送到飞书（distribute_status=completed）
```

---

## 目录结构

```
qingbao_notebook/
├── sources/                # 新闻资讯数据源
│   ├── kr36_ai.py         # 36氪AI频道
│   ├── kr36_travel.py     # 36氪汽车频道
│   ├── autohome_all.py    # 汽车之家
│   ├── aibase_news.py     # AIBase
│   └── dongchedi_newcar.py # 懂车帝
│
├── source_gov/            # 政府文件数据源
│   └── miit_wjfb.py       # 工信部文件发布
│
├── processor/             # API 服务
│   ├── app.py             # FastAPI 主程序
│   ├── database.py        # 数据库管理
│   ├── llm_client.py      # LLM 客户端
│   └── static/            # Web 前端
│
├── workers/               # 后台处理模块
│   ├── ai_worker.py       # AI 摘要 Worker
│   ├── distribute_worker.py # 分发 Worker
│   └── feishu_client.py   # 飞书客户端
│
├── task_scheduler/        # 配置目录
│   ├── sources.json       # 数据源配置
│   └── distribute_config.json # 分发配置
│
├── logs/                  # 日志目录
│   ├── scheduler.log      # 调度器日志
│   ├── ai_worker.log      # AI Worker 日志
│   └── distribute_worker.log # 分发日志
│
├── data/                  # 数据源运行时数据
├── data_server/           # 数据库文件
│   └── qingbao_zx.db
│
├── scheduler.py           # 统一调度器
└── .env                   # 环境配置
```

---

## 常见问题

### Q: 推送失败怎么办？

确保 processor 已启动且端口正确。检查 `.env` 中的 `PROCESSOR_URL` 配置。

### Q: AI 处理一直 pending？

1. 确认 `LLM_API_KEY` 已配置
2. 确认 AI Worker 已启动：`ps aux | grep ai_worker`
3. 查看日志：`tail -f logs/ai_worker.log`

### Q: 如何重新处理失败的文章？

```bash
# 通过 API 重试
curl -X POST http://localhost:8000/api/workers/retry-ai/文章ID \
  -H "X-API-Key: your-api-key"
```

### Q: 如何添加新的数据源？

1. 在 `sources/` 中创建抓取脚本
2. 在 `task_scheduler/sources.json` 中添加配置
3. 调度器会自动加载新配置

---

## 注意事项

1. **processor 必须先启动**：其他模块依赖 processor API
2. **API Key 安全**：`.env` 不应提交到版本库
3. **日志管理**：定期清理 `logs/` 目录
4. **数据库备份**：定期备份 `data_server/qingbao_zx.db`