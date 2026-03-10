# qbNoteBook — 情报获取与分析平台

情报的自动化采集、处理与分发平台。

## 平台架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  情报获取    │────▶│  情报处理    │────▶│  情报分发    │
│  sources/    │     │  processor/  │     │  (规划中)    │
└─────────────┘     └─────────────┘     └─────────────┘
       ▲
       │ 定时触发
┌─────────────┐
│  定时调度器   │
│  scheduler.py │
└─────────────┘
```

三大模块相对独立，支持插拔使用。

## 环境要求与安装

- Python >= 3.14
- 依赖：fastapi, uvicorn

```bash
uv sync
```

---

## 模块一：情报获取 (`sources/`)

| 情报源 | 模块 | 引擎 | 状态 |
|---|---|---|---|
| 36氪AI资讯 | `sources/kr36_ai.py` | `kr36_base.py` | ✅ 已完成 |
| 36氪汽车资讯 | `sources/kr36_travel.py` | `kr36_base.py` | ✅ 已完成 |
| 汽车之家全部资讯 | `sources/autohome_all.py` | `autohome_base.py` | ✅ 已完成 |
| AIBase资讯 | `sources/aibase_news.py` | `aibase_base.py` | ✅ 已完成 |

**核心特性：** 增量采集、反爬对抗、渐进全文、数据隔离、按天轮转日志、全文获取后自动推送至 processor 入库。AIBase 仅抓取列表首页（站点为前端分页，URL 参数 `?page=` 无效），同页内按 id 去重。

```bash
# 运行单个情报源（需先启动 processor 服务，否则推送会失败）
uv run python sources/kr36_ai.py
uv run python sources/kr36_travel.py
uv run python sources/autohome_all.py
uv run python sources/aibase_news.py
```

---

## 模块二：情报处理 (`processor/`)

基于 FastAPI + SQLite 的 Web 服务，接收各情报源抓取结果并提供结构化存储、查询与 Web 前端界面。

### 认证

写操作（POST/PATCH/DELETE）需要 API Key 认证，读操作（GET）保持开放。

- 首次启动服务时自动在项目根目录 `.env` 中生成 `PROCESSOR_API_KEY`
- 请求时通过 `X-API-Key` 请求头传递
- sources 脚本自动读取 `.env` 中的 Key 进行推送，无需手动配置

### API 概览

| 方法 | 路径 | 认证 | 功能 |
|---|---|---|---|
| `POST` | `/articles` | 需要 | 单条入库 |
| `POST` | `/articles/batch` | 需要 | 批量入库（自动去重） |
| `POST` | `/articles/check-duplicates` | 需要 | 查重检测 |
| `GET` | `/articles` | 不需要 | 分页查询（支持按来源/关键词/作者/时间过滤） |
| `GET` | `/articles/{id}` | 不需要 | 单条读取 |
| `PATCH` | `/articles/{id}` | 需要 | 部分修改 |
| `DELETE` | `/articles/{id}` | 需要 | 删除 |
| `GET` | `/stats` | 不需要 | 统计概览 |
| `GET` | `/api/trend/daily` | 不需要 | 每日文章数量趋势 |
| `GET` | `/api/trend/by-source` | 不需要 | 各数据源每日趋势 |
| `GET` | `/api/groups` | 不需要 | 数据源分组统计 |

### 入库字段

| 字段 | 说明 |
|---|---|
| `source_id` | 情报源标识（如 `kr36_ai`） |
| `source_name` | 情报源显示名 |
| `origin_id` | 源站文章ID（与 source_id 联合去重） |
| `title` | 标题 |
| `summary` | 摘要 |
| `content_text` | 正文纯文本 |
| `content_html` | 正文原始HTML |
| `author` | 作者 |
| `source_url` | 原文网址 |
| `publish_time` | 消息发布时间 |
| `fetch_time` | 抓取时间 |

### 启动服务

```bash
uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000

# 开发模式（自动重载）
uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000 --reload
```

启动后访问 http://localhost:8000 查看 Web 前端，http://localhost:8000/docs 查看交互式 API 文档。

### 存储

- 数据库文件：`data_server/qingbao_zx.db`（SQLite）
- 自动建表，零配置
- WAL 模式，支持并发读取

---

## 模块三：定时调度器 (`scheduler.py`)

按周期自动启动各情报源脚本，实现无人值守采集。

- **运行间隔：** 每 20 分钟执行一次
- **最大并行：** 10 个任务
- **同组串行：** 同一 `group`（根域名，如 `36kr.com`、`aibase.cn`）内的任务串行执行，避免同站访问过密
- **配置驱动：** 从 `task_scheduler/sources.json` 读取数据源列表，每次周期重新加载
- **日志：** `task_scheduler/scheduler.log`，状态写入 `task_scheduler/state.json`

```bash
uv run python scheduler.py
```

按 `Ctrl+C` 可安全退出。

---

## 部署方法

### 1. 本地开发 / 单机部署

```bash
# 1. 安装依赖
uv sync

# 2. 启动 processor 服务（必须先启动，sources 推送依赖此服务）
uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000

# 3. 另开终端，二选一：
# 方式 A：手动运行单个情报源
uv run python sources/kr36_ai.py

# 方式 B：启动定时调度器（自动按周期运行所有配置的数据源）
uv run python scheduler.py
```

### 2. 生产部署（processor 与 sources 分离）

当 processor 部署在远程服务器时：

1. 在 **processor 所在机器** 启动服务：
   ```bash
   uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000
   ```

2. 在 **sources 所在机器** 的 `.env` 中配置：
   ```
   PROCESSOR_API_KEY=<首次启动 processor 时自动生成的 key>
   PROCESSOR_URL=http://<processor 服务器 IP>:8000
   ```

3. 运行 sources 或 scheduler 时，推送会自动发往 `PROCESSOR_URL`。

### 3. 后台常驻运行

```bash
# 使用 nohup 后台运行 processor
nohup uv run uvicorn processor.app:app --host 0.0.0.0 --port 8000 > processor.log 2>&1 &

# 使用 nohup 后台运行 scheduler
nohup uv run python scheduler.py > scheduler_stdout.log 2>&1 &
```

---

## 注意事项

1. **processor 必须先启动**：sources 抓取到全文后会推送到 processor，若服务未启动，推送会失败（仅记录日志，不影响本地数据存储）。
2. **API Key 自动生成**：首次启动 processor 时会在 `.env` 中写入 `PROCESSOR_API_KEY`，请勿提交 `.env` 到版本库。
3. **远程部署**：sources 与 processor 分离部署时，需在 sources 端配置 `PROCESSOR_URL` 和 `PROCESSOR_API_KEY`。
4. **调度配置**：修改 `task_scheduler/sources.json` 可增删数据源、调整 `group`（根域名，同组串行）或 `enabled` 开关。
5. **单任务超时**：scheduler 中每个数据源脚本最长执行 10 分钟，超时会被终止。
6. **跨平台路径**：代码统一使用 `Path.as_posix()` 输出路径，避免 Windows 反斜杠在 Ubuntu 上产生乱码；建议在 Ubuntu 上部署生产环境。

---

## 目录结构

```
sources/
  push_to_processor.py  ← 推送公用模块（读取 .env 认证并发送到 processor）
  kr36_base.py         ← 36氪通用抓取引擎
  kr36_ai.py           ← 36氪AI频道
  kr36_travel.py       ← 36氪汽车频道
  autohome_base.py     ← 汽车之家通用抓取引擎
  autohome_all.py      ← 汽车之家全部资讯
  aibase_base.py       ← AIBase 通用抓取引擎（仅首页列表，按 id 去重）
  aibase_news.py       ← AIBase 资讯

processor/
  app.py               ← FastAPI 主服务
  auth.py              ← API Key 认证
  database.py          ← SQLite 数据库管理
  schemas.py           ← Pydantic 数据模型
  static/              ← Web 前端静态文件（可选）

task_scheduler/
  sources.json         ← 调度器数据源配置
  scheduler.log        ← 调度日志
  state.json           ← 最近一次执行状态

data/                  ← 情报源运行时数据
  kr36_ai/
  kr36_travel/
  autohome_all/
  aibase_news/

data_server/
  qingbao_zx.db        ← 情报数据库（SQLite）

.env                   ← API Key、PROCESSOR_URL 等配置（自动生成，不入版本库）
```
