# qbNoteBook — 情报获取与分析平台

情报的自动化采集、处理与分发平台。

## 平台架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  情报获取    │────▶│  情报处理    │────▶│  情报分发    │
│  sources/    │     │  processor/  │     │ distributor/ │
└─────────────┘     └─────────────┘     └─────────────┘
       ▲
       │ 定时触发
┌─────────────┐
│  统一调度器   │
│  scheduler.py │
└─────────────┘
```

三大模块相对独立，支持插拔使用。

## 环境要求与安装

- Python >= 3.14
- 依赖：fastapi, uvicorn, anthropic (可选，用于 AI 摘要)

```bash
uv sync
```

---

## 模块一：情报获取

情报获取分为两类数据源目录：

| 目录 | 用途 | 数据源 |
|------|------|--------|
| `sources/` | 普通新闻资讯 | 36氪、汽车之家、AIbase、懂车帝 |
| `source_gov/` | 政府文件 | 工信部文件发布 |

### 1. 普通新闻资讯 (`sources/`)

| 情报源 | 模块 | 引擎 | 状态 |
|---|---|---|---|
| 36氪AI资讯 | `sources/kr36_ai.py` | `kr36_base.py` | ✅ 已完成 |
| 36氪汽车资讯 | `sources/kr36_travel.py` | `kr36_base.py` | ✅ 已完成 |
| 汽车之家全部资讯 | `sources/autohome_all.py` | `autohome_base.py` | ✅ 已完成 |
| AIBase资讯 | `sources/aibase_news.py` | `aibase_base.py` | ✅ 已完成 |
| 懂车帝新车资讯 | `sources/dongchedi_newcar.py` | `dongchedi_base.py` | ✅ 已完成 |

### 2. 政府文件 (`source_gov/`)

| 情报源 | 模块 | 引擎 | 状态 |
|---|---|---|---|
| 工信部文件发布 | `source_gov/miit_wjfb.py` | `source_gov/miit_base.py` | ✅ 已完成 |

**核心特性：** 增量采集、反爬对抗、渐进全文、数据隔离、按天轮转日志、全文获取后自动推送至 processor 入库。

```bash
# 运行单个情报源（需先启动 processor 服务，否则推送会失败）
uv run python sources/kr36_ai.py
uv run python sources/dongchedi_newcar.py
uv run python source_gov/miit_wjfb.py
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

## 模块三：统一调度器 (`scheduler.py`)

支持两种调度模式的统一调度器：

### 调度模式

| 模式 | 配置字段 | 说明 | 示例 |
|------|----------|------|------|
| 周期调度 | `interval` | 每隔 N 分钟运行一次 | `20` = 每20分钟 |
| 定点调度 | `times` | 在指定时间点附近运行 | `[8, 12, 18, 23]` = 约8点、12点、18点、23点 |

**定点调度逻辑：**
- 在整点前后 5 分钟内触发（如 8:00-8:05）
- 每个时间点每天只触发一次
- 不要求精确时间，系统每 60 秒检查一次

### 配置示例

```json
{
  "sources": [
    {
      "id": "kr36_ai",
      "group": "36kr.com",
      "command": "python sources/kr36_ai.py",
      "interval": 20,
      "enabled": true
    },
    {
      "id": "miit_wjfb",
      "group": "miit.gov.cn",
      "command": "python source_gov/miit_wjfb.py",
      "times": [8, 12, 18, 23],
      "enabled": true
    }
  ]
}
```

### 特性

- **同组串行：** 同一 `group`（根域名）内的任务串行执行，避免同站访问过密
- **配置热更新：** 每次检查周期重新读取 `task_scheduler/sources.json`
- **日志：** `task_scheduler/scheduler.log`，状态写入 `task_scheduler/state.json`

```bash
uv run python scheduler.py
```

按 `Ctrl+C` 可安全退出。

---

## 模块四：情报分发 (`distributor/`)

基于 Claude API 的文章摘要与飞书表格分发。

### 功能

1. **AI 摘要**：使用 Claude API 生成文章摘要和要点
2. **飞书分发**：自动推送到飞书多维表格

### 配置

在 `.env` 中添加：

```env
CLAUDE_API_KEY=<Claude API Key>
FEISHU_APP_ID=<飞书应用 ID>
FEISHU_APP_SECRET=<飞书应用密钥>
FEISHU_SPREADSHEET_TOKEN=<飞书表格 Token>
FEISHU_SHEET_ID=<飞书 Sheet ID>
AUTO_DISTRIBUTE=true
```

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

# 方式 B：启动统一调度器（自动按周期/时间点运行所有配置的数据源）
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
4. **调度配置**：修改 `task_scheduler/sources.json` 可增删数据源、调整 `group`（根域名，同组串行）、`interval`（周期）、`times`（定点）或 `enabled` 开关。
5. **单任务超时**：scheduler 中每个数据源脚本最长执行 10 分钟，超时会被终止。
6. **跨平台路径**：代码统一使用 `Path.as_posix()` 输出路径，避免 Windows 反斜杠在 Ubuntu 上产生乱码；建议在 Ubuntu 上部署生产环境。

---

## 目录结构

```
sources/                    # 普通新闻资讯数据源
  push_to_processor.py      ← 推送公用模块
  kr36_base.py              ← 36氪通用抓取引擎
  kr36_ai.py                ← 36氪AI频道
  kr36_travel.py            ← 36氪汽车频道
  autohome_base.py          ← 汽车之家通用抓取引擎
  autohome_all.py           ← 汽车之家全部资讯
  aibase_base.py            ← AIBase 通用抓取引擎
  aibase_news.py            ← AIBase 资讯
  dongchedi_base.py         ← 懂车帝通用抓取引擎
  dongchedi_newcar.py       ← 懂车帝新车资讯

source_gov/                 # 政府文件数据源
  miit_base.py              ← 工信部通用抓取引擎
  miit_wjfb.py              ← 工信部文件发布

processor/
  app.py                    ← FastAPI 主服务
  auth.py                   ← API Key 认证
  database.py               ← SQLite 数据库管理
  schemas.py                ← Pydantic 数据模型
  static/                   ← Web 前端静态文件

distributor/
  summarizer.py             ← Claude API 摘要
  feishu.py                 ← 飞书表格推送
  router.py                 ← 分发路由

task_scheduler/
  sources.json              ← 调度器数据源配置
  scheduler.log             ← 调度日志
  state.json                ← 最近一次执行状态

data/                       ← 情报源运行时数据
  kr36_ai/
  kr36_travel/
  autohome_all/
  aibase_news/
  dongchedi_newcar/
  miit_wjfb/

data_server/
  qingbao_zx.db             ← 情报数据库（SQLite）

.env                        ← API Key、PROCESSOR_URL 等配置（自动生成，不入版本库）
scheduler.py                ← 统一调度器
```