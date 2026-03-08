# qbNoteBook — 情报获取与分析平台

AI领域情报的自动化采集、分析与输出平台。

## 功能模块

### 情报源采集

| 情报源 | 模块 | 状态 |
|---|---|---|
| 36氪AI资讯 | `sources/kr36_ai.py` | ✅ 已完成 |

### 36氪AI资讯 (`sources/kr36_ai.py`)

从 [36kr.com/information/AI](https://36kr.com/information/AI/) 自动采集最新AI资讯。

**核心特性：**
- 增量采集 — 基于历史记录比对，仅处理新增条目
- 反爬对抗 — UA轮换、随机延迟、Referer伪装、验证码检测
- 渐进全文 — 每轮限量获取全文，遇反爬立即停止，下轮自动继续
- 日志系统 — 双通道输出（控制台 + 文件），按天轮转保留30天

**运行方式：**

```bash
uv run sources/kr36_ai.py
```

**输出文件：**

```
output/
  kr36_ai_history.json   — 累积历史记录
  kr36_ai_latest.json    — 本轮运行结果
  kr36_articles/         — 全文存档（每篇一个JSON）
logs/
  kr36_ai.log            — 运行日志
```

## 环境要求

- Python >= 3.14
- 无第三方依赖（仅使用标准库）

## 安装

```bash
uv sync
```

