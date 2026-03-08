# qbNoteBook — 情报获取与分析平台

AI领域情报的自动化采集、分析与输出平台。

## 功能模块

### 情报源采集

| 情报源 | 模块 | 引擎 | 状态 |
|---|---|---|---|
| 36氪AI资讯 | `sources/kr36_ai.py` | `kr36_base.py` | ✅ 已完成 |
| 36氪汽车资讯 | `sources/kr36_travel.py` | `kr36_base.py` | ✅ 已完成 |
| 汽车之家全部资讯 | `sources/autohome_all.py` | `autohome_base.py` | ✅ 已完成 |

### 架构设计

```
sources/
  kr36_base.py       ← 36氪通用抓取引擎（共享逻辑）
  kr36_ai.py          ← 36氪AI频道（配置入口）
  kr36_travel.py      ← 36氪汽车频道（配置入口）
  autohome_base.py   ← 汽车之家通用抓取引擎（共享逻辑）
  autohome_all.py     ← 汽车之家全部资讯（配置入口）

data/                 ← 运行时数据（各源独立目录）
  kr36_ai/
    history.json      — 累积历史记录
    latest.json       — 本轮运行结果
    articles/         — 全文存档
    logs/
      kr36_ai.log     — 日志（按天轮转，保留30天）
  kr36_travel/
    ...（同上结构）
  autohome_all/
    ...（同上结构）
```

新增情报源只需创建一个配置文件即可，无需重复编写抓取逻辑。

### 核心特性

- **增量采集** — 基于历史记录比对，仅处理新增条目
- **反爬对抗** — UA轮换、随机延迟、Referer伪装、验证码检测
- **渐进全文** — 每轮限量获取全文，遇反爬立即停止，下轮自动继续
- **数据隔离** — 每个情报源独立目录，日志/输出/全文互不干扰
- **日志系统** — 双通道输出（控制台INFO + 文件DEBUG），按天轮转

## 运行方式

```bash
# 运行单个情报源
uv run sources/kr36_ai.py
uv run sources/kr36_travel.py
uv run sources/autohome_all.py
```

## 环境要求

- Python >= 3.14
- 无第三方依赖（仅使用标准库）

## 安装

```bash
uv sync
```
