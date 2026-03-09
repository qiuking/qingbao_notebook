"""
定时调度器 — 按周期启动 sources 脚本

功能:
  - 每 20 分钟启动一次 sources 目录下的所有可用数据源
  - 最多 10 个任务并行运行
  - 同一网站的任务串行执行（避免触发反爬限制）
  - 记录日志到 task_scheduler/scheduler.log

使用:
  uv run python scheduler.py
"""

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator

_PROJECT_ROOT = Path(__file__).resolve().parent
_LOG_DIR = _PROJECT_ROOT / "task_scheduler"
_CONFIG_FILE = _LOG_DIR / "sources.json"
_LOG_FILE = _LOG_DIR / "scheduler.log"
_STATE_FILE = _LOG_DIR / "state.json"

# 调度参数
INTERVAL_MINUTES = 20
MAX_PARALLEL_TASKS = 10


def _init_logging() -> logging.Logger:
    """初始化日志记录器"""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("scheduler")
    logger.setLevel(logging.DEBUG)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    # 使用 as_posix() 确保路径始终为正向斜杠，避免 Win/Ubuntu 混用时的乱码
    file_handler = logging.FileHandler(
        _LOG_FILE.as_posix(), encoding="utf-8", mode="a",
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    # 控制台 handler
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


@dataclass
class SourceInfo:
    """数据源信息"""
    source_id: str
    group: str  # 用于分组，同一组内串行执行
    command: str  # 运行命令


def _load_config(logger: logging.Logger) -> list[SourceInfo]:
    """从配置文件加载数据源信息"""
    if not _CONFIG_FILE.exists():
        logger.error("配置文件不存在: %s", _CONFIG_FILE.as_posix())
        return []

    try:
        data = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error("配置文件解析失败: %s", e)
        return []

    sources = []
    for item in data.get("sources", []):
        if not item.get("enabled", True):
            continue

        source = SourceInfo(
            source_id=item["id"],
            group=item.get("group", "default"),
            command=item.get("command", ""),
        )

        if not source.command:
            logger.warning("数据源 %s 未配置运行命令，已跳过", source.source_id)
            continue

        sources.append(source)

    logger.info("从配置文件加载 %d 个数据源", len(sources))
    return sources


def _run_source(command: str, source_id: str, logger: logging.Logger) -> dict:
    """同步执行单个数据源脚本"""
    import subprocess

    cmd = command.replace("python", sys.executable).split()
    start_time = time.time()

    try:
        # 使用 subprocess 执行，捕获输出
        # encoding='utf-8' 配合 errors='replace' 处理编码问题
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,  # 10 分钟超时
            cwd=_PROJECT_ROOT.as_posix(),
        )
        elapsed = time.time() - start_time

        if result.returncode == 0:
            status = "success"
            error_msg = ""
        else:
            status = "failed"
            error_msg = result.stderr[:500] if result.stderr else result.stdout[:500]

        return {
            "source_id": source_id,
            "status": status,
            "elapsed_sec": round(elapsed, 1),
            "error": error_msg,
        }

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        return {
            "source_id": source_id,
            "status": "timeout",
            "elapsed_sec": round(elapsed, 1),
            "error": f"执行超过 10 分钟被终止",
        }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "source_id": source_id,
            "status": "error",
            "elapsed_sec": round(elapsed, 1),
            "error": str(e)[:500],
        }


class TaskQueue:
    """任务队列：支持按组分组串行、总体并行限制"""

    def __init__(self, max_parallel: int = 10):
        self._max_parallel = max_parallel
        self._queues: dict[str, list[SourceInfo]] = {}  # group -> pending tasks
        self._running: dict[str, SourceInfo] = {}  # group -> running task
        self._results: list[dict] = []

    def add_tasks(self, sources: list[SourceInfo]):
        """添加任务，按组分组"""
        for s in sources:
            if s.group not in self._queues:
                self._queues[s.group] = []
            self._queues[s.group].append(s)

    def _get_next_task(self) -> SourceInfo | None:
        """获取下一个待执行任务（同组串行）"""
        for group, queue in self._queues.items():
            if queue and group not in self._running:
                return queue.pop(0)
        return None

    def start_next(self) -> SourceInfo | None:
        """启动下一个任务（如果并行槽位未满且有可用任务）"""
        if len(self._running) >= self._max_parallel:
            return None

        task = self._get_next_task()
        if task:
            self._running[task.group] = task
            return task
        return None

    def mark_done(self, source_id: str, group: str, result: dict):
        """标记任务完成"""
        self._running.pop(group, None)
        self._results.append(result)

    def get_running_count(self) -> int:
        return len(self._running)

    def has_pending(self) -> bool:
        """是否有待执行的任务"""
        return any(q for q in self._queues.values()) or bool(self._running)

    def get_results(self) -> list[dict]:
        return self._results


def _run_scheduler_cycle(sources: list[SourceInfo], logger: logging.Logger):
    """执行一次调度周期（真正的并行执行）"""
    import concurrent.futures

    queue = TaskQueue(max_parallel=MAX_PARALLEL_TASKS)
    queue.add_tasks(sources)

    logger.info("=" * 50)
    logger.info("调度周期开始")
    logger.info("总任务数=%d 最大并行=%d", len(sources), MAX_PARALLEL_TASKS)

    # 按组分组显示
    group_counts: dict[str, int] = {}
    for s in sources:
        group_counts[s.group] = group_counts.get(s.group, 0) + 1
    for g, c in group_counts.items():
        logger.info("  - %s: %d 个任务（串行执行）", g, c)

    start_time = time.time()
    results: list[dict] = []

    # 使用 ThreadPoolExecutor 实现真正的并行
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
        # 跟踪每个 group 当前运行的任务: group -> (task, future)
        running_tasks: dict[str, tuple[SourceInfo, concurrent.futures.Future]] = {}
        completed_groups: set[str] = set()  # 已完成的 group

        def submit_next_task() -> bool:
            """尝试启动下一个任务，返回是否成功启动"""
            task = queue.start_next()
            if task is None:
                return False

            logger.info("启动任务 source=%s group=%s cmd=%s",
                        task.source_id, task.group, task.command)

            # 提交任务到线程池
            future = executor.submit(_run_source, task.command, task.source_id, logger)
            running_tasks[task.group] = (task, future)
            return True

        # 第一阶段：尽快启动最多 MAX_PARALLEL_TASKS 个任务
        while submit_next_task() and len(running_tasks) < MAX_PARALLEL_TASKS:
            pass

        # 第二阶段：等待任务完成并补充新任务
        while running_tasks:
            # 使用 wait 等待任意任务完成（带超时以便检查新任务）
            done_futures = []
            for group, (task, future) in list(running_tasks.items()):
                if future.done():
                    done_futures.append((group, task, future))

            if not done_futures:
                # 没有任务完成，等待一下
                time.sleep(0.5)
                continue

            # 处理完成的任务
            for group, task, future in done_futures:
                # 从 running_tasks 中移除
                if group in running_tasks:
                    del running_tasks[group]

                # 获取结果
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "source_id": task.source_id,
                        "status": "error",
                        "elapsed_sec": 0,
                        "error": str(e),
                    }

                results.append(result)
                queue.mark_done(task.source_id, group, result)

                if result["status"] == "success":
                    logger.info("任务完成 source=%s elapsed=%.1fs",
                                result["source_id"], result["elapsed_sec"])
                else:
                    logger.warning("任务失败 source=%s status=%s error=%s",
                                   result["source_id"], result["status"],
                                   result.get("error", "")[:100])

                # 立即尝试启动该 group 的下一个任务
                submit_next_task()

    elapsed = time.time() - start_time

    # 汇总结果
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count

    logger.info("调度周期完成 elapsed=%.1fs success=%d failed=%d",
                elapsed, success_count, failed_count)

    # 保存状态
    _save_state(results, logger)

    return results


def _save_state(results: list[dict], logger: logging.Logger):
    """保存执行状态到文件"""
    try:
        state = {
            "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(results),
            "success": sum(1 for r in results if r["status"] == "success"),
            "failed": sum(1 for r in results if r["status"] != "success"),
            "results": results,
        }
        _STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2),
                               encoding="utf-8")
    except Exception as e:
        logger.error("保存状态失败: %s", e)


def run_scheduler():
    """主调度循环"""
    logger = _init_logging()

    logger.info("=" * 60)
    logger.info("定时调度器启动")
    logger.info("运行间隔: %d 分钟", INTERVAL_MINUTES)
    logger.info("最大并行任务: %d", MAX_PARALLEL_TASKS)
    logger.info("配置文件: %s", _CONFIG_FILE.as_posix())
    logger.info("=" * 60)

    try:
        while True:
            # 从配置文件加载数据源（每次都重新读取）
            sources = _load_config(logger)

            if not sources:
                logger.warning("未加载到有效的数据源，等待下次调度...")
            else:
                _run_scheduler_cycle(sources, logger)

            # 等待下一个周期
            logger.info("等待 %d 分钟后进行下一次调度...", INTERVAL_MINUTES)
            time.sleep(INTERVAL_MINUTES * 60)

    except KeyboardInterrupt:
        logger.info("调度器被用户中断")
    except Exception as e:
        logger.exception("调度器异常退出: %s", e)


if __name__ == "__main__":
    run_scheduler()