"""
统一调度器 — 支持周期调度和定点调度

功能:
  - 周期调度: 每 N 分钟运行一次（如每20分钟）
  - 定点调度: 在指定时间点附近运行（如 8:00, 12:00, 18:00, 23:00）
  - 同一网站的任务串行执行（避免触发反爬限制）
  - 记录日志到 task_scheduler/scheduler.log

配置说明:
  - interval: 周期调度，单位分钟（如 20 表示每20分钟）
  - times: 定点调度，小时列表（如 [8, 12, 18, 23] 在这些时间点附近运行）
  - 两者可同时配置，也可只配其一

使用:
  uv run python scheduler.py
"""

import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent
_LOG_DIR = _PROJECT_ROOT / "task_scheduler"
_CONFIG_FILE = _LOG_DIR / "sources.json"
_LOG_FILE = _LOG_DIR / "scheduler.log"
_STATE_FILE = _LOG_DIR / "state.json"

# 默认检查间隔（秒）
CHECK_INTERVAL = 60


def _init_logging() -> logging.Logger:
    """初始化日志记录器"""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("scheduler")
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


@dataclass
class SourceInfo:
    """数据源信息"""
    source_id: str
    group: str  # 用于分组，同一组内串行执行
    command: str
    # 调度配置
    interval: Optional[int] = None  # 周期调度间隔（分钟）
    times: Optional[list[int]] = None  # 定点调度时间点（小时）


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

        interval = item.get("interval")  # 分钟
        times = item.get("times")  # 小时列表

        # 至少要有一种调度配置
        if not interval and not times:
            logger.debug("数据源 %s 未配置调度时间，使用默认间隔20分钟", item.get("id"))
            interval = 20

        source = SourceInfo(
            source_id=item["id"],
            group=item.get("group", "default"),
            command=item.get("command", ""),
            interval=interval,
            times=times,
        )

        if not source.command:
            logger.warning("数据源 %s 未配置运行命令，已跳过", source.source_id)
            continue

        sources.append(source)

    logger.info("从配置文件加载 %d 个数据源", len(sources))
    return sources


def _load_state() -> dict:
    """加载运行状态"""
    if not _STATE_FILE.exists():
        return {}
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_state(state: dict, logger: logging.Logger):
    """保存运行状态"""
    try:
        state["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.error("保存状态失败: %s", e)


def _run_source(command: str, source_id: str, logger: logging.Logger) -> dict:
    """执行单个数据源脚本"""
    cmd = command.replace("python", sys.executable).split()
    start_time = time.time()

    try:
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
            "error": "执行超过 10 分钟被终止",
        }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "source_id": source_id,
            "status": "error",
            "elapsed_sec": round(elapsed, 1),
            "error": str(e)[:500],
        }


def _should_run_interval(source: SourceInfo, state: dict, now: datetime) -> bool:
    """检查周期调度是否需要运行"""
    if not source.interval:
        return False

    last_key = f"{source.source_id}_interval_last"
    last_run_str = state.get(last_key)

    if not last_run_str:
        return True

    try:
        last_run = datetime.fromisoformat(last_run_str)
        return (now - last_run) >= timedelta(minutes=source.interval)
    except ValueError:
        return True


def _should_run_times(source: SourceInfo, state: dict, now: datetime) -> bool:
    """检查定点调度是否需要运行

    定点调度逻辑：
    - 在指定时间点前后5分钟内触发
    - 每个时间点每天只触发一次
    """
    if not source.times:
        return False

    current_hour = now.hour
    current_minute = now.minute

    # 检查当前小时是否在配置的时间点中
    if current_hour not in source.times:
        return False

    # 只在整点前后5分钟内触发（0-5分钟）
    if current_minute > 5:
        return False

    # 检查今天这个时间点是否已经运行过
    today = now.strftime("%Y-%m-%d")
    run_key = f"{source.source_id}_times_{today}_{current_hour}"

    if state.get(run_key) == "done":
        return False

    return True


def _mark_interval_run(source: SourceInfo, state: dict):
    """标记周期调度已运行"""
    key = f"{source.source_id}_interval_last"
    state[key] = datetime.now().isoformat()


def _mark_times_run(source: SourceInfo, state: dict, now: datetime):
    """标记定点调度已运行"""
    today = now.strftime("%Y-%m-%d")
    key = f"{source.source_id}_times_{today}_{now.hour}"
    state[key] = "done"


def _cleanup_old_state(state: dict):
    """清理过期的状态记录"""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    keys_to_remove = []
    for key in state:
        # 清理定点调度的旧记录
        if "_times_" in key:
            if today not in key and yesterday not in key:
                keys_to_remove.append(key)

    for key in keys_to_remove:
        del state[key]


class TaskQueue:
    """任务队列：支持按组分组串行、总体并行限制"""

    def __init__(self, max_parallel: int = 10):
        self._max_parallel = max_parallel
        self._queues: dict[str, list[tuple[SourceInfo, str]]] = {}  # group -> [(source, reason), ...]
        self._running: dict[str, SourceInfo] = {}  # group -> running task
        self._results: list[dict] = []

    def add_task(self, source: SourceInfo, reason: str):
        """添加任务到队列"""
        if source.group not in self._queues:
            self._queues[source.group] = []
        self._queues[source.group].append((source, reason))

    def has_pending(self) -> bool:
        """是否有待执行的任务"""
        return any(q for q in self._queues.values()) or bool(self._running)

    def get_next_task(self) -> Optional[tuple[SourceInfo, str]]:
        """获取下一个待执行任务（同组串行）"""
        for group, queue in self._queues.items():
            if queue and group not in self._running:
                return queue.pop(0)
        return None

    def start_task(self, source: SourceInfo):
        """标记任务开始"""
        self._running[source.group] = source

    def finish_task(self, source: SourceInfo, result: dict):
        """标记任务完成"""
        self._running.pop(source.group, None)
        self._results.append(result)

    def get_running_count(self) -> int:
        return len(self._running)

    def get_results(self) -> list[dict]:
        return self._results


def run_scheduler():
    """主调度循环"""
    logger = _init_logging()

    logger.info("=" * 60)
    logger.info("统一调度器启动")
    logger.info("配置文件: %s", _CONFIG_FILE.as_posix())
    logger.info("检查间隔: %d 秒", CHECK_INTERVAL)
    logger.info("=" * 60)

    state = _load_state()

    try:
        while True:
            now = datetime.now()

            # 每次循环重新加载配置（支持热更新）
            sources = _load_config(logger)

            if not sources:
                logger.warning("未加载到有效的数据源，等待...")
            else:
                # 清理过期状态
                _cleanup_old_state(state)

                # 检查哪些任务需要运行
                queue = TaskQueue(max_parallel=10)

                for source in sources:
                    # 检查定点调度
                    if _should_run_times(source, state, now):
                        reason = f"定点调度 {now.hour}:00"
                        queue.add_task(source, reason)
                        _mark_times_run(source, state, now)

                    # 检查周期调度
                    elif _should_run_interval(source, state, now):
                        reason = f"周期调度 (间隔{source.interval}分钟)"
                        queue.add_task(source, reason)
                        _mark_interval_run(source, state)

                # 执行任务
                if queue.has_pending():
                    _run_tasks(queue, logger, state)

                _save_state(state, logger)

            # 等待下一次检查
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        logger.info("调度器被用户中断")
    except Exception as e:
        logger.exception("调度器异常退出: %s", e)


def _run_tasks(queue: TaskQueue, logger: logging.Logger, state: dict):
    """执行队列中的任务"""
    import concurrent.futures

    results = []
    running_tasks: dict[str, tuple[SourceInfo, str, concurrent.futures.Future]] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        def submit_next():
            task_info = queue.get_next_task()
            if task_info is None:
                return False
            source, reason = task_info
            logger.info("启动任务 source=%s reason=%s", source.source_id, reason)
            future = executor.submit(_run_source, source.command, source.source_id, logger)
            queue.start_task(source)
            running_tasks[source.group] = (source, reason, future)
            return True

        # 启动初始任务
        while submit_next() and len(running_tasks) < 10:
            pass

        # 等待完成并启动新任务
        while running_tasks:
            done_groups = []
            for group, (source, reason, future) in list(running_tasks.items()):
                if future.done():
                    done_groups.append(group)

            if not done_groups:
                time.sleep(0.5)
                continue

            for group in done_groups:
                source, reason, future = running_tasks.pop(group)
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "source_id": source.source_id,
                        "status": "error",
                        "elapsed_sec": 0,
                        "error": str(e),
                    }

                queue.finish_task(source, result)
                results.append(result)

                if result["status"] == "success":
                    logger.info("任务完成 source=%s elapsed=%.1fs", result["source_id"], result["elapsed_sec"])
                else:
                    logger.warning("任务失败 source=%s status=%s error=%s",
                                   result["source_id"], result["status"],
                                   result.get("error", "")[:100])

                submit_next()

    # 汇总
    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    logger.info("本轮完成 success=%d failed=%d", success, failed)


if __name__ == "__main__":
    run_scheduler()