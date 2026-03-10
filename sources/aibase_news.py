"""AIBase AI资讯情报源"""

import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.as_posix())

from aibase_base import SourceConfig, run  # noqa: E402

config = SourceConfig(
    source_id="aibase_news",
    source_name="AIbase资讯",
    source_url="https://news.aibase.cn/zh/news",
)

if __name__ == "__main__":
    run(config)
