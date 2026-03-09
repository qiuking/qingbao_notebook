"""36氪 AI 频道情报源"""

import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.as_posix())

from kr36_base import SourceConfig, run  # noqa: E402

config = SourceConfig(
    source_id="kr36_ai",
    source_name="36氪AI资讯",
    source_url="https://36kr.com/information/AI/",
)

if __name__ == "__main__":
    run(config)
