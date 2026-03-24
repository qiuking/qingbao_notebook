"""懂车帝新车资讯情报源

抓取 https://www.dongchedi.com/news/newcar 的汽车资讯。
"""

import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.as_posix())

from dongchedi_base import SourceConfig, run  # noqa: E402

config = SourceConfig(
    source_id="dongchedi_newcar",
    source_name="懂车帝新车资讯",
    source_url="https://www.dongchedi.com/news/newcar",
    max_content_per_run=8,
    delay_range=(3.0, 6.0),
    push_max_per_sec=2.0,
)

if __name__ == "__main__":
    run(config)