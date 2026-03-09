"""36氪 汽车频道情报源"""

import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.as_posix())

from kr36_base import SourceConfig, run  # noqa: E402

config = SourceConfig(
    source_id="kr36_travel",
    source_name="36氪汽车资讯",
    source_url="https://36kr.com/information/travel/",
)

if __name__ == "__main__":
    run(config)
