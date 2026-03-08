"""汽车之家 全部资讯情报源"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from autohome_base import SourceConfig, run  # noqa: E402

config = SourceConfig(
    source_id="autohome_all",
    source_name="汽车之家全部资讯",
    source_url="https://www.autohome.com.cn/all/",
)

if __name__ == "__main__":
    run(config)
