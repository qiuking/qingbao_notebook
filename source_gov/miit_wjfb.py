"""工信部装备工业一司文件发布情报源

抓取 https://www.miit.gov.cn/jgsj/zbys/wjfb/index.html 的政府文件。
"""

import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.as_posix())

from miit_base import SourceConfig, run  # noqa: E402

# 装备工业一司-文件发布 栏目配置
# API 参数从页面分析获取
config = SourceConfig(
    source_id="miit_wjfb",
    source_name="工信部文件发布",
    source_url="https://www.miit.gov.cn/jgsj/zbys/wjfb/index.html",
    # API 参数（从页面分析获取）
    web_id="8d828e408d90447786ddbe128d495e9e",
    tpl_set_id="209741b2109044b5b7695700b2bec37e",
    page_id="28ac65269a12494f81b5a832bce5f51c",
    # 抓取参数
    max_content_per_run=8,
    delay_range=(3.0, 6.0),
    push_max_per_sec=2.0,
)

if __name__ == "__main__":
    run(config)