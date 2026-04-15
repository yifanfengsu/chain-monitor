raw_events：系统“看到了什么”
parsed_events：系统“理解成了什么”
cases：系统“把这件事当成哪条持续故事在跟踪”

私有 data 文件：
- 默认读取 `data/addresses.json` 和 `data/lp_pools.json`
- 公开仓库缺少这些文件时，会分别以空地址簿 `{}` 和空池子簿 `[]` 启动，基础 import / unittest 仍可运行
- 需要本地注入时，直接把对应 JSON 文件放到 `data/` 目录即可
