raw_events：系统“看到了什么”
parsed_events：系统“理解成了什么”
cases：系统“把这件事当成哪条持续故事在跟踪”

私有 data 文件：
- 默认读取 `data/addresses.json` 和 `data/lp_pools.json`
- 可选读取 `data/entities.json` 和 `data/address_aliases.json`
- 可选读取 `data/clmm_managers.json`，用于本地注入已知 CLMM PositionManager（如 `protocol/chain/manager_address/abi_profile/enabled/note`）
- 公开仓库缺少这些文件时，会分别以空地址簿 `{}` 和空池子簿 `[]` 启动，基础 import / unittest 仍可运行
- `entities.json` 用来本地注入实体归属（`entity_id/entity_label/entity_type/cluster_tags`）
- `address_aliases.json` 用来本地覆盖地址别名、钱包职能和实体补丁（如 `wallet_function/entity_source/entity_confidence`）
- `clmm_managers.json` 用来本地启用 Uniswap v3 / v4 PositionManager registry；缺失时 CLMM parser 会优雅降级为不识别 manager
- 需要本地注入时，直接把对应 JSON 文件放到 `data/` 目录即可；公开仓库提供 `data/entities.example.json`、`data/address_aliases.example.json` 和 `data/clmm_managers.example.json` 作为最小 schema 参考
