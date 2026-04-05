# ERC20 Transfer(address,address,uint256) 事件签名。
ERC20_TRANSFER_EVENT_SIG = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
# Uniswap V2 Pair Swap 事件签名。
UNISWAP_V2_SWAP_EVENT_SIG = (
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
)
# Uniswap V3 Pool Swap 事件签名。
UNISWAP_V3_SWAP_EVENT_SIG = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)

# 已知 Swap 事件签名集合（用于日志裁剪与交易识别）。
SWAP_EVENT_SIGNATURES = {
    UNISWAP_V2_SWAP_EVENT_SIG,
    UNISWAP_V3_SWAP_EVENT_SIG,
}

# 监听时只保留这些事件，减少 payload。
INTERESTING_EVENT_SIGNATURES = {
    ERC20_TRANSFER_EVENT_SIG,
    *SWAP_EVENT_SIGNATURES,
}

# 常见以太坊主网 DEX Router 地址白名单。
DEX_ROUTERS = {
    "0x7a250d5630b4cf539739df2c5dacab4c659f2488",  # Uniswap V2 Router02
    "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3 SwapRouter
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap SwapRouter02
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b",  # Uniswap Universal Router
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",  # SushiSwap Router
    "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch Router v5
}

# 稳定币元数据（用于方向识别与单位换算）。
STABLE_TOKEN_METADATA = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
    "0x4fabb145d64652a948d72533023f6e7a623c7c53": {"symbol": "BUSD", "decimals": 18},
    "0x853d955acef822db058eb8505911ed77f175b99e": {"symbol": "FRAX", "decimals": 18},
    "0x0000000000085d4780b73119b644ae5ecd22b376": {"symbol": "TUSD", "decimals": 18},
    "0x8e870d67f660d95d5be530380d0ec0bd388289e1": {"symbol": "USDP", "decimals": 18},
}

# WETH 视为 ETH 报价腿的等价资产。
WETH_TOKEN_CONTRACT = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_TOKEN_METADATA = {
    WETH_TOKEN_CONTRACT: {"symbol": "WETH", "decimals": 18},
}

# 交易所 followup 常见主流资产。
WBTC_TOKEN_CONTRACT = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
WBTC_TOKEN_METADATA = {
    WBTC_TOKEN_CONTRACT: {"symbol": "WBTC", "decimals": 8},
}

# 稳定币地址快速查询集合。
STABLE_TOKEN_CONTRACTS = set(STABLE_TOKEN_METADATA.keys())
STABLE_TOKEN_SYMBOLS = {item["symbol"] for item in STABLE_TOKEN_METADATA.values()}

# ETH/WETH 等价资产集合。
ETH_EQUIVALENT_CONTRACTS = {WETH_TOKEN_CONTRACT}
ETH_EQUIVALENT_SYMBOLS = {"ETH", "WETH"}

# 交易所跨 token followup 使用的主流资产集合。
MAJOR_FOLLOWUP_ASSET_CONTRACTS = ETH_EQUIVALENT_CONTRACTS | {WBTC_TOKEN_CONTRACT}
MAJOR_FOLLOWUP_ASSET_SYMBOLS = ETH_EQUIVALENT_SYMBOLS | {"WBTC"}

# 交易报价资产集合：稳定币 + ETH/WETH。
QUOTE_TOKEN_METADATA = {
    **STABLE_TOKEN_METADATA,
    **WETH_TOKEN_METADATA,
    **WBTC_TOKEN_METADATA,
}
QUOTE_TOKEN_CONTRACTS = set(QUOTE_TOKEN_METADATA.keys())
QUOTE_TOKEN_SYMBOLS = STABLE_TOKEN_SYMBOLS | ETH_EQUIVALENT_SYMBOLS
LIQUIDATION_FOCUS_BASE_CONTRACTS = set(ETH_EQUIVALENT_CONTRACTS)
LIQUIDATION_FOCUS_QUOTE_CONTRACTS = set(STABLE_TOKEN_CONTRACTS)
LIQUIDATION_FOCUS_QUOTE_SYMBOLS = set(STABLE_TOKEN_SYMBOLS)

# token 基础质量提示（可被链下数据覆盖）。
TOKEN_QUALITY_HINTS = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {
        "liquidity_usd": 1_500_000_000.0,
        "volume_24h_usd": 10_000_000_000.0,
        "holder_distribution": 0.95,
        "contract_age_days": 3000,
    },
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {
        "liquidity_usd": 1_200_000_000.0,
        "volume_24h_usd": 9_000_000_000.0,
        "holder_distribution": 0.94,
        "contract_age_days": 2500,
    },
    "0x6b175474e89094c44da98b954eedeac495271d0f": {
        "liquidity_usd": 600_000_000.0,
        "volume_24h_usd": 1_500_000_000.0,
        "holder_distribution": 0.92,
        "contract_age_days": 2200,
    },
    WETH_TOKEN_CONTRACT: {
        "liquidity_usd": 2_500_000_000.0,
        "volume_24h_usd": 12_000_000_000.0,
        "holder_distribution": 0.97,
        "contract_age_days": 2600,
    },
    WBTC_TOKEN_CONTRACT: {
        "liquidity_usd": 1_200_000_000.0,
        "volume_24h_usd": 2_500_000_000.0,
        "holder_distribution": 0.96,
        "contract_age_days": 2200,
    },
}

# 行为分析默认窗口（秒）。
BEHAVIOR_WINDOW_SEC = 3600
