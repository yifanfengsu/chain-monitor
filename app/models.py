from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Event:
    """
    Parsing Layer 输出的统一事件模型。
    字段覆盖主流程最小必需信息，额外细节放入 metadata。
    """

    tx_hash: str
    address: str
    token: str | None
    amount: float
    side: str | None
    usd_value: float | None
    kind: str
    ts: int
    intent_type: str = "unknown_intent"
    intent_confidence: float = 0.0
    intent_stage: str = "preliminary"
    confirmation_score: float = 0.0
    intent_evidence: list[str] = field(default_factory=list)
    pricing_status: str = "unknown"
    pricing_source: str = "none"
    pricing_confidence: float = 0.0
    usd_value_available: bool = False
    usd_value_estimated: bool = False
    semantic_role: str = "unknown"
    strategy_role: str = "unknown"
    chain: str = "ethereum"
    event_id: str = ""
    case_id: str = ""
    parent_case_id: str | None = None
    followup_stage: str = ""
    followup_status: str = ""
    archive_ts: int = 0
    delivery_class: str = "drop"
    delivery_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Signal:
    """Strategy Layer 输出的统一信号模型。"""

    type: str
    confidence: float
    priority: int
    tier: str
    address: str
    token: str | None
    tx_hash: str
    usd_value: float
    reason: str
    behavior_type: str = "normal"
    address_score: float = 0.0
    token_score: float = 0.0
    quality_score: float = 0.0
    semantic: str = "unknown_flow"
    core_action: str = ""
    intent_type: str = "unknown_intent"
    intent_stage: str = "preliminary"
    confirmation_score: float = 0.0
    information_level: str = "normal"
    abnormal_ratio: float = 0.0
    pricing_confidence: float = 0.0
    cooldown_key: str = ""
    base_token_score: float = 0.0
    token_context_score: float = 0.0
    signal_id: str = ""
    event_id: str = ""
    case_id: str = ""
    parent_case_id: str | None = None
    followup_stage: str = ""
    followup_status: str = ""
    archive_ts: int = 0
    delivery_class: str = "drop"
    delivery_reason: str = ""
    context: dict[str, Any] = field(default_factory=dict)
    effective_threshold_usd: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AddressIntel:
    """运行中动态采集的候选地址画像。"""

    address: str
    first_seen_ts: int
    last_seen_ts: int
    seen_count: int = 0
    seen_tokens: set[str] = field(default_factory=set)
    top_counterparties: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    suspected_role: str = "unknown"
    role_confidence: float = 0.0
    exchange_interactions: int = 0
    router_interactions: int = 0
    protocol_interactions: int = 0
    avg_usd: float = 0.0
    max_usd: float = 0.0
    same_block_with_watch_count: int = 0
    same_token_resonance_count: int = 0
    candidate_score: float = 0.0
    candidate_status: str = "observed"


@dataclass
class BehaviorCase:
    """重点行为案例的生命周期状态。"""

    case_id: str
    watch_address: str
    token: str
    direction_bucket: str
    intent_type: str
    opened_at: int
    last_event_ts: int
    status: str = "open"
    stage: str = "initial"
    event_ids: list[str] = field(default_factory=list)
    signal_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    summary: str = ""
    followup_steps: list[str] = field(default_factory=list)
    root_tx_hash: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
