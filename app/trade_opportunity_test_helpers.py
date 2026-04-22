import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal


class StubStateManager:
    def __init__(self, records=None, outcome_records=None, recent_signal_records=None) -> None:
        self._records = list(records or [])
        self._outcome_records = dict(outcome_records or {})
        self._recent_signal_records = list(recent_signal_records or [])

    def get_recent_lp_outcome_records(self, limit: int = 500):
        return list(self._records)[-limit:]

    def get_lp_outcome_record(self, record_id: str):
        return dict(self._outcome_records.get(record_id) or {})

    def get_recent_signal_records(
        self,
        *,
        asset_symbol: str | None = None,
        since_ts: int | None = None,
        until_ts: int | None = None,
        limit: int = 120,
    ):
        rows = list(self._recent_signal_records)
        if asset_symbol:
            target = str(asset_symbol or "").strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("asset_symbol") or row.get("asset") or "").strip().upper() == target
            ]
        if since_ts is not None:
            rows = [
                row
                for row in rows
                if int(row.get("archive_ts") or row.get("timestamp") or row.get("ts") or 0) >= int(since_ts)
            ]
        if until_ts is not None:
            rows = [
                row
                for row in rows
                if int(row.get("archive_ts") or row.get("timestamp") or row.get("ts") or 0) <= int(until_ts)
            ]
        return rows[-limit:]


def make_outcome_row(
    *,
    direction_bucket: str = "buy_pressure",
    move_60s: float | None = 0.005,
    adverse: bool | None = False,
    status: str = "completed",
    source: str = "okx_mark",
) -> dict:
    return {
        "direction_bucket": direction_bucket,
        "lp_alert_stage": "confirm",
        "direction_adjusted_move_after_60s": move_60s,
        "adverse_by_direction_60s": adverse,
        "outcome_price_source": source,
        "outcome_windows": {
            "60s": {
                "status": status,
                "direction_adjusted_move_after": move_60s,
                "adverse_by_direction": adverse,
                "price_source": source,
                "followthrough_positive": bool(move_60s is not None and move_60s > 0.002),
            }
        },
    }


def make_outcome_record(
    *,
    record_id: str,
    move_30s: float | None = 0.003,
    move_60s: float | None = 0.005,
    move_300s: float | None = 0.007,
    adverse_30s: bool | None = False,
    adverse_60s: bool | None = False,
    adverse_300s: bool | None = False,
    status_30s: str = "completed",
    status_60s: str = "completed",
    status_300s: str = "completed",
    source: str = "okx_mark",
) -> dict:
    def _window(status: str, move: float | None, adverse: bool | None) -> dict:
        return {
            "status": status,
            "direction_adjusted_move_after": move,
            "adverse_by_direction": adverse,
            "followthrough_positive": bool(move is not None and move > 0.002),
            "price_source": source,
        }

    return {
        "record_id": record_id,
        "outcome_price_source": source,
        "outcome_windows": {
            "30s": _window(status_30s, move_30s, adverse_30s),
            "60s": _window(status_60s, move_60s, adverse_60s),
            "300s": _window(status_300s, move_300s, adverse_300s),
        },
    }


def make_recent_signal_row(
    *,
    asset_symbol: str = "ETH",
    ts: int = 1_710_000_000,
    operational_intent_key: str = "smart_money_entry_execution",
    confidence: float = 0.88,
    confirmation_score: float = 0.84,
    intent_stage: str = "confirmed",
    strategy_role: str = "smart_money_wallet",
    pair_label: str | None = None,
    liquidation_side: str = "",
    liquidation_stage: str = "",
) -> dict:
    pair = pair_label or f"{asset_symbol}/USDC"
    stage = liquidation_stage
    if not stage:
        if operational_intent_key == "liquidation_sell_pressure_release":
            stage = "execution"
        elif operational_intent_key == "liquidation_risk_building":
            stage = "risk"
        else:
            stage = "none"
    context = {
        "asset_symbol": asset_symbol,
        "pair_label": pair,
        "operational_intent_key": operational_intent_key,
        "operational_intent_family": operational_intent_key,
        "operational_intent_confidence": confidence,
        "confirmation_score": confirmation_score,
        "intent_stage": intent_stage,
        "strategy_role": strategy_role,
        "liquidation_side": liquidation_side,
        "liquidation_stage": stage,
        "timestamp": ts,
        "archive_ts": ts,
    }
    return {
        "signal_id": f"recent:{operational_intent_key}:{ts}",
        "asset_symbol": asset_symbol,
        "asset": asset_symbol,
        "pair_label": pair,
        "timestamp": ts,
        "archive_ts": ts,
        "operational_intent_key": operational_intent_key,
        "operational_intent_family": operational_intent_key,
        "operational_intent_confidence": confidence,
        "confirmation_score": confirmation_score,
        "intent_stage": intent_stage,
        "strategy_role": strategy_role,
        "liquidation_side": liquidation_side,
        "liquidation_stage": stage,
        "signal": {
            "context": dict(context),
            "metadata": dict(context),
        },
        "event": {
            "metadata": dict(context),
        },
    }


def make_event(
    *,
    intent_type: str = "pool_buy_pressure",
    ts: int = 1_710_000_000,
    asset_symbol: str = "ETH",
) -> Event:
    return Event(
        tx_hash=f"0xtradeopp{intent_type}{ts}",
        address="0xtradeopppool",
        token=asset_symbol,
        amount=1.0,
        side="买入" if intent_type == "pool_buy_pressure" else "卖出",
        usd_value=72_000.0,
        kind="swap",
        ts=ts,
        intent_type=intent_type,
        intent_stage="confirmed",
        intent_confidence=0.90,
        confirmation_score=0.88,
        pricing_status="exact",
        pricing_confidence=0.96,
        usd_value_available=True,
        strategy_role="lp_pool",
        metadata={
            "token_symbol": asset_symbol,
            "raw": {
                "lp_context": {
                    "pair_label": f"{asset_symbol}/USDC",
                    "pool_label": f"{asset_symbol}/USDC",
                    "base_token_symbol": asset_symbol,
                    "quote_token_symbol": "USDC",
                }
            },
        },
    )


def make_signal(event: Event, **overrides) -> Signal:
    is_buy = event.intent_type == "pool_buy_pressure"
    signal = Signal(
        type=event.intent_type,
        confidence=0.92,
        priority=1,
        tier="Tier 2",
        address=event.address,
        token=event.token,
        tx_hash=event.tx_hash,
        usd_value=float(event.usd_value or 0.0),
        reason="trade_opportunity_test",
        quality_score=0.90,
        semantic="pool_trade_pressure",
        intent_type=event.intent_type,
        intent_stage=event.intent_stage,
        confirmation_score=event.confirmation_score,
        pricing_confidence=event.pricing_confidence,
        delivery_class="observe",
        delivery_reason="unit_test",
        signal_id=f"sig:{event.intent_type}:{event.ts}",
    )
    outcome_record_id = f"outcome:{event.intent_type}:{event.ts}"
    context = {
        "user_tier": "research",
        "message_variant": "lp_directional",
        "lp_event": True,
        "pair_label": f"{event.token}/USDC",
        "asset_case_label": event.token,
        "asset_symbol": event.token,
        "asset_case_id": f"asset_case:{event.token}:trade_opportunity",
        "asset_case_direction": "buy_pressure" if is_buy else "sell_pressure",
        "asset_case_supporting_pair_count": 3,
        "asset_case_multi_pool": True,
        "lp_alert_stage": "confirm",
        "lp_confirm_scope": "broader_confirm",
        "lp_confirm_quality": "clean_confirm",
        "lp_absorption_context": "broader_buy_pressure_confirmed" if is_buy else "broader_sell_pressure_confirmed",
        "lp_broader_alignment": "confirmed",
        "lp_sweep_phase": "",
        "lp_sweep_continuation_score": 0.82,
        "lp_sweep_followthrough_score": 0.79,
        "lp_multi_pool_resonance": 3,
        "lp_same_pool_continuity": 2,
        "market_context_source": "live_public",
        "market_context_venue": "okx_perp",
        "alert_relative_timing": "confirming",
        "market_move_before_alert_30s": 0.002 if is_buy else -0.002,
        "market_move_before_alert_60s": 0.003 if is_buy else -0.003,
        "market_move_after_alert_60s": 0.004 if is_buy else -0.004,
        "basis_bps": 4.0 if is_buy else -4.0,
        "mark_index_spread_bps": 1.2 if is_buy else -1.2,
        "pool_quality_score": 0.74,
        "pair_quality_score": 0.76,
        "asset_case_quality_score": 0.81,
        "quality_score_brief": "历史传导较强",
        "trade_action_key": "LONG_CHASE_ALLOWED" if is_buy else "SHORT_CHASE_ALLOWED",
        "trade_action_label": "可顺势追多" if is_buy else "可顺势追空",
        "trade_action_direction": "long" if is_buy else "short",
        "trade_action_reason": "链上方向扩散",
        "trade_action_required_confirmation": "继续看 broader confirm",
        "trade_action_invalidated_by": "反向强信号",
        "trade_action_confidence": 0.90,
        "trade_action_debug": {"strength_score": 0.88},
        "asset_market_state_key": "LONG_CANDIDATE" if is_buy else "SHORT_CANDIDATE",
        "asset_market_state_label": "偏多候选" if is_buy else "偏空候选",
        "asset_market_state_reason": "更广方向确认已出现，但后验尚未稳定达标。",
        "asset_market_state_required_confirmation": "等待后验达标",
        "asset_market_state_invalidated_by": "反向强信号 / broader alignment 消失",
        "lp_outcome_record": {"record_id": outcome_record_id, "outcome_price_source": "okx_mark"},
        "outcome_tracking_key": outcome_record_id,
        "outcome_price_source": "okx_mark",
    }
    context.update(overrides)
    signal.context.update(context)
    signal.metadata.update(context)
    event.metadata.update(context)
    return signal
