"""
test_uc03.py – UC-03: Per-symbol threshold isolation.

Demonstrates:
  1. Setting a symbol-specific threshold changes only that symbol's sensitivity.
  2. Other symbols are completely unaffected.
  3. DOGEUSDT (threshold=2.5) alerts before BTCUSDT (threshold=3.0) given the
     same one-sided flow.

No real Kafka or PostgreSQL required.
"""
from __future__ import annotations

import json
from typing import Dict, List

import pytest

from scoring.checkpoint import NullCheckpointStore
from scoring.config import ScoringConfig
from scoring.registry import ScorerRegistry
from scoring.service import ScoringService


class RecordingProducer:
    def __init__(self):
        self.alerts: List[dict] = []

    def produce(self, topic, key, value):
        self.alerts.append(json.loads(value))

    def poll(self, _):
        pass

    def flush(self):
        pass

    def alerts_for(self, symbol: str) -> List[dict]:
        return [a for a in self.alerts if a["symbol"] == symbol]


def _msg(symbol: str, side: str, qty: float = 1.0) -> bytes:
    return json.dumps({
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "price": 1.0,
        "trade_time_ms": 1_700_000_000_000,
        "event_id": "uc03",
        "exchange": "binance",
        "trade_id": 0,
        "quote_quantity": qty,
    }).encode()


class TestUC03SymbolThresholdIsolation:
    """
    UC-03: Changing one symbol's threshold must not affect others.
    """

    def _make_service(self, thresholds: Dict[str, float], producer=None):
        cfg = ScoringConfig(
            ewma_alpha=0.4,
            min_trades=5,
            default_threshold=3.0,
            symbol_thresholds=thresholds,
            checkpoint_every=9999,
        )
        registry = ScorerRegistry(cfg)
        svc = ScoringService(
            config=cfg,
            registry=registry,
            checkpoint_store=NullCheckpointStore(),
            kafka_consumer=None,
            kafka_producer=producer,
        )
        svc.restore_checkpoints()
        return svc, registry

    # ------------------------------------------------------------------ #
    # Part A: Registry-level threshold isolation (no I/O needed)          #
    # ------------------------------------------------------------------ #

    def test_changing_doge_threshold_does_not_affect_btc(self):
        """
        UC-03 core: mutating DOGEUSDT's threshold in the registry must leave
        BTCUSDT and ETHUSDT thresholds unchanged.
        """
        cfg = ScoringConfig(
            ewma_alpha=0.1,
            min_trades=0,
            symbol_thresholds={"BTCUSDT": 3.0, "ETHUSDT": 3.0, "DOGEUSDT": 2.5},
            default_threshold=3.0,
        )
        reg = ScorerRegistry(cfg)
        btc = reg.get_or_create("BTCUSDT")
        eth = reg.get_or_create("ETHUSDT")
        doge = reg.get_or_create("DOGEUSDT")

        # Sanity: initial thresholds.
        assert btc.threshold == 3.0
        assert eth.threshold == 3.0
        assert doge.threshold == 2.5

        # Change DOGE to a very tight threshold.
        reg.update_threshold("DOGEUSDT", 0.5)

        assert doge.threshold == 0.5, "DOGE threshold should update"
        assert btc.threshold == 3.0, "BTC threshold must NOT change"
        assert eth.threshold == 3.0, "ETH threshold must NOT change"

    def test_btc_threshold_change_does_not_affect_eth_or_doge(self):
        cfg = ScoringConfig(
            ewma_alpha=0.1,
            min_trades=0,
            symbol_thresholds={"BTCUSDT": 3.0, "ETHUSDT": 3.0, "DOGEUSDT": 2.5},
            default_threshold=3.0,
        )
        reg = ScorerRegistry(cfg)
        btc = reg.get_or_create("BTCUSDT")
        eth = reg.get_or_create("ETHUSDT")
        doge = reg.get_or_create("DOGEUSDT")

        reg.update_threshold("BTCUSDT", 5.0)

        assert btc.threshold == 5.0
        assert eth.threshold == 3.0
        assert doge.threshold == 2.5

    # ------------------------------------------------------------------ #
    # Part B: Sensitivity demonstrates earlier alert with lower threshold  #
    # ------------------------------------------------------------------ #

    def test_lower_threshold_triggers_earlier(self):
        """
        DOGEUSDT with threshold=2.0 should alert before BTCUSDT (threshold=3.0)
        given the same one-sided flow.

        Because we use the same alpha and feed the same trade stream, the
        z_score trajectory is identical.  The only difference is the threshold.
        """
        cfg = ScoringConfig(
            ewma_alpha=0.4,
            min_trades=5,
            symbol_thresholds={"BTCUSDT": 3.0, "DOGEUSDT": 2.0},
            default_threshold=3.0,
        )
        reg = ScorerRegistry(cfg)
        btc = reg.get_or_create("BTCUSDT")
        doge = reg.get_or_create("DOGEUSDT")

        btc_first_alert = None
        doge_first_alert = None

        for i in range(100):
            r_btc = btc.process_trade(side="BUY", quantity=1.0, price=1.0, trade_time_ms=i)
            r_doge = doge.process_trade(side="BUY", quantity=1.0, price=1.0, trade_time_ms=i)

            if r_btc.is_toxic and btc_first_alert is None:
                btc_first_alert = i
            if r_doge.is_toxic and doge_first_alert is None:
                doge_first_alert = i

        assert doge_first_alert is not None, "DOGE should alert with one-sided flow"
        assert btc_first_alert is not None, "BTC should alert with one-sided flow"
        assert doge_first_alert <= btc_first_alert, (
            f"DOGE (threshold=2.0) should alert no later than BTC (threshold=3.0);"
            f" doge_first={doge_first_alert}, btc_first={btc_first_alert}"
        )

    # ------------------------------------------------------------------ #
    # Part C: Full service pipeline with two symbols                       #
    # ------------------------------------------------------------------ #

    def test_service_alerts_use_per_symbol_threshold(self):
        """
        Feed identical one-sided flow to BTCUSDT (threshold=3.0) and DOGEUSDT
        (threshold=2.5) through the service.  Both should eventually alert.
        The count of alerts for DOGE should be >= count for BTC (looser test).
        """
        producer = RecordingProducer()
        cfg = ScoringConfig(
            ewma_alpha=0.4,
            min_trades=5,
            symbol_thresholds={"BTCUSDT": 3.0, "DOGEUSDT": 2.5},
            default_threshold=3.0,
            checkpoint_every=9999,
        )
        reg = ScorerRegistry(cfg)
        svc = ScoringService(
            config=cfg,
            registry=reg,
            checkpoint_store=NullCheckpointStore(),
            kafka_consumer=None,
            kafka_producer=producer,
        )
        svc.restore_checkpoints()

        for _ in range(60):
            svc.process_message(_msg("BTCUSDT", "BUY"))
            svc.process_message(_msg("DOGEUSDT", "BUY"))

        btc_alerts = producer.alerts_for("BTCUSDT")
        doge_alerts = producer.alerts_for("DOGEUSDT")

        assert len(doge_alerts) > 0, "DOGEUSDT should have alerts"
        assert len(btc_alerts) > 0, "BTCUSDT should have alerts"
        assert len(doge_alerts) >= len(btc_alerts), (
            "DOGE (lower threshold) should alert at least as often as BTC"
        )

    def test_increasing_threshold_reduces_alerts(self):
        """
        After raising a symbol's threshold to an unreachably high value,
        no further alerts fire for that symbol even with continued one-sided flow.

        Note: with a constant one-sided signal, EWMA variance converges toward 0,
        so z_score can become very large (ewma / sqrt(epsilon)).  We therefore use
        float('inf') as the blocking threshold to guarantee no alert fires.
        """
        import math as _math

        producer = RecordingProducer()
        cfg = ScoringConfig(
            ewma_alpha=0.4,
            min_trades=5,
            symbol_thresholds={"BTCUSDT": 3.0},
            default_threshold=3.0,
            checkpoint_every=9999,
        )
        reg = ScorerRegistry(cfg)
        svc = ScoringService(
            config=cfg,
            registry=reg,
            checkpoint_store=NullCheckpointStore(),
            kafka_consumer=None,
            kafka_producer=producer,
        )
        svc.restore_checkpoints()

        # Phase 1: BUY burst → should produce alerts.
        for _ in range(60):
            svc.process_message(_msg("BTCUSDT", "BUY"))
        alerts_phase1 = len(producer.alerts_for("BTCUSDT"))
        assert alerts_phase1 > 0, "Phase 1 should produce at least one alert"

        # Phase 2: raise threshold to infinity → no alert can ever fire.
        reg.update_threshold("BTCUSDT", _math.inf)
        producer.alerts.clear()

        # Continue the burst – no new alerts should fire.
        for _ in range(30):
            svc.process_message(_msg("BTCUSDT", "BUY"))
        alerts_phase2 = len(producer.alerts_for("BTCUSDT"))
        assert alerts_phase2 == 0, (
            f"After raising threshold to inf, expected 0 alerts; got {alerts_phase2}"
        )
