"""
test_uc01.py – UC-01: One-sided burst → score threshold crossed → alert published.

This test demonstrates the full end-to-end path:
  1. A stream of balanced trades (no alert).
  2. A sudden one-sided BUY burst starts.
  3. The toxicity score eventually crosses the threshold.
  4. An alert is published to the alerts topic.

No real Kafka or PostgreSQL is used; we inject mocks.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from scoring.checkpoint import NullCheckpointStore
from scoring.config import ScoringConfig
from scoring.registry import ScorerRegistry
from scoring.service import ScoringService


class RecordingProducer:
    """Simple producer mock that records all published alerts."""

    def __init__(self):
        self.alerts: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes):
        self.alerts.append(json.loads(value))

    def poll(self, timeout: float):
        pass

    def flush(self):
        pass


def _make_msg(symbol: str, side: str, quantity: float = 1.0) -> bytes:
    return json.dumps({
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": 65000.0,
        "trade_time_ms": 1_700_000_000_000,
        "event_id": f"uc01-{side}-{quantity}",
        "exchange": "binance",
        "trade_id": 1,
        "quote_quantity": 65000.0 * quantity,
    }).encode()


class TestUC01OneSidedBurstTriggersAlert:
    """
    UC-01 scenario: balanced → burst → alert.

    Configuration chosen to make the test deterministic:
    - alpha=0.4  (fast adaptation so the burst is felt quickly)
    - min_trades=5 (short warm-up)
    - threshold=3.0
    """

    def _make_service(self):
        cfg = ScoringConfig(
            ewma_alpha=0.4,
            min_trades=5,
            default_threshold=3.0,
            symbol_thresholds={"BTCUSDT": 3.0},
            checkpoint_every=9999,
        )
        registry = ScorerRegistry(cfg)
        producer = RecordingProducer()
        svc = ScoringService(
            config=cfg,
            registry=registry,
            checkpoint_store=NullCheckpointStore(),
            kafka_consumer=None,
            kafka_producer=producer,
        )
        svc.restore_checkpoints()
        return svc, producer

    def test_no_alert_during_balanced_phase(self):
        """Phase 1: balanced flow should not fire any alert."""
        svc, producer = self._make_service()

        for i in range(30):
            side = "BUY" if i % 2 == 0 else "SELL"
            svc.process_message(_make_msg("BTCUSDT", side))

        assert len(producer.alerts) == 0, (
            f"No alert expected during balanced phase; got {len(producer.alerts)}"
        )

    def test_alert_fires_during_buy_burst(self):
        """Phase 2: one-sided BUY burst must trigger at least one alert."""
        svc, producer = self._make_service()

        # Warm up with balanced trades (below min_trades guard).
        for i in range(10):
            side = "BUY" if i % 2 == 0 else "SELL"
            svc.process_message(_make_msg("BTCUSDT", side))

        # Now simulate a toxic BUY burst.
        for _ in range(40):
            svc.process_message(_make_msg("BTCUSDT", "BUY", quantity=2.0))

        assert len(producer.alerts) > 0, (
            "Expected at least one toxicity alert after sustained BUY burst"
        )

    def test_alert_has_correct_fields(self):
        """Alerts must carry symbol, z_score, side, and timestamp."""
        svc, producer = self._make_service()

        # Warm up.
        for i in range(10):
            side = "BUY" if i % 2 == 0 else "SELL"
            svc.process_message(_make_msg("BTCUSDT", side))

        # Burst.
        for _ in range(40):
            svc.process_message(_make_msg("BTCUSDT", "BUY", quantity=2.0))

        assert len(producer.alerts) > 0, "Need at least one alert for field check"
        alert = producer.alerts[0]

        assert alert["symbol"] == "BTCUSDT"
        assert alert["alert_type"] == "toxicity"
        assert alert["z_score"] >= 3.0, f"z_score should be ≥ threshold; got {alert['z_score']}"
        assert alert["side"] == "BUY"
        assert "trade_time_ms" in alert
        assert "quantity" in alert

    def test_alert_fires_during_sell_burst(self):
        """UC-01 is direction-agnostic: a SELL burst must also trigger."""
        svc, producer = self._make_service()

        for i in range(10):
            side = "BUY" if i % 2 == 0 else "SELL"
            svc.process_message(_make_msg("BTCUSDT", side))

        for _ in range(40):
            svc.process_message(_make_msg("BTCUSDT", "SELL", quantity=2.0))

        assert len(producer.alerts) > 0, (
            "Expected toxicity alert after sustained SELL burst"
        )
        # The most recent alert should show a negative z_score.
        last_alert = producer.alerts[-1]
        assert last_alert["z_score"] <= -3.0, (
            f"Sell burst z_score should be ≤ -threshold; got {last_alert['z_score']}"
        )
