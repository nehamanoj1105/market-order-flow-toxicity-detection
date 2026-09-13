"""
test_service.py – Tests for the ScoringService message processing loop.

No real Kafka or PostgreSQL needed.  The service is constructed with mock
dependencies.

Covers:
- Each trade is processed EXACTLY ONCE (no double-processing).
- Restore happens BEFORE processing any trade.
- Toxic trade publishes an alert.
- Non-toxic trade does NOT publish an alert.
- process_message() skips malformed JSON gracefully.
- process_message() skips messages with empty symbol.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pytest

from scoring.checkpoint import NullCheckpointStore
from scoring.config import ScoringConfig
from scoring.registry import ScorerRegistry
from scoring.scorer import ScorerState
from scoring.service import ScoringService


# ---------------------------------------------------------------------------
# Helpers / Mocks
# ---------------------------------------------------------------------------

class MockProducer:
    """Captures alert publications for assertion."""

    def __init__(self):
        self.published: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes):
        self.published.append({
            "topic": topic,
            "key": key.decode(),
            "payload": json.loads(value),
        })

    def poll(self, timeout: float):
        pass

    def flush(self):
        pass


class MockCheckpointStore:
    """Tracks save calls for assertion."""

    def __init__(self, preloaded_states: Optional[List[ScorerState]] = None):
        self._states = preloaded_states or []
        self.save_calls: List[ScorerState] = []
        self.restore_was_called = False

    def ensure_schema(self):
        pass

    def save(self, state: ScorerState):
        self.save_calls.append(state)

    def save_all(self, states: List[ScorerState]):
        self.save_calls.extend(states)

    def restore_all(self) -> List[ScorerState]:
        self.restore_was_called = True
        return list(self._states)

    def close(self):
        pass


def _make_trade(
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    quantity: float = 1.0,
    price: float = 100.0,
    trade_time_ms: int = 1000,
    event_id: str = "test-event-id",
) -> bytes:
    return json.dumps({
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "trade_time_ms": trade_time_ms,
        "event_id": event_id,
        "exchange": "binance",
        "trade_id": 12345,
        "quote_quantity": price * quantity,
    }).encode()


def _make_service(
    symbol_thresholds=None,
    preloaded_states=None,
    producer=None,
    checkpoint_every: int = 9999,
    min_trades: int = 0,
    alpha: float = 0.3,
):
    cfg = ScoringConfig(
        ewma_alpha=alpha,
        min_trades=min_trades,
        default_threshold=3.0,
        symbol_thresholds=symbol_thresholds or {"BTCUSDT": 3.0, "ETHUSDT": 3.0},
        checkpoint_every=checkpoint_every,
    )
    registry = ScorerRegistry(cfg)
    store = MockCheckpointStore(preloaded_states=preloaded_states)
    return ScoringService(
        config=cfg,
        registry=registry,
        checkpoint_store=store,
        kafka_consumer=None,
        kafka_producer=producer,
    ), store


# ---------------------------------------------------------------------------
# Test: Exactly-once processing
# ---------------------------------------------------------------------------

class TestExactlyOnce:
    def test_each_trade_processed_once(self):
        """process_message() must increment trade_count by exactly 1 per call."""
        svc, _ = _make_service()
        svc.restore_checkpoints()

        msg = _make_trade(symbol="BTCUSDT", side="BUY", quantity=1.0)
        svc.process_message(msg)

        scorer = svc._registry.get("BTCUSDT")
        assert scorer.state.trade_count == 1, (
            f"Expected exactly 1 processed trade; got {scorer.state.trade_count}"
        )

    def test_ten_messages_ten_increments(self):
        svc, _ = _make_service()
        svc.restore_checkpoints()

        for _ in range(10):
            svc.process_message(_make_trade(symbol="BTCUSDT", side="BUY"))

        scorer = svc._registry.get("BTCUSDT")
        assert scorer.state.trade_count == 10

    def test_restore_then_process_no_double_count(self):
        """
        If state was restored (trade_count=50), processing 1 new trade should
        yield trade_count=51, not some other value.

        This directly tests that process_trade() is NOT called before restore
        and then again after restore.
        """
        preloaded = [ScorerState("BTCUSDT", ewma=0.5, ewma_var=0.1, trade_count=50)]
        svc, _ = _make_service(preloaded_states=preloaded)

        # Restore happens here (simulating startup).
        svc.restore_checkpoints()

        # Process ONE new trade.
        svc.process_message(_make_trade(symbol="BTCUSDT", side="BUY", quantity=1.0))

        scorer = svc._registry.get("BTCUSDT")
        assert scorer.state.trade_count == 51, (
            f"Expected 51 (50 restored + 1 new), got {scorer.state.trade_count}"
        )


# ---------------------------------------------------------------------------
# Test: Restore is called before processing
# ---------------------------------------------------------------------------

class TestRestoreBeforeProcessing:
    def test_restore_was_called(self):
        svc, store = _make_service()
        svc.restore_checkpoints()
        assert store.restore_was_called

    def test_restored_state_affects_scoring(self):
        """
        If we restore a state with ewma already pushed toward BUY, one more
        trade should still be toxic (threshold not reset).

        We restore a state representing a scorer that has already converged on
        a one-sided BUY signal: ewma=1.0, ewma_var=0.001 (near-zero variance).
        z = ewma / sqrt(ewma_var) = 1.0 / 0.0316 ≈ 31.6  >>  threshold=3.0.

        A new BUY(qty=1) reinforces the same direction, so the EWMA stays at
        ~1.0 and variance stays near zero.  The scorer must remain toxic.
        """
        # Converged steady BUY state.
        preloaded = [ScorerState("BTCUSDT", ewma=1.0, ewma_var=0.001, trade_count=50)]
        svc, _ = _make_service(preloaded_states=preloaded, min_trades=0)
        svc.restore_checkpoints()

        result = svc.process_message(_make_trade(symbol="BTCUSDT", side="BUY", quantity=1.0))
        assert result is not None
        assert result.is_toxic, (
            f"Expected toxic after restoring converged BUY state; z={result.z_score:.3f}"
        )


# ---------------------------------------------------------------------------
# Test: Alert publishing
# ---------------------------------------------------------------------------

class TestAlertPublishing:
    def test_toxic_trade_publishes_alert(self):
        """A strong buy burst should eventually cause an alert to be published."""
        producer = MockProducer()
        svc, _ = _make_service(producer=producer, alpha=0.5, min_trades=5)
        svc.restore_checkpoints()

        for i in range(50):
            svc.process_message(_make_trade(symbol="BTCUSDT", side="BUY", quantity=1.0))

        assert len(producer.published) > 0, "Expected at least one alert to be published"
        alert = producer.published[0]["payload"]
        assert alert["alert_type"] == "toxicity"
        assert alert["symbol"] == "BTCUSDT"
        assert "z_score" in alert
        assert "trade_time_ms" in alert

    def test_non_toxic_trade_no_alert(self):
        """Balanced flow should never publish an alert."""
        producer = MockProducer()
        # Use min_trades=5 so the warm-up guard prevents spurious early alerts.
        svc, _ = _make_service(producer=producer, alpha=0.1, min_trades=5)
        svc.restore_checkpoints()

        for i in range(200):
            side = "BUY" if i % 2 == 0 else "SELL"
            svc.process_message(_make_trade(symbol="BTCUSDT", side=side, quantity=1.0))

        assert len(producer.published) == 0, (
            f"Expected 0 alerts for balanced flow; got {len(producer.published)}"
        )


# ---------------------------------------------------------------------------
# Test: Malformed messages are skipped gracefully
# ---------------------------------------------------------------------------

class TestMalformedMessages:
    def test_invalid_json_skipped(self):
        svc, _ = _make_service()
        svc.restore_checkpoints()
        result = svc.process_message(b"this is not json")
        assert result is None

    def test_empty_symbol_skipped(self):
        svc, _ = _make_service()
        svc.restore_checkpoints()
        msg = json.dumps({"symbol": "", "side": "BUY", "quantity": 1.0,
                          "price": 100.0, "trade_time_ms": 0}).encode()
        result = svc.process_message(msg)
        assert result is None

    def test_missing_symbol_skipped(self):
        svc, _ = _make_service()
        svc.restore_checkpoints()
        msg = json.dumps({"side": "BUY", "quantity": 1.0,
                          "price": 100.0, "trade_time_ms": 0}).encode()
        result = svc.process_message(msg)
        assert result is None
