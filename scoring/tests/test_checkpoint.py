"""
test_checkpoint.py – Tests for scorer state persistence.

Uses an in-memory mock that implements the CheckpointStore interface so
no real PostgreSQL connection is needed.

Covers:
- save() stores state.
- restore_all() returns what was saved.
- A second save() overwrites (upsert semantics).
- ScorerState round-trips through to_dict() / from_dict().
- A restart does NOT reset the scorer to zero (the key requirement).
"""
from __future__ import annotations

from typing import Dict, List

import pytest

from scoring.checkpoint import NullCheckpointStore
from scoring.scorer import ScorerState
from scoring.registry import ScorerRegistry
from scoring.config import ScoringConfig


# ---------------------------------------------------------------------------
# Simple in-memory checkpoint store for testing
# ---------------------------------------------------------------------------

class InMemoryCheckpointStore:
    """Mimics CheckpointStore using a plain dict."""

    def __init__(self):
        self._data: Dict[str, dict] = {}

    def ensure_schema(self) -> None:
        pass

    def save(self, state: ScorerState) -> None:
        self._data[state.symbol] = state.to_dict()

    def save_all(self, states: List[ScorerState]) -> None:
        for s in states:
            self.save(s)

    def restore_all(self) -> List[ScorerState]:
        return [ScorerState.from_dict(d) for d in self._data.values()]

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# ScorerState serialisation
# ---------------------------------------------------------------------------

class TestScorerStateSerialization:
    def test_round_trip(self):
        state = ScorerState(symbol="BTCUSDT", ewma=1.23, ewma_var=0.45, trade_count=99)
        d = state.to_dict()
        restored = ScorerState.from_dict(d)
        assert restored.symbol == "BTCUSDT"
        assert restored.ewma == pytest.approx(1.23)
        assert restored.ewma_var == pytest.approx(0.45)
        assert restored.trade_count == 99

    def test_to_dict_keys(self):
        state = ScorerState(symbol="X", ewma=0.0, ewma_var=0.0, trade_count=0)
        d = state.to_dict()
        assert set(d.keys()) == {"symbol", "ewma", "ewma_var", "trade_count"}


# ---------------------------------------------------------------------------
# InMemory store operations
# ---------------------------------------------------------------------------

class TestInMemoryStore:
    def test_save_and_restore(self):
        store = InMemoryCheckpointStore()
        state = ScorerState(symbol="ETHUSDT", ewma=0.5, ewma_var=0.1, trade_count=20)
        store.save(state)

        restored = store.restore_all()
        assert len(restored) == 1
        assert restored[0].symbol == "ETHUSDT"
        assert restored[0].ewma == pytest.approx(0.5)

    def test_upsert_overwrites(self):
        store = InMemoryCheckpointStore()
        store.save(ScorerState("BTCUSDT", ewma=1.0, ewma_var=0.1, trade_count=10))
        store.save(ScorerState("BTCUSDT", ewma=2.0, ewma_var=0.2, trade_count=20))

        restored = store.restore_all()
        assert len(restored) == 1
        assert restored[0].ewma == pytest.approx(2.0)
        assert restored[0].trade_count == 20

    def test_save_all_multiple_symbols(self):
        store = InMemoryCheckpointStore()
        states = [
            ScorerState("BTCUSDT", ewma=1.0, ewma_var=0.1, trade_count=10),
            ScorerState("ETHUSDT", ewma=-0.5, ewma_var=0.2, trade_count=5),
            ScorerState("DOGEUSDT", ewma=0.0, ewma_var=0.0, trade_count=0),
        ]
        store.save_all(states)
        restored = store.restore_all()
        symbols = {s.symbol for s in restored}
        assert symbols == {"BTCUSDT", "ETHUSDT", "DOGEUSDT"}

    def test_restore_empty(self):
        store = InMemoryCheckpointStore()
        assert store.restore_all() == []


# ---------------------------------------------------------------------------
# Restart-does-not-reset-to-zero test (the key requirement)
# ---------------------------------------------------------------------------

class TestRestartDoesNotReset:
    """
    Simulates a service restart:
    1. Run the scorer for 50 trades, saving state.
    2. Recreate the registry (simulating a restart).
    3. Restore from the store BEFORE processing any new trade.
    4. Verify the scorer continues from where it left off.
    """

    def test_state_survives_restart(self):
        store = InMemoryCheckpointStore()
        cfg = ScoringConfig(ewma_alpha=0.1, min_trades=0, default_threshold=3.0,
                            symbol_thresholds={})

        # --- "First run" ---
        reg1 = ScorerRegistry(cfg)
        scorer1 = reg1.get_or_create("BTCUSDT")
        for i in range(50):
            scorer1.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=i)

        ewma_before = scorer1.state.ewma
        trade_count_before = scorer1.state.trade_count

        # Save checkpoint.
        store.save_all(reg1.all_states())

        # --- "Restart" ---
        reg2 = ScorerRegistry(cfg)

        # Restore BEFORE processing any trade.
        for state in store.restore_all():
            reg2.restore(state)

        scorer2 = reg2.get_or_create("BTCUSDT")

        # Verify the state was carried over.
        assert scorer2.state.trade_count == trade_count_before, (
            f"trade_count should be {trade_count_before}, got {scorer2.state.trade_count}"
        )
        assert scorer2.state.ewma == pytest.approx(ewma_before), (
            f"ewma should be ~{ewma_before:.6f}, got {scorer2.state.ewma:.6f}"
        )

        # Process one more trade and verify count increments correctly.
        result = scorer2.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=50)
        assert result.trade_count == trade_count_before + 1

    def test_null_store_restore_returns_empty(self):
        store = NullCheckpointStore()
        assert store.restore_all() == []

    def test_null_store_save_is_noop(self):
        store = NullCheckpointStore()
        state = ScorerState("X", ewma=1.0, ewma_var=0.5, trade_count=10)
        store.save(state)  # Should not raise.
        assert store.restore_all() == []
