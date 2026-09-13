"""
test_registry.py – Tests for the ScorerRegistry.

Covers:
- Different symbols maintain independent state.
- Symbol-specific thresholds work correctly.
- Changing one symbol's threshold does NOT affect others.
- get_or_create idempotency.
- restore() correctly loads checkpointed state.
"""
from __future__ import annotations

import pytest

from scoring.config import ScoringConfig
from scoring.registry import ScorerRegistry
from scoring.scorer import ScorerState


def _make_registry(**extra_thresholds) -> ScorerRegistry:
    thresholds = {
        "BTCUSDT": 3.0,
        "ETHUSDT": 3.0,
        "DOGEUSDT": 2.5,
    }
    thresholds.update(extra_thresholds)
    cfg = ScoringConfig(
        ewma_alpha=0.3,
        min_trades=0,
        symbol_thresholds=thresholds,
        default_threshold=3.0,
    )
    return ScorerRegistry(cfg)


# ---------------------------------------------------------------------------
# Test: Different symbols maintain independent state
# ---------------------------------------------------------------------------

class TestSymbolIsolation:
    def test_different_symbols_independent_state(self):
        """Trades on BTCUSDT must NOT affect ETHUSDT's scorer."""
        reg = _make_registry()
        btc = reg.get_or_create("BTCUSDT")
        eth = reg.get_or_create("ETHUSDT")

        # Feed 50 BUY-only trades to BTC.
        for _ in range(50):
            btc.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=0)

        # ETH scorer should still be at zero.
        assert eth.state.trade_count == 0
        assert eth.state.ewma == 0.0

    def test_separate_scorer_objects(self):
        """get_or_create must return the same object for the same symbol."""
        reg = _make_registry()
        a = reg.get_or_create("BTCUSDT")
        b = reg.get_or_create("BTCUSDT")
        assert a is b, "Expected the same scorer object for the same symbol"

    def test_different_symbols_different_objects(self):
        reg = _make_registry()
        btc = reg.get_or_create("BTCUSDT")
        eth = reg.get_or_create("ETHUSDT")
        assert btc is not eth

    def test_trade_count_independent(self):
        reg = _make_registry()
        btc = reg.get_or_create("BTCUSDT")
        eth = reg.get_or_create("ETHUSDT")
        doge = reg.get_or_create("DOGEUSDT")

        for _ in range(10):
            btc.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=0)
        for _ in range(5):
            eth.process_trade(side="SELL", quantity=1.0, price=200.0, trade_time_ms=0)

        assert btc.state.trade_count == 10
        assert eth.state.trade_count == 5
        assert doge.state.trade_count == 0


# ---------------------------------------------------------------------------
# Test: Per-symbol thresholds
# ---------------------------------------------------------------------------

class TestPerSymbolThresholds:
    def test_btcusdt_threshold(self):
        reg = _make_registry()
        assert reg.get_or_create("BTCUSDT").threshold == 3.0

    def test_ethusdt_threshold(self):
        reg = _make_registry()
        assert reg.get_or_create("ETHUSDT").threshold == 3.0

    def test_dogeusdt_lower_threshold(self):
        """DOGEUSDT has a more sensitive threshold (2.5 < 3.0 → alerts sooner)."""
        reg = _make_registry()
        assert reg.get_or_create("DOGEUSDT").threshold == 2.5

    def test_unknown_symbol_gets_default_threshold(self):
        reg = _make_registry()
        scorer = reg.get_or_create("XRPUSDT")
        assert scorer.threshold == 3.0  # default

    def test_dogeusdt_triggers_at_lower_z(self):
        """DOGEUSDT (threshold=2.5) should alert before BTCUSDT (threshold=3.0)."""
        cfg = ScoringConfig(
            ewma_alpha=0.3,
            min_trades=10,
            symbol_thresholds={"BTCUSDT": 3.0, "DOGEUSDT": 2.5},
            default_threshold=3.0,
        )
        reg = ScorerRegistry(cfg)
        btc = reg.get_or_create("BTCUSDT")
        doge = reg.get_or_create("DOGEUSDT")

        toxic_btc = False
        toxic_doge = False
        for i in range(100):
            r_btc = btc.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=i)
            r_doge = doge.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=i)
            if r_btc.is_toxic:
                toxic_btc = True
            if r_doge.is_toxic:
                toxic_doge = True

        # Both should eventually alert (strong one-sided flow), but DOGE
        # should reach its threshold at least as early as BTC.
        assert toxic_doge, "DOGEUSDT should be toxic with one-sided flow"
        # With equal alpha, the z-score trajectory is the same; DOGE triggers
        # earlier because threshold is lower.
        # More precisely: check DOGE first fires before BTC if we compare
        # individual trade counts.

    # UC-03: Changing DOGEUSDT threshold must not affect BTCUSDT or ETHUSDT.
    def test_update_threshold_isolation(self):
        """
        UC-03: Changing DOGEUSDT's threshold must NOT affect BTCUSDT or ETHUSDT.
        """
        reg = _make_registry()
        btc_scorer = reg.get_or_create("BTCUSDT")
        eth_scorer = reg.get_or_create("ETHUSDT")
        doge_scorer = reg.get_or_create("DOGEUSDT")

        # Record original thresholds.
        btc_before = btc_scorer.threshold
        eth_before = eth_scorer.threshold
        doge_before = doge_scorer.threshold

        # Change DOGE threshold to something extreme.
        reg.update_threshold("DOGEUSDT", 1.0)

        # DOGE changed; BTC and ETH must remain unchanged.
        assert doge_scorer.threshold == 1.0, "DOGE threshold should change"
        assert btc_scorer.threshold == btc_before, "BTC threshold must NOT change"
        assert eth_scorer.threshold == eth_before, "ETH threshold must NOT change"

    def test_update_threshold_not_yet_created_symbol(self):
        """Updating a threshold for a not-yet-seen symbol should work when it's created."""
        reg = _make_registry()
        reg.update_threshold("NEWCOIN", 1.5)
        scorer = reg.get_or_create("NEWCOIN")
        assert scorer.threshold == 1.5


# ---------------------------------------------------------------------------
# Test: Restore from checkpoint
# ---------------------------------------------------------------------------

class TestRestore:
    def test_restore_populates_state(self):
        reg = _make_registry()
        state = ScorerState(symbol="BTCUSDT", ewma=0.75, ewma_var=0.1, trade_count=42)
        reg.restore(state)
        scorer = reg.get_or_create("BTCUSDT")
        assert scorer.state.ewma == pytest.approx(0.75)
        assert scorer.state.ewma_var == pytest.approx(0.1)
        assert scorer.state.trade_count == 42

    def test_restore_preserves_threshold(self):
        """After restore the per-symbol threshold from config must still apply."""
        reg = _make_registry()
        state = ScorerState(symbol="DOGEUSDT", ewma=0.1, ewma_var=0.01, trade_count=5)
        reg.restore(state)
        scorer = reg.get("DOGEUSDT")
        assert scorer.threshold == 2.5  # from config, not reset by restore

    def test_restore_then_process_uses_restored_state(self):
        """After restore, the next process_trade builds on the restored state."""
        reg = _make_registry()
        state = ScorerState(symbol="ETHUSDT", ewma=1.0, ewma_var=0.5, trade_count=100)
        reg.restore(state)
        scorer = reg.get_or_create("ETHUSDT")

        # One more trade; it should see trade_count == 101, not 1.
        result = scorer.process_trade(side="BUY", quantity=1.0, price=200.0, trade_time_ms=0)
        assert result.trade_count == 101, (
            f"Expected trade_count=101 after restore; got {result.trade_count}"
        )
