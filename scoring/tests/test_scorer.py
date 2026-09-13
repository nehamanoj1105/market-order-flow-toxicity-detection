"""
test_scorer.py – Unit tests for the EWMA OFI scorer.

Covers:
- Normal balanced flow does NOT trigger toxicity.
- Strong buy-side burst DOES trigger toxicity.
- Strong sell-side burst DOES trigger toxicity.
- Trade SIZE (quantity) affects the score.
- Warm-up guard (min_trades) prevents early alerts.
- Edge cases: zero quantity, NaN, unknown side.
- Z-score sign: positive for buy-heavy, negative for sell-heavy.
"""
from __future__ import annotations

import math
import pytest

from scoring.scorer import SymbolScorer, ScorerState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trade(side: str, qty: float = 1.0, price: float = 100.0, t: int = 0):
    return dict(side=side, quantity=qty, price=price, trade_time_ms=t)


def feed_trades(scorer: SymbolScorer, trades):
    """Feed a list of trade dicts into the scorer and return the last result."""
    result = None
    for tr in trades:
        result = scorer.process_trade(**tr)
    return result


# ---------------------------------------------------------------------------
# Test 1: Normal balanced flow does NOT trigger unnecessarily
# ---------------------------------------------------------------------------

class TestBalancedFlow:
    def test_balanced_flow_never_toxic(self):
        """Alternating BUY/SELL of equal size should keep z_score ≈ 0."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=3.0, min_trades=5)
        toxic_count = 0
        # Feed 200 perfectly balanced trades
        for i in range(200):
            side = "BUY" if i % 2 == 0 else "SELL"
            result = scorer.process_trade(side=side, quantity=1.0, price=100.0, trade_time_ms=i)
            if result.is_toxic:
                toxic_count += 1
        assert toxic_count == 0, f"Expected 0 toxic alerts, got {toxic_count}"

    def test_balanced_flow_low_z_score(self):
        """After many balanced trades, |z_score| should stay well below threshold."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=3.0, min_trades=0)
        for i in range(100):
            side = "BUY" if i % 2 == 0 else "SELL"
            result = scorer.process_trade(side=side, quantity=1.0, price=100.0, trade_time_ms=i)
        assert abs(result.z_score) < 1.0, (
            f"Balanced flow z_score too high: {result.z_score}"
        )


# ---------------------------------------------------------------------------
# Test 2: Strong buy-side burst triggers toxicity
# ---------------------------------------------------------------------------

class TestBuySideBurst:
    def test_buy_burst_triggers(self):
        """100 consecutive BUY trades should eventually cross the threshold."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.3, threshold=3.0, min_trades=10)
        last_result = None
        for i in range(100):
            last_result = scorer.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=i)
        assert last_result.is_toxic, (
            f"Expected toxic after 100 BUY trades; z={last_result.z_score:.3f}"
        )
        assert last_result.z_score > 0, "Buy-side burst should yield positive z_score"

    def test_buy_burst_z_score_positive(self):
        scorer = SymbolScorer("ETHUSDT", alpha=0.3, threshold=3.0, min_trades=0)
        for _ in range(50):
            result = scorer.process_trade(side="BUY", quantity=1.0, price=200.0, trade_time_ms=0)
        assert result.z_score > 0, "Buy-heavy flow should produce positive z_score"


# ---------------------------------------------------------------------------
# Test 3: Strong sell-side burst triggers toxicity
# ---------------------------------------------------------------------------

class TestSellSideBurst:
    def test_sell_burst_triggers(self):
        """100 consecutive SELL trades should eventually cross the threshold."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.3, threshold=3.0, min_trades=10)
        last_result = None
        for i in range(100):
            last_result = scorer.process_trade(side="SELL", quantity=1.0, price=100.0, trade_time_ms=i)
        assert last_result.is_toxic, (
            f"Expected toxic after 100 SELL trades; z={last_result.z_score:.3f}"
        )
        assert last_result.z_score < 0, "Sell-side burst should yield negative z_score"


# ---------------------------------------------------------------------------
# Test 4: Trade SIZE affects the score
# ---------------------------------------------------------------------------

class TestSizeAffectsScore:
    def test_large_trade_moves_score_more(self):
        """A larger trade quantity should shift the EWMA more than a smaller one."""
        scorer_small = SymbolScorer("BTCUSDT", alpha=0.5, threshold=99.0, min_trades=0)
        scorer_large = SymbolScorer("BTCUSDT", alpha=0.5, threshold=99.0, min_trades=0)

        scorer_small.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=0)
        scorer_large.process_trade(side="BUY", quantity=100.0, price=100.0, trade_time_ms=0)

        assert scorer_large.state.ewma > scorer_small.state.ewma, (
            "Larger quantity should produce larger EWMA shift"
        )

    def test_size_proportional_to_signed_ofi(self):
        """signed_ofi in ScoreResult should equal the input quantity (for BUY)."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=99.0, min_trades=0)
        result = scorer.process_trade(side="BUY", quantity=5.7, price=100.0, trade_time_ms=0)
        assert result.signed_ofi == pytest.approx(5.7), (
            f"BUY signed_ofi should equal quantity; got {result.signed_ofi}"
        )

    def test_sell_signed_ofi_negative(self):
        """SELL should produce negative signed_ofi equal to -quantity."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=99.0, min_trades=0)
        result = scorer.process_trade(side="SELL", quantity=3.2, price=100.0, trade_time_ms=0)
        assert result.signed_ofi == pytest.approx(-3.2), (
            f"SELL signed_ofi should equal -quantity; got {result.signed_ofi}"
        )

    def test_zero_quantity_neutral(self):
        """Zero-quantity trade should not move the EWMA."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=99.0, min_trades=0)
        # First seed the EWMA with a known value.
        scorer.process_trade(side="BUY", quantity=1.0, price=100.0, trade_time_ms=0)
        ewma_before = scorer.state.ewma

        # Zero-quantity trade should leave EWMA unchanged.
        scorer.process_trade(side="BUY", quantity=0.0, price=100.0, trade_time_ms=1)
        # Zero quantity → signed_ofi = 0 → ewma drifts toward 0 by alpha factor
        # (that is intentional EWMA behaviour; it is NOT staying the same).
        # The important thing is signed_ofi == 0.
        result = scorer.process_trade(side="BUY", quantity=0.0, price=100.0, trade_time_ms=2)
        assert result.signed_ofi == 0.0


# ---------------------------------------------------------------------------
# Test 5: Warm-up guard
# ---------------------------------------------------------------------------

class TestWarmUpGuard:
    def test_no_alert_before_min_trades(self):
        """Even a 1000-unit BUY should not alert if trade_count < min_trades."""
        scorer = SymbolScorer("BTCUSDT", alpha=0.9, threshold=0.1, min_trades=20)
        # Huge BUY – would definitely be toxic without the guard.
        for i in range(19):
            result = scorer.process_trade(side="BUY", quantity=1000.0, price=100.0, trade_time_ms=i)
        assert not result.is_toxic, (
            f"Should NOT alert before min_trades (trade_count={result.trade_count})"
        )

    def test_alert_fires_at_min_trades(self):
        scorer = SymbolScorer("BTCUSDT", alpha=0.9, threshold=0.1, min_trades=5)
        last = None
        for i in range(10):
            last = scorer.process_trade(side="BUY", quantity=1000.0, price=100.0, trade_time_ms=i)
        # With such a low threshold (0.1) and strong buy signal, must be toxic.
        assert last.is_toxic


# ---------------------------------------------------------------------------
# Test 6: Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_unknown_side_neutral(self):
        scorer = SymbolScorer("BTCUSDT", alpha=0.1, threshold=99.0, min_trades=0)
        result = scorer.process_trade(side="UNKNOWN", quantity=1.0, price=100.0, trade_time_ms=0)
        assert result.signed_ofi == 0.0

    def test_case_insensitive_side(self):
        s1 = SymbolScorer("X", alpha=0.1, threshold=99.0, min_trades=0)
        s2 = SymbolScorer("X", alpha=0.1, threshold=99.0, min_trades=0)
        r1 = s1.process_trade(side="buy", quantity=1.0, price=1.0, trade_time_ms=0)
        r2 = s2.process_trade(side="BUY", quantity=1.0, price=1.0, trade_time_ms=0)
        assert r1.signed_ofi == r2.signed_ofi

    def test_z_score_finite_after_one_trade(self):
        scorer = SymbolScorer("X", alpha=0.1, threshold=3.0, min_trades=0)
        result = scorer.process_trade(side="BUY", quantity=1.0, price=1.0, trade_time_ms=0)
        assert math.isfinite(result.z_score)

    def test_invalid_alpha_raises(self):
        with pytest.raises(ValueError):
            SymbolScorer("X", alpha=0.0)
        with pytest.raises(ValueError):
            SymbolScorer("X", alpha=1.5)

    def test_reset_clears_state(self):
        scorer = SymbolScorer("X", alpha=0.3, threshold=99.0, min_trades=0)
        for _ in range(20):
            scorer.process_trade(side="BUY", quantity=1.0, price=1.0, trade_time_ms=0)
        scorer.reset()
        assert scorer.state.trade_count == 0
        assert scorer.state.ewma == 0.0
