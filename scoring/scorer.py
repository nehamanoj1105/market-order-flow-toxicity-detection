"""
scorer.py – EWMA-based Order-Flow Imbalance (OFI) toxicity scorer.

Algorithm
---------
Order-Flow Imbalance is the signed, volume-weighted trade flow:

    signed_ofi = +qty   (BUY aggressor)
    signed_ofi = -qty   (SELL aggressor)

We maintain an Exponentially Weighted Moving Average of the OFI signal and
its variance so that we can compute a Z-score in O(1) per trade without
storing the full history:

    ewma_new  = α * ofi + (1-α) * ewma
    ewma_var  = α * (ofi - ewma_old)² + (1-α) * ewma_var
    z_score   = ewma / sqrt(max(ewma_var, ε))

A score is "toxic" when |z_score| exceeds the configured threshold *and* the
scorer has seen at least `min_trades` trades (warm-up guard).

Consistency with the SRS volume-ratio toxicity definition
---------------------------------------------------------
The SRS defines toxicity in terms of the volume-weighted order-flow imbalance
ratio, conventionally expressed over a window as:

    OFI_ratio = (buy_volume − sell_volume) / (buy_volume + sell_volume)

This ratio lies in [−1, 1].  The EWMA approach here is numerically equivalent
for the following reason: the z-score divides the EWMA of the signed volume
(the numerator of OFI_ratio, exponentially weighted) by the EWMA standard
deviation of that same signal.  This performs the same normalisation as
dividing by total volume, but adaptively, using the signal's own historical
variability rather than a fixed window denominator.  Specifically:

  * Each trade contributes ±qty, so larger trades exert more influence
    (volume-weighted, as the SRS requires).
  * The z-score is dimensionless and bounded relative to historical noise,
    making it comparable across symbols with different price/volume scales.
  * A signal that consistently pushes in one direction will have a high EWMA
    and low variance — exactly the toxic regime identified by the OFI ratio.

The signed-volume EWMA z-score is therefore a per-trade, online approximation
of the windowed OFI ratio, with the benefit of O(1) computation and no need
to define a fixed window size.

Edge-case handling
------------------
* Zero or near-zero variance  → guarded by ε (first trade: z = 0).
* NaN / Inf in input          → sanitised before processing.
* Side values                 → accepts "BUY"/"SELL" and "buy"/"sell".
* Zero quantity               → treated as neutral (no OFI contribution).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ScorerState:
    """
    Serialisable state of one symbol's EWMA scorer.

    This is the *only* thing that needs to be checkpointed so the scorer can
    resume after a restart without resetting to zero.
    """

    symbol: str
    ewma: float = 0.0        # current EWMA of signed OFI
    ewma_var: float = 0.0    # current EWMA variance of signed OFI
    trade_count: int = 0     # total trades processed (used for warm-up guard)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "ewma": self.ewma,
            "ewma_var": self.ewma_var,
            "trade_count": self.trade_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ScorerState":
        return cls(
            symbol=d["symbol"],
            ewma=float(d["ewma"]),
            ewma_var=float(d["ewma_var"]),
            trade_count=int(d["trade_count"]),
        )


@dataclass
class ScoreResult:
    """Result returned by SymbolScorer.process_trade()."""

    symbol: str
    z_score: float
    ewma: float
    ewma_var: float
    trade_count: int
    is_toxic: bool
    # The trade fields that triggered the alert (useful for publishing).
    signed_ofi: float       # the per-trade OFI value (+qty or -qty)
    side: str
    quantity: float
    price: float
    trade_time_ms: int
    event_id: str = ""


class SymbolScorer:
    """
    Single-symbol EWMA OFI scorer.

    Parameters
    ----------
    symbol : str
        The market symbol this scorer is responsible for.
    alpha : float
        EWMA decay factor α ∈ (0, 1].  Default 0.1 (slow adaptation).
    threshold : float
        Z-score threshold above which a trade is flagged as toxic.
    min_trades : int
        Minimum trades before an alert can fire (warm-up guard).
    variance_epsilon : float
        Floor for EWMA variance to prevent division by zero.
    initial_state : ScorerState | None
        If provided, the scorer resumes from a checkpointed state.
    """

    def __init__(
        self,
        symbol: str,
        alpha: float = 0.1,
        threshold: float = 3.0,
        min_trades: int = 10,
        variance_epsilon: float = 1e-12,
        initial_state: Optional[ScorerState] = None,
    ) -> None:
        if not (0 < alpha <= 1):
            raise ValueError(f"alpha must be in (0, 1], got {alpha}")
        if threshold <= 0:
            raise ValueError(f"threshold must be > 0, got {threshold}")
        if min_trades < 0:
            raise ValueError(f"min_trades must be >= 0, got {min_trades}")

        self.symbol = symbol.upper()
        self.alpha = alpha
        self.threshold = threshold
        self.min_trades = min_trades
        self.variance_epsilon = variance_epsilon

        if initial_state is not None:
            self._state = initial_state
        else:
            self._state = ScorerState(symbol=self.symbol)

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    @property
    def state(self) -> ScorerState:
        """Read-only access to the current scorer state (for checkpointing)."""
        return self._state

    def restore_state(self, state: ScorerState) -> None:
        """
        Replace the internal state with a checkpointed one.

        IMPORTANT: call this *before* processing any new trade to avoid the
        double-processing bug where a trade is counted once before restore and
        again after restore.
        """
        self._state = state

    # ------------------------------------------------------------------
    # Core algorithm
    # ------------------------------------------------------------------

    def process_trade(
        self,
        side: str,
        quantity: float,
        price: float,
        trade_time_ms: int,
        event_id: str = "",
    ) -> ScoreResult:
        """
        Incorporate one trade into the running EWMA statistics and return a
        ScoreResult.

        Parameters
        ----------
        side : str
            "BUY" or "SELL" (case-insensitive).
        quantity : float
            Executed base-asset quantity (must be > 0).
        price : float
            Execution price in quote currency.
        trade_time_ms : int
            Exchange trade timestamp in Unix milliseconds.
        event_id : str
            Optional idempotency key from the ingestor.
        """
        # ---- Input sanitisation ------------------------------------------
        side_upper = side.upper() if isinstance(side, str) else ""
        if side_upper not in ("BUY", "SELL"):
            # Defensive: treat unknown side as neutral (no OFI contribution).
            side_upper = "NEUTRAL"

        if not (math.isfinite(quantity) and quantity > 0):
            quantity = 0.0
        if not math.isfinite(price):
            price = 0.0

        # ---- Compute signed OFI ------------------------------------------
        # Volume-weighted: BUY adds +qty, SELL adds -qty, neutral adds 0.
        if side_upper == "BUY":
            signed_ofi = quantity
        elif side_upper == "SELL":
            signed_ofi = -quantity
        else:
            signed_ofi = 0.0

        # ---- EWMA update -------------------------------------------------
        alpha = self.alpha
        old_ewma = self._state.ewma
        old_var = self._state.ewma_var

        # First observation: initialise from data rather than zero.
        if self._state.trade_count == 0:
            new_ewma = signed_ofi
            new_var = 0.0
        else:
            new_ewma = alpha * signed_ofi + (1 - alpha) * old_ewma
            # Variance: use the *old* ewma for the deviation term so the
            # update is well-defined on the first step.
            deviation = signed_ofi - old_ewma
            new_var = alpha * (deviation * deviation) + (1 - alpha) * old_var

        # ---- Update state ------------------------------------------------
        self._state.ewma = new_ewma
        self._state.ewma_var = new_var
        self._state.trade_count += 1

        # ---- Z-score -----------------------------------------------------
        # Special case: after the very first trade the variance is 0 by
        # definition (one observation has no spread), so z is undefined.
        # Return 0 to avoid division by near-zero giving a spurious alert.
        if self._state.trade_count == 1:
            z_score = 0.0
        else:
            std = math.sqrt(max(new_var, self.variance_epsilon))
            z_score = new_ewma / std

        # ---- Toxicity decision -------------------------------------------
        is_toxic = (
            self._state.trade_count >= self.min_trades
            and abs(z_score) >= self.threshold
        )

        return ScoreResult(
            symbol=self.symbol,
            z_score=z_score,
            ewma=new_ewma,
            ewma_var=new_var,
            trade_count=self._state.trade_count,
            is_toxic=is_toxic,
            signed_ofi=signed_ofi,
            side=side_upper,
            quantity=quantity,
            price=price,
            trade_time_ms=trade_time_ms,
            event_id=event_id,
        )

    def z_score(self) -> float:
        """Return the current Z-score without processing a trade."""
        std = math.sqrt(max(self._state.ewma_var, self.variance_epsilon))
        return self._state.ewma / std

    def reset(self) -> None:
        """Reset internal state to zero (for testing)."""
        self._state = ScorerState(symbol=self.symbol)
