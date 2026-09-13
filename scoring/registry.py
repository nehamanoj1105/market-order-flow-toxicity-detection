"""
registry.py – Per-symbol scorer registry.

Each symbol gets its own independent SymbolScorer.  State is never shared
across symbols.  The registry is the single authority for creating and
looking up scorers.

UC-03 note: every scorer in the registry has its own threshold, derived
from the config.  Changing DOGEUSDT's threshold only affects the DOGEUSDT
scorer.
"""
from __future__ import annotations

from typing import Dict, Optional

from .config import ScoringConfig
from .scorer import SymbolScorer, ScorerState


class ScorerRegistry:
    """
    Thread-safe-by-design registry of per-symbol scorers.

    In the current single-threaded Kafka consumer model the GIL is
    sufficient.  If the service is ever made multi-threaded a Lock should
    be added around mutations.

    Parameters
    ----------
    config : ScoringConfig
        Service-wide configuration (alpha, epsilon, per-symbol thresholds).
    """

    def __init__(self, config: ScoringConfig) -> None:
        self._config = config
        self._scorers: Dict[str, SymbolScorer] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_or_create(self, symbol: str) -> SymbolScorer:
        """
        Return the scorer for *symbol*, creating it if needed.

        The threshold used comes from `config.threshold_for(symbol)` so
        each symbol gets its own, independent limit.
        """
        sym = symbol.upper()
        if sym not in self._scorers:
            self._scorers[sym] = SymbolScorer(
                symbol=sym,
                alpha=self._config.ewma_alpha,
                threshold=self._config.threshold_for(sym),
                min_trades=self._config.min_trades,
                variance_epsilon=self._config.variance_epsilon,
            )
        return self._scorers[sym]

    def get(self, symbol: str) -> Optional[SymbolScorer]:
        """Return the scorer for *symbol* or None if it doesn't exist yet."""
        return self._scorers.get(symbol.upper())

    def restore(self, state: ScorerState) -> None:
        """
        Restore a single symbol's scorer from a checkpointed state.

        The scorer is created first (with the correct per-symbol threshold)
        and its state is then replaced by the checkpointed one.  This
        ensures the threshold from config is always honoured.
        """
        scorer = self.get_or_create(state.symbol)
        scorer.restore_state(state)

    def symbols(self) -> list[str]:
        """Return all symbols currently tracked."""
        return list(self._scorers.keys())

    def all_states(self) -> list[ScorerState]:
        """Return the current state of every scorer (for bulk checkpointing)."""
        return [s.state for s in self._scorers.values()]

    def update_threshold(self, symbol: str, threshold: float) -> None:
        """
        Change the Z-score threshold for *symbol* at runtime (UC-03).

        If the scorer doesn't exist yet it will be created when the first
        trade arrives; the new threshold will be used at that point.
        """
        sym = symbol.upper()
        self._config.symbol_thresholds[sym] = threshold
        if sym in self._scorers:
            self._scorers[sym].threshold = threshold

    def __len__(self) -> int:
        return len(self._scorers)
