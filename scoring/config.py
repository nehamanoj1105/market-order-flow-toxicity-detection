"""
config.py – Scoring service configuration.

All settings have environment-variable overrides so the service can be
tuned without rebuilding.  Per-symbol thresholds are the primary knob for
UC-03 (threshold isolation).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict


# ---------------------------------------------------------------------------
# Per-symbol Z-score thresholds (UC-03: changing one must not affect others)
# ---------------------------------------------------------------------------
DEFAULT_SYMBOL_THRESHOLDS: Dict[str, float] = {
    "BTCUSDT": 3.0,
    "ETHUSDT": 3.0,
    "DOGEUSDT": 2.5,
}

# Default threshold for symbols not listed above.
DEFAULT_THRESHOLD: float = 3.0


@dataclass
class ScoringConfig:
    """Fully resolved scoring service configuration."""

    # EWMA decay factor α ∈ (0, 1].  Smaller → slower adaptation.
    ewma_alpha: float = 0.1

    # Minimum number of trades before a score can trigger an alert.
    # Prevents false positives when the EWMA hasn't warmed up yet.
    min_trades: int = 10

    # Small constant added to EWMA variance to avoid division by zero.
    variance_epsilon: float = 1e-12

    # Default Z-score threshold.  Can be overridden per symbol.
    default_threshold: float = DEFAULT_THRESHOLD

    # Per-symbol thresholds; key = upper-cased symbol, value = threshold.
    symbol_thresholds: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_SYMBOL_THRESHOLDS)
    )

    # Kafka
    kafka_brokers: str = "localhost:9092"
    kafka_input_topic: str = "trades.normalized"
    kafka_alerts_topic: str = "trades.alerts"
    kafka_group_id: str = "tofd-scorer"
    kafka_auto_offset_reset: str = "earliest"

    # PostgreSQL
    pg_dsn: str = "postgresql://postgres:devpassword@localhost:5432/toxicflow"

    # Checkpoint interval: save state every N trades (per symbol).
    checkpoint_every: int = 100

    def threshold_for(self, symbol: str) -> float:
        """Return the Z-score threshold for *symbol* (UC-03 isolation)."""
        return self.symbol_thresholds.get(symbol.upper(), self.default_threshold)


def load_config() -> ScoringConfig:
    """Build ScoringConfig from environment variables with sane defaults."""

    def _float(key: str, default: float) -> float:
        raw = os.environ.get(key, "")
        if raw.strip():
            try:
                return float(raw)
            except ValueError:
                pass
        return default

    def _int(key: str, default: int) -> int:
        raw = os.environ.get(key, "")
        if raw.strip():
            try:
                return int(raw)
            except ValueError:
                pass
        return default

    def _str(key: str, default: str) -> str:
        return os.environ.get(key, default) or default

    # Start with compiled-in defaults then apply env overrides.
    symbol_thresholds = dict(DEFAULT_SYMBOL_THRESHOLDS)

    # Allow individual symbol overrides via SCORER_THRESHOLD_<SYMBOL>=<value>
    for key, val in os.environ.items():
        if key.startswith("SCORER_THRESHOLD_"):
            sym = key[len("SCORER_THRESHOLD_"):].upper()
            try:
                symbol_thresholds[sym] = float(val)
            except ValueError:
                pass

    return ScoringConfig(
        ewma_alpha=_float("SCORER_EWMA_ALPHA", 0.1),
        min_trades=_int("SCORER_MIN_TRADES", 10),
        variance_epsilon=_float("SCORER_VARIANCE_EPSILON", 1e-12),
        default_threshold=_float("SCORER_DEFAULT_THRESHOLD", DEFAULT_THRESHOLD),
        symbol_thresholds=symbol_thresholds,
        kafka_brokers=_str("KAFKA_BROKERS", "localhost:9092"),
        kafka_input_topic=_str("KAFKA_TOPIC", "trades.normalized"),
        kafka_alerts_topic=_str("KAFKA_ALERTS_TOPIC", "trades.alerts"),
        kafka_group_id=_str("KAFKA_GROUP_ID", "tofd-scorer"),
        kafka_auto_offset_reset=_str("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        pg_dsn=_str(
            "PG_DSN",
            "postgresql://postgres:devpassword@localhost:5432/toxicflow",
        ),
        checkpoint_every=_int("SCORER_CHECKPOINT_EVERY", 100),
    )
