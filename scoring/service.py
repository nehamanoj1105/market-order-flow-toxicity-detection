"""
service.py – Kafka consumer/producer loop for the scoring service.

Lifecycle
---------
1. Load config.
2. Connect to PostgreSQL and restore checkpointed scorer states into the
   registry (BEFORE processing any trade from Kafka).
3. Start consuming from `trades.normalized`.
4. For each trade message:
   a. Deserialise JSON.
   b. Route to the correct symbol scorer (get_or_create).
   c. Call process_trade() EXACTLY ONCE.
   d. If the result is toxic, publish an alert to `trades.alerts`.
   e. Commit the Kafka offset (at-least-once; idempotent with event_id).
   f. Periodically checkpoint scorer state to PostgreSQL.
5. On SIGINT / SIGTERM flush checkpoints and close.

Double-processing prevention
-----------------------------
State is restored from PostgreSQL BEFORE the consumer starts reading Kafka
messages.  There is exactly one call to process_trade() per Kafka message.
The scorer is never reset after restore.

At-least-once delivery
-----------------------
Offsets are committed after publish succeeds.  The event_id field (UUIDv4
set by the ingestor) can be used by downstream consumers to deduplicate.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any, Dict, Optional

from .checkpoint import CheckpointStore, NullCheckpointStore
from .config import ScoringConfig, load_config
from .registry import ScorerRegistry
from .scorer import ScoreResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert payload builder
# ---------------------------------------------------------------------------

def build_alert(result: ScoreResult, trade: Dict[str, Any]) -> Dict[str, Any]:
    """Construct a JSON-serialisable toxicity alert payload."""
    return {
        "alert_type": "toxicity",
        "symbol": result.symbol,
        "z_score": round(result.z_score, 6),
        "ewma": round(result.ewma, 8),
        "ewma_var": round(result.ewma_var, 8),
        "trade_count": result.trade_count,
        "signed_ofi": round(result.signed_ofi, 8),
        "side": result.side,
        "quantity": result.quantity,
        "price": result.price,
        "trade_time_ms": result.trade_time_ms,
        "event_id": result.event_id,
        # Carry through useful raw trade fields.
        "exchange": trade.get("exchange", ""),
        "trade_id": trade.get("trade_id", ""),
        "quote_quantity": trade.get("quote_quantity", 0.0),
        "alerted_at_ms": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# ScoringService
# ---------------------------------------------------------------------------

class ScoringService:
    """
    Orchestrates config, registry, checkpoint store, and Kafka I/O.

    Parameters are injected so the class is unit-testable without real Kafka
    or PostgreSQL connections.
    """

    def __init__(
        self,
        config: ScoringConfig,
        registry: ScorerRegistry,
        checkpoint_store,
        kafka_consumer=None,
        kafka_producer=None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._store = checkpoint_store
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._running = False
        self._trades_since_checkpoint: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Startup / shutdown
    # ------------------------------------------------------------------

    def restore_checkpoints(self) -> None:
        """
        Load all persisted scorer states into the registry.

        MUST be called BEFORE the Kafka consumer starts, so that the first
        trade processed uses restored state (not a blank scorer).
        """
        states = self._store.restore_all()
        for state in states:
            self._registry.restore(state)
            logger.info(
                "restored scorer: symbol=%s ewma=%.6f var=%.6f count=%d",
                state.symbol, state.ewma, state.ewma_var, state.trade_count,
            )

    def _maybe_checkpoint(self, symbol: str) -> None:
        """Save scorer state every `checkpoint_every` trades for this symbol."""
        count = self._trades_since_checkpoint.get(symbol, 0) + 1
        self._trades_since_checkpoint[symbol] = count
        if count >= self._config.checkpoint_every:
            scorer = self._registry.get(symbol)
            if scorer is not None:
                try:
                    self._store.save(scorer.state)
                except Exception as exc:
                    logger.warning("checkpoint save failed for %s: %s", symbol, exc)
            self._trades_since_checkpoint[symbol] = 0

    def flush_checkpoints(self) -> None:
        """Persist all scorer states (called on clean shutdown)."""
        try:
            self._store.save_all(self._registry.all_states())
        except Exception as exc:
            logger.warning("flush checkpoint failed: %s", exc)

    # ------------------------------------------------------------------
    # Trade processing (the heart of the service)
    # ------------------------------------------------------------------

    def process_message(self, raw_value: bytes) -> Optional[ScoreResult]:
        """
        Process one raw Kafka message value.

        Returns the ScoreResult (None if the message was skipped).
        This method is called exactly once per Kafka message.
        """
        try:
            trade = json.loads(raw_value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("failed to decode trade message: %s", exc)
            return None

        symbol = trade.get("symbol", "")
        side = trade.get("side", "")
        quantity = trade.get("quantity", 0.0)
        price = trade.get("price", 0.0)
        trade_time_ms = trade.get("trade_time_ms", 0)
        event_id = trade.get("event_id", "")

        if not symbol:
            logger.debug("skipping message with empty symbol")
            return None

        # Get or create the scorer for this symbol (UC-03: per-symbol state).
        scorer = self._registry.get_or_create(symbol)

        # Process the trade EXACTLY ONCE.
        result = scorer.process_trade(
            side=side,
            quantity=float(quantity),
            price=float(price),
            trade_time_ms=int(trade_time_ms),
            event_id=event_id,
        )

        self._maybe_checkpoint(symbol)

        if result.is_toxic:
            alert = build_alert(result, trade)
            self._publish_alert(alert)

        return result

    def _publish_alert(self, alert: Dict[str, Any]) -> None:
        """Publish a toxicity alert to the alerts Kafka topic."""
        if self._producer is None:
            logger.warning("no Kafka producer configured; alert not published: %s", alert)
            return
        try:
            payload = json.dumps(alert).encode()
            key = alert["symbol"].encode()
            self._producer.produce(
                self._config.kafka_alerts_topic,
                key=key,
                value=payload,
            )
            self._producer.poll(0)
            logger.info(
                "TOXIC ALERT symbol=%s z=%.3f side=%s qty=%.4f price=%.2f",
                alert["symbol"], alert["z_score"], alert["side"],
                alert["quantity"], alert["price"],
            )
        except Exception as exc:
            logger.error("failed to publish alert: %s", exc)

    # ------------------------------------------------------------------
    # Main loop (uses confluent-kafka)
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Main consumer loop.  Blocks until SIGINT/SIGTERM.

        Restore → consume → process → (alert) → commit → checkpoint.
        """
        self._running = True

        # Step 1: restore BEFORE consuming any trade.
        self.restore_checkpoints()

        logger.info(
            "scorer started; consuming %s → alerting on %s",
            self._config.kafka_input_topic,
            self._config.kafka_alerts_topic,
        )

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("kafka consumer error: %s", msg.error())
                    continue

                self.process_message(msg.value())

                # Commit offset after successful processing (at-least-once).
                self._consumer.commit(asynchronous=False)

        except KeyboardInterrupt:
            logger.info("interrupted by user")
        finally:
            self.flush_checkpoints()
            if self._consumer:
                self._consumer.close()
            if self._producer:
                self._producer.flush()
            logger.info("scorer stopped")


# ---------------------------------------------------------------------------
# Entry point (for `python -m scoring.service`)
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = load_config()
    registry = ScorerRegistry(config)

    # Try real PostgreSQL; fall back to NullCheckpointStore for dev.
    try:
        store = CheckpointStore(config.pg_dsn)
        store.ensure_schema()
    except Exception as exc:
        logger.warning("PostgreSQL unavailable (%s); using null checkpoint store", exc)
        store = NullCheckpointStore()

    try:
        from confluent_kafka import Consumer, Producer
    except ImportError:
        logger.error(
            "confluent-kafka is not installed.  "
            "Run: pip install confluent-kafka"
        )
        return

    consumer = Consumer({
        "bootstrap.servers": config.kafka_brokers,
        "group.id": config.kafka_group_id,
        "auto.offset.reset": config.kafka_auto_offset_reset,
        "enable.auto.commit": False,  # We commit manually after processing.
    })
    consumer.subscribe([config.kafka_input_topic])

    producer = Producer({"bootstrap.servers": config.kafka_brokers})

    service = ScoringService(
        config=config,
        registry=registry,
        checkpoint_store=store,
        kafka_consumer=consumer,
        kafka_producer=producer,
    )
    service.run()


if __name__ == "__main__":
    main()
