import asyncio
import json
import logging
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer

from app.core.database import get_db_pool, write_audit_log
from app.core.config import settings
from app.core.websocket import manager

logger = logging.getLogger("dashboard.alert_stream")


class AlertStream:
    def __init__(self):
        self.consumer = None
        self.task = None
        self.running = False

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            settings.ALERTS_TOPIC,
            bootstrap_servers=settings.KAFKA_BROKERS,
            group_id="dashboard-alerts",
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        await self.consumer.start()
        self.running = True
        self.task = asyncio.create_task(self._consume())

        logger.info("Alert WebSocket Kafka consumer started")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                if not self.running:
                    break

                try:
                    alert = json.loads(msg.value.decode("utf-8"))

                    if alert.get("alert_type") != "toxicity":
                        continue
                        
                    if alert.get("alert_type") == "toxicity":
                        # -------------------------------------------------
                        # 1. Persist alert in PostgreSQL
                        # -------------------------------------------------
                        pool = await get_db_pool()

                        async with pool.acquire() as conn:
                            await conn.execute(
                                """
                                INSERT INTO alerts (
                                    event_id,
                                    symbol,
                                    exchange,
                                    z_score,
                                    ewma,
                                    ewma_var,
                                    signed_ofi,
                                    side,
                                    quantity,
                                    price,
                                    trade_time_ms,
                                    alerted_at
                                )
                                VALUES (
                                    $1, $2, $3, $4, $5, $6,
                                    $7, $8, $9, $10, $11, $12
                                )
                                """,
                                alert.get("event_id"),
                                alert.get("symbol"),
                                alert.get("exchange"),
                                alert.get("z_score"),
                                alert.get("ewma"),
                                alert.get("ewma_var"),
                                alert.get("signed_ofi"),
                                alert.get("side"),
                                alert.get("quantity"),
                                alert.get("price"),
                                alert.get("trade_time_ms"),
                                (
                                    datetime.fromisoformat(alert["timestamp"].replace("Z", "+00:00"))
                                    if alert.get("timestamp")
                                    else datetime.now(timezone.utc)
                                ),
                            )

                        logger.info(
                            "Persisted toxicity alert: symbol=%s z_score=%s",
                            alert.get("symbol"),
                            alert.get("z_score"),
                        )

                        # -------------------------------------------------
                        # 2. Write audit log
                        # -------------------------------------------------
                        await write_audit_log(
                            "ALERT",
                            actor="system",
                            symbol=alert.get("symbol"),
                            event_id=alert.get("event_id"),
                            details={
                                "exchange": alert.get("exchange"),
                                "z_score": alert.get("z_score"),
                                "ewma": alert.get("ewma"),
                                "ewma_var": alert.get("ewma_var"),
                                "signed_ofi": alert.get("signed_ofi"),
                                "side": alert.get("side"),
                                "quantity": alert.get("quantity"),
                                "price": alert.get("price"),
                                "trade_time_ms": alert.get("trade_time_ms"),
                            },
                        )

                    # -----------------------------------------------------
                    # 3. Send alert to connected dashboard clients
                    # -----------------------------------------------------
                    logger.info(
                        "Consumed alert from Kafka: symbol=%s z_score=%s",
                        alert.get("symbol"),
                        alert.get("z_score"),
                    )

                    await manager.broadcast(alert)

                except Exception:
                    logger.exception("Failed to process alert message")

        except asyncio.CancelledError:
            pass

        finally:
            logger.info("Alert consumer stopped")

    async def stop(self):
        self.running = False

        if self.task:
            self.task.cancel()
            await asyncio.gather(
                self.task,
                return_exceptions=True,
            )

        if self.consumer:
            await self.consumer.stop()

        logger.info("Alert consumer closed")


alert_stream = AlertStream()
