"""
Synthetic alert generator for dashboard / UC-02 testing.

This bypasses ingestion, processing, and scoring and publishes controlled
synthetic toxicity alerts directly to the dashboard alert Kafka topic.

Use this only for dashboard demonstration/testing. Stop the normal
ingestion -> processing -> scoring pipeline first so real alerts do not
mix with the synthetic test data.

Default:
    Kafka: localhost:9092
    Topic: trades.alerts
    Alerts: 24
    Historical spacing: 5 minutes
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


SYMBOLS = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT"]
SIDES = ["BUY", "SELL"]

# Deliberately varied values so UC-02 filters are easy to demonstrate.
Z_SCORES = [
    3.10, -3.35, 4.20, -4.05,
    3.55, -3.75, 4.65, -4.30,
    3.25, -3.90, 5.10, -4.55,
    3.80, -3.20, 4.45, -5.05,
    3.40, -3.60, 4.85, -4.15,
    3.70, -3.45, 5.25, -4.75,
]

PRICES = {
    "BTCUSDT": 68000.0,
    "ETHUSDT": 2500.0,
    "DOGEUSDT": 0.17,
    "BNBUSDT": 610.0,
}

QUANTITIES = {
    "BTCUSDT": 0.015,
    "ETHUSDT": 0.40,
    "DOGEUSDT": 1500.0,
    "BNBUSDT": 0.80,
}


def build_alert(index: int, base_time: datetime) -> dict:
    symbol = SYMBOLS[index % len(SYMBOLS)]
    side = SIDES[index % len(SIDES)]
    z_score = Z_SCORES[index % len(Z_SCORES)]

    # Give every synthetic alert a distinct historical timestamp.
    alert_time = base_time - timedelta(minutes=(23 - index) * 5)
    trade_time_ms = int(alert_time.timestamp() * 1000)

    price = PRICES[symbol] * (1 + ((index % 5) - 2) * 0.001)
    quantity = QUANTITIES[symbol] * (1 + (index % 4) * 0.05)

    signed_ofi = 0.10 + (abs(z_score) / 20.0)
    if side == "SELL":
        signed_ofi *= -1

    return {
        "event_id": f"SYNTHETIC-{index + 1:03d}",
        "alert_type": "toxicity",
        "exchange": "BINANCE",
        "ewma": 0.0,
        "ewma_var": 0.0,
        "symbol": symbol,
        "side": side,
        "z_score": z_score,
        "signed_ofi": round(signed_ofi, 6),
        "price": round(price, 4),
        "quantity": round(quantity, 6),
        "trade_time_ms": trade_time_ms,
        "timestamp": alert_time.isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Publish controlled synthetic toxicity alerts for dashboard testing."
    )
    parser.add_argument(
        "--brokers",
        default="localhost:29092",
        help="Kafka bootstrap server(s), default: localhost:9092",
    )
    parser.add_argument(
        "--topic",
        default="trades.alerts",
        help="Kafka alert topic, default: trades.alerts",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=24,
        help="Number of alerts to generate, default: 24",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between published alerts in seconds, default: 0.5",
    )
    args = parser.parse_args()

    if args.count < 1:
        raise SystemExit("--count must be at least 1")

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    base_time = datetime.now(timezone.utc)

    print(f"Publishing {args.count} synthetic alerts to {args.topic}")
    print(f"Kafka: {args.brokers}")

    for index in range(args.count):
        alert = build_alert(index, base_time)

        producer.send(args.topic, value=alert)
        producer.flush()

        print(
            f"[{index + 1:02d}/{args.count}] "
            f"{alert['symbol']} "
            f"{alert['side']} "
            f"Z={alert['z_score']:.2f} "
            f"time={alert['timestamp']}"
        )

        if index < args.count - 1:
            time.sleep(args.delay)

    producer.flush()
    producer.close()

    print("Synthetic alert generation complete.")


if __name__ == "__main__":
    main()
