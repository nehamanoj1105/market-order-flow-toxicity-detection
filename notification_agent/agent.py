import json
import time
import logging

from websocket import create_connection, WebSocketException
from notifypy import Notify


WS_URL = "ws://localhost:8000/api/ws/alerts"

RECONNECT_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("tofd-notification-agent")


def show_notification(alert):
    symbol = alert.get("symbol", "UNKNOWN")
    side = alert.get("side", "")
    z_score = float(alert.get("z_score", 0))
    price = float(alert.get("price", 0))

    notification = Notify()

    notification.application_name = "TOFD"
    notification.title = f"TOXICITY ALERT — {symbol}"
    notification.urgency = "critical"

    notification.message = (
        f"{side} · Z {z_score:.2f}\n"
        f"Price: ${price:,.2f}"
    )
    notification.send(block=False)


def connect_and_listen():
    logger.info("Connecting to TOFD notification stream at %s...", WS_URL)

    ws = create_connection(
        WS_URL,
        timeout=30
    )

    logger.info("Connected to TOFD backend. Listening for toxicity alerts...")

    while True:
        message = ws.recv()

        if not message:
            continue

        alert = json.loads(message)

        if alert.get("alert_type") != "toxicity":
            continue

        logger.info(
            "🚨 TOXICITY ALERT | %s | %s | Z=%.2f | Price=$%s | Qty=%s",
            alert.get("symbol"),
            alert.get("side"),
            float(alert.get("z_score", 0)),
            f"{float(alert.get('price', 0)):,.2f}",
            f"{float(alert.get('quantity', 0)):,.4f}",
        )

        try:
            show_notification(alert)
        except Exception as exc:
            logger.warning("Desktop notification could not be displayed: %s", exc)


def main():
    logger.info("TOFD Notification Agent started")

    while True:
        try:
            connect_and_listen()

        except KeyboardInterrupt:
            logger.info("Notification agent stopped")
            break

        except (
            ConnectionRefusedError,
            WebSocketException,
            OSError
        ) as exc:
            logger.warning(
                "Backend unavailable: %s",
                exc
            )

        except Exception:
            logger.exception(
                "Unexpected notification-agent error"
            )

        logger.info(
            "Retrying in %d seconds...",
            RECONNECT_DELAY
        )

        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    main()
