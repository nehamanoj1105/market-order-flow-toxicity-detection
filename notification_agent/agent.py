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

    notification.message = (
        f"{side} · Z {z_score:.2f}\n"
        f"Price: ${price:,.2f}"
    )
    notification.launch = "http://localhost:8000/"
    notification.send()


def connect_and_listen():
    logger.info("Connecting to TOFD notification stream...")

    ws = create_connection(
        WS_URL,
        timeout=30
    )

    logger.info("Connected to TOFD backend")

    while True:
        message = ws.recv()

        if not message:
            continue

        alert = json.loads(message)

        if alert.get("alert_type") != "toxicity":
            continue

        logger.info(
            "Received toxicity alert: %s Z=%.2f",
            alert.get("symbol"),
            float(alert.get("z_score", 0))
        )

        try:
            show_notification(alert)
        except Exception:
            logger.exception("Failed to display system notification")


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
