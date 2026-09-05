#!/usr/bin/env python3

import csv
from pathlib import Path

INPUT = Path("data/BTCUSDT-trades-2026-09-04.csv")
OUTPUT = Path("data/sample_binance_trades.csv")

HEADER = [
    "id",
    "price",
    "qty",
    "quoteQty",
    "time",
    "isBuyerMaker",
    "isBestMatch",
]


def normalize_timestamp(ts: str) -> str:
    """
    Binance Spot data before 2025 uses milliseconds.
    Binance Spot data from 2025 onward uses microseconds.
    Convert microseconds -> milliseconds for our project format.
    """
    value = int(ts)

    if value > 10**14:
        value //= 1000

    return str(value)


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with INPUT.open("r", newline="", encoding="utf-8") as src:
        reader = csv.reader(src)

        first = next(reader)

        # Detect whether Binance file already has a header.
        if first and first[0].lower() in {"id", "trade id", "trade_id"}:
            reader = reader
        else:
            # First row is actual trade data.
            reader = iter([first] + list(reader))

        with OUTPUT.open("w", newline="", encoding="utf-8") as dst:
            writer = csv.writer(dst)
            writer.writerow(HEADER)

            count = 0

            for row in reader:
                if count == 20000:
            	    break
                if len(row) < 7:
                    continue

                trade_id = row[0]
                price = row[1]
                qty = row[2]
                quote_qty = row[3]
                timestamp = normalize_timestamp(row[4])
                is_buyer_maker = row[5].lower()
                is_best_match = row[6].lower()

                writer.writerow([
                    trade_id,
                    price,
                    qty,
                    quote_qty,
                    timestamp,
                    is_buyer_maker,
                    is_best_match,
                ])

                count += 1

    print(f"Wrote {count} trades to {OUTPUT}")


if __name__ == "__main__":
    main()
