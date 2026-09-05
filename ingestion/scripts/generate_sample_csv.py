#!/usr/bin/env python3
"""Generate a small, deterministic Binance-style trade dump for local testing.

The real dumps published at https://data.binance.vision are hundreds of
megabytes and are overkill for development, CI and demos. This script produces
a few megabytes of realistic data with a *planted* toxic burst, so the whole
pipeline (ingestor -> broker -> scorer -> dashboard) can be exercised and its
output can be predicted.

Output format matches the daily "trades" dumps:

    id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch

Usage:
    python3 generate_sample_csv.py --rows 20000 --out ../../data/sample_binance_trades.csv
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone


def build_rows(args) -> list[str]:
    rng = random.Random(args.seed)
    price = args.start_price
    trade_id = args.start_id
    ts = args.start_time_ms

    lines = ["id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch"]
    burst_start = args.burst_at_row
    burst_end = burst_start + args.burst_rows

    for i in range(args.rows):
        in_burst = burst_start <= i < burst_end

        # Geometric Brownian motion step: dt is one "trade tick".
        dt = 1.0 / (365 * 24 * 3600 * args.trades_per_second)
        drift = 0.0
        buy_prob = 0.5
        size_factor = 1.0
        if in_burst:
            # One-sided flow plus price impact: exactly what a toxicity
            # detector is supposed to notice.
            buy_prob = args.burst_imbalance
            drift = 4e-6 * args.burst_size_factor
            size_factor = args.burst_size_factor

        shock = args.volatility * math.sqrt(dt) * rng.gauss(0.0, 1.0)
        price *= math.exp(drift + shock)
        price = round(price / args.tick_size) * args.tick_size

        qty = args.base_qty * math.exp(0.6 * rng.gauss(0.0, 1.0)) * size_factor
        qty = max(round(qty, 8), 0.00001)

        is_buy = rng.random() < buy_prob
        # Binance's flag: True => the buyer was the maker => aggressor is SELL.
        is_buyer_maker = not is_buy
        quote = round(price * qty, 8)

        # Inter-arrival times: exponential, sped up during a burst.
        rate = args.trades_per_second * (args.burst_rate_factor if in_burst else 1.0)
        ts += int(-math.log(1.0 - rng.random()) / rate * 1000)

        trade_id += 1
        lines.append(
            f"{trade_id},{price:.2f},{qty:.8f},{quote:.8f},{ts},{str(is_buyer_maker).lower()},true"
        )
    return lines


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=20_000, help="number of trade rows (default: 20000)")
    p.add_argument("--out", default="-", help="output path, or - for stdout")
    p.add_argument("--symbol", default="BTCUSDT", help="symbol name (metadata only)")
    p.add_argument("--start-price", type=float, default=65000.0)
    p.add_argument("--tick-size", type=float, default=0.01)
    p.add_argument("--base-qty", type=float, default=0.05, help="median trade size in base asset")
    p.add_argument("--volatility", type=float, default=0.35, help="annualised sigma for the price path")
    p.add_argument("--trades-per-second", type=float, default=8.0, help="mean arrival rate")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-id", type=int, default=4_000_000_000)
    p.add_argument("--start-time", default=None,
                   help="ISO-8601 start timestamp (default: 2024-01-02 00:00:00 UTC)")
    p.add_argument("--burst-at-row", type=int, default=12_000, help="row index where the toxic burst starts")
    p.add_argument("--burst-rows", type=int, default=1_200, help="length of the toxic burst in rows")
    p.add_argument("--burst-imbalance", type=float, default=0.88, help="share of aggressive buys during the burst")
    p.add_argument("--burst-rate-factor", type=float, default=4.0, help="arrival rate multiplier during the burst")
    p.add_argument("--burst-size-factor", type=float, default=3.0, help="trade size multiplier during the burst")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.start_time:
        start = datetime.fromisoformat(args.start_time)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
    else:
        start = datetime(2024, 1, 2, tzinfo=timezone.utc)
    args.start_time_ms = int(start.timestamp() * 1000)

    lines = build_rows(args)
    text = "\n".join(lines) + "\n"
    if args.out == "-":
        sys.stdout.write(text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
        span = datetime.fromtimestamp(args.start_time_ms / 1000, tz=timezone.utc)
        last_ts = int(lines[-1].split(",")[4])
        end = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        print(
            f"wrote {len(lines) - 1} rows to {args.out} "
            f"({span.isoformat()} -> {end.isoformat()}, "
            f"burst rows {args.burst_at_row}..{args.burst_at_row + args.burst_rows})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
