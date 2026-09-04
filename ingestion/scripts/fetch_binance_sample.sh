#!/usr/bin/env bash
# Download a slice of real Binance trade data for local testing.
#
# Source: https://data.binance.vision (public, no API key required).
# A full daily dump for a liquid pair is 100-300 MB compressed and tens of
# millions of rows, so we keep only the first N rows.
#
# Usage:
#   ./fetch_binance_sample.sh                        # BTCUSDT, yesterday, 200k rows
#   SYMBOL=ETHUSDT DATE=2024-05-01 ROWS=50000 ./fetch_binance_sample.sh
#
# Output: ../../data/<SYMBOL>-trades-<DATE>.csv   (with header row added)

set -euo pipefail

SYMBOL="${SYMBOL:-BTCUSDT}"
DATE="${DATE:-$(date -u -d 'yesterday' +%F)}"
ROWS="${ROWS:-200000}"
OUT_DIR="${OUT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../data" && pwd)}"

mkdir -p "$OUT_DIR"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ZIP="${SYMBOL}-trades-${DATE}.zip"
URL="https://data.binance.vision/data/spot/daily/trades/${SYMBOL}/${ZIP}"

echo "downloading ${URL}" >&2
if ! curl -fsSL --retry 3 -o "${TMP}/${ZIP}" "${URL}"; then
  echo "download failed. Check the symbol/date: only dates from 2020-01-01 onwards" \
       "and symbols that traded that day are published." >&2
  exit 1
fi

echo "extracting first ${ROWS} rows" >&2
cd "$TMP"
unzip -q -o "$ZIP"

CSV="$(ls *.csv | head -n1)"
HEADER="id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch"
OUT="${OUT_DIR}/${SYMBOL}-trades-${DATE}.csv"

# Older dumps ship without a header row; detect and normalise.
if head -n1 "$CSV" | grep -qE '^[0-9]+,'; then
  { echo "$HEADER"; head -n "$ROWS" "$CSV"; } > "$OUT"
else
  head -n "$((ROWS + 1))" "$CSV" > "$OUT"
fi

echo "wrote $(wc -l < "$OUT") lines to ${OUT}" >&2
echo "run it with: CSV_PATH=${OUT} CSV_SPEED=50 ./ingestor" >&2
