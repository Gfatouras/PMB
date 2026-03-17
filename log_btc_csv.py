"""Log Kraken BTC stream updates to CSV in batched writes.

Requires:
    pip install websocket-client
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from fetch_btc import (
    DEFAULT_BOOK_DEPTH,
    DEFAULT_CHANNELS,
    DEFAULT_RECONNECT_SECONDS,
    DEFAULT_SYMBOL,
    DEFAULT_TICKER_EVENT_TRIGGER,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_WS_URL,
    compute_live_price,
    compute_mid_price,
    parse_channels,
    stream_updates,
)

DEFAULT_OUTPUT = "btc_stream_log.csv"
DEFAULT_FLUSH_ROWS = 100

CSV_FIELDS = [
    "received_ts",
    "channel",
    "type",
    "symbol",
    "ts",
    "last",
    "bid",
    "ask",
    "bid_qty",
    "ask_qty",
    "change_pct",
    "side",
    "qty",
    "price",
    "ord_type",
    "trade_id",
    "batch_size",
    "bid_price",
    "ask_price",
    "book_bid_qty",
    "book_ask_qty",
    "bid_updates",
    "ask_updates",
    "checksum",
    "mid",
    "live_price",
    "error",
]


def as_csv_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def update_to_row(channel: str, update: dict) -> dict:
    row = {field: "" for field in CSV_FIELDS}
    row["received_ts"] = datetime.now().isoformat()
    row["channel"] = channel
    row["type"] = as_csv_value(update.get("type"))
    row["symbol"] = as_csv_value(update.get("symbol"))
    row["ts"] = as_csv_value(update.get("ts"))

    if channel == "ticker":
        bid = update.get("bid")
        ask = update.get("ask")
        last = update.get("last")
        row["last"] = as_csv_value(last)
        row["bid"] = as_csv_value(bid)
        row["ask"] = as_csv_value(ask)
        row["bid_qty"] = as_csv_value(update.get("bid_qty"))
        row["ask_qty"] = as_csv_value(update.get("ask_qty"))
        row["change_pct"] = as_csv_value(update.get("change_pct"))
        row["mid"] = as_csv_value(compute_mid_price(bid, ask))
        row["live_price"] = as_csv_value(compute_live_price(last, bid, ask))
        return row

    if channel == "trade":
        price = update.get("price")
        row["side"] = as_csv_value(update.get("side"))
        row["qty"] = as_csv_value(update.get("qty"))
        row["price"] = as_csv_value(price)
        row["ord_type"] = as_csv_value(update.get("ord_type"))
        row["trade_id"] = as_csv_value(update.get("trade_id"))
        row["batch_size"] = as_csv_value(update.get("batch_size"))
        row["live_price"] = as_csv_value(price)
        return row

    if channel == "book":
        bid_price = update.get("bid_price")
        ask_price = update.get("ask_price")
        row["bid_price"] = as_csv_value(bid_price)
        row["ask_price"] = as_csv_value(ask_price)
        row["book_bid_qty"] = as_csv_value(update.get("bid_qty"))
        row["book_ask_qty"] = as_csv_value(update.get("ask_qty"))
        row["bid_updates"] = as_csv_value(update.get("bid_updates"))
        row["ask_updates"] = as_csv_value(update.get("ask_updates"))
        row["checksum"] = as_csv_value(update.get("checksum"))
        row["mid"] = as_csv_value(compute_mid_price(bid_price, ask_price))
        row["live_price"] = row["mid"]
        return row

    if channel == "error":
        row["error"] = as_csv_value(update.get("error", "unknown stream error"))

    return row


def flush_rows(csv_writer: csv.DictWriter, file_obj, buffer: list[dict]) -> int:
    if not buffer:
        return 0
    csv_writer.writerows(buffer)
    file_obj.flush()
    count = len(buffer)
    buffer.clear()
    return count


def parse_args():
    parser = argparse.ArgumentParser(description="Log Kraken BTC stream updates to CSV.")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT, help="CSV output path.")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL, help="Kraken symbol (default: BTC/USD).")
    parser.add_argument("--url", type=str, default=DEFAULT_WS_URL, help="Kraken WebSocket URL.")
    parser.add_argument(
        "--channels",
        type=parse_channels,
        default=list(DEFAULT_CHANNELS),
        help="Comma-separated channels: ticker,trade,book.",
    )
    parser.add_argument("--book-depth", type=int, default=DEFAULT_BOOK_DEPTH, help="Kraken book depth.")
    parser.add_argument(
        "--ticker-event-trigger",
        type=str,
        default=DEFAULT_TICKER_EVENT_TRIGGER,
        help="Ticker event trigger: trades or bbo.",
    )
    parser.add_argument(
        "--reconnect-seconds", type=float, default=DEFAULT_RECONNECT_SECONDS, help="Kraken reconnect delay."
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Kraken socket timeout.")
    parser.add_argument(
        "--flush-rows",
        type=int,
        default=DEFAULT_FLUSH_ROWS,
        help=f"Number of rows to buffer before writing to disk (default: {DEFAULT_FLUSH_ROWS}).",
    )
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after N logged rows (0 = unlimited).")
    parser.add_argument(
        "--skip-errors",
        action="store_true",
        help="Do not log stream error events.",
    )
    args = parser.parse_args()

    if args.flush_rows <= 0:
        raise SystemExit("--flush-rows must be > 0")
    if args.max_rows < 0:
        raise SystemExit("--max-rows must be >= 0")
    return args


def main():
    args = parse_args()

    output_path = Path(args.output).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists() or output_path.stat().st_size == 0

    buffered_rows: list[dict] = []
    total_written = 0

    with output_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
            csv_file.flush()

        print(
            f"Logging {','.join(args.channels)} for {args.symbol} to {output_path.resolve()} "
            f"(flush every {args.flush_rows} rows)"
        )

        try:
            for channel, update in stream_updates(
                symbol=args.symbol,
                url=args.url,
                channels=args.channels,
                book_depth=args.book_depth,
                ticker_event_trigger=args.ticker_event_trigger,
                reconnect_seconds=args.reconnect_seconds,
                timeout_seconds=args.timeout_seconds,
            ):
                if channel == "error" and args.skip_errors:
                    continue

                buffered_rows.append(update_to_row(channel, update))
                if args.max_rows > 0 and total_written + len(buffered_rows) >= args.max_rows:
                    extra = (total_written + len(buffered_rows)) - args.max_rows
                    if extra > 0:
                        del buffered_rows[-extra:]
                    total_written += flush_rows(writer, csv_file, buffered_rows)
                    break

                if len(buffered_rows) >= args.flush_rows:
                    total_written += flush_rows(writer, csv_file, buffered_rows)
                    print(f"Flushed {total_written} rows")
        except KeyboardInterrupt:
            pass
        finally:
            total_written += flush_rows(writer, csv_file, buffered_rows)

    print(f"Done. Total rows written: {total_written}")


if __name__ == "__main__":
    main()
