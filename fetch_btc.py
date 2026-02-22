"""Kraken v2 BTC stream (ticker/trade/book) with one-line in-place console updates.

Requires: pip install websocket-client
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime

from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

DEFAULT_WS_URL = "wss://ws.kraken.com/v2"
DEFAULT_SYMBOL = "BTC/USD"
DEFAULT_CHANNELS = ["ticker", "trade", "book"]
DEFAULT_BOOK_DEPTH = 10
DEFAULT_TICKER_EVENT_TRIGGER = "trades"
DEFAULT_RECONNECT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 20.0

ALLOWED_CHANNELS = {"ticker", "trade", "book"}
ALLOWED_BOOK_DEPTHS = {10, 25, 100, 500, 1000}
ALLOWED_TICKER_EVENT_TRIGGERS = {"trades", "bbo"}
STREAM_EXCEPTIONS = (
    OSError,
    ValueError,
    ConnectionError,
    json.JSONDecodeError,
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
)


def parse_channels(raw: str) -> list[str]:
    items = [x.strip().lower() for x in raw.split(",") if x.strip()]
    if not items:
        raise argparse.ArgumentTypeError("At least one channel is required.")
    bad = [x for x in items if x not in ALLOWED_CHANNELS]
    if bad:
        raise argparse.ArgumentTypeError(f"Invalid channel(s): {', '.join(bad)}")
    out, seen = [], set()
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def norm_symbol(value) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper().replace("XBT", "BTC")


def as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_ts(value) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone()
        except ValueError:
            pass
    return datetime.now()


def fmt_price(value):
    return "n/a" if value is None else f"{value:,.2f}"


def fmt_qty(value):
    return "n/a" if value is None else f"{value:,.6f}"


def fmt_level(price, qty):
    if price is None:
        return "n/a"
    return f"{price:,.2f}" if qty is None else f"{price:,.2f} ({qty:,.6f})"


def clip_terminal(line: str) -> str:
    width = max(20, shutil.get_terminal_size(fallback=(120, 24)).columns)
    return line[: width - 1] if len(line) >= width else line


def write_in_place(line: str, line_width: int) -> int:
    line = clip_terminal(line)
    line_width = max(line_width, len(line))
    sys.stdout.write("\r" + line.ljust(line_width))
    sys.stdout.flush()
    return line_width


def parse_args():
    parser = argparse.ArgumentParser(description="Kraken v2 BTC stream with separated ticker/trade/book handling.")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL, help="Kraken symbol (default: BTC/USD).")
    parser.add_argument("--url", type=str, default=DEFAULT_WS_URL, help="WebSocket URL.")
    parser.add_argument(
        "--channels",
        type=parse_channels,
        default=list(DEFAULT_CHANNELS),
        help="Comma-separated channels: ticker,trade,book.",
    )
    parser.add_argument("--book-depth", type=int, default=DEFAULT_BOOK_DEPTH, help="Book depth.")
    parser.add_argument("--ticker-event-trigger", type=str, default=DEFAULT_TICKER_EVENT_TRIGGER, help="trades or bbo.")
    parser.add_argument("--reconnect-seconds", type=float, default=DEFAULT_RECONNECT_SECONDS, help="Reconnect delay.")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Socket timeout.")
    parser.add_argument("--once", action="store_true", help="Print one update and exit.")
    args = parser.parse_args()

    args.symbol = norm_symbol(args.symbol)
    args.ticker_event_trigger = args.ticker_event_trigger.strip().lower()
    if args.book_depth not in ALLOWED_BOOK_DEPTHS:
        allowed = ", ".join(str(v) for v in sorted(ALLOWED_BOOK_DEPTHS))
        raise SystemExit(f"--book-depth must be one of: {allowed}")
    if args.ticker_event_trigger not in ALLOWED_TICKER_EVENT_TRIGGERS:
        allowed = ", ".join(sorted(ALLOWED_TICKER_EVENT_TRIGGERS))
        raise SystemExit(f"--ticker-event-trigger must be one of: {allowed}")
    return args


def build_subscriptions(symbol: str, channels: list[str], book_depth: int, ticker_trigger: str):
    requests = []
    for req_id, channel in enumerate(channels, start=1):
        params = {"channel": channel, "symbol": [symbol]}
        if channel == "ticker":
            params["snapshot"] = True
            params["event_trigger"] = ticker_trigger
        elif channel == "trade":
            params["snapshot"] = False
        elif channel == "book":
            params["snapshot"] = True
            params["depth"] = book_depth
        requests.append({"method": "subscribe", "params": params, "req_id": req_id})
    return requests


def best_level(levels, is_bid: bool):
    if not isinstance(levels, list):
        return None, None
    points = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price, qty = as_float(level.get("price")), as_float(level.get("qty"))
        if price is not None:
            points.append((price, qty))
    if not points:
        return None, None
    return max(points, key=lambda x: x[0]) if is_bid else min(points, key=lambda x: x[0])


def parse_ticker(payload: dict):
    data = payload.get("data")
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return None
    row = data[0]
    symbol = norm_symbol(row.get("symbol"))
    if not symbol:
        return None
    return {
        "type": str(payload.get("type") or "update"),
        "symbol": symbol,
        "last": as_float(row.get("last")),
        "bid": as_float(row.get("bid")),
        "ask": as_float(row.get("ask")),
        "bid_qty": as_float(row.get("bid_qty")),
        "ask_qty": as_float(row.get("ask_qty")),
        "change_pct": as_float(row.get("change_pct")),
        "ts": as_ts(row.get("timestamp")),
    }


def parse_trade(payload: dict):
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        return None
    rows = [x for x in data if isinstance(x, dict)]
    if not rows:
        return None
    row = rows[-1]
    symbol = norm_symbol(row.get("symbol"))
    if not symbol:
        return None
    return {
        "type": str(payload.get("type") or "update"),
        "symbol": symbol,
        "side": str(row.get("side") or "unknown").upper(),
        "qty": as_float(row.get("qty")),
        "price": as_float(row.get("price")),
        "ord_type": str(row.get("ord_type") or "unknown"),
        "trade_id": row.get("trade_id"),
        "batch_size": len(rows),
        "ts": as_ts(row.get("timestamp")),
    }


def parse_book(payload: dict):
    data = payload.get("data")
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return None
    row = data[0]
    symbol = norm_symbol(row.get("symbol"))
    if not symbol:
        return None
    bids, asks = row.get("bids"), row.get("asks")
    bid_price, bid_qty = best_level(bids, True)
    ask_price, ask_qty = best_level(asks, False)
    return {
        "type": str(payload.get("type") or "update"),
        "symbol": symbol,
        "bid_price": bid_price,
        "bid_qty": bid_qty,
        "ask_price": ask_price,
        "ask_qty": ask_qty,
        "bid_updates": len(bids) if isinstance(bids, list) else 0,
        "ask_updates": len(asks) if isinstance(asks, list) else 0,
        "checksum": row.get("checksum"),
        "ts": as_ts(row.get("timestamp")),
    }


def render_ticker(update: dict, previous_last):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tick = update["ts"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    parts = [
        f"[{now}] [ticker:{update['type']}] {update['symbol']}",
        f"last={fmt_price(update['last'])}",
        f"bid={fmt_level(update['bid'], update['bid_qty'])}",
        f"ask={fmt_level(update['ask'], update['ask_qty'])}",
    ]
    mid = compute_mid_price(update["bid"], update["ask"])
    if mid is not None:
        parts.append(f"mid={fmt_price(mid)}")
    if update["last"] is not None and previous_last not in (None, 0):
        move = update["last"] - previous_last
        parts.append(f"move={move:+.2f} ({(move / previous_last) * 100:+.4f}%)")
    if update["change_pct"] is not None:
        parts.append(f"chg24h={update['change_pct']:+.2f}%")
    parts.append(f"tick={tick}")
    return " ".join(parts)


def render_trade(update: dict):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tick = update["ts"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    trade_id = update["trade_id"] if update["trade_id"] is not None else "n/a"
    return (
        f"[{now}] [trade:{update['type']}] {update['symbol']} {update['side']} "
        f"qty={fmt_qty(update['qty'])} price={fmt_price(update['price'])} id={trade_id} "
        f"ord={update['ord_type']} batch={update['batch_size']} tick={tick}"
    )


def render_book(update: dict, state: dict):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tick = update["ts"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    bid_price, bid_qty = state.get("bid_price"), state.get("bid_qty")
    ask_price, ask_qty = state.get("ask_price"), state.get("ask_qty")
    spread = "n/a" if (bid_price is None or ask_price is None) else f"{(ask_price - bid_price):,.2f}"
    mid = compute_mid_price(bid_price, ask_price)
    checksum = update["checksum"] if update["checksum"] is not None else "n/a"
    return (
        f"[{now}] [book:{update['type']}] {update['symbol']} "
        f"bid={fmt_level(bid_price, bid_qty)} ask={fmt_level(ask_price, ask_qty)} "
        f"mid={fmt_price(mid)} spread={spread} "
        f"updates(b/a)={update['bid_updates']}/{update['ask_updates']} checksum={checksum} tick={tick}"
    )


def compute_mid_price(bid_price, ask_price):
    bid = as_float(bid_price)
    ask = as_float(ask_price)
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def compute_live_price(last_trade_price, bid_price, ask_price):
    """Return a single display price: latest trade when available, otherwise quote midpoint."""
    trade = as_float(last_trade_price)
    if trade is not None:
        return trade
    return compute_mid_price(bid_price, ask_price)


def stream_updates(
    *,
    symbol: str = DEFAULT_SYMBOL,
    url: str = DEFAULT_WS_URL,
    channels: list[str] | None = None,
    book_depth: int = DEFAULT_BOOK_DEPTH,
    ticker_event_trigger: str = DEFAULT_TICKER_EVENT_TRIGGER,
    reconnect_seconds: float = DEFAULT_RECONNECT_SECONDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
):
    """Yield parsed Kraken updates as (channel, update_dict), reconnecting on errors."""
    symbol = norm_symbol(symbol)
    channels = list(channels if channels is not None else DEFAULT_CHANNELS)
    if not channels:
        raise ValueError("At least one channel is required.")
    reconnect_seconds = max(reconnect_seconds, 0.1)
    timeout_seconds = max(timeout_seconds, 1.0)

    while True:
        ws = None
        try:
            ws = create_connection(url, timeout=timeout_seconds)
            for req in build_subscriptions(symbol, channels, book_depth, ticker_event_trigger):
                ws.send(json.dumps(req, separators=(",", ":")))

            while True:
                payload = json.loads(ws.recv())
                if not isinstance(payload, dict):
                    continue
                if payload.get("method") == "subscribe":
                    if payload.get("success") is False:
                        raise ConnectionError(f"Subscribe failed: {payload.get('error', 'unknown error')}")
                    continue
                if payload.get("method") == "unsubscribe":
                    continue

                channel = payload.get("channel")
                if channel == "ticker":
                    update = parse_ticker(payload)
                elif channel == "trade":
                    update = parse_trade(payload)
                elif channel == "book":
                    update = parse_book(payload)
                else:
                    continue

                if update:
                    yield channel, update
        except STREAM_EXCEPTIONS as exc:
            yield "error", {"error": str(exc), "ts": datetime.now()}
            time.sleep(reconnect_seconds)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except OSError:
                    pass


def main():
    args = parse_args()
    reconnect_seconds = max(args.reconnect_seconds, 0.1)
    previous_ticker_last: dict[str, float] = {}
    top_of_book: dict[str, dict] = {}
    line_width = 0

    try:
        for channel, u in stream_updates(
            symbol=args.symbol,
            url=args.url,
            channels=args.channels,
            book_depth=args.book_depth,
            ticker_event_trigger=args.ticker_event_trigger,
            reconnect_seconds=args.reconnect_seconds,
            timeout_seconds=args.timeout_seconds,
        ):
            if channel == "error":
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                line_width = write_in_place(
                    f"[{now}] Stream error: {u['error']}; reconnecting in {reconnect_seconds:.1f}s", line_width
                )
                continue

            if channel == "ticker":
                line = render_ticker(u, previous_ticker_last.get(u["symbol"]))
                if u["last"] is not None:
                    previous_ticker_last[u["symbol"]] = u["last"]
            elif channel == "trade":
                line = render_trade(u)
            elif channel == "book":
                state = top_of_book.setdefault(
                    u["symbol"], {"bid_price": None, "bid_qty": None, "ask_price": None, "ask_qty": None}
                )
                if u["bid_price"] is not None:
                    state["bid_price"], state["bid_qty"] = u["bid_price"], u["bid_qty"]
                if u["ask_price"] is not None:
                    state["ask_price"], state["ask_qty"] = u["ask_price"], u["ask_qty"]
                line = render_book(u, state)
            else:
                continue

            line_width = write_in_place(line, line_width)
            if args.once:
                return
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
