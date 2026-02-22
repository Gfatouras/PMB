"""Live BTC bid/ask/mid chart from Kraken top-of-book updates.

Requires:
    pip install pyqtgraph PyQt6 websocket-client
"""

from __future__ import annotations

import argparse
import math
import queue
import threading
import time
from collections import deque
from datetime import datetime

try:
    import pyqtgraph as pg
    from pyqtgraph.Qt import QtCore, QtWidgets
except ImportError as exc:
    raise SystemExit("Missing dependency. Install: pip install pyqtgraph PyQt6 websocket-client") from exc

from fetch_btc import (
    DEFAULT_BOOK_DEPTH,
    DEFAULT_SYMBOL,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_WS_URL,
    compute_mid_price,
    stream_updates,
)

SMA_WINDOW_UPDATES = 25


def parse_args():
    parser = argparse.ArgumentParser(description="Live BTC chart of bid, ask, and mid (bid/ask average).")
    parser.add_argument("--symbol", type=str, default=DEFAULT_SYMBOL, help="Kraken symbol (default: BTC/USD).")
    parser.add_argument("--url", type=str, default=DEFAULT_WS_URL, help="Kraken WebSocket URL.")
    parser.add_argument("--book-depth", type=int, default=DEFAULT_BOOK_DEPTH, help="Kraken book depth.")
    parser.add_argument("--window-minutes", type=float, default=5.0, help="Rolling plot window in minutes.")
    parser.add_argument("--refresh-ms", type=int, default=100, help="UI refresh interval in milliseconds.")
    parser.add_argument("--reconnect-seconds", type=float, default=2.0, help="Kraken reconnect delay.")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Kraken socket timeout.")
    args = parser.parse_args()

    if args.window_minutes <= 0:
        raise SystemExit("--window-minutes must be > 0")
    if args.refresh_ms <= 0:
        raise SystemExit("--refresh-ms must be > 0")
    return args


def fmt_price(value):
    return "n/a" if value is None else f"${value:,.2f}"


class BookFeedWorker(threading.Thread):
    def __init__(self, out_queue: queue.SimpleQueue, stop_event: threading.Event, args):
        super().__init__(daemon=True)
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.args = args

    def run(self):
        last_bid = None
        last_ask = None

        for channel, update in stream_updates(
            symbol=self.args.symbol,
            url=self.args.url,
            channels=["book"],
            book_depth=self.args.book_depth,
            reconnect_seconds=self.args.reconnect_seconds,
            timeout_seconds=self.args.timeout_seconds,
        ):
            if self.stop_event.is_set():
                return

            if channel == "error":
                self.out_queue.put({"type": "error", "error": update.get("error", "unknown stream error")})
                continue

            if channel != "book":
                continue

            bid = update.get("bid_price")
            ask = update.get("ask_price")
            if bid is not None:
                last_bid = bid
            if ask is not None:
                last_ask = ask

            mid = compute_mid_price(last_bid, last_ask)
            if last_bid is None or last_ask is None or mid is None:
                continue

            ts = update.get("ts")
            ts_epoch = ts.timestamp() if isinstance(ts, datetime) else time.time()
            self.out_queue.put({"type": "tick", "ts": ts_epoch, "bid": last_bid, "ask": last_ask, "mid": mid})


class BidAskMidChart(QtWidgets.QWidget):
    def __init__(self, args):
        super().__init__()
        self.args = args
        self.window_seconds = args.window_minutes * 60.0

        self.queue: queue.SimpleQueue = queue.SimpleQueue()
        self.stop_event = threading.Event()
        self.worker = BookFeedWorker(self.queue, self.stop_event, args)

        self.last_error = None
        self.xs = deque()
        self.bids = deque()
        self.asks = deque()
        self.mids = deque()
        self.mid_ma3 = deque()

        self._build_ui()
        self.worker.start()

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.refresh_ui)
        self.timer.start(args.refresh_ms)

    def closeEvent(self, event):
        self.stop_event.set()
        super().closeEvent(event)

    def _build_ui(self):
        self.setWindowTitle(f"{self.args.symbol} Bid / Ask / Mid")
        self.resize(1280, 860)
        self.setStyleSheet(
            """
            QWidget {
                background-color: #0d1117;
                color: #dbe4ee;
                font-family: Segoe UI;
            }
            QLabel#Title {
                font-size: 30px;
                font-weight: 700;
                color: #f3f7fb;
            }
            QLabel#Sub {
                font-size: 14px;
                color: #93a5b8;
            }
            QLabel#Cap {
                font-size: 12px;
                color: #7f92a6;
                font-weight: 700;
            }
            QLabel#Val {
                font-size: 28px;
                font-weight: 700;
            }
            """
        )

        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(8)

        self.title = QtWidgets.QLabel("BTC Bid / Ask / Avg (Mid)")
        self.title.setObjectName("Title")
        self.subtitle = QtWidgets.QLabel("Waiting for Kraken book data...")
        self.subtitle.setObjectName("Sub")
        root.addWidget(self.title)
        root.addWidget(self.subtitle)

        stats_row = QtWidgets.QHBoxLayout()
        stats_row.setSpacing(24)
        self.bid_val = self._make_stat(stats_row, "BID", "#52d273")
        self.ask_val = self._make_stat(stats_row, "ASK", "#ff6b6b")
        self.mid_val = self._make_stat(stats_row, "AVG (MID)", "#5cc7ff")
        self.spread_val = self._make_stat(stats_row, "SPREAD", "#ffcc66")
        stats_row.addStretch(1)
        root.addLayout(stats_row)

        pg.setConfigOptions(antialias=False)
        chart = pg.GraphicsLayoutWidget()
        chart.setBackground("#0d1117")
        root.addWidget(chart, stretch=1)

        self.plot = chart.addPlot(axisItems={"bottom": pg.DateAxisItem(orientation="bottom")})
        self.plot.showGrid(x=True, y=True, alpha=0.20)
        self.plot.setClipToView(True)
        self.plot.setDownsampling(auto=True, mode="peak")
        self.plot.setLabel("left", "BTC Price (USD)")
        self.plot.setLabel("bottom", "Time")

        self.bid_curve = self.plot.plot([], [], pen=pg.mkPen((82, 210, 115, 128), width=1.8))
        self.ask_curve = self.plot.plot([], [], pen=pg.mkPen((255, 107, 107, 128), width=1.8))
        self.mid_curve = self.plot.plot([], [], pen=pg.mkPen("#5cc7ff", width=2.2))
        self.mid_dot = pg.ScatterPlotItem(size=8, pen=pg.mkPen("#5cc7ff", width=2), brush=pg.mkBrush("#0d1117"))
        self.plot.addItem(self.mid_dot)

        legend = self.plot.addLegend(offset=(10, 10))
        legend.addItem(self.bid_curve, "Bid")
        legend.addItem(self.ask_curve, "Ask")
        legend.addItem(self.mid_curve, "Avg (Mid)")

        chart.nextRow()
        self.ma_plot = chart.addPlot(axisItems={"bottom": pg.DateAxisItem(orientation="bottom")})
        self.ma_plot.setXLink(self.plot)
        self.ma_plot.showGrid(x=True, y=True, alpha=0.20)
        self.ma_plot.setClipToView(True)
        self.ma_plot.setDownsampling(auto=True, mode="peak")
        self.ma_plot.setLabel("left", f"{SMA_WINDOW_UPDATES}-point SMA")
        self.ma_plot.setLabel("bottom", "Time")
        self.ma_curve = self.ma_plot.plot([], [], pen=pg.mkPen("#ffd166", width=2.0))
        self.ma_dot = pg.ScatterPlotItem(size=7, pen=pg.mkPen("#ffd166", width=2), brush=pg.mkBrush("#0d1117"))
        self.ma_plot.addItem(self.ma_dot)

    def _make_stat(self, parent_layout, label: str, color: str):
        col = QtWidgets.QVBoxLayout()
        cap = QtWidgets.QLabel(label)
        cap.setObjectName("Cap")
        val = QtWidgets.QLabel("n/a")
        val.setObjectName("Val")
        val.setStyleSheet(f"color: {color};")
        col.addWidget(cap)
        col.addWidget(val)
        parent_layout.addLayout(col)
        return val

    def _drain_queue(self):
        changed = False
        while True:
            try:
                event = self.queue.get_nowait()
            except queue.Empty:
                break

            kind = event.get("type")
            if kind == "error":
                self.last_error = event.get("error", "unknown stream error")
                continue
            if kind != "tick":
                continue

            self.xs.append(event["ts"])
            self.bids.append(float(event["bid"]))
            self.asks.append(float(event["ask"]))
            self.mids.append(float(event["mid"]))
            if len(self.mids) >= SMA_WINDOW_UPDATES:
                # Standard trailing SMA over the most recent SMA_WINDOW_UPDATES mids.
                mids_list = list(self.mids)
                ma3 = sum(mids_list[-SMA_WINDOW_UPDATES:]) / float(SMA_WINDOW_UPDATES)
            else:
                ma3 = math.nan
            self.mid_ma3.append(ma3)
            changed = True

        cutoff = time.time() - self.window_seconds
        while self.xs and self.xs[0] < cutoff:
            self.xs.popleft()
            self.bids.popleft()
            self.asks.popleft()
            self.mids.popleft()
            self.mid_ma3.popleft()
            changed = True
        return changed

    def _update_stats(self):
        if not self.mids:
            self.bid_val.setText("n/a")
            self.ask_val.setText("n/a")
            self.mid_val.setText("n/a")
            self.spread_val.setText("n/a")
            status = "Waiting for Kraken book data..."
            if self.last_error:
                status = f"Stream error: {self.last_error}"
            self.subtitle.setText(status)
            return

        bid = self.bids[-1]
        ask = self.asks[-1]
        mid = self.mids[-1]
        spread = ask - bid

        self.bid_val.setText(fmt_price(bid))
        self.ask_val.setText(fmt_price(ask))
        self.mid_val.setText(fmt_price(mid))
        self.spread_val.setText(f"${spread:,.2f}")
        self.subtitle.setText(f"Top-of-book updates from Kraken | {self.args.symbol}")

    def _update_plot(self):
        if not self.xs:
            status = "waiting for data..."
            if self.last_error:
                status = f"waiting for data... stream error: {self.last_error}"
            self.setWindowTitle(f"{self.args.symbol} | {status}")
            return

        x_vals = list(self.xs)
        bid_vals = list(self.bids)
        ask_vals = list(self.asks)
        mid_vals = list(self.mids)
        ma_vals = list(self.mid_ma3)

        self.bid_curve.setData(x_vals, bid_vals)
        self.ask_curve.setData(x_vals, ask_vals)
        self.mid_curve.setData(x_vals, mid_vals)
        self.mid_dot.setData([x_vals[-1]], [mid_vals[-1]])
        self.ma_curve.setData(x_vals, ma_vals)
        if ma_vals and not math.isnan(ma_vals[-1]):
            self.ma_dot.setData([x_vals[-1]], [ma_vals[-1]])
        else:
            self.ma_dot.setData([], [])

        right = x_vals[-1]
        left = max(right - self.window_seconds, x_vals[0])
        self.plot.setXRange(left, right, padding=0.0)
        self.ma_plot.setXRange(left, right, padding=0.0)

        self.setWindowTitle(
            f"{self.args.symbol} | bid={bid_vals[-1]:,.2f} | ask={ask_vals[-1]:,.2f} | mid={mid_vals[-1]:,.2f}"
        )

    def refresh_ui(self):
        self._drain_queue()
        self._update_stats()
        self._update_plot()


def main():
    args = parse_args()
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    window = BidAskMidChart(args)
    app.aboutToQuit.connect(window.stop_event.set)
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
