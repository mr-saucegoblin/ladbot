"""
Generate a 1-year stock price chart with 50/200-day moving averages.
Returns the path to a temporary PNG file. Caller is responsible for cleanup.
"""

import os
import tempfile
import datetime

import numpy
import pandas
import requests
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — must be set before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patheffects import withStroke


# ── 80s synthwave palette ──────────────────────────────────────────────────────
_BG      = "#0d0221"   # deep space purple
_PANEL   = "#120028"   # slightly lighter purple
_PRICE   = "#00f5ff"   # neon cyan
_MA50    = "#ff2d78"   # hot pink
_MA200   = "#ffd700"   # gold/yellow
_TEXT    = "#c77dff"   # soft lavender
_GRID    = "#2a0a4a"   # dark purple grid
_GLOW    = "#00f5ff"   # cyan glow (matches price)


def _neon(color: str):
    """Path effect that mimics a neon glow."""
    return [withStroke(linewidth=6, foreground=color, alpha=0.25)]


def generate_chart(ticker: str) -> str | None:
    """
    Download 1 year of daily close for *ticker*, plot with 50/200-day MAs
    in an 80s synthwave style. Returns temp PNG path, or None on failure.
    """
    try:
        from_date = (datetime.date.today() - datetime.timedelta(days=730)).isoformat()
        r = requests.get(
            "https://financialmodelingprep.com/stable/historical-price-eod/full",
            params={"symbol": ticker, "from": from_date, "apikey": os.getenv("FMP_API_KEY")},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    if not data or not isinstance(data, list) or len(data) < 10:
        return None

    df = pandas.DataFrame(data)[["date", "close"]].rename(columns={"close": "Close"})
    df["date"] = pandas.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    close = df["Close"]
    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    # Trim display to last 1 year, MAs computed on full 2-year history
    one_year_ago = close.index[-1] - pandas.DateOffset(years=1)
    close = close[close.index >= one_year_ago]
    ma50  = ma50[ma50.index >= one_year_ago]
    ma200 = ma200[ma200.index >= one_year_ago]

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(_BG)
    ax.set_facecolor(_PANEL)

    # ── scanline overlay (subtle horizontal bands for retro CRT feel) ──────────
    ylim_pad = (close.max() - close.min()) * 0.15
    y_bot = close.min() - ylim_pad
    y_top = close.max() + ylim_pad
    scanline_ys = numpy.linspace(y_bot, y_top, 120)
    for y in scanline_ys[::2]:
        ax.axhspan(y, y + (y_top - y_bot) / 120,
                   color="#ffffff", alpha=0.012, zorder=0)

    # ── horizon grid (perspective lines from bottom) ───────────────────────────
    for spine in ax.spines.values():
        spine.set_color(_MA50)
        spine.set_linewidth(1.2)

    ax.grid(axis="y", color="#3d1a6e", linestyle="-", linewidth=0.6, alpha=0.8, zorder=1)
    ax.grid(axis="x", color="#3d1a6e", linestyle="-", linewidth=0.4, alpha=0.5, zorder=1)

    # ── neon fill under price ──────────────────────────────────────────────────
    ax.fill_between(close.index, close, y_bot,
                    alpha=0.07, color=_PRICE, zorder=2)

    # ── price + MA lines with glow ─────────────────────────────────────────────
    ax.plot(ma200.index, ma200, color=_MA200, linewidth=1.4, label="200-day MA",
            zorder=3, path_effects=_neon(_MA200), linestyle="--")
    ax.plot(ma50.index,  ma50,  color=_MA50,  linewidth=1.6, label="50-day MA",
            zorder=4, path_effects=_neon(_MA50))
    ax.plot(close.index, close, color=_PRICE, linewidth=2.0, label="Price",
            zorder=5, path_effects=_neon(_GLOW))

    # ── axes ───────────────────────────────────────────────────────────────────
    ax.set_ylim(y_bot, y_top)
    ax.set_xlim(close.index[0], close.index[-1])
    ax.tick_params(colors=_TEXT, labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center")
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.yaxis.set_tick_params(colors=_TEXT)

    # ── title ──────────────────────────────────────────────────────────────────
    display = ticker.replace(".TO", "").replace(".V", "")
    ax.set_title(
        f"▶  {display}  —  1 YEAR",
        color=_MA50, fontsize=14, fontweight="bold", pad=12, loc="left",
        path_effects=[withStroke(linewidth=4, foreground=_MA50, alpha=0.3)],
    )

    # ── legend ─────────────────────────────────────────────────────────────────
    legend = ax.legend(
        fontsize=8, framealpha=0.4,
        facecolor="#1a0a35", edgecolor=_MA50, labelcolor=_TEXT,
    )

    plt.tight_layout(pad=1.2)

    tmp = tempfile.NamedTemporaryFile(suffix=f"_{display}.png", delete=False)
    fig.savefig(tmp.name, dpi=150, facecolor=_BG)
    plt.close(fig)
    return tmp.name
