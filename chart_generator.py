"""
Generate a 1-year stock price chart with 50/200-day moving averages.
Returns the path to a temporary PNG file. Caller is responsible for cleanup.
"""

import os
import tempfile

import matplotlib
matplotlib.use("Agg")  # non-interactive backend — must be set before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import yfinance as yf


_BG = "#1e2124"
_PANEL = "#2c2f33"
_PRICE = "#ffffff"
_MA50 = "#f0b132"    # gold
_MA200 = "#e74c3c"   # red
_TEXT = "#99aab5"
_GRID = "#3a3d42"


def generate_chart(ticker: str) -> str | None:
    """
    Download 1 year of daily OHLC for *ticker*, plot close price + 50/200-day
    MAs, and save to a temp PNG.  Returns the file path, or None on failure.
    """
    try:
        df = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=True)
    except Exception:
        return None

    if df.empty or len(df) < 10:
        return None

    close = df["Close"].squeeze()
    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    fig, ax = plt.subplots(figsize=(10, 4.5))
    fig.patch.set_facecolor(_BG)
    ax.set_facecolor(_PANEL)

    ax.plot(close.index, close,  color=_PRICE, linewidth=1.5, label="Price", zorder=3)
    ax.plot(ma50.index,  ma50,   color=_MA50,  linewidth=1.2, label="50-day MA", zorder=2)
    ax.plot(ma200.index, ma200,  color=_MA200, linewidth=1.2, label="200-day MA", zorder=2)

    # subtle fill under price line
    ax.fill_between(close.index, close, close.min() * 0.98,
                    alpha=0.08, color=_PRICE)

    # axes styling
    for spine in ax.spines.values():
        spine.set_color(_GRID)
    ax.tick_params(colors=_TEXT, labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center")
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.grid(color=_GRID, linestyle="--", linewidth=0.5, alpha=0.6)

    # title + legend
    display = ticker.replace(".TO", "").replace(".V", "")
    ax.set_title(f"{display}  — 1 Year", color=_PRICE, fontsize=13,
                 fontweight="bold", pad=10, loc="left")
    legend = ax.legend(fontsize=8, framealpha=0.3, facecolor=_PANEL,
                       edgecolor=_GRID, labelcolor=_TEXT)

    plt.tight_layout(pad=1.2)

    tmp = tempfile.NamedTemporaryFile(suffix=f"_{display}.png", delete=False)
    fig.savefig(tmp.name, dpi=150, facecolor=_BG)
    plt.close(fig)
    return tmp.name
