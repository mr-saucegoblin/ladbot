"""
preview_picks.py — See what !picks would post, without posting it.

Builds the same posts the Discord command would send, generates the same charts,
and writes a single self-contained HTML file styled like Discord. Opens it in your
browser. Nothing is sent to Discord and nothing leaves your machine — the charts are
embedded as base64 so the file works on its own.

Usage:
    python preview_picks.py
    python preview_picks.py --top 10
    python preview_picks.py --no-charts    # text only, much faster
    python preview_picks.py --no-open      # write the file, don't launch a browser
"""

import argparse
import base64
import html
import os
import re
import tempfile
import webbrowser
from datetime import datetime

import quant_picks
from chart_generator import generate_chart

OUT = os.path.join(tempfile.gettempdir(), "ladbot_picks_preview.html")


def md_to_html(text: str) -> str:
    """
    Render the Discord markdown subset the posts actually use.

    Escape first, then apply formatting — the reverse order would let a company
    name containing < or & break the page.
    """
    t = html.escape(text)
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<em>\1</em>", t)
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    return t.replace("\n", "<br>")


def chart_data_uri(ticker: str) -> str | None:
    """Generate the chart and inline it, so the HTML file stands alone."""
    path = generate_chart(ticker)
    if not path:
        return None
    try:
        with open(path, "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    finally:
        os.remove(path)


CSS = """
:root { color-scheme: dark; }
body { margin:0; background:#313338; color:#dbdee1;
       font:15px/1.45 "gg sans","Segoe UI",system-ui,sans-serif; }
.wrap { max-width:780px; margin:0 auto; padding:24px 16px 64px; }
h1 { font-size:15px; color:#949ba4; font-weight:600; letter-spacing:.04em;
     text-transform:uppercase; margin:0 0 4px; }
.sub { color:#949ba4; font-size:13px; margin:0 0 24px; }
.msg { display:flex; gap:14px; padding:14px 0; border-top:1px solid #3f4147; }
.msg:first-of-type { border-top:none; }
.avatar { width:40px; height:40px; border-radius:50%; flex:0 0 40px;
          background:linear-gradient(135deg,#00f5ff,#ff2d78); }
.body { min-width:0; flex:1; }
.who { font-weight:600; color:#f2f3f5; margin-bottom:3px; }
.who span { font-weight:400; font-size:12px; color:#949ba4; margin-left:8px; }
.content strong { color:#fff; font-weight:600; }
.content code { background:#2b2d31; padding:1px 5px; border-radius:4px;
                font-family:ui-monospace,Consolas,monospace; font-size:13px; }
img.chart { width:100%; max-width:100%; border-radius:8px; margin-top:10px;
            border:1px solid #3f4147; display:block; }
.note { color:#949ba4; font-size:12px; margin-top:6px; font-style:italic; }
@media (max-width:480px){ .avatar{width:32px;height:32px;flex-basis:32px} }
"""


def build_html(posts, tickers, charts, want_charts=True) -> str:
    rows = []
    for i, post in enumerate(posts):
        img = ""
        # want_charts distinguishes "--no-charts was passed" from "the chart failed".
        # Without it the no-charts run printed "chart unavailable" five times, which
        # reads like a broken pipeline rather than a flag doing its job.
        if want_charts and i > 0 and (i - 1) < len(tickers):
            tk = tickers[i - 1]
            uri = charts.get(tk)
            img = (f'<img class="chart" alt="{html.escape(tk)} price chart" src="{uri}">'
                   if uri else f'<div class="note">chart failed to generate for '
                               f'{html.escape(tk)}</div>')
        rows.append(
            '<div class="msg"><div class="avatar"></div><div class="body">'
            '<div class="who">Ladbot <span>BOT · today</span></div>'
            f'<div class="content">{md_to_html(post)}</div>{img}'
            '</div></div>'
        )
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (f"<title>!picks preview</title><style>{CSS}</style>"
            f'<div class="wrap"><h1>!picks preview</h1>'
            f'<p class="sub">Generated {stamp} — not posted to Discord</p>'
            f'{"".join(rows)}</div>')


def main(top, want_charts, open_browser):
    posts, tickers, err = quant_picks.build_posts(top)
    if err:
        raise SystemExit(err)

    charts = {}
    if want_charts:
        for tk in tickers:
            print(f"  chart: {tk}")
            charts[tk] = chart_data_uri(tk)

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(build_html(posts, tickers, charts, want_charts))

    size = os.path.getsize(OUT) / 1024
    print(f"\nWrote {OUT}  ({size:,.0f} KB, {len(posts)} posts, "
          f"{sum(1 for v in charts.values() if v)} charts)")
    if open_browser:
        webbrowser.open(f"file:///{OUT.replace(os.sep, '/')}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--no-charts", action="store_true")
    p.add_argument("--no-open", action="store_true")
    a = p.parse_args()
    main(a.top, not a.no_charts, not a.no_open)
