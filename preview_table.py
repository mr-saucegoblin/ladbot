"""
preview_table.py — Synthwave HTML preview of the model's top N as a single table.

One row per pick, not one card per pick. The card layout gave every name its own
block of labelled rows, which read as ten small tables stacked up: you could study
one pick but you could not scan the list or compare two names without scrolling.

Shares the palette with chart_generator.py so the preview and the price charts look
like one thing. Writes a self-contained file and opens it; nothing is posted and
nothing leaves the machine.

Usage:
    python preview_table.py
    python preview_table.py --top 20
    python preview_table.py --charts        # chart gallery under the table
    python preview_table.py --no-open
"""

import argparse
import base64
import html
import os
import tempfile
import webbrowser
from datetime import datetime

import quant_picks as qp

# Same palette as chart_generator.py — one visual language across both.
BG, PANEL, CYAN = "#0d0221", "#120028", "#00f5ff"
PINK, GOLD, LAV, GRID = "#ff2d78", "#ffd700", "#c77dff", "#2a0a4a"

OUT = os.path.join(tempfile.gettempdir(), "ladbot_picks_table.html")

CSS = f"""
:root {{ color-scheme: dark; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; background:{BG}; color:{LAV};
        font:14px/1.45 "Segoe UI",system-ui,sans-serif;
        background-image:
          linear-gradient(transparent 97%, {GRID} 97%),
          linear-gradient(90deg, transparent 97%, {GRID} 97%);
        background-size: 44px 44px; }}
.wrap {{ max-width:980px; margin:0 auto; padding:30px 16px 60px; }}
h1 {{ margin:0 0 4px; font-size:22px; letter-spacing:.14em; text-transform:uppercase;
      color:{CYAN}; text-shadow:0 0 12px {CYAN}66, 0 0 32px {CYAN}33; font-weight:700; }}
.sub {{ color:{LAV}; opacity:.7; font-size:12px; margin:0 0 20px;
        font-family:ui-monospace,Consolas,monospace; }}

table {{ width:100%; border-collapse:collapse; background:{PANEL};
         border:1px solid {GRID}; border-radius:10px; overflow:hidden; }}
thead th {{ font:600 10.5px/1 ui-monospace,Consolas,monospace; letter-spacing:.13em;
            text-transform:uppercase; color:{LAV}; opacity:.65; text-align:left;
            padding:11px 10px; border-bottom:1px solid {GRID};
            background:#16002f; position:sticky; top:0; }}
tbody td {{ padding:9px 10px; border-bottom:1px solid {GRID}; vertical-align:middle; }}
tbody tr:last-child td {{ border-bottom:none; }}
tbody tr:hover {{ background:#1a0636; }}

.rk {{ font:700 14px/1 ui-monospace,Consolas,monospace; color:{GOLD};
       text-shadow:0 0 8px {GOLD}44; width:30px; text-align:right; }}
.tk {{ font:700 14px/1.25 ui-monospace,Consolas,monospace; color:{CYAN};
       text-shadow:0 0 6px {CYAN}33; white-space:nowrap; }}
.co {{ display:block; font:400 10.5px/1.3 "Segoe UI",system-ui,sans-serif;
       color:{LAV}; opacity:.55; max-width:180px; overflow:hidden;
       text-overflow:ellipsis; white-space:nowrap; }}
.num {{ font-family:ui-monospace,Consolas,monospace; font-size:12px;
        white-space:nowrap; opacity:.8; }}
.sc {{ font:700 13px/1 ui-monospace,Consolas,monospace; color:{GOLD}; text-align:right;
       white-space:nowrap; }}

/* Chips: the label carries the direction, the number carries how far. Cyan pushed
   the score up, pink pushed it down — the same two colours as the price charts. */
.chips {{ display:flex; flex-wrap:wrap; gap:5px; }}
.chip {{ font:600 11px/1 ui-monospace,Consolas,monospace; padding:4px 7px;
         border-radius:5px; white-space:nowrap; }}
.chip b {{ font-weight:700; margin-left:4px; opacity:.85; }}
.up {{ background:rgba(0,245,255,.12); color:{CYAN}; border:1px solid rgba(0,245,255,.33); }}
.dn {{ background:rgba(255,45,120,.12); color:{PINK}; border:1px solid rgba(255,45,120,.33); }}

.gal {{ display:grid; grid-template-columns:repeat(2,1fr); gap:12px; margin-top:22px; }}
.gal figure {{ margin:0; }}
.gal img {{ width:100%; border-radius:8px; border:1px solid {GRID}; display:block; }}
.gal figcaption {{ font:600 11px/1.6 ui-monospace,Consolas,monospace; color:{CYAN};
                   opacity:.8; }}
.foot {{ margin-top:18px; font-size:11.5px; opacity:.55; line-height:1.7;
         font-family:ui-monospace,Consolas,monospace; }}

@media (max-width:720px) {{
  .co, thead th.hide, tbody td.hide {{ display:none; }}
  .wrap {{ padding:20px 10px 40px; }}
  .gal {{ grid-template-columns:1fr; }}
}}
"""


def chart_uri(ticker):
    from chart_generator import generate_chart
    path = generate_chart(ticker)
    if not path:
        return None
    try:
        with open(path, "rb") as fh:
            return "data:image/png;base64," + base64.b64encode(fh.read()).decode()
    finally:
        os.remove(path)


def money(v):
    """Market cap at a glance — billions for real companies, millions for micro caps."""
    if not v:
        return "—"
    v = float(v)
    return f"${v / 1e9:.1f}B" if v >= 1e9 else f"${v / 1e6:.0f}M"


def row(p):
    e = html.escape
    sector = qp.SECTOR_CODE.get(p.get("sector") or "", (p.get("sector") or "—")[:4])
    fsc = p.get("piotroski_score")
    chips = "".join(
        f'<span class="chip {"up" if c["helped"] else "dn"}">'
        f'{e(c["label"])}<b>{e(c["score"])}</b></span>'
        for c in qp.driver_chips(p, 3)
    )
    return (
        f'<tr><td class="rk">{p["rank"]}</td>'
        f'<td class="tk">{e((p["ticker"] or "").removesuffix(".TO"))}'
        f'<span class="co">{e(p.get("company") or "")}</span></td>'
        f'<td class="num hide">{e(sector)}</td>'
        f'<td class="num hide">{money(p.get("market_cap"))}</td>'
        f'<td class="num hide">{int(fsc) if fsc is not None else "—"}/9</td>'
        f'<td class="sc">{float(p["score"]):+.2f}</td>'
        f'<td><div class="chips">{chips}</div></td></tr>'
    )


def main(top, want_charts, open_browser):
    picks, snap = qp.load_latest_picks(top)
    if not picks:
        raise SystemExit("No scored picks yet — run 9_score.py --save first.")

    # Charts go in a gallery under the table rather than inside a cell: a 400px
    # image in a table row defeats the point of having rows you can scan.
    gallery = ""
    if want_charts:
        figs = []
        for p in picks:
            print(f"  chart: {p['ticker']}")
            uri = chart_uri(p["ticker"])
            if uri:
                figs.append(f'<figure><figcaption>{html.escape(p["ticker"])}'
                            f'</figcaption><img src="{uri}" alt=""></figure>')
        if figs:
            gallery = f'<div class="gal">{"".join(figs)}</div>'

    size = qp.universe_size(snap)
    scope = f"{size} names ranked" if size else "TSX main board"
    tag = picks[0].get("model_tag") or "n/a"

    head = ('<thead><tr><th></th><th>Ticker</th><th class="hide">Sec</th>'
            '<th class="hide">Cap</th><th class="hide">F</th>'
            '<th style="text-align:right">Score</th>'
            '<th>What drove the score</th></tr></thead>')
    body = "".join(row(p) for p in picks)

    doc = (
        f"<!doctype html><meta charset='utf-8'>"
        f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>LadBot — Top {len(picks)} Holdings</title><style>{CSS}</style>"
        f'<div class="wrap"><h1>LadBot — Top {len(picks)} Holdings</h1>'
        f'<p class="sub">snapshot {snap} &nbsp;·&nbsp; {scope} &nbsp;·&nbsp; '
        f'model {html.escape(tag)}</p>'
        f'<table>{head}<tbody>{body}</tbody></table>{gallery}'
        f'<p class="foot">Each chip names a direction and how far the stock sits '
        f'that way, 0–100 against its sector peers. '
        f'<span style="color:{CYAN}">Cyan</span> pushed the score up, '
        f'<span style="color:{PINK}">pink</span> pushed it down.<br>'
        f'generated {datetime.now():%Y-%m-%d %H:%M}</p></div>'
    )

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(doc)
    print(f"\nWrote {OUT}  ({os.path.getsize(OUT) / 1024:,.0f} KB, {len(picks)} picks)")
    if open_browser:
        webbrowser.open("file:///" + OUT.replace(os.sep, "/"))


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--top", type=int, default=10)
    a.add_argument("--charts", action="store_true", help="chart gallery below the table")
    a.add_argument("--no-open", action="store_true")
    g = a.parse_args()
    main(g.top, g.charts, not g.no_open)
