"""
table_image.py — render the model's top N as a PNG, in the same synthwave style
as chart_generator.py.

Why an image instead of a text table: a Discord code block gets eight ANSI colours
and one monospace font, and bold glyphs there are wider than regular ones, so the
columns fight you. matplotlib draws whatever we want — the real #00f5ff, the neon
glow path effect the charts already use, rounded chips, alternating row bands. The
table then looks like it belongs next to the price charts instead of beside them.

What it costs: the numbers stop being selectable text, and a screen reader cannot
read them. quant_picks.build_table_embed() is kept for that reason — same data,
copyable, and it is the fallback if rendering ever fails.

Returns a path to a temporary PNG. The caller deletes it.
"""

import tempfile

import matplotlib
matplotlib.use("Agg")  # non-interactive backend — must be set before pyplot import
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from matplotlib.patheffects import withStroke

import quant_picks as qp

# ── 80s synthwave palette — identical to chart_generator.py ────────────────────
_BG    = "#0d0221"   # deep space purple
_PANEL = "#120028"   # slightly lighter purple
_CYAN  = "#00f5ff"   # neon cyan
_PINK  = "#ff2d78"   # hot pink
_GOLD  = "#ffd700"   # gold/yellow
_TEXT  = "#c77dff"   # soft lavender
_GRID  = "#2a0a4a"   # dark purple grid

MONO = "DejaVu Sans Mono"   # ships with matplotlib, so Railway has it too
SANS = "DejaVu Sans"

# Layout in axis units: x runs 0-100 across the width, y is one unit per row.
# Keeping everything in these units means the figure can grow with the row count
# without re-tuning any position.
X_RANK, X_TICK, X_SEC, X_CAP, X_SCORE, X_CHIP = 2.6, 6.0, 26.0, 34.0, 48.0, 52.0
ROW_H_INCHES = 0.40
CHAR_W = 0.78        # width of one monospace character, in x units, at CHIP_FS
CHIP_FS = 9.5
# Widest label is 10 characters ("low op mgn"), widest score is 3 ("100"), plus
# internal padding. Fixed so the three chip columns line up on every row.
CHIP_W = 14 * CHAR_W + 2.4
CHIP_GAP = 1.4


def _clip(text: str, n: int) -> str:
    """Trim to n characters on a word boundary, with an ellipsis if anything went.

    A hard slice produced "Tidewater Midstream and In" and "Doman Building
    Materials G", which read as typos rather than truncation.
    """
    text = text.strip()
    if len(text) <= n:
        return text
    cut = text[:n].rsplit(" ", 1)[0]
    return (cut if cut else text[:n]) + "…"


def _neon(color, lw=4, alpha=0.30):
    """The same glow the price lines use, so the two images share a vocabulary."""
    return [withStroke(linewidth=lw, foreground=color, alpha=alpha)]


def _chip(ax, slot, y, label, score, helped):
    """
    A rounded pill: trait name on the left, its 0-100 score on the right.

    Every chip is the SAME width, and chips sit in fixed slots rather than being
    packed end to end. Packing sized each pill to its text, so a short label on one
    row shifted every chip after it and the columns sagged — the same defect the
    ANSI table had. A fixed slot costs a little whitespace after "buybacks" and
    buys a score column you can read straight down.
    """
    colour = _CYAN if helped else _PINK
    x = X_CHIP + slot * (CHIP_W + CHIP_GAP)

    ax.add_patch(FancyBboxPatch(
        (x, y - 0.30), CHIP_W, 0.60,
        boxstyle="round,pad=0.15,rounding_size=0.25",
        facecolor=colour, alpha=0.13, edgecolor=colour, linewidth=0.9,
        zorder=2,
    ))
    ax.text(x + 1.0, y, label, color=colour, fontsize=CHIP_FS, fontfamily=MONO,
            va="center", ha="left", alpha=0.92, zorder=3)
    ax.text(x + CHIP_W - 1.0, y, score, color=colour,
            fontsize=CHIP_FS, fontfamily=MONO, fontweight="bold",
            va="center", ha="right", zorder=3,
            path_effects=_neon(colour, lw=2.5, alpha=0.35))


def render(picks, snap, scope, out_path=None) -> str:
    n = len(picks)
    # 1.75 units of title, n rows, 1.0 of footer.
    units = n + 2.75
    fig = plt.figure(figsize=(11.5, units * ROW_H_INCHES), dpi=150)
    fig.patch.set_facecolor(_BG)

    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(units, 0)          # inverted: y grows downward, like rows
    ax.axis("off")
    ax.set_facecolor(_BG)

    # ── title ──────────────────────────────────────────────────────────────────
    ax.text(X_RANK, 0.72, f"▶  LADBOT  —  TOP {n} HOLDINGS",
            color=_PINK, fontsize=16, fontweight="bold", fontfamily=SANS,
            va="center", ha="left", path_effects=_neon(_PINK, lw=5, alpha=0.30))
    ax.text(100 - X_RANK, 0.76, str(snap), color=_TEXT, fontsize=9.5,
            fontfamily=MONO, va="center", ha="right", alpha=0.65)

    # ── column headings ────────────────────────────────────────────────────────
    y_head = 1.72
    for x, label, ha in ((X_RANK + 1.1, "#", "right"), (X_TICK, "TICKER", "left"),
                         (X_SEC, "SECTOR", "left"), (X_CAP, "MKT CAP", "left"),
                         (X_SCORE, "SCORE", "right"),
                         (X_CHIP, "WHAT DROVE THE SCORE", "left")):
        ax.text(x, y_head, label, color=_TEXT, fontsize=8, fontfamily=MONO,
                fontweight="bold", va="center", ha=ha, alpha=0.5)
    # Just above the first row's band (which starts at 2.45 - 0.42), not through it.
    ax.plot([X_RANK - 0.6, X_CHIP + 3 * (CHIP_W + CHIP_GAP) - CHIP_GAP],
            [2.00] * 2, color=_GRID, linewidth=1.1, zorder=1)

    # ── rows ───────────────────────────────────────────────────────────────────
    for i, p in enumerate(picks):
        y = 2.45 + i

        # Alternating band. Low alpha so it separates rows without becoming a
        # stripe pattern that competes with the chips.
        if i % 2 == 0:
            ax.add_patch(FancyBboxPatch(
                (X_RANK - 0.6, y - 0.42), 100 - 2 * X_RANK + 1.2, 0.84,
                boxstyle="round,pad=0,rounding_size=0.12",
                facecolor=_PANEL, edgecolor="none", zorder=0,
            ))

        ax.text(X_RANK + 1.1, y, str(p["rank"]), color=_GOLD, fontsize=11,
                fontfamily=MONO, fontweight="bold", va="center", ha="right",
                path_effects=_neon(_GOLD, lw=3, alpha=0.28))

        tick = (p["ticker"] or "").removesuffix(".TO")
        ax.text(X_TICK, y - 0.12, tick, color=_CYAN, fontsize=12.5,
                fontfamily=MONO, fontweight="bold", va="center", ha="left",
                path_effects=_neon(_CYAN, lw=4, alpha=0.30))
        company = _clip(p.get("company") or "", 28)
        ax.text(X_TICK, y + 0.26, company, color=_TEXT, fontsize=6.8,
                fontfamily=SANS, va="center", ha="left", alpha=0.52)

        raw = p.get("sector") or ""
        ax.text(X_SEC, y, qp.SECTOR_CODE.get(raw, raw[:4]), color=_TEXT,
                fontsize=9, fontfamily=MONO, va="center", ha="left", alpha=0.75)

        mc = p.get("market_cap")
        cap = ("—" if not mc else
               f"${float(mc)/1e9:.1f}B" if float(mc) >= 1e9 else
               f"${float(mc)/1e6:.0f}M")
        ax.text(X_CAP, y, cap, color=_TEXT, fontsize=9, fontfamily=MONO,
                va="center", ha="left", alpha=0.75)

        ax.text(X_SCORE, y, f"{float(p['score']):+.2f}", color=_GOLD, fontsize=10.5,
                fontfamily=MONO, fontweight="bold", va="center", ha="right",
                alpha=0.92)

        for slot, c in enumerate(qp.driver_chips(p, 3)):
            _chip(ax, slot, y, c["label"], c["score"], c["helped"])

    # ── footer ─────────────────────────────────────────────────────────────────
    ax.text(X_RANK, units - 0.55,
            f"{scope}   ·   chip score = how strongly it shows that trait "
            f"(0-100 vs sector)   ·   cyan helped, pink hurt   ·   not advice",
            color=_TEXT, fontsize=7.5, fontfamily=MONO, va="center", ha="left",
            alpha=0.45)

    path = out_path or tempfile.NamedTemporaryFile(
        suffix="_ladbot_picks.png", delete=False).name
    fig.savefig(path, facecolor=_BG, dpi=150)
    plt.close(fig)
    return path


def render_latest(n: int = 10, out_path=None):
    """Convenience wrapper: pull the newest scored picks and render them."""
    picks, snap = qp.load_latest_picks(n)
    if not picks:
        return None, ("No scored picks yet — run `9_score.py --save` "
                      "to publish a snapshot.")
    size = qp.universe_size(snap)
    scope = f"ranked {size} TSX names" if size else "TSX main board"
    return render(picks, snap, scope, out_path), None


if __name__ == "__main__":
    import argparse
    a = argparse.ArgumentParser()
    a.add_argument("--top", type=int, default=10)
    a.add_argument("--out", default="picks_table.png")
    g = a.parse_args()
    path, err = render_latest(g.top, g.out)
    print(err or f"Wrote {path}")
