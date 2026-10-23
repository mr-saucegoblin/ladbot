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

# Layout in axis units: x runs 0-100 across the width, y is one unit per text
# line. Everything is expressed in these units so the figure can grow with the row
# count without any position needing to be re-tuned.
#
# PORTRAIT ON PURPOSE. The first version was 11.5 x 5.1 inches - a 2.25:1
# landscape strip. Discord scales an image to the message width, so on a phone
# that became roughly 400px wide and the 9pt type rendered at about 4pt: legible
# on a desktop, useless on the device most people read it on. Two lines per pick
# instead of one trades height, which scrolls, for width, which does not.
FIG_W_INCHES = 7.0
LINE_H_INCHES = 0.30
LINES_PER_PICK = 2.25

# Characters that fit on the meta line between the ticker and the score, at
# 8pt sans in a 1050px-wide render. Measured, not guessed.
META_BUDGET = 72

X_LEFT, X_RIGHT = 3.0, 97.0
X_TICK = 9.0
# Four chips, not three. Only the top three were ever stored before, so a pick
# whose fourth-largest driver was improving margins showed no sign of it - and on
# the October 2026 top ten that was half the list. 9_score.py now stores every
# contribution; this shows one more of them.
N_CHIPS = 4
CHIP_FS = 10.0
CHIP_GAP = 1.6
CHIP_W = (X_RIGHT - X_LEFT - (N_CHIPS - 1) * CHIP_GAP) / N_CHIPS
CHAR_W = CHIP_W / 17.0                              # 14 chars + padding per chip


def _clip(text: str, n: int) -> str:
    """Trim to n characters on a word boundary, with an ellipsis if anything went.

    A hard slice produced "Tidewater Midstream and In" and "Doman Building
    Materials G", which read as typos rather than as truncation.
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

    Every chip is the same width and sits in a fixed slot rather than being packed
    against its neighbour. Packing sized each pill to its text, so a short label
    shifted every chip after it and the columns sagged - the same defect the ANSI
    table had. A fixed slot wastes a little space after "buybacks" and buys a score
    column that reads straight down.
    """
    colour = _CYAN if helped else _PINK
    x = X_LEFT + slot * (CHIP_W + CHIP_GAP)

    # No outline, and no glow on the score.
    #
    # A 1px stroke around a small pill reads as a cheap border rather than as neon:
    # at this size the line is the same visual weight as the text inside it, so the
    # eye sees ten boxes before it sees any data. A slightly stronger fill with no
    # edge gives the same separation and stays quiet. Likewise the withStroke halo
    # that works on a 15pt ticker just looks like blur on a 12pt number, so the
    # glow is reserved for the large type where it reads as intended.
    ax.add_patch(FancyBboxPatch(
        (x, y - 0.42), CHIP_W, 0.84,
        boxstyle="round,pad=0.12,rounding_size=0.3",
        facecolor=colour, alpha=0.16, edgecolor="none", zorder=2,
    ))
    # `sector` is categorical, so it has no percentile and `score` is empty. Left
    # aligned with a blank right half it reads as a rendering fault rather than as
    # a feature without a number, so it gets centred instead.
    if not score:
        ax.text(x + CHIP_W / 2, y, label, color=colour, fontsize=CHIP_FS,
                fontfamily=MONO, va="center", ha="center", alpha=0.85, zorder=3)
        return

    ax.text(x + 1.1, y, label, color=colour, fontsize=CHIP_FS, fontfamily=MONO,
            va="center", ha="left", alpha=0.85, zorder=3)
    ax.text(x + CHIP_W - 1.1, y, score, color=colour, fontsize=CHIP_FS,
            fontfamily=MONO, fontweight="bold", va="center", ha="right", zorder=3)


def render(picks, snap, scope, out_path=None) -> str:
    n = len(picks)
    # 2.6 of title, the picks, then 2.3 of footer. The footer needs more room
    # than its two lines suggest: text is centred on its y, so the last line's
    # descenders fall below it and clip against the figure edge.
    units = 2.6 + n * LINES_PER_PICK + 2.3
    fig = plt.figure(figsize=(FIG_W_INCHES, units * LINE_H_INCHES), dpi=150)
    fig.patch.set_facecolor(_BG)

    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(units, 0)          # inverted: y grows downward, like rows
    ax.axis("off")
    ax.set_facecolor(_BG)

    # ── title ──────────────────────────────────────────────────────────────────
    ax.text(X_LEFT, 0.95, f"▶  LADBOT", color=_PINK, fontsize=19,
            fontweight="bold", fontfamily=SANS, va="center", ha="left",
            path_effects=_neon(_PINK, lw=5, alpha=0.30))
    ax.text(X_RIGHT, 0.95, f"TOP {n}", color=_CYAN, fontsize=15,
            fontweight="bold", fontfamily=SANS, va="center", ha="right",
            path_effects=_neon(_CYAN, lw=4, alpha=0.28))
    ax.text(X_LEFT, 1.75, f"holdings  ·  {snap}  ·  {scope}",
            color=_TEXT, fontsize=8.5, fontfamily=MONO, va="center", ha="left",
            alpha=0.6)
    ax.plot([X_LEFT, X_RIGHT], [2.25] * 2, color=_GRID, linewidth=1.2, zorder=1)

    # ── one block per pick: identity line, then the chips ──────────────────────
    for i, p in enumerate(picks):
        y = 3.1 + i * LINES_PER_PICK

        if i % 2 == 0:
            ax.add_patch(FancyBboxPatch(
                (X_LEFT - 1.4, y - 0.62), X_RIGHT - X_LEFT + 2.8,
                LINES_PER_PICK - 0.12,
                boxstyle="round,pad=0,rounding_size=0.2",
                facecolor=_PANEL, edgecolor="none", zorder=0,
            ))

        ax.text(X_LEFT + 2.2, y, str(p["rank"]), color=_GOLD, fontsize=12,
                fontfamily=MONO, fontweight="bold", va="center", ha="right",
                path_effects=_neon(_GOLD, lw=3, alpha=0.28))

        tick = (p["ticker"] or "").removesuffix(".TO")
        ax.text(X_TICK, y, tick, color=_CYAN, fontsize=15, fontfamily=MONO,
                fontweight="bold", va="center", ha="left",
                path_effects=_neon(_CYAN, lw=4, alpha=0.30))

        raw = p.get("sector") or ""
        mc = p.get("market_cap")
        cap = ("—" if not mc else
               f"${float(mc)/1e9:.1f}B" if float(mc) >= 1e9 else
               f"${float(mc)/1e6:.0f}M")
        # Sector spelled out, not coded. "Matl" and "CCyc" exist because the
        # Discord text table has 48 columns to work with; here there is room, and
        # a reader should not have to decode an abbreviation to learn a stock is
        # in Basic Materials.
        #
        # The meta line runs from the ticker to the score - about 72 characters at
        # 8pt sans in a 1050px render. Sector and cap are fixed by the data, so the
        # company name takes whatever is left rather than a fixed clip. Otherwise
        # "Communication Services" plus a long name would run under the score.
        sector = raw or "—"
        budget = META_BUDGET - len(sector) - len(cap) - 6   # 6 = two separators
        meta = f"{_clip(p.get('company') or '', max(budget, 12))}  ·  {sector}  ·  {cap}"
        ax.text(X_TICK + 11.0, y + 0.06, meta, color=_TEXT, fontsize=8,
                fontfamily=SANS, va="center", ha="left", alpha=0.6)

        ax.text(X_RIGHT, y, f"{float(p['score']):+.2f}", color=_GOLD,
                fontsize=13, fontfamily=MONO, fontweight="bold",
                va="center", ha="right", alpha=0.95)

        for slot, c in enumerate(qp.driver_chips(p, N_CHIPS)):
            _chip(ax, slot, y + 1.12, c["label"], c["score"], c["helped"])

    # ── footer ─────────────────────────────────────────────────────────────────
    ax.text(X_LEFT, units - 1.55,
            "chip score = how strongly it shows that trait, 0-100 vs sector",
            color=_TEXT, fontsize=7.5, fontfamily=MONO, va="center", ha="left",
            alpha=0.45)
    ax.text(X_LEFT, units - 0.95,
            "cyan helped the score  ·  pink hurt it",
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
