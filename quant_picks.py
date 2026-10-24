"""
quant_picks.py — Serve the quant model's latest picks to Discord.

Reads rows the scoring run already wrote to the `predictions` table and turns them
into posts, with Claude explaining each name from the model's own SHAP breakdown.

Nothing here computes a prediction. A Discord command has to answer in seconds and
the pipeline takes tens of minutes, so the split is deliberate:

    scheduled (heavy)                        this module (instant)
      2_fmp_pipeline.py --incremental          SELECT from predictions
      6_feature_compute.py --as-of DATE        ask Claude for the rationale
      9_score.py --date DATE --save            format posts

It also means everyone asking on the same day gets the same answer from the same
timestamped row, rather than a fresh roll of the dice each time.
"""

import json
import os
from datetime import date

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

TOP_N = 5


# Table labels that name the DIRECTION, not just the measure.
#
# Each entry is (label when the percentile is low, label when it is high). This
# exists because a bare measure name plus a raw percentile is ambiguous in exactly
# the way that has already caused confusion twice: "cheap P/S 12" left a reader
# asking whether 12 meant cheap or expensive, and "size 3" gave no clue why a tiny
# company scoring 3 was a point in its favour.
#
# Naming the direction removes the question. Low market cap prints "small cap",
# low price-to-sales prints "cheap P/S", and the number that follows is always
# "how far in the direction the label names, 0-100". Every number on the table
# then reads the same way and the footer says it once.
#
# Labels are <= 10 characters; SHORT_LABELS runs to 19 and does not fit.
DIRECTIONAL_LABELS = {
    "market_cap":               ("small cap", "large cap"),
    "ps_ratio_pct":             ("cheap P/S", "rich P/S"),
    "dynamic_ps":               ("cheap P/S", "rich P/S"),
    "dynamic_ev_ebitda":        ("cheap EV", "rich EV"),
    "dynamic_pfcf":             ("cheap FCF", "rich FCF"),
    "dynamic_fcf_yield_sect":   ("low FCF yd", "FCF yield"),
    "shares_change_1yr":        ("buybacks", "dilution"),
    "shares_change_1yr_sect":   ("buybacks", "dilution"),
    "return_12m_1m_vol_adj":    ("weak 12m", "momentum"),
    "return_12m_1m":            ("weak 12m", "momentum"),
    "return_12m_1m_sect":       ("weak 12m", "momentum"),
    "return_3m":                ("weak 3m", "3m return"),
    "gross_margin":             ("low margin", "margin"),
    "ttm_gross_margin":         ("low margin", "margin"),
    "operating_margin":         ("low op mgn", "op margin"),
    "gross_margin_trend":       ("mgn fading", "mgn rising"),
    "piotroski_sect":           ("weak F-Sc", "F-Score"),
    "ttm_roic_sect":            ("low ROIC", "ROIC"),
    "ttm_op_cf_to_assets_sect": ("weak cash", "cash flow"),
    "consecutive_eps_beats":    ("EPS misses", "EPS beats"),
    "revenue_growth_qoq":       ("rev drop", "rev growth"),
    "oil_beta":                 ("oil-light", "oil beta"),
    "sector":                   ("sector", "sector"),
}

# Four-character sector codes. Full names cost 12 columns and tell a reader almost
# nothing the ticker does not.
SECTOR_CODE = {
    "Financial Services": "Fin",      "Basic Materials": "Matl",
    "Consumer Cyclical": "CCyc",      "Consumer Defensive": "CDef",
    "Communication Services": "Comm", "Real Estate": "RE",
    "Information Technology": "Tech", "Technology": "Tech",
    "Industrials": "Ind",             "Energy": "Enrg",
    "Utilities": "Util",              "Healthcare": "Hlth",
}


def _directional(feature: str, pct) -> tuple[str, str]:
    """
    Return (label, score) where the label names the direction and the score says
    how far in that direction, 0-100.

    A percentile below 50 takes the low label and is flipped, so 3rd-percentile
    market cap prints as ("small cap", "97") rather than ("size", "3").
    """
    base = feature[:-5] if feature.endswith("_sect") else feature
    pair = DIRECTIONAL_LABELS.get(feature) or DIRECTIONAL_LABELS.get(base)
    if not isinstance(pct, (int, float)):
        return ((pair[1] if pair else base.replace("_", " "))[:10], "")
    v = pct * 100
    low = v < 50
    if pair:
        label = pair[0] if low else pair[1]
    else:
        label = base.replace("_", " ")[:10]
    return label[:10], str(round(100 - v if low else v))


def _helped(pick: dict, n: int = 2) -> str:
    """The n biggest POSITIVE contributors, as 'label score' joined by a middle dot."""
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    pos = sorted((d.get("positive") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))[:n]
    if not pos:
        return "-"
    bits = []
    for item in pos:
        label, score = _directional(item["feature"], item.get("percentile"))
        bits.append(f"{label} {score}".strip())
    return " · ".join(bits)


def driver_chips(pick: dict, n: int = 3) -> list[dict]:
    """
    The n largest contributors as compact chips, using the same direction-aware
    vocabulary as the Discord table.

    top_factors() returns the long-form wording for prose ("operating margin",
    "lower quarter"), which needs a reader to join three pieces together before
    "low margin helped" becomes clear. These chips name the direction in the label
    itself, so a row reads without decoding: "low op mgn 92", cyan for helped.

    Each chip: label, score (0-100 in the direction named), helped, contribution.
    """
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    merged = ([{**x, "helped": True} for x in (d.get("positive") or [])] +
              [{**x, "helped": False} for x in (d.get("negative") or [])])
    merged.sort(key=lambda x: -abs(x.get("contribution", 0)))
    out = []
    for item in merged[:n]:
        label, score = _directional(item["feature"], item.get("percentile"))
        out.append({
            "label": label,
            "score": score,
            "helped": item["helped"],
            "contribution": item.get("contribution", 0.0),
            "feature": item["feature"],
        })
    return out


def universe_size(snap) -> int | None:
    """
    How many names the model actually ranked on that date.

    Queried rather than hardcoded: the header used to claim "~420 liquid TSX names"
    and was still claiming it after 122 depositary receipts were purged. A number
    that has to be remembered is a number that goes stale.
    """
    try:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                # Rows in the latest model run, not rows in feature_snapshots.
                # A snapshot exists for every main-tier name; the model only ranks
                # the ones that cleared 9_score.py's staleness and risk-metric
                # gates, so counting snapshots overstated the universe by ~130.
                cur.execute(
                    "SELECT COUNT(*) AS n FROM predictions WHERE snapshot_date = %s "
                    "AND model_tag = (SELECT model_tag FROM predictions "
                    "WHERE snapshot_date = %s ORDER BY created_at DESC LIMIT 1)",
                    (snap, snap))
                row = cur.fetchone()
                return row["n"] if row and row["n"] else None
        finally:
            conn.close()
    except Exception:
        return None


# Discord renders a fenced block tagged `ansi` with real SGR colour, which is the
# only way to get the chart palette into a chat message.
#
# chart_generator.py           nearest SGR code
#   CYAN  #00f5ff      ->      36
#   PINK  #ff2d78      ->      35  magenta - the signature synthwave accent
#   GOLD  #ffd700      ->      33
#   GRID  #2a0a4a      ->      30  grey, for anything that should recede
#
# NO BOLD. The "1;" prefix is deliberately absent everywhere. Discord renders bold
# glyphs wider than regular ones in the code-block font, so a bold column and a
# regular column of the same character count do not occupy the same width, and the
# table visibly drifts out of alignment down the page.
ANSI = {
    "reset":  "\u001b[0m",
    "header": "\u001b[0;30m",   # grey headings - the data should be the loud part
    "rank":   "\u001b[0;33m",   # gold numerals
    "tick":   "\u001b[0;36m",   # cyan
    "sec":    "\u001b[0;30m",   # grey, recedes
    "label":  "\u001b[0;35m",   # pink - the trait name
    "score":  "\u001b[0;36m",   # cyan - the number that carries the meaning
    "dim":    "\u001b[0;30m",   # separators
    "down":   "\u001b[0;31m",   # red, for a driver that hurt
}

# Column widths, in characters. Every row - including the header - is built from
# these same constants by _row(), so the header cannot drift away from the data the
# way it did when the two were formatted independently.
W_RANK, W_TICK, W_SEC, W_LABEL, W_SCORE = 2, 7, 5, 10, 3


def _c(key: str, text: str) -> str:
    """
    Wrap text in one SGR colour.

    Padding goes INSIDE the wrapper, so callers pad first and colour second; that
    keeps the escape sequences out of the width arithmetic entirely. The escapes
    are invisible but still count toward the message limit, which is why this goes
    in an embed description (4096) rather than a plain message (2000).
    """
    return f"{ANSI[key]}{text}{ANSI['reset']}"


def _cell(pick_item) -> tuple[str, str]:
    """One driver as (label, score) strings, already padded to column width."""
    if pick_item is None:
        return " " * W_LABEL, " " * W_SCORE
    label, score = _directional(pick_item["feature"], pick_item.get("percentile"))
    return f"{label[:W_LABEL]:<{W_LABEL}}", f"{score:>{W_SCORE}}"


def _row(rank: str, tick: str, sec: str, cells, colour: bool) -> str:
    """
    Build one line. Used for the header and every data row, so the columns are
    defined in exactly one place.

    `cells` is a list of (label, score) pairs already padded by _cell(). Scores are
    right-aligned inside their own column rather than trailing the label, which is
    what makes the numbers form a readable column instead of scattering.
    """
    parts = [
        _c("rank", rank) if colour else rank,
        "  ",
        _c("tick", tick) if colour else tick,
        _c("sec", sec) if colour else sec,
    ]
    for i, (label, score) in enumerate(cells):
        if i:
            parts.append("  ")
        parts.append(_c("label", label) if colour else label)
        parts.append(" ")
        parts.append(_c("score", score) if colour else score)
    return "".join(parts)


def _driver_cells(pick: dict, n: int = 2):
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    pos = sorted((d.get("positive") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))[:n]
    pos += [None] * (n - len(pos))
    return [_cell(x) for x in pos]


def model_blurb(picks: list[dict], size: int | None) -> str:
    """
    One short paragraph describing what produced these picks.

    Every number in it is derived rather than written down, so it cannot drift
    from the model the way a hardcoded description would. model_tag carries the
    objective, the horizon and the tree count; the stored drivers carry the
    feature count, now that 9_score.py keeps all of them instead of the top five.

    NO PERFORMANCE NUMBER. It says what the model does, never what it returns.

    An earlier version quoted "roughly 2-3.5% per quarter" from the backtest.
    Deliberately removed: in a chat post a historical range is read as a
    forecast, and that one rests on 40 overlapping months - about 13 independent
    periods - with feature selection that used test-window data. Describing the
    method is defensible; publishing an expected return is not, and this is the
    wrong surface to carry the caveats that would make it so.
    """
    tag = (picks[0].get("model_tag") or "") if picks else ""
    kind = "ranking" if tag.startswith("lambdarank") else "classification"
    horizon = "3-month" if "_3m" in tag else "forward"
    trees = tag.rsplit("_it", 1)[-1] if "_it" in tag else "?"

    n_feat = 0
    if picks:
        d = picks[0].get("drivers") or {}
        if isinstance(d, str):
            d = json.loads(d)
        n_feat = len(d.get("positive") or []) + len(d.get("negative") or [])

    universe = f"{size} TSX names" if size else "the TSX main board"
    feats = f"{n_feat} factors" if n_feat else "a factor set"

    return (
        f"A LightGBM {kind} model ({trees} trees) scores {universe} on {feats} "
        f"— momentum, valuation, quality, size — and orders them by "
        f"expected {horizon} return relative to the market, adjusted for "
        f"volatility. Retrained from scratch every week on month-end data back "
        f"to 2019. Chips below show what moved each score — the model's own "
        f"reasoning, not a recommendation."
    )


def build_table_embed(n: int = 10) -> tuple[dict | None, str | None]:
    """
    The same table as build_table(), coloured and column-aligned for an embed.

    Three things keep it aligned where the first version drifted: no bold anywhere,
    one _row() builder shared by the header and the data, and fixed-width driver
    cells with right-aligned scores instead of free text joined by a separator.

    Returns a payload for the caller to turn into discord.Embed, so this module
    needs no discord.py import and stays testable from a terminal.
    """
    picks, snap = load_latest_picks(n)
    if not picks:
        return None, ("No scored picks yet - run `9_score.py --save` "
                      "to publish a snapshot.")

    head = _row(f"{'#':>{W_RANK}}", f"{'TICKER':<{W_TICK}}", f"{'SEC':<{W_SEC}}",
                [(f"{'WHAT HELPED':<{W_LABEL}}", " " * W_SCORE),
                 (f"{'':<{W_LABEL}}", " " * W_SCORE)], colour=False)
    rows = [_c("header", head.rstrip())]
    for p in picks:
        tick = (p["ticker"] or "").removesuffix(".TO")[:W_TICK]
        raw = p.get("sector") or ""
        sec = SECTOR_CODE.get(raw, raw[:4])
        rows.append(_row(f"{p['rank']:>{W_RANK}}", f"{tick:<{W_TICK}}",
                         f"{sec:<{W_SEC}}", _driver_cells(p, 2), colour=True))

    size = universe_size(snap)
    scope = f"ranked {size} TSX names" if size else "TSX main board"
    return {
        "title": f"LadBot \u2014 Top {len(picks)} Holdings",
        "blurb": model_blurb(picks, size),
        "body": "```ansi\n" + "\n".join(rows) + "\n```",
        "footer": (f"{snap}  \u00b7  {scope}  \u00b7  score = how strongly it "
                   f"shows that trait (0-100 vs sector)"),
        "color": 0xFF2D78,
    }, None


def build_table(n: int = 10) -> tuple[str | None, str | None]:
    """
    A compact monospace table - one message instead of ten posts and ten charts.

    Width is the whole design constraint. Discord code blocks do not wrap; they
    scroll sideways, so anything over ~48 columns is unreadable on a phone. The
    budget is spent as: rank 2, ticker 8, sector 5, drivers the rest.

    Savings over the previous version: the .TO suffix is dropped (every name is
    TSX), sector names are coded to 4 characters, and labels come from
    DIRECTIONAL_LABELS.
    That paid for a SECOND driver per row, which is the actual improvement - one
    factor rarely explains a pick.
    """
    picks, snap = load_latest_picks(n)
    if not picks:
        return None, ("No scored picks yet - run `9_score.py --save` "
                      "to publish a snapshot.")

    rows = [f"{'#':>2}  {'TICKER':<8}{'SEC':<5}WHAT HELPED"]
    rows.append("─" * 47)
    for p in picks:
        tick = (p["ticker"] or "").removesuffix(".TO")[:7]
        raw = p.get("sector") or ""
        sec = SECTOR_CODE.get(raw, raw[:4])
        rows.append(f"{p['rank']:>2}  {tick:<8}{sec:<5}{_helped(p)}")

    size = universe_size(snap)
    scope = f"ranked {size} TSX names" if size else "TSX main board"
    header = (
        f"**LadBot · Top {len(picks)} Holdings · {snap}**\n"
        f"```\n" + "\n".join(rows) + "\n```\n"
        f"*{scope} · score = how strongly it shows that trait "
        f"(0-100, vs sector)*"
    )
    return header, None


def _conn():
    url = os.getenv("QUANT_DB_URL")
    if not url:
        raise RuntimeError("QUANT_DB_URL not set in .env")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


def load_latest_picks(n: int = TOP_N) -> tuple[list[dict], date | None]:
    """
    Top n names from the most recent scoring run, with the context a reader needs.

    Returns ([], None) when nothing has been scored yet, so the caller can say so
    rather than present an empty list as though it were a result.
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.predictions') AS t")
            if cur.fetchone()["t"] is None:
                return [], None

            cur.execute("SELECT MAX(snapshot_date) AS d FROM predictions")
            snap = cur.fetchone()["d"]
            if snap is None:
                return [], None

            # Filter to ONE model run. A snapshot can hold several: the table's
            # UNIQUE key is (snapshot_date, ticker, model_tag), deliberately, so
            # retraining doesn't overwrite an earlier version's calls and the two
            # can be compared later. Without the filter, ORDER BY rank returns
            # rank 1 from every stored run — which produced a "top 5" containing
            # the same ticker twice under #1 and #1.
            cur.execute("""
                WITH latest AS (
                    SELECT model_tag
                    FROM predictions
                    WHERE snapshot_date = %s
                    GROUP BY model_tag
                    ORDER BY MAX(created_at) DESC
                    LIMIT 1
                )
                SELECT p.ticker, p.rank, p.score, p.decile, p.drivers, p.model_tag,
                       f.sector, f.market_cap, f.piotroski_score, f.altman_z_score,
                       f.total_vol, f.market_beta, f.return_12m_1m, f.dynamic_pb,
                       t.name AS company
                FROM predictions p
                LEFT JOIN feature_snapshots f
                       ON f.ticker = p.ticker AND f.snapshot_date = p.snapshot_date
                LEFT JOIN tickers t ON t.ticker = p.ticker
                WHERE p.snapshot_date = %s
                  AND p.model_tag = (SELECT model_tag FROM latest)
                ORDER BY p.rank ASC
                LIMIT %s
            """, (snap, snap, n))
            return [dict(r) for r in cur.fetchall()], snap
    finally:
        conn.close()
