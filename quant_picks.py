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

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-sonnet-4-6"   # matches tweet_generator.py
TOP_N = 5

# Plain-English names for the features Claude will see. Without these it would be
# guessing at what `ttm_op_cf_to_assets_sect` means, and a wrong guess becomes a
# confident sentence in a post about a real company.
FEATURE_LABELS = {
    "dynamic_pe": "price-to-earnings", "dynamic_ps": "price-to-sales",
    "dynamic_pb": "price-to-book", "dynamic_ev_ebitda": "EV/EBITDA",
    "dynamic_pfcf": "price-to-free-cash-flow",
    "dynamic_earnings_yield": "earnings yield", "dynamic_fcf_yield": "free cash flow yield",
    "piotroski_score": "Piotroski F-Score (9-point financial strength)",
    "altman_z_score": "Altman Z (distress risk)", "altman_z_pct": "Altman Z vs sector peers",
    "ttm_roa": "return on assets", "ttm_roe": "return on equity",
    "ttm_roic": "return on invested capital",
    "ttm_operating_margin": "operating margin", "operating_margin": "operating margin",
    "gross_margin": "gross margin", "fcf_margin": "free cash flow margin",
    "ttm_operating_cf_to_assets": "operating cash flow vs assets",
    "operating_cf_to_assets": "operating cash flow vs assets",
    # The sector-percentile column abbreviates this one, so the _sect lookup needs
    # the shortened stem too.
    "ttm_op_cf_to_assets": "operating cash flow vs assets",
    "ttm_revenue_growth": "revenue growth", "revenue_growth_yoy": "revenue growth YoY",
    "revenue_growth_qoq": "revenue growth QoQ", "ttm_earnings_growth": "earnings growth",
    "revenue_acceleration": "accelerating revenue", "earnings_acceleration": "accelerating earnings",
    "gross_margin_trend": "margin trend",
    "return_12m_1m": "12-month momentum", "return_3m": "3-month return",
    "return_1m": "1-month return", "high_52w_proximity": "proximity to 52-week high",
    "rsi_14": "RSI (overbought/oversold)", "volume_ratio_90d": "volume vs 90-day average",
    "return_12m_1m_vol_adj": "volatility-adjusted 12-month momentum",
    "return_3m_vol_adj": "volatility-adjusted 3-month return",
    "shares_change_1yr": "share count change (negative = buybacks)",
    "shares_change_3yr": "3-year share count change",
    "total_vol": "total volatility", "idio_vol": "company-specific volatility",
    "market_beta": "market beta", "market_r2": "how much the market explains the stock",
    "gold_beta": "sensitivity to gold", "oil_beta": "sensitivity to oil",
    "cadusd_beta": "sensitivity to CAD/USD", "rates_beta": "sensitivity to interest rates",
    "debt_to_assets": "debt vs assets", "ttm_debt_to_assets": "debt vs assets",
    "net_debt_to_ebitda": "net debt vs EBITDA", "ttm_net_debt_to_ebitda": "net debt vs EBITDA",
    "current_ratio": "current ratio", "ttm_current_ratio": "current ratio",
    "interest_coverage": "interest coverage",
    "consecutive_eps_beats": "consecutive earnings beats",
    "latest_eps_surprise_pct": "latest earnings surprise",
    "market_cap": "market cap", "avg_dollar_volume_90d": "daily trading volume",
    "sector": "sector",
}


def _label(feature: str) -> str:
    """Readable name, noting when a feature is measured against sector peers."""
    if feature in FEATURE_LABELS:
        return FEATURE_LABELS[feature]
    if feature.endswith("_sect"):
        base = feature[:-5]
        return f"{FEATURE_LABELS.get(base, base)} vs sector peers"
    return feature.replace("_", " ")


# Compact tags for the table view. FEATURE_LABELS is written for Claude to read in
# prose; these have to fit a phone-width column, so they trade precision for brevity.
SHORT_LABELS = {
    "return_12m_1m_vol_adj": "12m momentum", "return_12m_1m": "12m momentum",
    "return_12m_1m_sect": "12m mom vs sector", "return_3m": "3m return",
    "shares_change_1yr": "buybacks", "shares_change_1yr_sect": "buybacks vs sector",
    "shares_change_3yr": "3yr buybacks",
    "ps_ratio_pct": "cheap P/S vs sector", "dynamic_ps": "cheap P/S",
    "dynamic_pfcf": "cheap on FCF", "dynamic_ev_ebitda": "cheap EV/EBITDA",
    "dynamic_fcf_yield_sect": "FCF yield vs sector",
    "gross_margin": "gross margin", "ttm_gross_margin": "gross margin",
    "operating_margin": "op margin", "gross_margin_trend": "margin trend",
    "piotroski_sect": "F-Score vs sector", "ttm_roic_sect": "ROIC vs sector",
    "ttm_op_cf_to_assets_sect": "cash flow vs sector",
    "consecutive_eps_beats": "earnings beats", "revenue_growth_qoq": "revenue growth",
    "market_cap": "size", "oil_beta": "oil sensitivity", "sector": "sector",
}


def _short(feature: str) -> str:
    if feature in SHORT_LABELS:
        return SHORT_LABELS[feature]
    base = feature[:-5] if feature.endswith("_sect") else feature
    lbl = SHORT_LABELS.get(base, base.replace("_", " "))
    return f"{lbl} vs sector" if feature.endswith("_sect") else lbl


def short_reason(pick: dict) -> str:
    """
    One compact tag naming the biggest contributor, e.g. "12m momentum (95th)".

    Derived straight from the stored SHAP values rather than asking Claude — it is
    deterministic, instant, free, and cannot hallucinate. The prose explanations are
    worth a model call; a four-word tag is not.
    """
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    pos = d.get("positive") or []
    if not pos:
        return "—"
    top = pos[0]
    pct = top.get("percentile")
    where = f" ({_ordinal(round(pct * 100))})" if isinstance(pct, (int, float)) else ""
    return f"{_short(top['feature'])}{where}"


# Features where a LOW percentile means MORE of the thing the label names.
#
# This is the trap the raw numbers set. "buybacks (8th)" looks weak but means the
# company shrank its share count more than 92% of the market, because the underlying
# column measures share-count CHANGE — negative is a buyback. Same for valuation: a
# low price-to-sales percentile means cheap, which is the thing you want. Meanwhile
# "op margin (30th)" really does mean weak margins. Identical-looking numbers,
# opposite meanings, and no way to tell from the digits.
VALUATION_FEATURES = {
    "ps_ratio_pct", "pe_ratio_pct", "ev_to_ebitda_pct",
    "dynamic_ps", "dynamic_pe", "dynamic_pb", "dynamic_pfcf", "dynamic_ev_ebitda",
}
BUYBACK_FEATURES = {
    "shares_change_1yr", "shares_change_1yr_sect", "shares_change_3yr",
}
INVERTED_FEATURES = VALUATION_FEATURES | BUYBACK_FEATURES

# Nouns phrased so the inverted reading is already baked in — "share buybacks"
# rather than "share count change".
PLAIN_NOUNS = {
    "shares_change_1yr": "share buybacks",
    "shares_change_1yr_sect": "buybacks vs sector peers",
    "shares_change_3yr": "3-year buybacks",
    # Neutral measure names. These used to read "cheap on sales vs sector", which
    # put the direction in the noun and left the position modifying an unstated
    # scale — so "cheap on sales vs sector, upper quarter" could be read as either
    # very cheap or expensive. The position phrase now carries the direction and
    # the noun just names what is being measured.
    "ps_ratio_pct": "price-to-sales vs sector",
    "dynamic_ps": "price-to-sales",
    "dynamic_pe": "price-to-earnings",
    "dynamic_pb": "price-to-book",
    "dynamic_pfcf": "price-to-cash-flow",
    "dynamic_ev_ebitda": "EV/EBITDA",
    "pe_ratio_pct": "price-to-earnings vs sector",
    "ev_to_ebitda_pct": "EV/EBITDA vs sector",
    "return_12m_1m_vol_adj": "12m momentum, risk-adj",
    "return_12m_1m": "12-month momentum",
    "return_12m_1m_sect": "momentum vs sector peers",
    "market_cap": "company size",
    "operating_margin": "operating margin",
    "gross_margin": "gross margin",
    "ttm_gross_margin": "gross margin",
    "gross_margin_trend": "margin trend",
    "revenue_growth_qoq": "revenue growth",
    "consecutive_eps_beats": "earnings beats in a row",
    "piotroski_sect": "financial strength vs sector",
    "ttm_roic_sect": "return on capital vs sector",
    "ttm_op_cf_to_assets_sect": "cash generation vs sector",
    "dynamic_fcf_yield_sect": "free cash flow yield vs sector",
    "oil_beta": "sensitivity to oil",
    "sector": "sector",
}


def plain_noun(feature: str) -> str:
    if feature in PLAIN_NOUNS:
        return PLAIN_NOUNS[feature]
    base = feature[:-5] if feature.endswith("_sect") else feature
    noun = PLAIN_NOUNS.get(base, base.replace("_", " "))
    return f"{noun} vs sector peers" if feature.endswith("_sect") else noun


def plain_position(feature: str, pct) -> str:
    """
    Where the stock sits, in words, already corrected for inverted features.

    For an inverted feature the percentile is flipped first, so 8th percentile on
    share-count change renders as "more than 92% of names" rather than a number the
    reader has to mentally reverse.
    """
    # `sector` is categorical, so it has no percentile — the model is reacting to
    # which industry the name is in, not to a value on a scale. Say that instead of
    # printing "not available", which reads like missing data.
    if feature == "sector":
        return "which industry it's in"
    if not isinstance(pct, (int, float)):
        return "not available"
    p = round(pct * 100)

    # Valuation and buyback features need their own vocabulary. A generic "upper
    # quarter" is meaningless unless the reader already knows which end of the
    # scale is good, so each phrase states the comparison outright.
    if feature in VALUATION_FEATURES:
        cheap = 100 - p                      # low price ratio = cheap
        if cheap >= 90: return f"cheaper than {cheap}% of peers"
        if cheap >= 60: return "cheaper than most peers"
        if cheap >= 40: return "priced around the middle"
        if cheap >= 10: return "pricier than most peers"
        return f"pricier than {100 - cheap}% of peers"

    if feature in BUYBACK_FEATURES:
        bought = 100 - p                     # low share-count change = buyback
        if bought >= 90: return f"bought back more than {bought}% of names"
        if bought >= 60: return "buying back more than most"
        if bought >= 40: return "share count roughly flat"
        if bought >= 10: return "issuing more shares than most"
        return f"issued more shares than {100 - bought}% of names"

    if p >= 95: return "top 5% of the market"
    if p >= 90: return f"higher than {p}% of names"
    if p >= 75: return "upper quarter"
    if p >= 60: return "above average"
    if p >= 40: return "middle of the pack"
    if p >= 25: return "below average"
    if p >= 10: return "lower quarter"
    return f"bottom {max(p, 1)}%"


def top_factors(pick: dict, n: int = 3) -> list[dict]:
    """
    The n largest contributors to this pick's score, positive and negative together,
    ordered by magnitude. Each carries a short label, the percentile, the raw SHAP
    contribution, and whether it helped or hurt.

    Taken straight from the stored breakdown — no model call, no interpretation.
    """
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    pos = sorted((d.get("positive") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))
    neg = sorted((d.get("negative") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))

    # Genuinely the top n by magnitude. An earlier version reserved the last slot
    # for a headwind, which meant BNT showed a -0.047 negative in place of its
    # +0.161 third-biggest driver — presented under a heading that claimed to be
    # the top three. The headwind is worth showing, but as its own labelled thing
    # (see main_headwind), not by displacing a real driver.
    chosen = sorted(pos + neg, key=lambda x: -abs(x.get("contribution", 0)))[:n]
    out = []
    for item in chosen:
        pct = item.get("percentile")
        out.append({
            "label": _short(item["feature"]),
            "noun": plain_noun(item["feature"]),
            "position": plain_position(item["feature"], pct),
            "feature": item["feature"],
            "percentile": pct,
            "pct_text": _ordinal(round(pct * 100)) if isinstance(pct, (int, float)) else "n/a",
            "contribution": item.get("contribution", 0.0),
            "helped": item.get("contribution", 0.0) > 0,
        })
    return out


def main_headwind(pick: dict, min_share: float = 0.10,
                  exclude: set[str] | None = None) -> dict | None:
    """
    The largest negative contributor, if it is big enough to be worth a line.

    Reported separately from the top drivers rather than competing with them for a
    slot. The threshold matters: for most top-ranked names the biggest headwind is
    a rounding error next to the drivers, and giving a -0.03 the same visual weight
    as a +0.52 overstates it. Only shown at >= min_share of the largest driver.
    """
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    neg = sorted((d.get("negative") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))
    pos = d.get("positive") or []
    if not neg or not pos:
        return None

    # Skip anything the caller is already displaying. top_factors returns the true
    # top n by magnitude, which can legitimately include a negative — CVE.TO's
    # largest single contributor was company size at -0.?? — and without this the
    # same row printed twice, once among the drivers and again as the headwind.
    exclude = exclude or set()
    neg = [x for x in neg if x["feature"] not in exclude]
    if not neg:
        return None

    biggest = max(abs(x.get("contribution", 0)) for x in pos) or 1
    top = neg[0]
    if abs(top.get("contribution", 0)) / biggest < min_share:
        return None
    pct = top.get("percentile")
    return {
        "noun": plain_noun(top["feature"]),
        "position": plain_position(top["feature"], pct),
        "contribution": top.get("contribution", 0.0),
        "share": abs(top.get("contribution", 0.0)) / biggest,
    }


def factor_sentence(pick: dict, n: int = 3) -> str:
    """
    One plain sentence naming the top n factors — built mechanically, so it costs
    nothing and cannot drift from the numbers it describes.
    """
    fs = top_factors(pick, n)
    if not fs:
        return "No breakdown stored."
    helped = [f"{f['noun']} ({f['position']})" for f in fs if f["helped"]]
    hurt   = [f"{f['noun']} ({f['position']})" for f in fs if not f["helped"]]
    parts = []
    if helped:
        joined = helped[0] if len(helped) == 1 else \
                 ", ".join(helped[:-1]) + " and " + helped[-1]
        parts.append(f"Ranked on {joined}")
    if hurt:
        joined = hurt[0] if len(hurt) == 1 else \
                 ", ".join(hurt[:-1]) + " and " + hurt[-1]
        parts.append(("held back by " if parts else "Held back by ") + joined)
    return "; ".join(parts) + "."


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


# Discord renders a fenced block tagged `ansi` with real SGR colours. The palette
# deliberately echoes the chart palette: cyan for what helped, gold for rank, grey
# for scaffolding. Only codes 30-37 exist, so these are the closest Discord can get
# to #00f5ff and #ffd700.
ANSI = {
    "reset": "\u001b[0m",
    "rank":  "\u001b[0;33m",   # gold-ish
    "tick":  "\u001b[1;36m",   # bright cyan, bold
    "sec":   "\u001b[0;30m",   # grey, recedes
    "label": "\u001b[0;37m",   # plain white
    "score": "\u001b[0;36m",   # cyan - the number that carries the meaning
    "dim":   "\u001b[0;30m",   # header and separators
    "down":  "\u001b[0;31m",   # red, for a driver that hurt
}


def _c(key: str, text: str) -> str:
    """
    Wrap text in one SGR colour.

    The escapes are invisible but still count toward Discord's message limit, which
    is why this goes in an embed description (4096 chars) rather than a plain
    message (2000). Ten coloured rows run about 1,400 characters.
    """
    return f"{ANSI[key]}{text}{ANSI['reset']}"


def _helped_ansi(pick: dict, n: int = 2) -> str:
    """Same content as _helped(), with the score coloured so the eye lands on it."""
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    pos = sorted((d.get("positive") or []),
                 key=lambda x: -abs(x.get("contribution", 0)))[:n]
    if not pos:
        return _c("dim", "-")
    bits = []
    for item in pos:
        label, score = _directional(item["feature"], item.get("percentile"))
        bits.append(f"{_c('label', label)} {_c('score', score)}")
    return _c("dim", " \u00b7 ").join(bits)


def build_table_embed(n: int = 10) -> tuple[dict | None, str | None]:
    """
    The same table as build_table(), coloured and shaped for a Discord embed.

    Plain text in a code fence is legible but flat - ten rows of identical grey
    under a heavy rule. An embed adds a coloured left border, a real title and a
    small-type footer, and the `ansi` fence lets the ticker and the score carry the
    chart palette. The header rule is gone: the embed border already separates the
    block from the conversation, so the rule was only adding weight.

    Returns a payload for the caller to turn into discord.Embed, so this module
    needs no discord.py import and stays testable from a terminal.
    """
    picks, snap = load_latest_picks(n)
    if not picks:
        return None, ("No scored picks yet - run `9_score.py --save` "
                      "to publish a snapshot.")

    rows = [_c("dim", f"{'#':>2}  {'TICKER':<8}{'SEC':<6}WHAT HELPED")]
    for p in picks:
        tick = (p["ticker"] or "").removesuffix(".TO")[:7]
        raw = p.get("sector") or ""
        sec = SECTOR_CODE.get(raw, raw[:4])
        rows.append(
            f"{_c('rank', format(p['rank'], '>2'))}  "
            f"{_c('tick', format(tick, '<8'))}"
            f"{_c('sec', format(sec, '<6'))}"
            f"{_helped_ansi(p)}"
        )

    size = universe_size(snap)
    scope = f"ranked {size} TSX names" if size else "TSX main board"
    return {
        "title": f"LadBot \u2014 Top {len(picks)} Holdings",
        "body": "```ansi\n" + "\n".join(rows) + "\n```",
        "footer": (f"{snap}  \u00b7  {scope}  \u00b7  score = how strongly it "
                   f"shows that trait (0-100 vs sector)  \u00b7  not advice"),
        "color": 0x00F5FF,
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
        f"*{scope} · score = how strongly it shows that trait (0-100, "
        f"vs sector) · not advice · `!picks {min(len(picks), 5)}` "
        f"for write-ups*"
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


def _ordinal(n: int) -> str:
    """1st, 2nd, 3rd, 4th — and 11th/12th/13th, which break the simple rule."""
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }".replace(" ", "")


def _where(pct) -> str:
    """
    Say in words where a percentile sits, so the direction cannot be misread.

    The first version left Claude to interpret a bare "5th percentile" and it
    reliably got this backwards — narrating a 5th-percentile operating margin as
    "among the leanest cost structures" because the SHAP contribution was positive,
    and an 86th-percentile revenue growth as weak because the contribution was
    negative. It was reasoning backwards from the sign of the contribution to
    invent a fundamental that justified it.

    Encoding the plain meaning here removes the inference. The model rewarding a
    bottom-decile margin is a real and slightly odd property of this model; the
    post should say so rather than dress it up.
    """
    if not isinstance(pct, (int, float)):
        return "value unavailable"
    p = round(pct * 100)
    if p >= 90:  band = "near the TOP of the universe"
    elif p >= 70: band = "in the upper range"
    elif p >= 30: band = "around the middle"
    elif p >= 10: band = "in the lower range"
    else:         band = "near the BOTTOM of the universe"
    return f"{_ordinal(p)} percentile — {band}"


def _drivers_block(pick: dict) -> str:
    """Render one pick's SHAP breakdown as text Claude can reason over."""
    d = pick.get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    lines = []
    fams = d.get("families") or {}
    if fams:
        lines.append("  Factor families (contribution to score): " +
                     ", ".join(f"{k} {v:+.3f}" for k, v in fams.items()))
    for key, sign in [("positive", "RAISED the score"), ("negative", "LOWERED the score")]:
        for item in (d.get(key) or []):
            lines.append(f"  {sign}: {_label(item['feature'])} = "
                         f"{_where(item.get('percentile'))} "
                         f"[contribution {item['contribution']:+.3f}]")
    return "\n".join(lines) if lines else "  (no breakdown stored)"


def _prompt(picks: list[dict], snap: date) -> str:
    blocks = []
    for p in picks:
        mc = p.get("market_cap")
        mc_s = f"${float(mc)/1e9:.1f}B" if mc else "n/a"
        f = p.get("piotroski_score")
        blocks.append(
            f"\n#{p['rank']} {p['ticker']} — {p.get('company') or p['ticker']}\n"
            f"  Sector: {p.get('sector')} | Market cap: {mc_s} | "
            f"Model score: {p['score']:.3f}\n"
            f"  Piotroski F-Score: {f if f is not None else 'n/a'}/9\n"
            f"{_drivers_block(p)}"
        )

    return f"""You are the analyst voice for an anonymous Canadian quant research Discord.

A LightGBM ranking model scored every liquid TSX name as of {snap} and these are its
top {len(picks)}. For each one you are given the model's actual SHAP breakdown — the
exact per-feature contributions that produced its score.

## The picks
{"".join(blocks)}

## Write one post per pick

Format, using Discord markdown (** for bold, no hashtags, no emoji):

**#N TICKER — Company Name**
Two to three short sentences. Lead with whatever contributed most. Keep it under
60 words — this is a Discord post, not a research note.

## Rules — the first one matters most

1. **Never infer a company's fundamentals from the direction of a contribution.**
   Each line above already states plainly where the value sits ("near the BOTTOM of
   the universe"). Say that. Do NOT reason backwards from a positive contribution to
   conclude the underlying number must be good.

   The model sometimes rewards things that sound bad — a bottom-decile operating
   margin, say. When that happens, write it straight: "the model favoured it despite
   margins near the bottom of the universe". That is an honest and interesting
   sentence. Inventing a reason why a low margin is secretly good is not.

2. **Describe only what the data says.** No news, no catalysts, no management
   commentary, no "the market is pricing in...". You have contributions and
   percentiles — nothing else. If the data doesn't explain why, say the model
   weighted it without speculating about why.

3. Percentiles are relative to the other ~420 liquid TSX names that month.

4. This is a statistical ranking, not advice. No price targets, no "strong buy".

5. Separate posts with a line containing only ---

Write exactly {len(picks)} posts and nothing else."""


def explain_picks(picks: list[dict], snap: date) -> list[str]:
    """
    Ask Claude to narrate each pick from its SHAP breakdown.

    Falls back to a plain rendering of the numbers if the API call fails — a bot
    command that prints the data unadorned is better than one that errors out.
    """
    if not picks:
        return []
    try:
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model=MODEL, max_tokens=4000,
            messages=[{"role": "user", "content": _prompt(picks, snap)}],
        )
        text = resp.content[0].text.strip()
        posts = [p.strip() for p in text.split("\n---\n") if p.strip()]
        if len(posts) == len(picks):
            return posts
        return [text]            # model ignored the separator — send as one block
    except Exception as e:
        print(f"quant_picks: Claude call failed ({e}) — falling back to raw numbers")
        return [f"**#{p['rank']} {p['ticker']}**\n{_drivers_block(p)}" for p in picks]


def build_posts(n: int = TOP_N) -> tuple[list[str], list[str], str | None]:
    """
    Returns (posts, tickers, error).

    tickers is parallel to posts[1:] so the caller can attach a chart per pick,
    matching how the Friday thematic post works.
    """
    picks, snap = load_latest_picks(n)
    if not picks:
        return [], [], ("No scored picks yet — run `9_score.py --save` "
                        "to publish a snapshot.")

    # Universe size is queried, never hardcoded. This line claimed "~420 liquid
    # TSX names" for months, including after 122 depositary receipts were purged.
    size = universe_size(snap)
    scope = f"of {size} TSX names" if size else "of the TSX main board"
    header = (f"**LadBot — Top {len(picks)} Holdings**\n"
              f"Snapshot: {snap}  |  top {len(picks)} {scope}\n"
              f"Model: `{picks[0].get('model_tag') or 'n/a'}`\n"
              f"Statistical ranking, not advice. ↓")
    return [header] + explain_picks(picks, snap), [p["ticker"] for p in picks], None
