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

import calendar
import json
import os
import time
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import requests

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



# ── published picks, and the 3-month positions they imply ────────────────────
#
# THE STRATEGY, stated once so the code below has something to be checked
# against:
#
#   Every publication is a 3-month call. A name not already held is bought at
#   the open on the first trading day after it is published, held for three
#   calendar months, and sold at the open on the first trading day at or after
#   maturity. If it is still being published when its position matures, a fresh
#   3-month position opens. Equal weight per position. No costs, no dividends.
#
# Why three months and not "hold while it stays in the top ten": the model is
# `lambdarank_forward_return_3m` and the walk-forward that produced +3.46% at
# t=2.28 ranked the universe at a month end, bought the top decile and held
# three months. It never sold early. A weekly-rebalanced version of this list is
# a different strategy that has never been measured, so publishing its return
# would be quoting a number with nothing behind it.
#
# The uncomfortable consequence, kept on purpose: a name dropping out of the top
# ten is NOT a sell. The call was "outperforms over three months", so the
# position runs to maturity whatever the next week's ranking says. That is also
# what makes the published number honest - a name the model goes quiet about
# cannot disappear from the record.
#
# MEMBERSHIP IS RECORDED PER WEEK, not just on first appearance. An earlier
# version stored one row per ticker (`pick_history`) and so could not answer
# "what was in the list on Oct 23" or "when did ARG drop out", which is exactly
# what a strategy return needs. pick_history is superseded and no longer read.

HOLD_MONTHS = 3

# How stale the standing list may be and still justify a roll. Picks publish
# weekly, so a gap wider than this means three missed weeks - a broken pipeline,
# not a quiet one. At that point nobody knows whether the model still likes the
# name, and rolling on a months-old list would invent a recommendation it never
# made. The honest action is to let the position mature and stop.
ROLL_STALENESS_DAYS = 21

PUBLISHED_PICKS_DDL = """
    CREATE TABLE IF NOT EXISTS published_picks (
        snapshot_date DATE    NOT NULL,
        ticker        TEXT    NOT NULL,
        rank          INTEGER,
        created_at    TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (snapshot_date, ticker)
    )
"""


def _add_months(d: date, n: int) -> date:
    """
    d plus n calendar months, clamped into a short month.

    Jan 31 + 1 month has no answer, so it becomes Feb 28. Without the clamp the
    date constructor raises and a single awkward entry date takes the post down.
    """
    total = d.month - 1 + n
    year, month = d.year + total // 12, total % 12 + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def record_published(picks: list[dict], snap) -> int:
    """
    Record which names were published on `snap`. Returns rows newly written.

    One row per (snapshot, ticker): this is the membership history the strategy
    is computed from, so it has to capture every week, not just the first time a
    name shows up. The week a name drops out is unrecoverable after the fact -
    nothing else in the database says the bot had been publishing it.

    `snap` is the snapshot date, not today. A Sunday post of Friday's list
    records Friday, so the entry lands on the following Monday's open, which is
    the first price a reader could have traded at.

    Never raises. Failing to record must not stop the post going out.
    """
    if not picks or snap is None:
        return 0
    try:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(PUBLISHED_PICKS_DDL)
                cur.execute("SELECT COUNT(*) AS n FROM published_picks")
                before = cur.fetchone()["n"]
                cur.executemany(
                    "INSERT INTO published_picks (snapshot_date, ticker, rank) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    [(snap, p["ticker"], p.get("rank")) for p in picks])
                cur.execute("SELECT COUNT(*) AS n FROM published_picks")
                after = cur.fetchone()["n"]
            conn.commit()
            return after - before
        finally:
            conn.close()
    except Exception as e:
        print(f"[picks] could not record published picks: {e}")
        return 0


def _price_series(cur, tickers: list[str], since: date) -> dict:
    """{ticker: [(date, open, close), ...]} ascending, in one query."""
    cur.execute("""
        SELECT ticker, date, open, close FROM price_history
         WHERE ticker = ANY(%s) AND date >= %s
         ORDER BY ticker, date
    """, (tickers, since))
    out: dict = {}
    for r in cur.fetchall():
        out.setdefault(r["ticker"], []).append(
            (r["date"], float(r["open"]) if r["open"] else None,
             float(r["close"]) if r["close"] else None))
    return out


def positions(live: bool = True) -> list[dict]:
    """
    Every 3-month position the published lists imply, closed and open.

    One rule generates both the first buy and the rolls: a publication opens a
    position only when nothing is open for that ticker at the time. A repeat
    while held does nothing, and the first publication at or after maturity
    starts the next one - which is exactly "roll if still in the list".

    Returns [] on any failure, so the post degrades to no strategy line rather
    than to no post.
    """
    try:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.published_picks') AS t")
                if cur.fetchone()["t"] is None:
                    return []
                cur.execute("SELECT snapshot_date, ticker FROM published_picks "
                            "ORDER BY ticker, snapshot_date")
                rows = cur.fetchall()
                if not rows:
                    return []
                by_ticker: dict = {}
                for r in rows:
                    by_ticker.setdefault(r["ticker"], []).append(r["snapshot_date"])
                first = min(r["snapshot_date"] for r in rows)
                prices = _price_series(cur, list(by_ticker), first)
        finally:
            conn.close()
    except Exception as e:
        print(f"[picks] could not build positions: {e}")
        return []

    quotes = live_prices(list(by_ticker)) if live else {}
    calendar_dates = sorted({d for ds in by_ticker.values() for d in ds})
    out = []

    for ticker, dates in by_ticker.items():
        series = prices.get(ticker) or []
        if not series:
            continue
        held = set(dates)
        last_date, _, last_close = series[-1]
        pending = sorted(dates)
        i = 0

        while i < len(pending):
            published = pending[i]
            entry = next((r for r in series
                          if r[0] > published and r[1] is not None), None)
            if entry is None:
                break                          # the next open has not traded yet

            while True:
                entry_date, entry_px, _ = entry
                maturity = _add_months(entry_date, HOLD_MONTHS)
                exit_row = next((r for r in series
                                 if r[0] >= maturity and r[1] is not None), None)

                if exit_row is not None:
                    exit_date, exit_px, _ = exit_row
                    state, as_of = "closed", None
                else:
                    q = quotes.get(ticker)
                    exit_px = q["price"] if q else last_close
                    exit_date = q["at"].date() if q and q.get("at") else last_date
                    state, as_of = "open", (q or {}).get("at")

                if entry_px and exit_px:
                    out.append({
                        "ticker": ticker, "published": published,
                        "entry_date": entry_date, "entry_price": entry_px,
                        "maturity": maturity, "exit_date": exit_date,
                        "exit_price": exit_px, "state": state,
                        "ret": exit_px / entry_px - 1,
                        "live": state == "open" and ticker in quotes,
                        "as_of": as_of,
                    })

                if exit_row is None:
                    i = len(pending)           # still running; nothing follows it
                    break

                # Roll if the name was still being published AS OF maturity.
                #
                # Not "at the next publication after maturity": that waited for
                # the following Friday and sat in cash for four days at every
                # roll, which is both untested and a drag the backtest never
                # had. The latest list on or before maturity is the one that
                # was standing at the time, so a name still in it rolls
                # straight on, selling and rebuying at the same open.
                standing = max((d for d in calendar_dates if d <= maturity),
                               default=None)
                fresh = (standing is not None
                         and (maturity - standing).days <= ROLL_STALENESS_DAYS)
                if fresh and standing in held:
                    published, entry = standing, exit_row
                    continue

                # Dropped before maturity: go flat, and pick up again only at
                # the next publication after this position ended.
                while i < len(pending) and pending[i] <= maturity:
                    i += 1
                break
    return out


def attach_returns(picks: list[dict], snap, live: bool = True) -> list[dict]:
    """
    Give each pick the return of its CURRENTLY OPEN position.

    Not "since first ever published": under a rolling 3-month strategy a name
    held continuously for a year is four separate calls, and the row should show
    the one running now. The row therefore resets at each roll, which is the
    visible consequence of the roll rule.
    """
    if not picks:
        return picks
    open_by_ticker = {p["ticker"]: p for p in positions(live=live)
                      if p["state"] == "open"}
    for p in picks:
        pos = open_by_ticker.get(p["ticker"])
        if not pos:
            p["ret"] = None
            continue
        p["ret"] = pos["ret"]
        p["entry_date"] = pos["entry_date"]
        p["entry_price"] = pos["entry_price"]
        p["first_posted"] = pos["published"]
        p["maturity"] = pos["maturity"]
        p["last_price"] = pos["exit_price"]
        p["price_live"] = pos["live"]
        p["price_as_of"] = pos["as_of"] or pos["exit_date"]
    return picks


def price_as_of(picks: list[dict]) -> str:
    """
    Where the current prices came from, for the label.

    Read off the quote's own timestamp rather than assumed from the clock, so a
    Sunday post says "Oct 23 16:00 ET" by itself: the last trade on a closed
    market is Friday's close, and no weekend special case is needed to say so.
    """
    stamps = [p["price_as_of"] for p in picks
              if p.get("price_live") and p.get("price_as_of")]
    if stamps:
        t = max(stamps).astimezone(ZoneInfo("America/Toronto"))
        return f"as of {t:%b} {t.day} {t:%H:%M} ET"
    dates = [p["price_as_of"] for p in picks
             if not p.get("price_live") and p.get("price_as_of")]
    if dates:
        d = max(dates)
        return f"as of {d:%b} {d.day} close"
    return ""


def strategy_summary(live: bool = True) -> str:
    """
    The headline: how every 3-month call has done, including names long gone.

    Equal weight PER POSITION, which is the unit the backtest reported (decile
    spread per 3 months) and the only one that does not need a capital model
    invented for it - weekly cohorts overlap, so "the portfolio return" would
    depend on an allocation rule nobody has specified.

    Closed and open are stated separately once any have closed. They are
    different claims: a closed position is a realised number, an open one is a
    mark that can still move.
    """
    pos = positions(live=live)
    if not pos:
        return ""
    rets = [p["ret"] for p in pos]
    avg = sum(rets) / len(rets)
    closed = [p for p in pos if p["state"] == "closed"]

    bits = [f"strategy {avg * 100:+.1f}% avg per 3-month call",
            f"{len(pos)} call{'' if len(pos) == 1 else 's'}"]
    if closed:
        done = sum(p["ret"] for p in closed) / len(closed)
        bits.append(f"{len(closed)} closed {done * 100:+.1f}%")
    else:
        bits.append("none matured yet")
    return "  ·  ".join(bits)


# How long a batch of quotes stays good. One !picks calls attach_returns twice -
# once for the embed, once for the image - and without this that is two
# identical FMP calls a second apart. Also absorbs someone pressing !picks
# repeatedly.
QUOTE_TTL_SECONDS = 120
_QUOTE_CACHE: dict = {"at": 0.0, "data": {}}


def live_prices(tickers: list[str]) -> dict:
    """
    Last traded price per ticker from FMP, or {} if anything goes wrong.

    One batched call for the whole list, not one per name: /stable/batch-quote
    takes a comma-separated `symbols`. The singular /stable/quote returns an
    empty list for a comma list, which looks like "no data" rather than an
    error, so the plural endpoint is load-bearing here.

    Returns {} rather than raising. A quote service being down must degrade the
    post to stored closes, not stop it: price_history is always there, and a
    slightly stale number beats no picks.

    On a Sunday this returns Friday's close anyway, because that is the last
    trade. The label downstream reads the quote's own timestamp rather than
    assuming, so the weekend post says Friday without any special case.
    """
    key = os.getenv("FMP_API_KEY")
    if not key or not tickers:
        return {}

    now = time.time()
    if _QUOTE_CACHE["data"] and now - _QUOTE_CACHE["at"] < QUOTE_TTL_SECONDS:
        return _QUOTE_CACHE["data"]

    try:
        r = requests.get("https://financialmodelingprep.com/stable/batch-quote",
                         params={"symbols": ",".join(tickers), "apikey": key},
                         timeout=8)
        r.raise_for_status()
        out = {}
        for row in r.json() or []:
            sym, px, ts = row.get("symbol"), row.get("price"), row.get("timestamp")
            if sym and px:
                out[sym] = {
                    "price": float(px),
                    "at": datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None,
                }
        _QUOTE_CACHE.update(at=now, data=out)
        return out
    except Exception as e:
        print(f"[picks] live quotes unavailable, falling back to stored close: {e}")
        return {}


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
W_RET = 7      # "+12.4%" plus a space, or "new"


def _ret_cell(pick: dict) -> str:
    """The since-first-published return, padded to its column.

    "new" rather than "0.0%" when no entry price exists yet: a name
    published on Sunday off Friday's snapshot has no Monday open to enter
    at, and a zero would claim a flat week that has not happened.
    """
    r = pick.get("ret")
    text = "new" if r is None else f"{r * 100:+.1f}%"
    return f"{text:>{W_RET}}"


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


def turnover(snap, n: int = 10) -> dict | None:
    """
    How the top n changed since the previous scored snapshot.

    Returns {"prev": date, "entered": [...], "exited": [...], "reordered": bool},
    or None when there is nothing to compare against — or when the query fails,
    because a decoration on a Discord post must never be the thing that stops the
    post going out.

    Worth knowing before reading the output: the answer is usually "nothing
    changed". The factors are 12-month momentum, TTM margins and quarterly
    fundamentals, so a weekly re-score has almost no new information to act on —
    Oct 16 to Oct 23 held all ten names and only shuffled the middle ranks. That
    is a property of a 3-month-horizon model, not a fault, and it is exactly why
    the line is worth printing: the week it says something other than "unchanged"
    is the week a holder needs to look.

    The comparison is against the previous SNAPSHOT, not against "last week", and
    it prints that date. If a Friday run is ever skipped, the honest statement is
    "unchanged since Oct 16", not a claim about a week that was never scored.
    """
    try:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(snapshot_date) AS d FROM predictions "
                            "WHERE snapshot_date < %s", (snap,))
                row = cur.fetchone()
                prev = row["d"] if row else None
                if prev is None:
                    return None

                def top(d):
                    # Same one-model-run filter as load_latest_picks(): a single
                    # snapshot_date can hold several model_tags (2026-09-30 holds
                    # two), and mixing them would report phantom turnover.
                    cur.execute("""
                        WITH latest AS (
                            SELECT model_tag FROM predictions
                            WHERE snapshot_date = %s
                            GROUP BY model_tag
                            ORDER BY MAX(created_at) DESC
                            LIMIT 1
                        )
                        SELECT ticker FROM predictions
                        WHERE snapshot_date = %s
                          AND model_tag = (SELECT model_tag FROM latest)
                        ORDER BY rank
                        LIMIT %s
                    """, (d, d, n))
                    return [r["ticker"] for r in cur.fetchall()]

                now, before = top(snap), top(prev)
                if not now or not before:
                    return None
                return {
                    "prev": prev,
                    "entered": [t for t in now if t not in before],
                    "exited": [t for t in before if t not in now],
                    "reordered": now != before,
                }
        finally:
            conn.close()
    except Exception:
        return None


def _names(tickers: list[str], cap: int = 5) -> str:
    """
    A ticker list for prose, bounded so a heavy-turnover week cannot run long.

    Five is well above what this model does in a week - Oct 16 to Oct 23 changed
    nothing at all - but a quarterly fundamentals refresh can land several names
    at once, and an unbounded list would push the line past the table width.
    """
    shown = [t.removesuffix(".TO") for t in tickers[:cap]]
    extra = len(tickers) - len(shown)
    return ", ".join(shown) + (f" and {extra} more" if extra else "")


def turnover_note(snap, n: int = 10) -> str:
    """
    The one line above the table that is allowed to differ week to week.

    Everything else there (model type, universe size, horizon) is identical
    every Friday, which is why it was cut to a single line and the rest moved to
    !model. A paragraph nobody's eye stops on is worse than no paragraph.

    Names that ENTERED the list are already visible in the table underneath it.
    Names that DROPPED OUT appear nowhere else in the post, so this line is the
    only place they are reported, and the dropped list is never the one trimmed
    first. _names() caps each side at five independently for that reason.

    Register matches !model: declarative, no contractions, no em dash. "Added"
    and "Dropped" rather than "in" and "out", which read as a trade blotter.
    """
    t = turnover(snap, n)
    if not t:
        return ""

    # "Oct 6", not "Oct 06": %d zero-pads single digits.
    when = f"{t['prev']:%b} {t['prev'].day}"

    if not t["entered"] and not t["exited"]:
        return (f"Constituents unchanged since {when}, ranks reordered."
                if t["reordered"] else f"Unchanged since {when}.")

    # max(), not len(entered): with a fixed n the two are equal, but if the
    # universe ever shrinks below n they diverge and the larger is the honest
    # count of what moved.
    k = max(len(t["entered"]), len(t["exited"]))
    out = [f"{k} change{'' if k == 1 else 's'} since {when}."]
    if t["entered"]:
        out.append(f"Added {_names(t['entered'])}.")
    if t["exited"]:
        out.append(f"Dropped {_names(t['exited'])}.")
    return " ".join(out)


def model_blurb(picks: list[dict], size: int | None, snap=None) -> str:
    """
    Two short lines above the table: what the model is, and what moved.

    This was a five-sentence paragraph, and it was the same five sentences every
    week. Read once it is informative; read weekly it is wallpaper, and wallpaper
    above the table trains people to skip the space where something that mattered
    might one day appear. So the standing description moved to !model, where it is
    read on purpose, and what is left is one identity line plus turnover_note() —
    the only part with any reason to change.

    Every value is still derived rather than written down, so it cannot drift from
    the model the way a hardcoded description would. model_tag carries the
    objective and the horizon; the stored drivers carry the factor count.

    NO PERFORMANCE NUMBER, here or in model_detail(). An earlier version quoted
    "roughly 2-3.5% per quarter" from the backtest. Deliberately removed: in a
    chat post a historical range is read as a forecast, and that one rests on 40
    overlapping months — about 13 independent periods — with feature selection
    that used test-window data. Describing the method is defensible; publishing an
    expected return is not, and this is the wrong surface for the caveats that
    would make it so.
    """
    tag = (picks[0].get("model_tag") or "") if picks else ""
    kind = "ranking" if tag.startswith("lambdarank") else "classification"
    horizon = "3-month" if "_3m" in tag else "forward"
    n_feat = len(model_features(picks))

    bits = [f"LightGBM {kind} model",
            f"{size} TSX names" if size else "TSX main board",
            f"{n_feat} factors" if n_feat else "a factor set",
            f"{horizon} horizon"]

    note = turnover_note(snap, len(picks)) if snap and picks else ""
    tail = " ".join(x for x in (note, "`!model` for methodology.") if x)
    return "  ·  ".join(bits) + "\n" + tail


def model_features(picks: list[dict]) -> list[str]:
    """
    The factors the model actually used, read back off a stored prediction.

    Derived rather than listed, for the same reason universe_size() is queried:
    a hardcoded set of 16 names survives exactly until the next feature cut. Any
    pick carries all of them now that 9_score.py stores every contribution
    instead of the top five.
    """
    if not picks:
        return []
    d = picks[0].get("drivers") or {}
    if isinstance(d, str):
        d = json.loads(d)
    names = [x["feature"] for x in (d.get("positive") or [])]
    names += [x["feature"] for x in (d.get("negative") or [])]
    return sorted(set(names))


def _field(name: str, value: str) -> tuple[str, str]:
    """
    One embed field, truncated to Discord's limit.

    A field value over 1024 characters does not get trimmed by Discord, it makes
    the API reject the whole message - so !model would fail entirely rather than
    print a slightly short factor list. The list is derived from whatever
    9_score.py stored, so its length is not under this file's control.
    """
    if len(value) > 1024:
        value = value[:1021].rstrip() + "..."
    return (name[:256], value)


def model_detail() -> tuple[dict | None, str | None]:
    """
    The standing methodology note, for !model. Returns (payload, error).

    Register: a fund factsheet. Declarative, no contractions, correct technical
    terms used rather than avoided, numbers with their units.

    This took three passes and the two failure modes are worth recording,
    because they pull in opposite directions and the obvious fix for one causes
    the other.

    1. "Sounds like AI wrote it." The cause was RHETORICAL STRUCTURE, not
       vocabulary: a bolded label in front of every single sentence, an em dash
       per clause, each point framed as "it does not do X, it does Y", and a
       closing aphorism ("what the model did, which is not the same as saying it
       was right"). That is the register of generated prose.

    2. "Too casual or basic." Caused by fixing (1) the wrong way - flattening
       everything to spoken English with contractions and vague quantifiers
       ("a few valuation multiples", "the top 10 go up", "122 of them in here").
       Plain is not the same as informal, and avoiding a term is not the same as
       not padding around it.

    So: formal register, precise terms (query group, cross-sectional, early
    stopping), and NO rhetorical scaffolding. Sections are real embed fields, so
    a header is a header rather than bold text imitating one.

    Returns a payload rather than a discord.Embed, so this module still needs no
    discord.py import and can be read from a terminal, same as
    build_table_embed().

    NO PERFORMANCE NUMBER here either - see model_blurb() for why.
    """
    picks, snap = load_latest_picks(10)
    if not picks:
        return None, ("No scored picks yet - run `9_score.py --save` "
                      "to publish a snapshot.")

    tag = picks[0].get("model_tag") or ""
    feats = model_features(picks)
    size = universe_size(snap)
    trees = tag.rsplit("_it", 1)[-1] if "_it" in tag else None

    universe = f"the {size} TSX names" if size else "the TSX names"
    tree_bit = (
        f" Tree count is set by early stopping rather than specified: training "
        f"halts when performance on a held-out tail ceases to improve, which "
        f"produced the `it{trees}` suffix in the model tag. The value varies "
        f"between runs."
        if trees else "")

    fields = [
        _field(f"Factor set ({len(feats)})",
               "Momentum, valuation multiples, profitability, capital "
               "allocation, growth and size. The majority are ranked "
               "cross-sectionally within sector rather than applied on an "
               "absolute basis.\n"
               "```\n" + ", ".join(feats) + "\n```"),

        _field("Training",
               f"Fitted on month-end cross-sections from 2019 to the present, "
               f"refit from scratch on each run with no state carried "
               f"forward.{tree_bit}"),

        _field("Universe exclusions",
               "Canadian Depositary Receipts are excluded. These are CAD-hedged "
               "wrappers on US-listed companies rather than Canadian operating "
               "businesses; 122 were present in the universe and were "
               "distorting every sector-relative factor."),

        _field("Interpretation",
               "Scores are comparable only within a single month. The ranking "
               "objective is defined over per-month query groups and is "
               "invariant to the absolute level of scores within a group, so no "
               "cross-month scale is established. Use rank rather than score "
               "for week-over-week comparison.\n\n"
               "Chips report each name's factor attributions, largest first. "
               "Cyan denotes a positive contribution to the score, pink a "
               "negative one. Attributions describe the inputs to the ranking "
               "and are not a measure of forecast accuracy."),
    ]

    return {
        "title": "LadBot — Model Methodology",
        # "It's a LightGBM model, so gradient-boosted trees" was the first
        # version and it explained nothing: LightGBM is a library, gradient
        # boosting is the algorithm inside it, and the "so" only parses for a
        # reader who already knew that. Naming both terms is not the same as
        # stating the mechanism, so the mechanism is stated.
        "intro": (f"A LightGBM gradient-boosted decision tree model. The score "
                  f"is the summed output of a sequence of shallow trees, each "
                  f"fitted to the gradient of the ranking loss left by those "
                  f"preceding it. The objective is cross-sectional ranking "
                  f"rather than return forecasting: each month the model orders "
                  f"{universe} that clear the data-quality gates, and the top "
                  f"{len(picks)} are published."),
        "fields": fields,
        "footer": f"{tag}  ·  snapshot {snap}",
        "color": 0xFF2D78,
    }, None


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

    picks = attach_returns(picks, snap)

    head = _row(f"{'#':>{W_RANK}}", f"{'TICKER':<{W_TICK}}", f"{'SEC':<{W_SEC}}",
                [(f"{'WHAT HELPED':<{W_LABEL}}", " " * W_SCORE),
                 (f"{'SINCE':>{W_RET}}", "")], colour=False)
    rows = [_c("header", head.rstrip())]
    for p in picks:
        tick = (p["ticker"] or "").removesuffix(".TO")[:W_TICK]
        raw = p.get("sector") or ""
        sec = SECTOR_CODE.get(raw, raw[:4])
        # One driver cell, not two: the return is now one of the essentials and
        # keeping both drivers alongside it pushes the line past ~50 columns,
        # which scrolls sideways on a phone. The image carries four drivers.
        rows.append(_row(f"{p['rank']:>{W_RANK}}", f"{tick:<{W_TICK}}",
                         f"{sec:<{W_SEC}}",
                         _driver_cells(p, 1) + [(_ret_cell(p), "")], colour=True))

    size = universe_size(snap)
    scope = f"ranked {size} TSX names" if size else "TSX main board"
    return {
        "title": f"LadBot \u2014 Top {len(picks)} Holdings",
        "blurb": model_blurb(picks, size, snap),
        "body": "```ansi\n" + "\n".join(rows) + "\n```",
        "footer": (f"{snap}  \u00b7  {scope}  \u00b7  "
                   + (strategy_summary() or
                      "SINCE = return of the open 3-month call")),
        "color": 0xFF2D78,
        # Handed back so the caller can record what it actually posted
        # without running the query a second time.
        "picks": picks,
        "snap": snap,
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
