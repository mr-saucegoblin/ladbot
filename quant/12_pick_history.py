"""
12_pick_history.py - seed pick_history from the snapshots already published.

Going forward the bot records a name the moment it posts it (quant_picks.
record_published). This is the one-off that fills in what was published before
that code existed, so the returns do not all start from today.

WHICH SNAPSHOTS COUNT AS PUBLISHED

Not every row in `predictions` was ever shown to anyone. 2026-09-30 was a
Wednesday run against the pre-purge 410-name universe, under a different model
(it231/it131), before the Discord picks post existed. Treating it as a
recommendation would date TOT, PBL and TWM from 2026-10-01 on the strength of a
list nobody saw.

So the published series starts at the first Friday snapshot, and that date is a
constant here rather than a filter buried in a query. If it is wrong, it is one
line, and --dry-run shows exactly what changes.

Usage:
    python quant/12_pick_history.py --dry-run     # show what would be written
    python quant/12_pick_history.py               # write it
    python quant/12_pick_history.py --show        # print current table + returns
"""

import argparse
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_db import get_conn

# The first snapshot the bot actually published. Everything before this was a
# development run. See the module docstring.
SERIES_START = datetime.date(2026, 10, 16)

# How many names a post shows. Historical membership is frozen at what was
# published at the time, which is the whole reason this table exists rather
# than a `rank <= PICKS_COUNT` query that would move with the constant.
PUBLISHED_TOP_N = 10

DDL = """
    CREATE TABLE IF NOT EXISTS pick_history (
        ticker       TEXT PRIMARY KEY,
        first_posted DATE NOT NULL,
        first_rank   INTEGER,
        created_at   TIMESTAMPTZ DEFAULT NOW()
    )
"""

# One row per (snapshot, ticker) for the published top N, using only the most
# recent model run of each snapshot - a snapshot can hold several, and mixing
# them would invent names that were never top ten in the run that was posted.
PUBLISHED = """
    WITH chosen AS (
        SELECT snapshot_date, model_tag,
               ROW_NUMBER() OVER (PARTITION BY snapshot_date
                                  ORDER BY MAX(created_at) DESC) AS rn
        FROM predictions
        GROUP BY snapshot_date, model_tag
    )
    SELECT p.snapshot_date, p.ticker, p.rank
    FROM predictions p
    JOIN chosen c ON c.snapshot_date = p.snapshot_date
                 AND c.model_tag = p.model_tag AND c.rn = 1
    WHERE p.rank <= %s AND p.snapshot_date >= %s
    ORDER BY p.snapshot_date, p.rank
"""


def first_publications(cur):
    """{ticker: (first_posted, rank_on_that_date)} across the published series."""
    cur.execute(PUBLISHED, (PUBLISHED_TOP_N, SERIES_START))
    first = {}
    for r in cur.fetchall():
        if r["ticker"] not in first:          # rows arrive oldest-first
            first[r["ticker"]] = (r["snapshot_date"], r["rank"])
    return first


def show(cur):
    """
    Print the table and the returns, using the SAME pricing path as the bot.

    This had its own SQL against price_history and so quoted the last stored
    close, while Discord quotes a live quote. The two disagreed badly - ARG read
    -13.74% here and -3.19% there on the same entry price, because the stored
    close was five days old. One concept must have one implementation, so this
    calls quant_picks.attach_returns() and prints what the post will print.
    """
    import quant_picks as qp

    cur.execute("SELECT to_regclass('public.pick_history') AS t")
    if cur.fetchone()["t"] is None:
        print("  pick_history does not exist yet. Run without --show first.")
        return

    cur.execute("""SELECT ticker, first_posted, first_rank FROM pick_history
                   ORDER BY first_posted, first_rank""")
    rows = cur.fetchall()
    if not rows:
        print("  pick_history is empty.")
        return

    cur.execute("SELECT MAX(snapshot_date) AS d FROM predictions")
    snap = cur.fetchone()["d"]

    picks = qp.attach_returns([{"ticker": r["ticker"]} for r in rows], snap)
    by_ticker = {p["ticker"]: p for p in picks}

    print(f"  {'ticker':<9}{'published':<12}{'entry':<12}{'open':>8}{'now':>8}{'return':>9}")
    rets = []
    for r in rows:
        p = by_ticker.get(r["ticker"], {})
        ret = p.get("ret")
        if ret is None:
            op = lp = "-"
            rs = "new"
        else:
            rets.append(ret)
            op = f"{float(p['entry_price']):.2f}"
            lp = f"{float(p['last_price']):.2f}"
            rs = f"{ret * 100:+.2f}%"
        print(f"  {r['ticker'].replace('.TO', ''):<9}{str(r['first_posted']):<12}"
              f"{str(p.get('entry_date') or '-'):<12}{op:>8}{lp:>8}{rs:>9}")

    if rets:
        up = sum(1 for x in rets if x > 0)
        print(f"\n  equal weighted: {sum(rets) / len(rets) * 100:+.2f}%   "
              f"{up}/{len(rets)} up   {len(rets)} priced of {len(rows)}")
    stamp = qp.price_as_of(picks)
    if stamp:
        print(f"  prices {stamp}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="show what would be written")
    ap.add_argument("--show", action="store_true", help="print the table and current returns")
    args = ap.parse_args()

    with get_conn() as conn, conn.cursor() as cur:
        if args.show:
            show(cur)
            return

        cur.execute(DDL)
        first = first_publications(cur)
        if not first:
            print(f"  No published snapshots on or after {SERIES_START}. Nothing to do.")
            return

        cur.execute("SELECT ticker FROM pick_history")
        have = {r["ticker"] for r in cur.fetchall()}
        todo = {t: v for t, v in first.items() if t not in have}

        print(f"  published series starts {SERIES_START}, top {PUBLISHED_TOP_N}")
        print(f"  {len(first)} distinct names published, {len(have)} already recorded, "
              f"{len(todo)} to add\n")
        for t, (d, rank) in sorted(todo.items(), key=lambda kv: (kv[1][0], kv[1][1])):
            print(f"    {t.replace('.TO', ''):<9} first published {d}  at #{rank}")

        if args.dry_run:
            print("\n  --dry-run: nothing written.")
            return
        if not todo:
            print("\n  Nothing to add.")
            return

        cur.executemany(
            "INSERT INTO pick_history (ticker, first_posted, first_rank) "
            "VALUES (%s, %s, %s) ON CONFLICT (ticker) DO NOTHING",
            [(t, d, rank) for t, (d, rank) in todo.items()])
        conn.commit()
        print(f"\n  Wrote {len(todo)} rows.\n")
        show(cur)


if __name__ == "__main__":
    main()
