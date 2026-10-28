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
    CREATE TABLE IF NOT EXISTS published_picks (
        snapshot_date DATE    NOT NULL,
        ticker        TEXT    NOT NULL,
        rank          INTEGER,
        created_at    TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (snapshot_date, ticker)
    )
"""

# pick_history (one row per ticker, first appearance only) is SUPERSEDED. It
# could not say what the list held in a given week or when a name dropped out,
# which is exactly what a strategy return needs. Nothing reads it any more.

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


def membership(cur):
    """[(snapshot_date, ticker, rank)] for every published top N."""
    cur.execute(PUBLISHED, (PUBLISHED_TOP_N, SERIES_START))
    return [(r["snapshot_date"], r["ticker"], r["rank"])
            for r in cur.fetchall()]


def show(cur):
    """
    Print every 3-month position the published lists imply, via the SAME code
    path the bot uses.

    An earlier version had its own SQL against price_history and so quoted the
    last stored close while Discord quoted a live one. They disagreed by ten
    points on ARG. One concept, one implementation.
    """
    import quant_picks as qp

    pos = qp.positions()
    if not pos:
        print("  No positions yet. Nothing published, or no price after it.")
        return

    print(f"  {'ticker':<8}{'published':<12}{'entry':<12}{'matures':<12}"
          f"{'in':>8}{'now':>8}{'return':>9}  state")
    for p in sorted(pos, key=lambda x: (x["entry_date"], x["ticker"])):
        print(f"  {p['ticker'].replace('.TO', ''):<8}{str(p['published']):<12}"
              f"{str(p['entry_date']):<12}{str(p['maturity']):<12}"
              f"{p['entry_price']:>8.2f}{p['exit_price']:>8.2f}"
              f"{p['ret'] * 100:>8.2f}%  {p['state']}")
    print("\n  " + (qp.strategy_summary() or ""))


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
        rows = membership(cur)
        if not rows:
            print(f"  No published snapshots on or after {SERIES_START}.")
            return

        cur.execute("SELECT snapshot_date, ticker FROM published_picks")
        have = {(r["snapshot_date"], r["ticker"]) for r in cur.fetchall()}
        todo = [r for r in rows if (r[0], r[1]) not in have]

        weeks = sorted({r[0] for r in rows})
        print(f"  published series starts {SERIES_START}, top {PUBLISHED_TOP_N}")
        print(f"  {len(weeks)} published week(s): "
              + ", ".join(str(w) for w in weeks))
        print(f"  {len(rows)} membership rows, {len(have)} recorded, "
              f"{len(todo)} to add\n")
        for d, t, rank in todo:
            print(f"    {d}  #{rank:<3} {t.replace('.TO', '')}")

        if args.dry_run:
            print("\n  --dry-run: nothing written.")
            return
        if not todo:
            print("\n  Nothing to add.\n")
            show(cur)
            return

        cur.executemany(
            "INSERT INTO published_picks (snapshot_date, ticker, rank) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", todo)
        conn.commit()
        print(f"\n  Wrote {len(todo)} rows.\n")
        show(cur)


if __name__ == "__main__":
    main()
