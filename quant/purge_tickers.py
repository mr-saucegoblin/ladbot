"""
purge_tickers.py — Remove tickers and all their data from every table.

Deletes children first (FK constraints point at tickers.ticker), then the
tickers row itself, all inside one transaction so a failure leaves nothing
half-removed.

Selection is explicit — either a literal ticker list or a named preset — so a
purge can never silently widen beyond what was reviewed.

Usage:
    python purge_tickers.py --preset foreign-currency   # non-CAD/USD reporters
    python purge_tickers.py --cdr-list --dry-run        # reviewed CDR list
    python purge_tickers.py --tickers AYM.TO,WGX.TO
    python purge_tickers.py --preset foreign-currency --dry-run
"""

import argparse
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# Every table carrying a ticker FK. tickers itself is deleted last.
CHILD_TABLES = [
    "analyst_estimates", "analyst_ratings", "balance_sheets", "cash_flows",
    "dividends", "earnings_call_transcripts", "earnings_surprises",
    "feature_snapshots", "financial_scores", "income_statements",
    "insider_trades", "institutional_ownership", "key_metrics",
    "news_articles", "price_history", "price_targets", "upgrades_downgrades",
]

PRESETS = {
    # Reporters in a currency we have no FX series for. Converting their
    # fundamentals to CAD isn't possible, so their size/valuation features
    # would be silently wrong rather than merely missing.
    "foreign-currency": """
        SELECT ticker FROM tickers
        WHERE reported_currency IS NOT NULL
          AND reported_currency NOT IN ('CAD', 'USD')
    """,
    # Not operating companies: depositary receipts (CAD-hedged wrappers around
    # foreign stocks) and funds/ETFs. Requires backfill_ticker_flags.py to have
    # run. Deliberately keyed off is_adr rather than country — country='US' also
    # covers Brookfield, Restaurant Brands and Newmont, which are legitimate.
    #
    # is_fund alone is NOT trustworthy: FMP flagged InterRent REIT as a fund while
    # leaving Canadian Apartment Properties and Boardwalk — the same business —
    # unflagged. So a fund must ALSO have no income statements, which is what
    # actually separates an investment vehicle from an operating company.
    "adr-and-funds": """
        SELECT t.ticker FROM tickers t
        WHERE t.is_adr IS TRUE
           OR (t.is_fund IS TRUE
               AND NOT EXISTS (SELECT 1 FROM income_statements i WHERE i.ticker = t.ticker))
    """,
}


def get_conn():
    url = os.getenv("QUANT_DB_URL")
    if not url:
        raise RuntimeError("QUANT_DB_URL not set in .env")
    return psycopg2.connect(url)


def resolve_tickers(cur, args) -> list[str]:
    if args.tickers:
        return [t.strip() for t in args.tickers.split(",") if t.strip()]
    if args.cdr_list:
        # The reviewed depositary-receipt list. Intersected with what is actually
        # in the DB so the confirmation count reflects real deletions rather than
        # the file's length. See cdr_list.py for why this is a file and not a flag.
        from cdr_list import load_cdr_exclusions
        wanted = sorted(load_cdr_exclusions())
        if not wanted:
            print("cdr_exclusions.txt is empty or missing.")
            return []
        cur.execute("SELECT ticker FROM tickers WHERE ticker = ANY(%s)", (wanted,))
        present = [r[0] for r in cur.fetchall()]
        print(f"cdr_exclusions.txt lists {len(wanted)}; "
              f"{len(present)} are present in the database.")
        return present
    cur.execute(PRESETS[args.preset])
    return [r[0] for r in cur.fetchall()]


def run(args):
    conn = get_conn()
    cur = conn.cursor()

    try:
        tickers = resolve_tickers(cur, args)
        if not tickers:
            print("Nothing matched — nothing to purge.")
            return

        cur.execute(
            "SELECT ticker, name, reported_currency FROM tickers WHERE ticker = ANY(%s) ORDER BY ticker",
            (tickers,),
        )
        print(f"About to purge {len(tickers)} ticker(s):")
        for t, n, c in cur.fetchall():
            print(f"  {t:<12} {(n or '')[:40]:<42} {c}")

        if args.dry_run:
            print("\n--dry-run: nothing deleted.")
            return

        total = 0
        for table in CHILD_TABLES:
            cur.execute(f"DELETE FROM {table} WHERE ticker = ANY(%s)", (tickers,))
            if cur.rowcount:
                total += cur.rowcount
                print(f"  {table:<28} {cur.rowcount:>8,} rows deleted")

        cur.execute("DELETE FROM tickers WHERE ticker = ANY(%s)", (tickers,))
        print(f"  {'tickers':<28} {cur.rowcount:>8,} rows deleted")

        conn.commit()
        print(f"\nCommitted. Total child rows deleted: {total:,}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR — rolled back, nothing deleted: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--preset", choices=sorted(PRESETS), help="Named selection")
    group.add_argument("--tickers", help="Comma-separated ticker list")
    group.add_argument("--cdr-list", action="store_true",
                       help="Purge every ticker in the reviewed cdr_exclusions.txt")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted, change nothing")
    run(parser.parse_args())
