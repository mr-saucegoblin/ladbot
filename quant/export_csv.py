"""
export_csv.py — Export fundamentals joined with price context to CSV for spot-checking.

Rather than dumping the full price_history table (~1.5M rows across 660 tickers),
this joins each fundamentals row to the most recent price on-or-before the date
that data became known (filed_at when FMP has it, else fiscal_date) — the price
context an analyst would have actually seen alongside that data point.

Usage:
    python export_csv.py
"""

import os
import pathlib
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

OUT_DIR = pathlib.Path(__file__).parent / "exports"

TICKERS_QUERY = "SELECT * FROM tickers ORDER BY ticker"

FUNDAMENTALS_QUERY = """
    SELECT
        i.ticker, i.period, i.fiscal_date, i.filed_at,
        i.revenue, i.gross_profit, i.operating_income, i.net_income, i.ebitda, i.eps, i.shares_outstanding,
        b.total_assets, b.total_liabilities, b.total_equity, b.cash_and_equivalents, b.total_debt, b.net_debt,
        c.operating_cash_flow, c.capex, c.free_cash_flow, c.dividends_paid,
        k.market_cap, k.enterprise_value, k.pe_ratio, k.price_to_sales, k.price_to_book, k.price_to_fcf,
        k.ev_to_ebitda, k.ev_to_operating_cf, k.ev_to_fcf, k.earnings_yield, k.fcf_yield, k.dividend_yield,
        k.roe, k.roa, k.roic, k.debt_to_equity, k.net_debt_to_ebitda, k.current_ratio,
        k.interest_coverage, k.payout_ratio,
        p.date AS price_asof_date, p.close AS price_close
    FROM income_statements i
    LEFT JOIN balance_sheets b ON b.ticker = i.ticker AND b.period = i.period AND b.fiscal_date = i.fiscal_date
    LEFT JOIN cash_flows     c ON c.ticker = i.ticker AND c.period = i.period AND c.fiscal_date = i.fiscal_date
    LEFT JOIN key_metrics    k ON k.ticker = i.ticker AND k.period = i.period AND k.fiscal_date = i.fiscal_date
    LEFT JOIN LATERAL (
        SELECT date, close FROM price_history ph
        WHERE ph.ticker = i.ticker AND ph.date <= COALESCE(i.filed_at, i.fiscal_date)
        ORDER BY ph.date DESC LIMIT 1
    ) p ON true
    ORDER BY i.ticker, i.period, i.fiscal_date
"""

DIVIDENDS_QUERY = """
    SELECT
        d.ticker, d.ex_dividend_date, d.payment_date, d.dividend, d.adjusted_dividend,
        p.date AS price_asof_date, p.close AS price_close
    FROM dividends d
    LEFT JOIN LATERAL (
        SELECT date, close FROM price_history ph
        WHERE ph.ticker = d.ticker AND ph.date <= d.ex_dividend_date
        ORDER BY ph.date DESC LIMIT 1
    ) p ON true
    ORDER BY d.ticker, d.ex_dividend_date
"""

EARNINGS_QUERY = """
    SELECT
        e.ticker, e.fiscal_date, e.estimated_eps, e.actual_eps, e.surprise_eps, e.surprise_eps_pct,
        p.date AS price_asof_date, p.close AS price_close
    FROM earnings_surprises e
    LEFT JOIN LATERAL (
        SELECT date, close FROM price_history ph
        WHERE ph.ticker = e.ticker AND ph.date <= e.fiscal_date
        ORDER BY ph.date DESC LIMIT 1
    ) p ON true
    ORDER BY e.ticker, e.fiscal_date
"""

MACRO_QUERY = """
    SELECT series, date, value, source
    FROM macro_data
    ORDER BY series, date
"""

def export(name: str, query: str, conn) -> None:
    df = pd.read_sql(query, conn)
    out_path = OUT_DIR / f"{name}.csv"
    df.to_csv(out_path, index=False)
    print(f"  {name:<28} {len(df):>8,} rows  -> {out_path}")


def run():
    OUT_DIR.mkdir(exist_ok=True)
    # Plain connection (not quant_db.get_conn()'s RealDictCursor) — pandas.read_sql
    # needs tuple-style rows; a dict-cursor silently corrupts the result (confirmed:
    # every row came back as the literal column-name strings instead of real data).
    conn = psycopg2.connect(os.getenv("QUANT_DB_URL"))
    try:
        print("Exporting CSVs for spot-checking...\n")
        export("tickers", TICKERS_QUERY, conn)
        export("fundamentals_with_price", FUNDAMENTALS_QUERY, conn)
        export("dividends_with_price", DIVIDENDS_QUERY, conn)
        export("earnings_surprises_with_price", EARNINGS_QUERY, conn)
        export("macro_data", MACRO_QUERY, conn)
    finally:
        conn.close()
    print(f"\nDone. Files in {OUT_DIR}")


if __name__ == "__main__":
    run()
