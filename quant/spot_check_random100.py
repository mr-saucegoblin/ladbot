"""
spot_check_random100.py — One-off: latest fundamentals+price snapshot for 100
random tickers, for manually cross-referencing against Yahoo/Google Finance
to sanity-check the FMP pipeline's data.

Usage:
    python spot_check_random100.py
"""

import os
import pathlib
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

OUT_PATH = pathlib.Path(__file__).parent / "exports" / "spot_check_random100.csv"

QUERY = """
    WITH latest_quarter AS (
        SELECT DISTINCT ON (i.ticker)
            i.ticker, i.fiscal_date, i.filed_at,
            i.revenue, i.net_income, i.eps, i.shares_outstanding,
            k.market_cap, k.pe_ratio, k.price_to_book, k.ev_to_ebitda,
            k.roe, k.roa, k.debt_to_equity, k.dividend_yield, k.current_ratio
        FROM income_statements i
        LEFT JOIN key_metrics k
            ON k.ticker = i.ticker AND k.period = i.period AND k.fiscal_date = i.fiscal_date
        WHERE i.period = 'quarterly'
        ORDER BY i.ticker, i.fiscal_date DESC
    )
    SELECT
        t.ticker, t.name, t.sector, t.industry,
        lq.fiscal_date, lq.filed_at,
        lq.revenue, lq.net_income, lq.eps, lq.shares_outstanding,
        lq.market_cap, lq.pe_ratio, lq.price_to_book, lq.ev_to_ebitda,
        lq.roe, lq.roa, lq.debt_to_equity, lq.dividend_yield, lq.current_ratio,
        p.date AS price_asof_date, p.close AS price_close
    FROM latest_quarter lq
    JOIN tickers t ON t.ticker = lq.ticker
    LEFT JOIN LATERAL (
        SELECT date, close FROM price_history ph
        WHERE ph.ticker = lq.ticker AND ph.date <= COALESCE(lq.filed_at, lq.fiscal_date)
        ORDER BY ph.date DESC LIMIT 1
    ) p ON true
    WHERE lq.market_cap IS NOT NULL
      AND t.ticker !~ '-P[A-Z]+.TO$'  -- exclude preferred shares (e.g. RY-PZ.TO, TD-PFK.TO)
    ORDER BY RANDOM()
    LIMIT 100
"""


def run():
    OUT_PATH.parent.mkdir(exist_ok=True)
    # Plain connection (not quant_db.get_conn()'s RealDictCursor) — pandas.read_sql
    # needs tuple-style rows; a dict-cursor silently corrupts the result (confirmed:
    # every row came back as the literal column-name strings instead of real data).
    conn = psycopg2.connect(os.getenv("QUANT_DB_URL"))
    try:
        df = pd.read_sql(QUERY, conn)
        df.to_csv(OUT_PATH, index=False)
        print(f"{len(df)} rows -> {OUT_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
