"""
quant_db.py — PostgreSQL connection and schema for the quant research system.

Separate from database.py (which is SQLite for the Discord bot). Never import this
from discord_bot.py or any existing bot file.

Usage:
    from quant_db import get_conn, init_schema

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tickers")

Run directly to initialise schema:
    python quant_db.py
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

QUANT_DB_URL = os.environ.get("QUANT_DB_URL")


def get_conn() -> psycopg2.extensions.connection:
    """Return a psycopg2 connection. Rows are returned as dicts (RealDictCursor)."""
    if not QUANT_DB_URL:
        raise RuntimeError("QUANT_DB_URL not set in environment. Add it to .env")
    return psycopg2.connect(
        QUANT_DB_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def init_schema():
    """Create all tables if they don't exist. Safe to call multiple times."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:

            # ── tickers ──────────────────────────────────────────────────────────
            # One row per company. Canonical source of truth for covered tickers.
            # reported_currency: the currency the FINANCIAL STATEMENTS are in, which
            # is often NOT the trading currency. ~38% of this TSX universe reports in
            # USD while trading in CAD — mixing the two silently corrupts market_cap,
            # any size factor, and any ratio computed as price/fundamentals.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tickers (
                    id                 SERIAL PRIMARY KEY,
                    ticker             TEXT UNIQUE NOT NULL,   -- e.g. AAUC.TO
                    name               TEXT,
                    sector             TEXT,
                    industry           TEXT,
                    reported_currency  TEXT,                   -- 'CAD', 'USD', ...
                    fmp_covered        BOOLEAN DEFAULT TRUE,
                    created_at         TIMESTAMPTZ DEFAULT NOW(),
                    updated_at         TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so new
            # columns need an explicit idempotent migration.
            cur.execute("""
                ALTER TABLE tickers ADD COLUMN IF NOT EXISTS reported_currency TEXT
            """)
            # is_adr distinguishes Canadian Depositary Receipts (CAD-hedged wrappers
            # around foreign stocks — Broadcom, Eli Lilly, Cisco) from genuinely
            # foreign-domiciled TSX operators (Brookfield, Restaurant Brands). The
            # `country` field does NOT separate these — both read 'US'.
            cur.execute("""
                ALTER TABLE tickers ADD COLUMN IF NOT EXISTS is_adr BOOLEAN
            """)
            cur.execute("""
                ALTER TABLE tickers ADD COLUMN IF NOT EXISTS is_fund BOOLEAN
            """)

            # ── price_history ────────────────────────────────────────────────────
            # Daily OHLCV. One row per ticker per trading day.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id        SERIAL PRIMARY KEY,
                    ticker    TEXT NOT NULL REFERENCES tickers(ticker),
                    date      DATE NOT NULL,
                    open      NUMERIC(12, 4),
                    high      NUMERIC(12, 4),
                    low       NUMERIC(12, 4),
                    close     NUMERIC(12, 4),
                    adj_close NUMERIC(12, 4),
                    volume    BIGINT,
                    UNIQUE (ticker, date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
                ON price_history (ticker, date DESC)
            """)

            # ── income_statements ────────────────────────────────────────────────
            # Annual and quarterly P&L. filed_at = FMP's fillingDate field.
            # Always query with filed_at <= your_backtest_date for point-in-time safety.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS income_statements (
                    id                  SERIAL PRIMARY KEY,
                    ticker              TEXT NOT NULL REFERENCES tickers(ticker),
                    period              TEXT NOT NULL,    -- 'annual' or 'quarterly'
                    fiscal_date         DATE NOT NULL,    -- end of reporting period
                    filed_at            DATE,             -- when FMP says it was filed
                    revenue             NUMERIC(18, 2),
                    gross_profit        NUMERIC(18, 2),
                    operating_income    NUMERIC(18, 2),
                    net_income          NUMERIC(18, 2),
                    ebitda              NUMERIC(18, 2),
                    eps                 NUMERIC(10, 4),
                    shares_outstanding  BIGINT,
                    UNIQUE (ticker, period, fiscal_date)
                )
            """)

            # ── balance_sheets ───────────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS balance_sheets (
                    id                    SERIAL PRIMARY KEY,
                    ticker                TEXT NOT NULL REFERENCES tickers(ticker),
                    period                TEXT NOT NULL,
                    fiscal_date           DATE NOT NULL,
                    filed_at              DATE,
                    total_assets          NUMERIC(18, 2),
                    total_liabilities     NUMERIC(18, 2),
                    total_equity          NUMERIC(18, 2),
                    cash_and_equivalents  NUMERIC(18, 2),
                    total_debt            NUMERIC(18, 2),
                    net_debt              NUMERIC(18, 2),
                    UNIQUE (ticker, period, fiscal_date)
                )
            """)

            # ── cash_flows ───────────────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cash_flows (
                    id                   SERIAL PRIMARY KEY,
                    ticker               TEXT NOT NULL REFERENCES tickers(ticker),
                    period               TEXT NOT NULL,
                    fiscal_date          DATE NOT NULL,
                    filed_at             DATE,
                    operating_cash_flow  NUMERIC(18, 2),
                    capex                NUMERIC(18, 2),   -- negative value, stored as-is
                    free_cash_flow       NUMERIC(18, 2),
                    dividends_paid       NUMERIC(18, 2),
                    UNIQUE (ticker, period, fiscal_date)
                )
            """)

            # ── key_metrics ──────────────────────────────────────────────────────
            # Historical pre-computed valuation and quality ratios per fiscal period.
            # FMP calculates these using prices at the time of the filing, making them
            # point-in-time safe for backtesting when queried with filed_at <= cutoff.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS key_metrics (
                    id                       SERIAL PRIMARY KEY,
                    ticker                   TEXT NOT NULL REFERENCES tickers(ticker),
                    period                   TEXT NOT NULL,   -- 'annual' or 'quarterly'
                    fiscal_date              DATE NOT NULL,
                    market_cap               NUMERIC(18, 2),
                    enterprise_value         NUMERIC(18, 2),
                    pe_ratio                 NUMERIC(14, 4),
                    price_to_sales           NUMERIC(14, 4),
                    price_to_book            NUMERIC(14, 4),
                    price_to_fcf             NUMERIC(14, 4),
                    ev_to_ebitda             NUMERIC(14, 4),
                    ev_to_operating_cf       NUMERIC(14, 4),
                    ev_to_fcf                NUMERIC(14, 4),
                    earnings_yield           NUMERIC(14, 6),
                    fcf_yield                NUMERIC(14, 6),
                    dividend_yield           NUMERIC(14, 6),
                    roe                      NUMERIC(14, 6),
                    roa                      NUMERIC(14, 6),
                    roic                     NUMERIC(14, 6),
                    debt_to_equity           NUMERIC(14, 4),
                    net_debt_to_ebitda       NUMERIC(14, 4),
                    current_ratio            NUMERIC(14, 4),
                    interest_coverage        NUMERIC(14, 4),
                    payout_ratio             NUMERIC(14, 4),
                    UNIQUE (ticker, period, fiscal_date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_key_metrics_ticker_date
                ON key_metrics (ticker, fiscal_date DESC)
            """)

            # ── financial_scores ─────────────────────────────────────────────────
            # Quality and fraud detection scores computed per fiscal period.
            # Piotroski F-Score (0-9), Altman Z-Score, Beneish M-Score.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS financial_scores (
                    id               SERIAL PRIMARY KEY,
                    ticker           TEXT NOT NULL REFERENCES tickers(ticker),
                    fiscal_date      DATE NOT NULL,
                    piotroski_score  INTEGER CHECK (piotroski_score BETWEEN 0 AND 9),
                    altman_z_score   NUMERIC(10, 4),
                    beneish_m_score  NUMERIC(10, 4),
                    UNIQUE (ticker, fiscal_date)
                )
            """)

            # ── dividends ────────────────────────────────────────────────────────
            # Full dividend payment history. One row per ex-dividend event.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dividends (
                    id                SERIAL PRIMARY KEY,
                    ticker            TEXT NOT NULL REFERENCES tickers(ticker),
                    ex_dividend_date  DATE NOT NULL,
                    payment_date      DATE,
                    record_date       DATE,
                    declared_date     DATE,
                    dividend          NUMERIC(10, 6),
                    adjusted_dividend NUMERIC(10, 6),
                    UNIQUE (ticker, ex_dividend_date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dividends_ticker_date
                ON dividends (ticker, ex_dividend_date DESC)
            """)

            # ── earnings_surprises ───────────────────────────────────────────────
            # Historical EPS beat/miss versus consensus estimate at reporting date.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS earnings_surprises (
                    id                SERIAL PRIMARY KEY,
                    ticker            TEXT NOT NULL REFERENCES tickers(ticker),
                    fiscal_date       DATE NOT NULL,
                    estimated_eps     NUMERIC(10, 4),
                    actual_eps        NUMERIC(10, 4),
                    surprise_eps      NUMERIC(10, 4),
                    surprise_eps_pct  NUMERIC(10, 4),
                    UNIQUE (ticker, fiscal_date)
                )
            """)

            # ── price_targets ────────────────────────────────────────────────────
            # Individual analyst price targets. One row per analyst per publish date.
            # Allows tracking analyst history and building consensus time series.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_targets (
                    id                  SERIAL PRIMARY KEY,
                    ticker              TEXT NOT NULL REFERENCES tickers(ticker),
                    published_date      DATE NOT NULL,
                    analyst_name        TEXT,
                    analyst_company     TEXT,
                    price_target        NUMERIC(12, 4),
                    adj_price_target    NUMERIC(12, 4),
                    price_when_posted   NUMERIC(12, 4),
                    UNIQUE (ticker, published_date, analyst_company)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_targets_ticker_date
                ON price_targets (ticker, published_date DESC)
            """)

            # ── analyst_ratings ──────────────────────────────────────────────────
            # Historical consensus ratings snapshot per date.
            # rating_score is FMP's numeric encoding (1=Strong Buy ... 5=Strong Sell).
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analyst_ratings (
                    id           SERIAL PRIMARY KEY,
                    ticker       TEXT NOT NULL REFERENCES tickers(ticker),
                    rating_date  DATE NOT NULL,
                    rating       TEXT,           -- e.g. 'Strong Buy', 'Buy', 'Hold'
                    rating_score INTEGER,
                    buy          INTEGER,
                    hold         INTEGER,
                    sell         INTEGER,
                    strong_buy   INTEGER,
                    strong_sell  INTEGER,
                    UNIQUE (ticker, rating_date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_analyst_ratings_ticker_date
                ON analyst_ratings (ticker, rating_date DESC)
            """)

            # ── analyst_estimates ────────────────────────────────────────────────
            # Consensus forward estimates per fiscal period.
            # retrieved_at = when we fetched this row — required for PIT safety because
            # FMP silently backfills historical estimates with later revisions.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analyst_estimates (
                    id                         SERIAL PRIMARY KEY,
                    ticker                     TEXT NOT NULL REFERENCES tickers(ticker),
                    period                     TEXT NOT NULL,   -- 'annual' or 'quarterly'
                    fiscal_date                DATE NOT NULL,
                    retrieved_at               TIMESTAMPTZ,
                    estimated_revenue_avg      NUMERIC(18, 2),
                    estimated_revenue_high     NUMERIC(18, 2),
                    estimated_revenue_low      NUMERIC(18, 2),
                    estimated_ebitda_avg       NUMERIC(18, 2),
                    estimated_net_income_avg   NUMERIC(18, 2),
                    estimated_eps_avg          NUMERIC(10, 4),
                    estimated_eps_high         NUMERIC(10, 4),
                    estimated_eps_low          NUMERIC(10, 4),
                    num_analysts_revenue       INTEGER,
                    num_analysts_eps           INTEGER,
                    UNIQUE (ticker, period, fiscal_date)
                )
            """)

            # ── news_articles ────────────────────────────────────────────────────
            # One row per article per ticker. sentiment is FMP's label (if provided);
            # sentiment_score is numeric when available (some endpoints omit it).
            cur.execute("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id               SERIAL PRIMARY KEY,
                    ticker           TEXT NOT NULL REFERENCES tickers(ticker),
                    published_date   TIMESTAMPTZ NOT NULL,
                    title            TEXT,
                    url              TEXT NOT NULL,
                    source           TEXT,
                    sentiment        TEXT,
                    sentiment_score  NUMERIC(6, 4),
                    UNIQUE (ticker, url)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_articles_ticker_date
                ON news_articles (ticker, published_date DESC)
            """)

            # ── macro_data ───────────────────────────────────────────────────────
            # Non-ticker time series: commodity prices, forex, rates, economic data.
            # series examples: 'GCUSD' (gold), 'CLUSD' (crude), 'HGUSD' (copper),
            #   'CADUSD', 'US10Y', 'US2Y', 'CPI_CA', 'GDP_CA', 'BOCRATE'
            cur.execute("""
                CREATE TABLE IF NOT EXISTS macro_data (
                    id      SERIAL PRIMARY KEY,
                    series  TEXT NOT NULL,
                    date    DATE NOT NULL,
                    value   NUMERIC(18, 6),
                    source  TEXT,   -- 'fmp', 'boc', 'fred'
                    UNIQUE (series, date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_macro_data_series_date
                ON macro_data (series, date DESC)
            """)

            # ── upgrades_downgrades ──────────────────────────────────────────────
            # Analyst rating changes and price target revisions per ticker.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS upgrades_downgrades (
                    id                  SERIAL PRIMARY KEY,
                    ticker              TEXT NOT NULL REFERENCES tickers(ticker),
                    published_date      DATE NOT NULL,
                    analyst_company     TEXT,
                    analyst_name        TEXT,
                    action              TEXT,   -- 'upgrade','downgrade','initiated','reiterated'
                    from_grade          TEXT,
                    to_grade            TEXT,
                    from_price_target   NUMERIC(12, 4),
                    to_price_target     NUMERIC(12, 4),
                    UNIQUE (ticker, published_date, analyst_company, action)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_upgrades_downgrades_ticker_date
                ON upgrades_downgrades (ticker, published_date DESC)
            """)

            # ── earnings_call_transcripts ────────────────────────────────────────
            # Raw transcript text + Claude sentiment score.
            # Use transcript_date (not fiscal_date) for PIT correctness.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS earnings_call_transcripts (
                    id                      SERIAL PRIMARY KEY,
                    ticker                  TEXT NOT NULL REFERENCES tickers(ticker),
                    fiscal_date             DATE NOT NULL,
                    period                  TEXT,           -- 'annual' or 'quarterly'
                    transcript_date         DATE,
                    content                 TEXT,
                    claude_sentiment_score  NUMERIC(6, 4),  -- -1.0 to 1.0
                    claude_sentiment_label  TEXT,           -- 'positive','neutral','negative'
                    claude_scored_at        TIMESTAMPTZ,
                    UNIQUE (ticker, fiscal_date)
                )
            """)

            # ── institutional_ownership ──────────────────────────────────────────
            # Quarterly institutional ownership snapshots.
            # Use filing_date (not period_end) for PIT correctness — filings lag ~45 days.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS institutional_ownership (
                    id                      SERIAL PRIMARY KEY,
                    ticker                  TEXT NOT NULL REFERENCES tickers(ticker),
                    filing_date             DATE NOT NULL,
                    period_end              DATE,
                    total_holders           INTEGER,
                    shares_held             BIGINT,
                    pct_outstanding         NUMERIC(8, 4),
                    change_in_shares        BIGINT,
                    UNIQUE (ticker, filing_date)
                )
            """)

            # ── feature_snapshots ────────────────────────────────────────────────
            # One row per ticker per snapshot_date. All ML features + forward returns.
            # All features are point-in-time correct as of snapshot_date.
            # forward_return_* are filled in retrospectively once prices are available.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS feature_snapshots (
                    id              SERIAL PRIMARY KEY,
                    ticker          TEXT NOT NULL REFERENCES tickers(ticker),
                    snapshot_date   DATE NOT NULL,

                    -- Metadata
                    sector                  TEXT,
                    market_cap              NUMERIC(18, 2),
                    avg_dollar_volume_90d   NUMERIC(18, 2),
                    tier                    TEXT,   -- 'main' or 'early_watch'

                    -- Profitability
                    roa                     NUMERIC(14, 6),
                    roe                     NUMERIC(14, 6),
                    roic                    NUMERIC(14, 6),
                    gross_margin            NUMERIC(14, 6),
                    operating_margin        NUMERIC(14, 6),
                    fcf_margin              NUMERIC(14, 6),
                    operating_cf_to_assets  NUMERIC(14, 6),

                    -- Growth
                    revenue_growth_yoy      NUMERIC(14, 6),
                    revenue_growth_qoq      NUMERIC(14, 6),
                    earnings_growth_yoy     NUMERIC(14, 6),

                    -- Leverage / Safety
                    debt_to_assets          NUMERIC(14, 6),
                    net_debt_to_ebitda      NUMERIC(14, 6),
                    interest_coverage       NUMERIC(14, 6),
                    current_ratio           NUMERIC(14, 6),
                    altman_z_score          NUMERIC(14, 6),

                    -- Valuation (absolute)
                    ev_to_ebitda            NUMERIC(14, 6),
                    pe_ratio                NUMERIC(14, 6),
                    price_to_sales          NUMERIC(14, 6),
                    price_to_book           NUMERIC(14, 6),
                    price_to_fcf            NUMERIC(14, 6),
                    fcf_yield               NUMERIC(14, 6),
                    earnings_yield          NUMERIC(14, 6),

                    -- Valuation (sector-relative percentile, 0.0-1.0)
                    ev_to_ebitda_pct        NUMERIC(6, 4),
                    pe_ratio_pct            NUMERIC(6, 4),
                    ps_ratio_pct            NUMERIC(6, 4),

                    -- Capital allocation
                    shares_change_1yr       NUMERIC(14, 6),  -- negative = buyback
                    shares_change_3yr       NUMERIC(14, 6),

                    -- Analyst
                    num_analysts            INTEGER,
                    upgrades_90d            INTEGER,
                    downgrades_90d          INTEGER,
                    eps_revision_3m_pct     NUMERIC(14, 6),
                    revenue_revision_3m_pct NUMERIC(14, 6),
                    price_target_change_3m_pct  NUMERIC(14, 6),

                    -- Earnings quality
                    consecutive_eps_beats   INTEGER,
                    latest_eps_surprise_pct NUMERIC(14, 6),
                    earnings_call_sentiment NUMERIC(6, 4),   -- -1.0 to 1.0
                    filing_lag_days         INTEGER,

                    -- Momentum / Technical
                    return_12m_1m           NUMERIC(14, 6),
                    return_3m               NUMERIC(14, 6),
                    return_1m               NUMERIC(14, 6),
                    high_52w_proximity      NUMERIC(14, 6),  -- price / 52w high
                    rsi_14                  NUMERIC(8, 4),
                    volume_ratio_90d        NUMERIC(14, 6),
                    relative_strength_vs_tsx_3m NUMERIC(14, 6),

                    -- Macro context (as of snapshot_date)
                    gold_change_3m          NUMERIC(14, 6),
                    oil_change_3m           NUMERIC(14, 6),
                    copper_change_3m        NUMERIC(14, 6),
                    cadusd_change_3m        NUMERIC(14, 6),
                    yield_curve_spread      NUMERIC(14, 6),  -- 10y minus 2y
                    tsx_return_3m           NUMERIC(14, 6),

                    -- News / Sentiment
                    news_count_30d          INTEGER,
                    news_count_90d          INTEGER,
                    news_velocity_accel     NUMERIC(14, 6),
                    news_sentiment_avg_30d  NUMERIC(8, 4),

                    -- Institutional
                    institutional_ownership_pct     NUMERIC(8, 4),
                    institutional_ownership_change  NUMERIC(8, 4),

                    -- Target variables (filled in retrospectively)
                    forward_return_1m       NUMERIC(14, 6),
                    forward_return_3m       NUMERIC(14, 6),
                    forward_return_6m       NUMERIC(14, 6),
                    forward_return_12m      NUMERIC(14, 6),

                    UNIQUE (ticker, snapshot_date)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_feature_snapshots_ticker_date
                ON feature_snapshots (ticker, snapshot_date DESC)
            """)

            # ── insider_trades ───────────────────────────────────────────────────
            # Insider buy/sell filings. UNIQUE key guards against re-filing duplicates.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS insider_trades (
                    id                  SERIAL PRIMARY KEY,
                    ticker              TEXT NOT NULL REFERENCES tickers(ticker),
                    filing_date         DATE NOT NULL,
                    transaction_date    DATE,
                    reporter_name       TEXT,
                    reporter_title      TEXT,
                    transaction_type    TEXT,   -- e.g. 'P-Purchase', 'S-Sale'
                    shares              NUMERIC(18, 2),
                    price               NUMERIC(12, 4),
                    value               NUMERIC(18, 2),
                    shares_owned_after  NUMERIC(18, 2),
                    UNIQUE (ticker, filing_date, reporter_name, transaction_type, shares)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker_date
                ON insider_trades (ticker, filing_date DESC)
            """)

            # ── Migrations for existing deployments ──────────────────────────
            # ADD COLUMN IF NOT EXISTS is safe to run repeatedly.
            cur.execute("""
                ALTER TABLE analyst_estimates
                ADD COLUMN IF NOT EXISTS retrieved_at TIMESTAMPTZ
            """)

            # TTM + dynamic valuation + growth quality features
            new_cols = [
                ("feature_snapshots", "dynamic_pe",            "NUMERIC(14, 6)"),
                ("feature_snapshots", "dynamic_ps",            "NUMERIC(14, 6)"),
                ("feature_snapshots", "dynamic_pb",            "NUMERIC(14, 6)"),
                ("feature_snapshots", "dynamic_ev_ebitda",     "NUMERIC(14, 6)"),
                ("feature_snapshots", "dynamic_pfcf",          "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_revenue_growth",    "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_earnings_growth",   "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_gross_margin",      "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_fcf_margin",        "NUMERIC(14, 6)"),
                ("feature_snapshots", "revenue_acceleration",  "NUMERIC(14, 6)"),
                ("feature_snapshots", "earnings_acceleration", "NUMERIC(14, 6)"),
                ("feature_snapshots", "gross_margin_trend",    "NUMERIC(14, 6)"),
                ("feature_snapshots", "fcf_conversion",        "NUMERIC(14, 6)"),
                # TTM twins of the annual-sourced profitability/leverage ratios.
                # The annual versions are kept rather than overwritten: they're
                # audited (quarterlies aren't), and a single column that silently
                # falls back to annual when TTM is unavailable would mean
                # different things on different rows — which a model can't learn
                # a consistent relationship from.
                ("feature_snapshots", "ttm_roa",                   "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_roe",                   "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_roic",                  "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_operating_margin",      "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_operating_cf_to_assets","NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_debt_to_assets",        "NUMERIC(14, 6)"),
                # Altman Z' inputs. FMP returns all three on the balance-sheet
                # endpoint but they were never mapped, which left X1 (working
                # capital / assets) and X2 (retained earnings / assets) with no
                # source data at all. Filled by backfill_balance_sheet_fields.py
                # for existing rows, and by 2_fmp_pipeline.py going forward.
                ("balance_sheets", "current_assets",      "NUMERIC(18, 2)"),
                ("balance_sheets", "current_liabilities", "NUMERIC(18, 2)"),
                ("balance_sheets", "retained_earnings",   "NUMERIC(18, 2)"),
                # Piotroski was computed into financial_scores from 2026-09-30 but
                # never surfaced as a feature — feature_snapshots had no column for
                # it, so the whole quarterly-TTM score was invisible to the model.
                ("feature_snapshots", "piotroski_score", "INTEGER"),
                # Per-company factor sensitivities, from 104 weeks of weekly returns.
                # The *_change_3m macro columns are identical for every ticker on a
                # date and so cannot rank stocks within it; these vary per company
                # and make those columns usable as the regime half of the pair.
                # Each *_r2 says how much of the stock's variance the factor really
                # explains, separating a true exposure from a noisy fit.
                ("feature_snapshots", "gold_beta",     "NUMERIC(14, 6)"),
                ("feature_snapshots", "gold_r2",       "NUMERIC(8, 6)"),
                ("feature_snapshots", "oil_beta",      "NUMERIC(14, 6)"),
                ("feature_snapshots", "oil_r2",        "NUMERIC(8, 6)"),
                ("feature_snapshots", "cadusd_beta",   "NUMERIC(14, 6)"),
                ("feature_snapshots", "cadusd_r2",     "NUMERIC(8, 6)"),
                ("feature_snapshots", "rates_beta",    "NUMERIC(14, 6)"),
                ("feature_snapshots", "rates_r2",      "NUMERIC(8, 6)"),
                ("feature_snapshots", "market_beta",   "NUMERIC(14, 6)"),
                ("feature_snapshots", "market_r2",     "NUMERIC(8, 6)"),
                # Total and company-specific volatility, annualised, from the same
                # weekly return series.
                ("feature_snapshots", "total_vol",     "NUMERIC(14, 6)"),
                ("feature_snapshots", "idio_vol",      "NUMERIC(14, 6)"),
                # Sector-relative rank of Altman Z'. The raw score is not comparable
                # across sectors in this universe — its median is 1.18, below
                # Altman's 1.23 distress threshold, because asset-heavy businesses
                # (rails, pipelines, utilities, REITs) score structurally low on
                # working capital and asset turnover. A within-sector percentile
                # makes the signal usable and tames the long X4 tail.
                ("feature_snapshots", "altman_z_pct",  "NUMERIC(6, 4)"),
                # Live replacements for FMP's earnings_yield / fcf_yield, which were
                # priced at the fiscal period end and so sat frozen for months.
                # Yield form stays monotonic through zero, unlike the P/E ratio form.
                ("feature_snapshots", "dynamic_earnings_yield", "NUMERIC(14, 6)"),
                ("feature_snapshots", "dynamic_fcf_yield",      "NUMERIC(14, 6)"),
                # Quarterly-refreshing twins for the two key_metrics ratios that
                # lacked one. FMP's come off the annual statement (~1.97 updates per
                # ticker-year); these track the quarterlies (~5).
                ("feature_snapshots", "ttm_current_ratio",      "NUMERIC(14, 6)"),
                ("feature_snapshots", "ttm_net_debt_to_ebitda", "NUMERIC(14, 6)"),
                # Within-sector percentiles + vol-scaled momentum. See
                # SECTOR_PERCENTILES in 6_feature_compute.py for why these exist:
                # 93% of the backtest's edge came from selection inside sectors,
                # but only 4 of 68 features were measured sector-relative.
                ("feature_snapshots", "dynamic_pb_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "dynamic_fcf_yield_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "piotroski_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_roa_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_roic_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_operating_margin_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_op_cf_to_assets_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_debt_to_assets_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_net_debt_to_ebitda_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_revenue_growth_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "ttm_earnings_growth_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "return_12m_1m_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "return_3m_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "shares_change_1yr_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "total_vol_sect", "NUMERIC(6, 4)"),
                ("feature_snapshots", "return_12m_1m_vol_adj", "NUMERIC(14, 6)"),
                ("feature_snapshots", "return_3m_vol_adj", "NUMERIC(14, 6)"),
            ]
            for table, col, dtype in new_cols:
                cur.execute(f"""
                    ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)

        conn.commit()
        print("Schema initialised successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    init_schema()
    print(f"Connected to: {QUANT_DB_URL.split('@')[-1] if QUANT_DB_URL else 'N/A'}")
