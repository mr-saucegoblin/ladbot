"""
9_score.py — Score a live snapshot and produce a ranked pick list.

Trains a final model on every labelled row available, then applies it to a snapshot
that has features but no forward return yet — the rows 7_eda.py deliberately
filters out, which are exactly the ones worth predicting.

Usage:
    python 9_score.py                      # latest unlabelled snapshot
    python 9_score.py --date 2026-09-30    # a specific snapshot
    python 9_score.py --top 25             # how many names to print
    python 9_score.py --save               # also write to the predictions table

THE THING MOST LIKELY TO GO WRONG HERE is train/serve skew. The model was trained on
features converted to within-date percentile RANKS (see the cross-sectional ranking
block in 7_eda.py), not on raw values. Feed it a raw dynamic_pb of 1.5 instead of
"0.38 of this month's names" and it returns confident nonsense with no error raised
anywhere. apply_cross_sectional_rank() below must stay in step with 7_eda.py; that
duplication is a real risk and the reason both carry this warning.

Ranking is within-date, so scoring a single snapshot is self-contained: this month's
~450 names are ranked against each other and nothing else is needed.
"""

import argparse
import datetime
import importlib.util
import json
import pathlib

import numpy as np
import pandas as pd

HERE     = pathlib.Path(__file__).parent
PARQUET  = HERE / "quant_clean.parquet"
MANIFEST = HERE / "quant_clean_manifest.json"

# Reuse 8_model's helpers rather than copying them — a module whose name starts with
# a digit can't be imported normally, hence the explicit loader.
_spec = importlib.util.spec_from_file_location("m8", HERE / "8_model.py")
m8 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m8)


# ── Exclusions — must mirror 7_eda.py ─────────────────────────────────────────
CORRUPT_PRICE_TICKERS = {"ACT.TO"}
TIERS_TO_MODEL        = {"main"}

# A name must have actually traded near the snapshot date to be buyable.
#
# This filter has no counterpart in 7_eda.py and must not be given one. Delisted
# tickers are deliberately KEPT in the database so backtests aren't
# survivorship-biased, and 6_feature_compute.py writes a snapshot row for every
# ticker every month whether or not it still trades — falling back to the last
# price it can find. In the backtest that is harmless, because those rows have no
# forward return and 7_eda.py only loads labelled rows.
#
# Scoring has no such filter, so the dead names come straight through: 126 of 551
# names in the 2026-09-30 snapshot had not traded since September, and the first
# run put Yamana Gold (acquired in 2023, last trade 2023-04-03) at rank 22 of a
# buy list. Backtests need the dead companies; a pick list must never contain one.
MAX_TRADE_STALENESS_DAYS = 10

# A name must have a computed risk profile to appear on a pick list.
#
# total_vol and the betas need 52 weeks of weekly returns, so anything listed within
# the past year has them all NaN. LightGBM learns a split direction for NaN, and on
# the first live run it had learned "missing idio_vol -> good": CPKR.TO, listed
# 2025-10-02 with 248 days of history, came out ranked #1 with the missing value as
# its third-largest positive contributor.
#
# That is backwards. A short history is an absence of evidence, not a merit, and in
# training those rows are rare enough that whatever the model learned about them is
# noise. Backtests can keep them; a buy list cannot.
REQUIRE_RISK_METRICS = ["total_vol", "market_beta"]


def load_live_snapshot(snapshot_date=None) -> tuple[pd.DataFrame, datetime.date]:
    """
    Pull one snapshot's raw features straight from Postgres.

    Cannot reuse 7_eda.py's loader: its WHERE clause requires a non-NULL forward
    return, which excludes every row we want to score.
    """
    from quant_db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if snapshot_date is None:
                # Newest snapshot that has features but no realised 1m return yet.
                cur.execute("""
                    SELECT MAX(snapshot_date) AS d FROM feature_snapshots
                    WHERE forward_return_1m IS NULL
                """)
                snapshot_date = cur.fetchone()["d"]
            # LEFT JOIN the last trade date so the caller can drop names that have
            # stopped trading — see MAX_TRADE_STALENESS_DAYS.
            cur.execute("""
                SELECT f.*, p.last_trade
                FROM feature_snapshots f
                LEFT JOIN (
                    SELECT ticker, MAX(date) AS last_trade
                    FROM price_history GROUP BY ticker
                ) p ON p.ticker = f.ticker
                WHERE f.snapshot_date = %s
            """, (snapshot_date,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise SystemExit(f"No rows found for snapshot {snapshot_date}")

    df = pd.DataFrame([dict(r) for r in rows])
    # last_trade must be named here or the coercion below turns a column of dates
    # into all-NaN, and pandas 3.0 then returns datetime64 (not object) from
    # .dt.date on an all-null column, so comparing it to a date raises. Same bug
    # class that breaks ASTL.TO in 6_feature_compute.py.
    non_numeric = {"ticker", "sector", "tier", "snapshot_date", "last_trade"}
    for c in df.columns:
        if c not in non_numeric:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    return df, snapshot_date


def apply_cross_sectional_rank(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Convert each numeric feature to its percentile rank among this snapshot's names.

    Mirrors the ranking block in 7_eda.py. `sector` stays categorical. Any feature
    missing from the snapshot is created as all-NaN so the column order handed to
    LightGBM matches training exactly — a reordered or absent column is another way
    to get silent nonsense.
    """
    out = df.copy()
    for c in feature_cols:
        if c not in out.columns:
            out[c] = np.nan
    for c in feature_cols:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].rank(pct=True)
    return out


def train_final(parquet: pd.DataFrame, manifest: dict, feature_cols: list[str]):
    """
    Fit on every labelled row, with a short tail held back for early stopping.

    For a live prediction you want the model to have seen everything you know, so
    there is no test split here — validation already happened in 8_model.py. The
    tail-val exists only to decide when to stop boosting.
    """
    rank_target = manifest.get("target_rank", "target_rank")
    use_rank    = rank_target in parquet.columns

    df = parquet.copy()
    if use_rank:
        df = df[df[rank_target].notna()].copy()
        df[rank_target] = df[rank_target].astype(int)

    dates   = sorted(df["snapshot_date"].unique())
    n_val   = max(1, int(len(dates) * m8.VAL_FRACTION))
    embargo = datetime.timedelta(days=manifest["embargo_days"])
    val_start = dates[-n_val]
    fit_end   = val_start - embargo

    fit = df[df["snapshot_date"] <= fit_end]
    val = df[df["snapshot_date"] >= val_start]

    target = rank_target if use_rank else manifest["target"]
    print(f"  training on {len(fit):,} rows ({fit['snapshot_date'].min()} .. "
          f"{fit['snapshot_date'].max()}), {len(val):,} held back for early stopping")
    booster = m8.train(fit, val, feature_cols, target, quick=False,
                       verbose=True, rank=use_rank)
    return booster, use_rank


def main(snapshot_date, top_n, save):
    if not PARQUET.exists():
        raise SystemExit("quant_clean.parquet missing — run 7_eda.py first.")
    parquet  = pd.read_parquet(PARQUET)
    parquet["snapshot_date"] = pd.to_datetime(parquet["snapshot_date"]).dt.date
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    feature_cols = manifest["feature_cols"]

    print("=" * 74)
    print("  TRAIN FINAL MODEL")
    print("=" * 74)
    booster, used_rank = train_final(parquet, manifest, feature_cols)

    live, snap = load_live_snapshot(snapshot_date)
    print("\n" + "=" * 74)
    print(f"  SCORING SNAPSHOT {snap}")
    print("=" * 74)
    print(f"  {len(live)} rows pulled")

    before = len(live)
    live = live[~live["ticker"].isin(CORRUPT_PRICE_TICKERS)]
    live = live[live["tier"].isin(TIERS_TO_MODEL)]
    print(f"  after exclusions (ACT.TO, tier={sorted(TIERS_TO_MODEL)}): "
          f"{len(live)} rows  (-{before - len(live)})")

    # Drop anything that has stopped trading. Without this the list contains
    # companies that were acquired or wound up years ago — see the constant.
    stale_cut = snap - datetime.timedelta(days=MAX_TRADE_STALENESS_DAYS)
    lt = pd.to_datetime(live["last_trade"], errors="coerce").dt.date
    dead = live[lt.isna() | (lt < stale_cut)]
    live = live[lt.notna() & (lt >= stale_cut)]
    print(f"  after dropping names not traded since {stale_cut}: "
          f"{len(live)} rows  (-{len(dead)} delisted/halted)")
    if len(dead):
        sample = ", ".join(dead.sort_values("ticker")["ticker"].head(8))
        print(f"    e.g. {sample}{' ...' if len(dead) > 8 else ''}")

    # Drop names with no risk profile — see REQUIRE_RISK_METRICS.
    have_risk = pd.Series(True, index=live.index)
    for c in REQUIRE_RISK_METRICS:
        if c in live.columns:
            have_risk &= live[c].notna()
    short_hist = live[~have_risk]
    live = live[have_risk]
    print(f"  after requiring {'/'.join(REQUIRE_RISK_METRICS)}: {len(live)} rows  "
          f"(-{len(short_hist)} with under a year of history)")
    if len(short_hist):
        sample = ", ".join(short_hist.sort_values("ticker")["ticker"].head(8))
        print(f"    e.g. {sample}{' ...' if len(short_hist) > 8 else ''}")

    ranked = apply_cross_sectional_rank(live, feature_cols)
    X = m8.prepare(ranked, feature_cols)

    # Align categorical levels to training, or the encoding silently differs.
    fit_cats = m8.prepare(parquet, feature_cols)
    for c in X.columns:
        if str(X[c].dtype) == "category":
            X[c] = X[c].astype(pd.CategoricalDtype(categories=fit_cats[c].cat.categories))

    live = live.copy()
    live["score"] = booster.predict(X, num_iteration=booster.best_iteration)
    live["rank"]  = live["score"].rank(ascending=False, method="first").astype(int)
    live["decile"] = pd.qcut(live["score"].rank(method="first"), 10,
                             labels=False) + 1
    live = live.sort_values("rank")

    cols = ["rank", "ticker", "sector", "score", "market_cap", "piotroski_score",
            "altman_z_score", "dynamic_pb", "return_12m_1m", "total_vol"]
    cols = [c for c in cols if c in live.columns]

    def fmt(r):
        mc = r.get("market_cap")
        return (f"  {int(r['rank']):>4}  {r['ticker']:<11}{str(r.get('sector'))[:20]:<22}"
                f"{r['score']:>8.3f}"
                f"{('$' + format(mc / 1e9, '.1f') + 'B') if pd.notna(mc) else '':>10}"
                f"{(r.get('piotroski_score') if pd.notna(r.get('piotroski_score')) else float('nan')):>6.0f}"
                f"{(r.get('total_vol') if pd.notna(r.get('total_vol')) else float('nan')):>8.2f}")

    print(f"\n  TOP {top_n} — the model's buy list")
    print(f"  {'rank':>4}  {'ticker':<11}{'sector':<22}{'score':>8}{'mcap':>10}{'F':>6}{'vol':>8}")
    print("  " + "-" * 70)
    for _, r in live.head(top_n).iterrows():
        print(fmt(r))

    print(f"\n  BOTTOM 10 — what it would avoid")
    print("  " + "-" * 70)
    for _, r in live.tail(10).iterrows():
        print(fmt(r))

    # Sanity: a top decile that is uniformly high-volatility is a risk bet, not a pick.
    t, b = live[live.decile == 10], live[live.decile == 1]
    print("\n  sanity check — top vs bottom decile (raw values):")
    for c in ["total_vol", "market_beta", "market_cap", "piotroski_score"]:
        if c in live.columns and t[c].notna().any():
            print(f"    {c:<18}{t[c].median():>12,.2f}{b[c].median():>14,.2f}")

    drivers = explain_picks(booster, X, live, feature_cols, top_n)

    if save:
        save_predictions(live, snap, booster, used_rank, manifest, drivers)
    else:
        print("\n  (not saved — pass --save to write to the predictions table)")


# Which family each feature belongs to, so a 68-number explanation collapses into
# something readable. Anything unmapped falls into "other".
FAMILIES = {
    "VALUE": ["dynamic_pe","dynamic_ps","dynamic_pb","dynamic_ev_ebitda","dynamic_pfcf",
              "dynamic_earnings_yield","dynamic_fcf_yield","ev_to_ebitda_pct",
              "pe_ratio_pct","ps_ratio_pct","dynamic_pb_sect",
              "dynamic_fcf_yield_sect"],
    "QUALITY": ["piotroski_score","altman_z_score","altman_z_pct","roa","roe","roic",
                "ttm_roa","ttm_roe","ttm_roic","operating_cf_to_assets",
                "ttm_operating_cf_to_assets","interest_coverage","current_ratio",
                "ttm_current_ratio","gross_margin","operating_margin","fcf_margin",
                "ttm_gross_margin","ttm_fcf_margin","ttm_operating_margin","fcf_conversion",
                "piotroski_sect","ttm_roa_sect","ttm_roic_sect",
                "ttm_op_cf_to_assets_sect","ttm_operating_margin_sect"],
    "GROWTH": ["revenue_growth_yoy","revenue_growth_qoq","earnings_growth_yoy",
               "ttm_revenue_growth","ttm_earnings_growth","revenue_acceleration",
               "earnings_acceleration","gross_margin_trend",
               "ttm_revenue_growth_sect","ttm_earnings_growth_sect"],
    "MOMENTUM": ["return_12m_1m","return_3m","return_1m","high_52w_proximity","rsi_14",
                 "return_12m_1m_sect","return_3m_sect",
                 "return_12m_1m_vol_adj","return_3m_vol_adj"],
    "LEVERAGE": ["debt_to_assets","ttm_debt_to_assets","net_debt_to_ebitda",
                 "ttm_net_debt_to_ebitda","ttm_debt_to_assets_sect",
                 "ttm_net_debt_to_ebitda_sect"],
    "RISK/VOL": ["total_vol","idio_vol","market_beta","market_r2","total_vol_sect"],
    "MACRO BETA": ["gold_beta","gold_r2","oil_beta","oil_r2","cadusd_beta","cadusd_r2",
                   "rates_beta","rates_r2"],
    "SIZE/LIQ": ["market_cap","avg_dollar_volume_90d","volume_ratio_90d"],
    "EARNINGS": ["consecutive_eps_beats","latest_eps_surprise_pct"],
    "CAPITAL": ["shares_change_1yr","shares_change_3yr","shares_change_1yr_sect"],
    "SECTOR": ["sector"],
}


def explain_picks(booster, X, live, feature_cols, top_n):
    """
    Break each pick's score into per-feature contributions using TreeSHAP.

    LightGBM's pred_contrib returns an exact decomposition: one column per feature
    plus a base value, and they sum to the raw score. So "this name scored 0.49"
    becomes "0.12 of it came from valuation, 0.08 from low volatility, and momentum
    cost it 0.03" — which is checkable rather than a story told after the fact.

    Feature values shown alongside are within-date percentile ranks: 0.92 means the
    name sits in the 92nd percentile of this month's universe on that measure.
    """
    contrib = booster.predict(X, pred_contrib=True,
                              num_iteration=booster.best_iteration)
    contrib = pd.DataFrame(contrib[:, :-1], columns=feature_cols, index=X.index)

    fam_of = {f: fam for fam, feats in FAMILIES.items() for f in feats}
    # Collected per ticker and returned so save_predictions can persist them. The
    # Discord bot reads these rather than recomputing SHAP, which would mean
    # shipping the model and the whole feature frame to the bot process.
    drivers: dict[str, dict] = {}

    print("\n" + "=" * 74)
    print(f"  WHY — contribution breakdown for the top {top_n}")
    print("=" * 74)
    print("  families are summed SHAP contributions to the score; +ve pushed it up")

    for _, row in live.head(top_n).iterrows():
        c = contrib.loc[row.name]
        fam = {}
        for f, v in c.items():
            fam[fam_of.get(f, "other")] = fam.get(fam_of.get(f, "other"), 0.0) + v
        fam = {k: v for k, v in sorted(fam.items(), key=lambda kv: -abs(kv[1]))}

        print(f"\n  {int(row['rank']):>3}. {row['ticker']:<11}{str(row.get('sector'))[:24]:<26}"
              f"score {row['score']:+.3f}")
        bits = [f"{k} {v:+.3f}" for k, v in list(fam.items())[:5] if abs(v) > 0.004]
        print(f"       {'  |  '.join(bits)}")

        # Named `ranked`, not `drivers`: the accumulator dict above owns that name,
        # and reusing it here silently replaced the dict with this Series on the
        # first iteration, so the function returned a Series instead of the map.
        ranked = c.sort_values()
        tops = list(ranked.tail(3).items())[::-1]
        bots = [x for x in ranked.head(2).items() if x[1] < -0.004]
        for f, v in tops:
            pv = X.loc[row.name, f]
            pv_s = f"{pv:.2f}" if isinstance(pv, (int, float, np.floating)) and pd.notna(pv) else str(pv)
            print(f"         + {f:<28}{v:+.3f}   (pctile {pv_s})")
        for f, v in bots:
            pv = X.loc[row.name, f]
            pv_s = f"{pv:.2f}" if isinstance(pv, (int, float, np.floating)) and pd.notna(pv) else str(pv)
            print(f"         - {f:<28}{v:+.3f}   (pctile {pv_s})")

        def _pct(f):
            v = X.loc[row.name, f]
            return None if not isinstance(v, (int, float, np.floating)) or pd.isna(v) else round(float(v), 3)

        drivers[row["ticker"]] = {
            "families": {k: round(v, 4) for k, v in fam.items() if abs(v) > 0.004},
            "positive": [{"feature": f, "contribution": round(v, 4), "percentile": _pct(f)}
                         for f, v in tops],
            "negative": [{"feature": f, "contribution": round(v, 4), "percentile": _pct(f)}
                         for f, v in bots],
        }

    return drivers


def save_predictions(live, snap, booster, used_rank, manifest, drivers=None):
    """Persist the ranked list so it can be scored against reality later."""
    from quant_db import get_conn
    model_tag = (f"{'lambdarank' if used_rank else 'binary'}"
                 f"_{manifest.get('target_horizon')}"
                 f"_it{booster.best_iteration}")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id            SERIAL PRIMARY KEY,
                    snapshot_date DATE NOT NULL,
                    ticker        TEXT NOT NULL,
                    score         NUMERIC(18, 8),
                    rank          INTEGER,
                    decile        INTEGER,
                    model_tag     TEXT,
                    -- SHAP breakdown for the ranked names, so the Discord bot can
                    -- explain a pick without loading the model or the feature frame.
                    drivers       JSONB,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (snapshot_date, ticker, model_tag)
                )
            """)
            cur.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS drivers JSONB")
            from psycopg2.extras import execute_values, Json
            drivers = drivers or {}
            execute_values(cur, """
                INSERT INTO predictions
                    (snapshot_date, ticker, score, rank, decile, model_tag, drivers)
                VALUES %s
                ON CONFLICT (snapshot_date, ticker, model_tag) DO UPDATE SET
                    score = EXCLUDED.score, rank = EXCLUDED.rank,
                    decile = EXCLUDED.decile, drivers = EXCLUDED.drivers
            """,
            # itertuples() yields namedtuples — attribute access only, no
            # subscripts. 'rank' survives as a real field name (checked against
            # _fields), so r.rank is correct; r["rank"] raises TypeError.
            [(snap, r.ticker, float(r.score), int(r.rank), int(r.decile), model_tag,
                   Json(drivers[r.ticker]) if r.ticker in drivers else None)
                  for r in live.itertuples()])
            conn.commit()
        print(f"\n  saved {len(live)} predictions  (model_tag = {model_tag})")
    finally:
        conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="snapshot date, e.g. 2026-09-30")
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--save", action="store_true",
                   help="write the ranked list to the predictions table")
    a = p.parse_args()
    d = datetime.date.fromisoformat(a.date) if a.date else None
    main(d, a.top, a.save)
