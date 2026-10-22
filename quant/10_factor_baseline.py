"""
10_factor_baseline.py — Does a feature predict returns, on its own, with no model?

For each feature: every month, rank the universe by it and measure the rank
correlation with the forward return. Average those monthly correlations and you have
the information coefficient (IC) — the standard way to ask whether a signal works.

Why this is worth more than another model run:

  * It uses all ~90 months, not just the out-of-sample slice. Nothing is fitted, so
    there is no model to overfit.

    But "nothing is fitted" does NOT mean "no leakage". The moment these t-stats are
    used to CHOOSE which features a model gets, the choice is a fitted decision, and
    a choice made over all 90 months has seen the walk-forward's test windows. The
    walk-forward then scores that model on the very period that helped select its
    inputs. Use --through <train_end> whenever the output will drive a feature cut.
  * The answer is a property of the market, not of a model. "Price-to-book has a mean
    IC of 0.03 in Canadian main-board names" stays true regardless of what LightGBM
    does with it.
  * It gives the model a bar to clear. If a one-line sort does as well as the model,
    the model is not adding anything.

t-stats are computed on NON-OVERLAPPING windows. With monthly snapshots and a 3-month
forward return, consecutive months share two thirds of their return window, so the
naive t over all 90 months overstates significance by roughly sqrt(3).

Usage:
    python 10_factor_baseline.py
    python 10_factor_baseline.py --feature operating_margin   # one feature, in detail
    python 10_factor_baseline.py --top 25

    # score every candidate on TRAINING data only, to re-cut without leakage:
    python 7_eda.py --keep-weak
    python 10_factor_baseline.py --parquet quant_clean_all.parquet            --through 2024-06-30 --top 100
"""

import argparse
import json
import pathlib

import numpy as np
import pandas as pd

HERE      = pathlib.Path(__file__).parent
N_DECILES = 10


def load(parquet="quant_clean.parquet", through=None):
    """
    Load a feature parquet and optionally truncate it at `through`.

    `through` exists so a feature's t-stat can be measured on TRAINING data only.
    Measuring it over all 90 months and then feeding the survivors to a walk-forward
    backtest leaks the test windows into the feature set: the model is handed exactly
    the columns that happened to work in the period it is about to be scored on.
    Pass --through <train_end> to see which features would have been chosen by
    someone standing at that date with no knowledge of the future.
    """
    pq = HERE / parquet
    mf = HERE / (pathlib.Path(parquet).stem + "_manifest.json")
    if not pq.exists():
        raise SystemExit(f"{parquet} missing — run 7_eda.py first.")
    df = pd.read_parquet(pq)
    man = json.loads(mf.read_text(encoding="utf-8"))
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    if through:
        cut = pd.Timestamp(through)
        before = len(df)
        df = df[df["snapshot_date"] <= cut]
        print(f"  --through {through}: {before:,} -> {len(df):,} rows "
              f"({df['snapshot_date'].nunique()} months, "
              f"train-only feature selection)")
    return df, man


def monthly_ic(df: pd.DataFrame, feature: str, ret: str) -> pd.Series:
    """Spearman rank correlation between feature and forward return, per month."""
    out = {}
    for d, g in df.groupby("snapshot_date"):
        sub = g[[feature, ret]].dropna()
        if len(sub) < 30 or sub[feature].nunique() < 5:
            continue
        out[d] = sub[feature].corr(sub[ret], method="spearman")
    return pd.Series(out).dropna()


def indep_t(x: pd.Series, step: int) -> float:
    """
    t-stat averaged over the `step` possible non-overlapping subsamples.

    Taking every `step`-th month gives independent observations; which offset you
    start from is arbitrary, so all of them are averaged rather than cherry-picked.
    """
    ts = []
    for off in range(step):
        s = x.iloc[off::step]
        if len(s) > 3 and s.std():
            ts.append(s.mean() / (s.std() / np.sqrt(len(s))))
    return float(np.mean(ts)) if ts else float("nan")


def decile_spread(df: pd.DataFrame, feature: str, ret: str) -> tuple[float, float]:
    """Mean forward return of the top feature-decile minus the bottom, and hit rate."""
    rows = []
    for d, g in df.groupby("snapshot_date"):
        sub = g[[feature, ret]].dropna()
        if len(sub) < N_DECILES * 3:
            continue
        q = pd.qcut(sub[feature].rank(method="first"), N_DECILES, labels=False)
        hi, lo = sub[ret][q == N_DECILES - 1].mean(), sub[ret][q == 0].mean()
        rows.append(hi - lo)
    if not rows:
        return float("nan"), float("nan")
    s = pd.Series(rows)
    return s.mean(), (s > 0).mean()


def main(feature, top, horizon_step, parquet, through):
    df, man = load(parquet, through)
    ret = man.get("target_horizon", "forward_return_3m")
    feats = [feature] if feature else [c for c in man["feature_cols"]
                                       if c in df.columns
                                       and pd.api.types.is_numeric_dtype(df[c])]

    print(f"Factor baselines vs {ret} — {df['snapshot_date'].nunique()} months, "
          f"{len(df):,} rows")
    print(f"t-stats use every {horizon_step}th month (non-overlapping windows)\n")

    rows = []
    for f in feats:
        ic = monthly_ic(df, f, ret)
        if len(ic) < 12:
            continue
        spread, hit = decile_spread(df, f, ret)
        rows.append({
            "feature": f, "mean_ic": ic.mean(), "t": indep_t(ic, horizon_step),
            "ic_pos": (ic > 0).mean(), "spread": spread, "spread_hit": hit,
            "months": len(ic),
        })

    res = pd.DataFrame(rows).sort_values("t", key=abs, ascending=False)
    print(f"  {'feature':<32}{'mean IC':>9}{'t':>8}{'IC>0':>8}"
          f"{'decile spread':>15}{'wins':>7}")
    print("  " + "-" * 79)
    for _, r in res.head(top).iterrows():
        flag = "  <<" if abs(r["t"]) >= 2 else ""
        print(f"  {r['feature']:<32}{r['mean_ic']:>+9.4f}{r['t']:>+8.2f}"
              f"{r['ic_pos']:>8.0%}{r['spread']:>+15.2%}{r['spread_hit']:>7.0%}{flag}")

    print(f"\n  << marks |t| >= 2. A POSITIVE IC means a HIGH value of the feature")
    print(f"  went with a HIGH forward return; negative means the reverse.")
    return res


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--feature", default=None, help="analyse a single feature")
    p.add_argument("--top", type=int, default=30)
    p.add_argument("--step", type=int, default=3,
                   help="months per independent window (3 for a 3-month horizon)")
    p.add_argument("--parquet", default="quant_clean.parquet",
                   help="use quant_clean_all.parquet to score every candidate")
    p.add_argument("--through", default=None,
                   help="only use months up to this date, e.g. 2024-06-30 "
                        "(train-only selection, no test-window leakage)")
    a = p.parse_args()
    main(a.feature, a.top, a.step, a.parquet, a.through)
