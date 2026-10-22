"""
11_selection_stability.py — Was the feature cut honest?

7_eda.py drops any feature whose univariate t-stat is below WEAK_CUTOFF. Those
t-stats were measured over all 90 months, which includes the windows the
walk-forward backtest is scored on. So the model was handed the columns that
happened to work in the period it was about to be graded on. That is selection
bias, and it inflates the backtest by an unknown amount.

This script measures the amount. It computes each feature's t-stat twice:

    full   — all 90 months, which is what the current cut used
    train  — only months <= TRAIN_END, which is all an honest selector could see

Then it applies the same cutoff to both and compares the surviving sets. The
question is not "which set is better" (the train set is, by construction, the only
defensible one) but "how different are they". If they largely agree, the live
backtest number is close to honest. If they diverge, the real number is lower and
feature selection has to move inside each walk-forward fold.

Reads quant_clean_all.parquet, so run this first:

    python 7_eda.py --keep-weak
    python 11_selection_stability.py
    python 11_selection_stability.py --cutoff 1.0 --train-end 2024-06-30
"""

import argparse
import importlib.util
import json
import pathlib

import numpy as np
import pandas as pd

HERE = pathlib.Path(__file__).parent

# The IC and t-stat helpers live in 10_factor_baseline.py. A module whose name
# starts with a digit is not a legal identifier, so a plain `import` is impossible
# and it has to be loaded by file path. Reusing them matters more than convenience:
# if this script reimplemented the maths the comparison would be against a
# different definition of "t-stat" and would prove nothing.
_spec = importlib.util.spec_from_file_location(
    "factor_baseline", HERE / "10_factor_baseline.py")
_fb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fb)

# The 16 features the current model actually runs on, so the report can say
# directly which of them an honest selector would have thrown away.
CURRENT_16 = [
    "sector", "market_cap", "gross_margin", "operating_margin",
    "revenue_growth_qoq", "ps_ratio_pct", "consecutive_eps_beats",
    "dynamic_ev_ebitda", "gross_margin_trend", "oil_beta",
    "dynamic_fcf_yield_sect", "piotroski_sect", "ttm_roic_sect",
    "ttm_op_cf_to_assets_sect", "shares_change_1yr_sect",
    "return_12m_1m_vol_adj",
]


def t_table(df, feats, ret, step):
    """t-stat and mean IC for every feature, on whatever slice of df is passed."""
    rows = {}
    for f in feats:
        ic = _fb.monthly_ic(df, f, ret)
        if len(ic) < 8:
            continue
        rows[f] = {"t": _fb.indep_t(ic, step), "ic": ic.mean(), "months": len(ic)}
    return pd.DataFrame(rows).T


def main(cutoff, train_end, step, parquet):
    pq = HERE / parquet
    mf = HERE / (pathlib.Path(parquet).stem + "_manifest.json")
    if not pq.exists():
        raise SystemExit(f"{parquet} missing — run: python 7_eda.py --keep-weak")

    df = pd.read_parquet(pq)
    man = json.loads(mf.read_text(encoding="utf-8"))
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    ret = man.get("target_horizon", "forward_return_3m")

    feats = [c for c in man["feature_cols"]
             if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]

    cut = pd.Timestamp(train_end)
    tr = df[df["snapshot_date"] <= cut]

    print(f"  candidates: {len(feats)} numeric features, target {ret}")
    print(f"  full  : {df['snapshot_date'].nunique()} months "
          f"({df['snapshot_date'].min():%Y-%m} .. {df['snapshot_date'].max():%Y-%m})")
    print(f"  train : {tr['snapshot_date'].nunique()} months "
          f"({tr['snapshot_date'].min():%Y-%m} .. {tr['snapshot_date'].max():%Y-%m})")
    print(f"  cutoff: keep |t| >= {cutoff}, non-overlapping step {step}")

    full, train = t_table(df, feats, ret, step), t_table(tr, feats, ret, step)

    keep_full = set(full.index[full["t"].abs() >= cutoff])
    keep_train = set(train.index[train["t"].abs() >= cutoff])

    both = keep_full & keep_train
    only_full = keep_full - keep_train       # chosen ONLY because of test-window data
    only_train = keep_train - keep_full      # an honest selector would have taken these

    print()
    print(f"  survive on full sample : {len(keep_full)}")
    print(f"  survive on train only  : {len(keep_train)}")
    print(f"  agree                  : {len(both)}")
    jac = len(both) / max(len(keep_full | keep_train), 1)
    print(f"  overlap (Jaccard)      : {jac:.0%}")

    cmp = full[["t"]].rename(columns={"t": "t_full"}).join(
        train[["t"]].rename(columns={"t": "t_train"}), how="outer")
    cmp["flip"] = np.sign(cmp["t_full"]) != np.sign(cmp["t_train"])

    print()
    print("  KEPT ON FULL BUT NOT ON TRAIN — these are the contaminated picks")
    sub = cmp.loc[sorted(only_full)].sort_values("t_full", key=abs, ascending=False)
    if sub.empty:
        print("    (none — the cut was stable)")
    for f, r in sub.iterrows():
        star = "  SIGN FLIP" if r["flip"] else ""
        print(f"    {f:<34} t_full {r['t_full']:+.2f}   t_train {r['t_train']:+.2f}{star}")

    print()
    print("  KEPT ON TRAIN BUT NOT ON FULL — an honest cut would have included these")
    sub = cmp.loc[sorted(only_train)].sort_values("t_train", key=abs, ascending=False)
    if sub.empty:
        print("    (none)")
    for f, r in sub.iterrows():
        star = "  SIGN FLIP" if r["flip"] else ""
        print(f"    {f:<34} t_train {r['t_train']:+.2f}   t_full {r['t_full']:+.2f}{star}")

    print()
    print(f"  THE {len(CURRENT_16)} FEATURES THE MODEL ACTUALLY USES")
    print("  (sector is categorical, so it has no univariate t-stat)")
    print(f"    {'feature':<34} {'t_train':>8} {'t_full':>8}   verdict")
    survivors = 0
    for f in CURRENT_16:
        if f not in cmp.index:
            print(f"    {f:<34} {'—':>8} {'—':>8}   not scored")
            continue
        r = cmp.loc[f]
        ok = abs(r["t_train"]) >= cutoff
        survivors += ok
        v = "survives honest cut" if ok else "WOULD BE DROPPED"
        if ok and np.sign(r["t_full"]) != np.sign(r["t_train"]):
            v = "survives, BUT SIGN FLIPPED"
        print(f"    {f:<34} {r['t_train']:+8.2f} {r['t_full']:+8.2f}   {v}")
    print(f"\n    {survivors} of {len(CURRENT_16)} would survive selection "
          f"made at {train_end}")

    out = HERE / "selection_stability.csv"
    cmp.sort_values("t_train", key=abs, ascending=False).to_csv(out)
    print(f"\n  Saved: {out.name}")
    print("\n  Read it this way: high overlap means the backtest is roughly honest.")
    print("  Low overlap, or sign flips among the 16, means the +6.53% spread is")
    print("  partly an artifact of choosing features with hindsight.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--cutoff", type=float, default=1.0,
                   help="keep features with |t| >= this (7_eda.py used 1.0)")
    p.add_argument("--train-end", default="2024-06-30")
    p.add_argument("--step", type=int, default=3,
                   help="months per independent window (3 for a 3-month horizon)")
    p.add_argument("--parquet", default="quant_clean_all.parquet")
    a = p.parse_args()
    main(a.cutoff, a.train_end, a.step, a.parquet)
