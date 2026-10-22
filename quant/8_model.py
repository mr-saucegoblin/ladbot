"""
8_model.py — Train and honestly evaluate a LightGBM ranker on quant_clean.parquet.

Reads quant_clean.parquet plus quant_clean_manifest.json (written by 7_eda.py) and
trains a binary classifier on `outperforms` — whether a stock beat the MEDIAN stock
on its own snapshot date. The target is cross-sectional on purpose: "did it go up"
is mostly a question about the calendar, and a model trained on that learns market
timing rather than stock selection.

Everything about the evaluation here is built to avoid flattering the model:

  * Split by DATE, never randomly, with an embargo of exactly one label horizon at
    the seam. Snapshots are monthly, so at a 3-month horizon neighbouring rows share
    two thirds of their return window and without the gap the last training labels
    would describe the first test period. At a 1-month horizon the overlap nearly
    vanishes, which is why that horizon yields ~92 independent periods against ~31.
    The horizon itself is set in 7_eda.py and travels in the manifest.
  * Validation for early stopping is carved out of the TRAIN period only, with its
    own embargo, so the test set is touched exactly once.
  * The headline number is not accuracy. It's what the top-ranked names actually
    returned. A 54%-accurate model can lose money and a 51%-accurate one can make
    plenty — it depends entirely on the size of the moves it catches.

Usage:
    python 8_model.py
    python 8_model.py --quick     # fewer rounds, for a fast sanity check
"""

import argparse
import datetime
import json
import pathlib
import warnings

warnings.filterwarnings("ignore")

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, roc_auc_score

HERE          = pathlib.Path(__file__).parent
PARQUET       = HERE / "quant_clean.parquet"
MANIFEST      = HERE / "quant_clean_manifest.json"
MODEL_OUT     = HERE / "quant_model.txt"
PRED_OUT      = HERE / "quant_test_predictions.csv"

# Fraction of the TRAIN period held back for early stopping. Taken from the end of
# the train window (not sampled at random) so it mimics predicting forward.
VAL_FRACTION  = 0.15

N_DECILES     = 10


# ── Load ───────────────────────────────────────────────────────────────────────

def load() -> tuple[pd.DataFrame, dict]:
    if not PARQUET.exists() or not MANIFEST.exists():
        raise SystemExit(
            f"Missing {PARQUET.name} or {MANIFEST.name} — run 7_eda.py first."
        )
    df = pd.read_parquet(PARQUET)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    return df, manifest


# ── Split ──────────────────────────────────────────────────────────────────────

def make_split(df: pd.DataFrame, train_end, test_start, test_end, embargo):
    """
    Carve fit / val / test for one train-end date.

        [-------- fit --------][emb][-- val --][emb][---- test ----]

    Validation comes off the END of the train period rather than being sampled
    from it, because early stopping should be judged on predicting forward — the
    only thing the model ever actually does.
    """
    train_dates = sorted(df.loc[df["snapshot_date"] <= train_end, "snapshot_date"].unique())
    if len(train_dates) < 12:
        return None, None, None

    n_val_dates = max(1, int(len(train_dates) * VAL_FRACTION))
    val_start   = train_dates[-n_val_dates]
    fit_end     = val_start - embargo

    fit  = df[df["snapshot_date"] <= fit_end]
    val  = df[(df["snapshot_date"] >= val_start) & (df["snapshot_date"] <= train_end)]
    test = df[(df["snapshot_date"] >= test_start) & (df["snapshot_date"] <= test_end)]
    return fit, val, test


def walk_forward_folds(df: pd.DataFrame, embargo, step_months: int = 12):
    """
    Expanding-window folds: each refit trains on everything available up to its
    cut date, then is scored on the following year, which it has never seen.

    This is not the same as simply moving one train/test boundary later. That
    trade buys training rows by destroying test coverage. Here each fold's test
    window is *kept* — the out-of-sample predictions are pooled across folds — so
    training data and test coverage both grow. The final fold also trains on the
    most recent regime, which a fixed early boundary never gets to see.
    """
    dates = sorted(df["snapshot_date"].unique())
    first, last = dates[0], dates[-1]

    folds = []
    # Leave ~3.5 years before the first refit so fold 1 has a usable training set.
    cut = datetime.date(first.year + 3, 6, 30)
    while True:
        test_start = cut + embargo
        test_end   = datetime.date(cut.year + 1, cut.month, cut.day)
        if test_start > last:
            break
        folds.append((cut, test_start, min(test_end, last)))
        cut = datetime.date(cut.year + step_months // 12, cut.month, cut.day)
    return folds


def temporal_split(df: pd.DataFrame, manifest: dict):
    """
    Train / validation / test, all cut by date and all embargoed.

    Layout:
        [-------- train --------][emb][-- val --][emb][---- test ----]

    The validation block sits at the END of the train period rather than being
    sampled from it, because early stopping should be judged on predicting forward,
    which is the only thing the model will ever actually do.
    """
    train_end    = datetime.date.fromisoformat(manifest["train_end"])
    test_start   = datetime.date.fromisoformat(manifest["test_start"])
    embargo      = datetime.timedelta(days=manifest["embargo_days"])

    in_train_period = df["snapshot_date"] <= train_end
    train_dates     = sorted(df.loc[in_train_period, "snapshot_date"].unique())

    # Carve validation off the tail of the train period, then embargo it away from
    # the rows used for fitting.
    n_val_dates   = max(1, int(len(train_dates) * VAL_FRACTION))
    val_start     = train_dates[-n_val_dates]
    fit_end       = val_start - embargo

    fit  = df[df["snapshot_date"] <= fit_end]
    val  = df[(df["snapshot_date"] >= val_start) & (df["snapshot_date"] <= train_end)]
    test = df[df["snapshot_date"] >= test_start]

    print("=" * 70)
    print("  SPLIT (temporal, embargoed)")
    print("=" * 70)
    for name, part in [("fit", fit), ("val", val), ("test", test)]:
        if part.empty:
            print(f"  {name:<6} EMPTY")
            continue
        print(f"  {name:<6} {part['snapshot_date'].min()} .. {part['snapshot_date'].max()}"
              f"   {len(part):>7,} rows   {part['ticker'].nunique():>3} tickers"
              f"   target mean {part['outperforms'].mean():.3f}")
    dropped = len(df) - len(fit) - len(val) - len(test)
    print(f"  {'(embargo + gaps discarded)':<30} {dropped:>7,} rows")
    return fit, val, test


# ── Train ──────────────────────────────────────────────────────────────────────

def prepare(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Select the feature columns and give `sector` a categorical dtype.

    NaNs are left in place deliberately — LightGBM learns a default direction for
    missing values at each split, so a gap is information rather than a row to drop.
    """
    X = df[feature_cols].copy()
    for c in X.columns:
        if X[c].dtype == object or isinstance(X[c].dtype, pd.StringDtype):
            X[c] = X[c].astype("category")
    return X


def group_sizes(df: pd.DataFrame) -> list[int]:
    """
    Rows per snapshot_date, in order — LightGBM's `group` argument for ranking.

    It tells lambdarank which rows compete against each other. Each month is one
    "query": the model is scored on how well it orders that month's ~450 names,
    never on comparisons between a 2019 stock and a 2024 one. The frame must
    already be sorted by snapshot_date or the group boundaries land in the wrong
    places and the objective silently trains on nonsense.
    """
    return df.groupby("snapshot_date", sort=True).size().tolist()


def train(fit, val, feature_cols, target, quick: bool, verbose: bool = True,
          rank: bool = False):
    if rank:
        # lambdarank needs rows grouped by query, so sort by date first.
        fit = fit.sort_values("snapshot_date")
        val = val.sort_values("snapshot_date")

    X_fit, y_fit = prepare(fit, feature_cols), fit[target]
    X_val, y_val = prepare(val, feature_cols), val[target]

    # Align validation categories to the fitted ones, or LightGBM sees a different
    # encoding at scoring time.
    for c in X_val.columns:
        if str(X_fit[c].dtype) == "category":
            X_val[c] = X_val[c].astype(
                pd.CategoricalDtype(categories=X_fit[c].cat.categories)
            )

    params = {
        "objective":        "binary",
        "metric":           "auc",
        "learning_rate":    0.03,
        "num_leaves":       31,
        # The effective sample is far smaller than the row count — 90 monthly
        # snapshots contain only ~30 non-overlapping 3-month periods — so the model
        # is kept deliberately small and regularised to resist memorising periods.
        "min_data_in_leaf": 200,
        "feature_fraction": 0.7,
        "bagging_fraction": 0.8,
        "bagging_freq":     1,
        "lambda_l2":        5.0,
        "verbose":          -1,
        "seed":             42,
    }

    if rank:
        # lambdarank optimises the ORDER of each month's list rather than a
        # per-row probability. Two things follow from that:
        #
        #   * the label is a 0-9 grade, and the default label_gain of 2^i - 1
        #     makes grade 9 worth 511 against grade 5's 31 — so misplacing a big
        #     winner costs far more than misplacing a marginal one. That is the
        #     gap the binary target left: hit rate ranked correctly while mean
        #     return stayed flat, because 0/1 treats +0.1% and +40% identically.
        #
        #   * NDCG@50 is the early-stopping metric. 50 of ~450 names is roughly
        #     the top decile — the part of the list you would actually trade, and
        #     the only part worth optimising.
        params["objective"]     = "lambdarank"
        params["metric"]        = "ndcg"
        params["ndcg_eval_at"]  = [50]
        params["lambdarank_truncation_level"] = 100

    rounds = 200 if quick else 3000

    if verbose:
        print("\n" + "=" * 70)
        print("  TRAINING")
        print("=" * 70)
    ds_fit = lgb.Dataset(X_fit, y_fit,
                         group=group_sizes(fit) if rank else None)
    ds_val = lgb.Dataset(X_val, y_val,
                         group=group_sizes(val) if rank else None)

    booster = lgb.train(
        params,
        ds_fit,
        num_boost_round=rounds,
        valid_sets=[ds_val],
        valid_names=["val"],
        callbacks=[
            lgb.early_stopping(100, verbose=False),
            lgb.log_evaluation(period=200 if verbose else 0),
        ],
    )
    if verbose:
        metric, score = next(iter(booster.best_score["val"].items()))
        print(f"  best iteration: {booster.best_iteration}  "
              f"val {metric} {score:.4f}")
    return booster


def predict_frame(booster, test, feature_cols, fit, target, horizon) -> pd.DataFrame:
    """Score one test block and return it with predictions attached."""
    X_test = prepare(test, feature_cols)
    X_fit  = prepare(fit, feature_cols)
    for c in X_test.columns:
        if str(X_test[c].dtype) == "category":
            X_test[c] = X_test[c].astype(
                pd.CategoricalDtype(categories=X_fit[c].cat.categories)
            )
    out = test[["ticker", "snapshot_date", horizon, target]].copy()
    out = out.rename(columns={horizon: "fwd_return"})
    out["pred_proba"] = booster.predict(X_test, num_iteration=booster.best_iteration)
    return out


def run_walk_forward(df, manifest, feature_cols, target, horizon, quick, rank=False):
    """
    Train one model per fold and pool every fold's out-of-sample predictions into
    a single evaluation set, so the headline numbers rest on far more test data
    than a single split can provide.
    """
    embargo = datetime.timedelta(days=manifest["embargo_days"])
    folds   = walk_forward_folds(df, embargo)
    if not folds:
        raise SystemExit("Not enough history for walk-forward folds.")

    print("=" * 70)
    print(f"  WALK-FORWARD — {len(folds)} expanding-window folds")
    print("=" * 70)

    preds, rows = [], []
    for i, (cut, ts, te) in enumerate(folds, 1):
        fit, val, test = make_split(df, cut, ts, te, embargo)
        if fit is None or test is None or test.empty or val.empty:
            print(f"  fold {i}: skipped (insufficient data)")
            continue

        train_target = manifest["target_rank"] if rank else target
        booster = train(fit, val, feature_cols, train_target, quick,
                        verbose=False, rank=rank)
        p = predict_frame(booster, test, feature_cols, fit, target, horizon)
        p["fold"] = i
        preds.append(p)

        auc = roc_auc_score(p[target], p["pred_proba"])
        d   = p.dropna(subset=["fwd_return"]).copy()
        d["decile"] = d.groupby("snapshot_date")["pred_proba"].transform(
            lambda s: pd.qcut(s.rank(method="first"), N_DECILES, labels=False) + 1)
        spread = (d[d.decile == N_DECILES]["fwd_return"].mean()
                  - d[d.decile == 1]["fwd_return"].mean())
        rows.append((i, cut, ts, te, len(fit), len(test), auc, spread,
                     d["fwd_return"].mean()))

    print(f"\n  {'fold':<6}{'trained through':<17}{'tested':<24}"
          f"{'train':>8}{'test':>7}{'AUC':>8}{'spread':>9}{'univ':>8}")
    print("  " + "-" * 87)
    for i, cut, ts, te, nf, nt, auc, sp, um in rows:
        print(f"  {i:<6}{str(cut):<17}{str(ts)+' .. '+str(te):<24}"
              f"{nf:>8,}{nt:>7,}{auc:>8.4f}{sp:>+9.2%}{um:>+8.2%}")

    pooled = pd.concat(preds, ignore_index=True)
    print(f"\n  pooled out-of-sample: {len(pooled):,} rows across "
          f"{pooled['snapshot_date'].nunique()} months "
          f"({pooled['snapshot_date'].min()} .. {pooled['snapshot_date'].max()})")

    auc = roc_auc_score(pooled[target], pooled["pred_proba"])
    acc = (None if rank else
           accuracy_score(pooled[target], (pooled["pred_proba"] > 0.5).astype(int)))
    base = max(pooled[target].mean(), 1 - pooled[target].mean())
    print("\n" + "=" * 70)
    print("  POOLED OUT-OF-SAMPLE — CLASSIFICATION")
    print("=" * 70)
    print(f"  AUC                 {auc:.4f}      (0.50 = no skill)")
    if acc is None:
        print("  accuracy            n/a (ranking objective emits scores)")
    else:
        print(f"  accuracy            {acc:.4f}")
        print(f"  majority baseline   {base:.4f}")
        print(f"  edge over baseline  {acc - base:+.4f}")
    return pooled


# ── Evaluate ───────────────────────────────────────────────────────────────────

def evaluate(booster, test, feature_cols, target, fit, horizon, rank: bool = False):
    X_test = prepare(test, feature_cols)
    X_fit  = prepare(fit, feature_cols)   # built once, not once per column
    for c in X_test.columns:
        if str(X_test[c].dtype) == "category":
            X_test[c] = X_test[c].astype(
                pd.CategoricalDtype(categories=X_fit[c].cat.categories)
            )

    proba = booster.predict(X_test, num_iteration=booster.best_iteration)
    y     = test[target].values

    # AUC is rank-based, so it works on lambdarank's raw scores as well as on
    # binary probabilities, which keeps the two objectives comparable.
    auc = roc_auc_score(y, proba)

    print("\n" + "=" * 70)
    print("  TEST SET — CLASSIFICATION")
    print("=" * 70)
    print(f"  AUC                 {auc:.4f}      (0.50 = no skill)")
    if rank:
        # Accuracy is undefined here: lambdarank emits an unbounded relevance
        # score, not a probability, so thresholding it at 0.5 is meaningless.
        print("  accuracy            n/a (ranking objective emits scores, not probabilities)")
    else:
        acc = accuracy_score(y, (proba > 0.5).astype(int))
        base_rate = y.mean()
        base_acc = max(base_rate, 1 - base_rate)   # always guess the majority class
        print(f"  accuracy            {acc:.4f}")
        print(f"  majority baseline   {base_acc:.4f}      <- beat this, or the model adds nothing")
        print(f"  edge over baseline  {acc - base_acc:+.4f}")

    out = test[["ticker", "snapshot_date", horizon, target]].copy()
    out = out.rename(columns={horizon: "fwd_return"})
    out["pred_proba"] = proba
    return out


def decile_report(pred: pd.DataFrame, horizon: str):
    """
    The part that actually matters: rank each date's stocks by predicted probability,
    bucket into deciles, and report what those buckets really returned.

    Ranking WITHIN each snapshot date, not across the whole test set, because that's
    how the model would be used — on any given month you pick from the names
    available that month. Pooling across dates would let a bull market's decile 10
    be filled entirely with rows from one lucky quarter.
    """
    df = pred.dropna(subset=["fwd_return"]).copy()
    if df.empty:
        print("\n  No forward returns in the test set — skipping decile report.")
        return

    # qcut raises if a date has fewer rows than buckets, which can happen at the
    # tail of the test window where labels are still filling in. Drop those dates
    # rather than let one thin month abort the whole report.
    counts     = df.groupby("snapshot_date")["pred_proba"].transform("size")
    thin       = int((counts < N_DECILES).sum())
    if thin:
        print(f"\n  ({thin} rows on dates with fewer than {N_DECILES} names — excluded "
              f"from deciles)")
        df = df[counts >= N_DECILES]

    df["decile"] = (
        df.groupby("snapshot_date")["pred_proba"]
          .transform(lambda s: pd.qcut(s.rank(method="first"), N_DECILES, labels=False) + 1)
    )

    print("\n" + "=" * 70)
    print("  TEST SET — WHAT THE RANKING ACTUALLY RETURNED")
    print("=" * 70)
    # std and mean/std are printed for every decile, permanently. A model can
    # manufacture a large raw spread simply by buying the most volatile names in
    # the universe — which is exactly what the lambdarank run did on its first
    # outing, posting +6.79% while running 3.4x the volatility of the bottom
    # decile and a WORSE return per unit of risk. Without these two columns that
    # looks like a discovery instead of a leveraged bet.
    print(f"  {'decile':<8}{'n':>7}{'mean ret':>11}{'median':>9}"
          f"{'hit rate':>10}{'std':>9}{'ret/std':>9}")
    print("  " + "-" * 63)
    for d, part in df.groupby("decile"):
        r, sd = part["fwd_return"].mean(), part["fwd_return"].std()
        print(f"  {int(d):<8}{len(part):>7,}{r:>10.2%}"
              f"{part['fwd_return'].median():>9.2%}"
              f"{(part['fwd_return'] > 0).mean():>10.1%}"
              f"{sd:>9.2%}{(r / sd if sd else float('nan')):>9.2f}")

    top    = df[df["decile"] == N_DECILES]["fwd_return"]
    bottom = df[df["decile"] == 1]["fwd_return"]
    spread = top.mean() - bottom.mean()
    print("  " + "-" * 63)
    label = horizon.replace("forward_return_", "")
    print(f"  top decile minus bottom decile: {spread:+.2%} per {label}")
    print(f"  universe mean over same rows:   {df['fwd_return'].mean():+.2%}")

    # The honest comparison. If the top decile only wins by carrying more risk,
    # its return/std will be no better than the bottom's and the raw spread is
    # compensation rather than skill.
    rs_top = top.mean() / top.std() if top.std() else float("nan")
    rs_bot = bottom.mean() / bottom.std() if bottom.std() else float("nan")
    print(f"  return/std  top {rs_top:.2f}  vs  bottom {rs_bot:.2f}   "
          f"(vol ratio {top.std() / bottom.std():.2f}x)"
          if bottom.std() else "")
    if rs_top <= rs_bot:
        print("  ^ top decile is NOT better risk-adjusted — the raw spread is a "
              "risk premium, not alpha")

    # A spread that only shows up in a couple of quarters is noise, not signal.
    per_date = (
        df[df["decile"].isin([1, N_DECILES])]
        .groupby(["snapshot_date", "decile"])["fwd_return"].mean().unstack()
    )
    if {1, N_DECILES}.issubset(per_date.columns):
        per_date["spread"] = per_date[N_DECILES] - per_date[1]
        wins = (per_date["spread"] > 0).mean()
        print(f"  months where top beat bottom:   {wins:.1%} "
              f"({int((per_date['spread'] > 0).sum())} of {len(per_date)})")
        print("  ^ a spread carried by one or two months is noise, not signal")


def importance_report(booster, feature_cols):
    # Importance must cover the SAME trees that produce the predictions. Without
    # num_iteration, LightGBM sums over every tree it built — including the ~100
    # rounds after the best one that early stopping rejected. When early stopping
    # fires almost immediately (best_iteration=3 of 103 rounds), the default would
    # report the importance of a 103-tree model while the scored model has 3 trees.
    imp = pd.DataFrame({
        "feature": booster.feature_name(),
        "gain":    booster.feature_importance("gain",
                                              iteration=booster.best_iteration),
    }).sort_values("gain", ascending=False)
    imp["share"] = imp["gain"] / imp["gain"].sum()

    print("\n" + "=" * 70)
    print("  FEATURE IMPORTANCE (top 20 by gain)")
    print("=" * 70)
    for _, r in imp.head(20).iterrows():
        bar = "#" * int(r["share"] * 200)
        print(f"  {r['feature']:<30}{r['share']:>7.1%}  {bar}")

    dead = imp[imp["gain"] == 0]["feature"].tolist()
    if dead:
        print(f"\n  never used ({len(dead)}): {', '.join(dead[:12])}"
              + (" ..." if len(dead) > 12 else ""))
    return imp


# ── Main ───────────────────────────────────────────────────────────────────────

def main(quick: bool, walk_forward: bool = False, rank: bool = False):
    df, manifest = load()
    feature_cols = [c for c in manifest["feature_cols"] if c in df.columns]
    target       = manifest["target"]

    print(f"Loaded {len(df):,} rows, {len(feature_cols)} features, target='{target}'")
    print(f"Tiers modelled: {manifest.get('tiers_modelled')}  "
          f"| excluded tickers: {manifest.get('excluded_tickers')}")
    print(f"Horizon: {manifest.get('target_horizon')}  "
          f"| cross-sectional ranks: {manifest.get('cross_sectional_rank')}")

    fit, val, test = temporal_split(df, manifest)
    if fit.empty or val.empty or test.empty:
        raise SystemExit("A split is empty — check train_end / test_start in the manifest.")

    overlap = set(fit["ticker"]) & set(test["ticker"])
    print(f"\n  tickers in both fit and test: {len(overlap)} "
          f"(expected — the split is by date, not by company)")

    horizon = manifest.get("target_horizon", "forward_return_3m")

    if rank:
        # lambdarank needs an integer grade; drop the rare row a thin month left NaN.
        before = len(df)
        df = df[df[manifest["target_rank"]].notna()].copy()
        df[manifest["target_rank"]] = df[manifest["target_rank"]].astype(int)
        print(f"Objective: lambdarank on '{manifest['target_rank']}' "
              f"({manifest['rank_grades']} grades); dropped {before - len(df)} ungraded rows")

    if walk_forward:
        pooled = run_walk_forward(df, manifest, feature_cols, target, horizon,
                                  quick, rank=rank)
        decile_report(pooled, horizon)
        pooled.to_csv(PRED_OUT, index=False)
        print(f"\nSaved: {PRED_OUT.name}  ({len(pooled):,} pooled predictions)")
        return

    train_target = manifest["target_rank"] if rank else target
    booster = train(fit, val, feature_cols, train_target, quick, rank=rank)
    pred    = evaluate(booster, test, feature_cols, target, fit, horizon, rank=rank)
    decile_report(pred, horizon)
    importance_report(booster, feature_cols)

    booster.save_model(str(MODEL_OUT), num_iteration=booster.best_iteration)
    pred.to_csv(PRED_OUT, index=False)
    print(f"\nSaved: {MODEL_OUT.name}")
    print(f"Saved: {PRED_OUT.name}  ({len(pred):,} test predictions)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true",
                        help="200 rounds instead of 3000, for a fast sanity check")
    parser.add_argument("--walk-forward", action="store_true",
                        help="Expanding-window folds, refit annually; pools every "
                             "fold's out-of-sample predictions into one evaluation")
    parser.add_argument("--rank", action="store_true",
                        help="Train with lambdarank on the 0-9 graded target, "
                             "weighting errors at the top of each month's list, "
                             "instead of binary classification")
    args = parser.parse_args()
    main(quick=args.quick, walk_forward=args.walk_forward, rank=args.rank)
