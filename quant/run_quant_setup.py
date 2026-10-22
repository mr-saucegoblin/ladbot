"""
run_quant_setup.py — One-time full quant system setup.

Runs the entire pipeline in sequence:
  1. Apply DB schema
  2. Pull all FMP data (650 tickers, ~3-4 hours)  → 2_fmp_pipeline.py
  3. Pull macro data (commodities, forex, rates)    → 3_macro_pipeline.py
  4. Compute Piotroski F-Scores                    → 5_factor_compute.py
  5. Compute feature snapshots                      → 6_feature_compute.py

Note: 1_fetch_universe.py is a separate utility — run it manually when you
      want to refresh the ticker universe (not needed every time).

Run once from your local machine with QUANT_DB_URL pointing to Railway Postgres.

Usage:
    python run_quant_setup.py
    python run_quant_setup.py --start-from macro   # resume after a failed run
"""

import argparse
import datetime
import importlib.util
import sys
import time
from tqdm import tqdm

STEPS = ["schema", "fmp", "macro", "factors", "features"]

STEP_FILES = {
    "fmp":      "2_fmp_pipeline.py",
    "macro":    "3_macro_pipeline.py",
    "factors":  "5_factor_compute.py",
    "features": "6_feature_compute.py",
}

STEP_LABELS = {
    "schema":   "Apply DB schema                   (< 1 min)",
    "fmp":      "Pull FMP data  — 2_fmp_pipeline.py     (~3-4 hrs)",
    "macro":    "Pull macro     — 3_macro_pipeline.py   (~10 min)",
    "factors":  "Piotroski      — 5_factor_compute.py   (~10 min)",
    "features": "Feature snaps  — 6_feature_compute.py  (~1 hr)",
}


def load_step(filename: str):
    """Load a numbered script (e.g. '2_fmp_pipeline.py') as a module."""
    spec = importlib.util.spec_from_file_location("_step", filename)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_step(name: str, skip_from: str | None, overall_bar: tqdm) -> bool:
    if skip_from and STEPS.index(name) < STEPS.index(skip_from):
        tqdm.write(f"  >>  Skipping: {name}")
        overall_bar.update(1)
        return True

    t0    = time.time()
    label = STEP_LABELS[name]
    tqdm.write(f"\n{'='*60}")
    tqdm.write(f"  STEP {STEPS.index(name)+1}/{len(STEPS)}: {label}")
    tqdm.write(f"  Started: {datetime.datetime.now().strftime('%H:%M:%S')}")
    tqdm.write(f"{'='*60}")
    overall_bar.set_postfix_str(name)

    try:
        if name == "schema":
            from quant_db import init_schema
            init_schema()

        else:
            filename = STEP_FILES[name]
            mod = load_step(filename)
            if name == "features":
                mod.run(test_mode=False)
            else:
                mod.run()

        elapsed = time.time() - t0
        tqdm.write(f"\n  ok  {name} complete — {elapsed/60:.1f} min")
        overall_bar.update(1)
        return True

    except Exception as e:
        elapsed = time.time() - t0
        tqdm.write(f"\n  x  {name} FAILED after {elapsed/60:.1f} min: {e}")
        overall_bar.update(1)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-from",
        choices=STEPS,
        default=None,
        help="Skip all steps before this one (use after a failed run)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  QUANT SYSTEM — FULL DATA + FEATURE REFRESH")
    print("  Estimated total time: 4-6 hours")
    print("  Leave this terminal running.")
    print("=" * 60)
    print("\n  Steps:")
    for i, step in enumerate(STEPS, 1):
        skip = args.start_from and STEPS.index(step) < STEPS.index(args.start_from)
        icon = ">>" if skip else "○"
        print(f"    {icon} {i}. {STEP_LABELS[step]}")
    print()
    print("  After this completes, run the model scripts:")
    print("    python 7_eda.py       # EDA + visualizations")
    print("    python 8_model.py     # train LogReg / DT / RF")
    print("    python 9_results.py   # evaluation + charts")
    print()

    start       = time.time()
    overall_bar = tqdm(
        total=len(STEPS), desc="Overall", unit="step",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} steps [{elapsed}<{remaining}] {postfix}",
    )

    for step in STEPS:
        success = run_step(step, args.start_from, overall_bar)
        if not success:
            overall_bar.close()
            print(f"\n  Pipeline stopped at '{step}'.")
            print(f"  Fix the error then resume with:")
            print(f"    python run_quant_setup.py --start-from {step}")
            sys.exit(1)

    overall_bar.close()
    total_hrs = (time.time() - start) / 3600
    print(f"\n{'='*60}")
    print(f"  ALL STEPS COMPLETE")
    print(f"  Total time: {total_hrs:.1f} hours")
    print(f"  Finished: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Next: python 7_eda.py")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
