"""
pipeline_runner.py — run the quant pipeline end to end, unattended.

Each step is a separate process, not an import. The pipeline scripts are named
`2_fmp_pipeline.py` and so on, which are not legal Python identifiers, and more
importantly they are long-running jobs that allocate heavily and call sys.exit on
bad input. A subprocess gives each one its own memory that is definitely returned
when it ends, and a non-zero exit code to check, and means a SystemExit in step 3
cannot take the Discord bot down with it.

The weekly run deliberately skips 7_eda.py and 8_model.py:

  * 8_model.py only evaluates. It reports how good the model is, which does not
    change because a week of prices arrived.
  * 9_score.py trains its own model from quant_clean.parquet, so 7_eda.py only
    needs re-running when the feature set or the training window changes.

Re-run those two by hand monthly, or whenever features change.

Usage:
    python pipeline_runner.py                 # full weekly run
    python pipeline_runner.py --dry-run       # print the plan, run nothing
    python pipeline_runner.py --as-of 2026-10-23
"""

import datetime
import pathlib
import subprocess
import sys
import time

QUANT_DIR = pathlib.Path(__file__).parent / "quant"

# (args, timeout_seconds, what it does)
#
# Timeouts are generous: these are wall-clock bound by FMP's rate limit, not by
# CPU, so a slow night is normal and a hang is the thing worth catching.
STEPS = [
    (["2_fmp_pipeline.py", "--incremental"], 5400,
     "prices and fundamentals"),
    (["3_macro_pipeline.py"], 900,
     "macro series"),
    (["5_factor_compute.py"], 2700,
     "Piotroski and Altman"),
    (["6_feature_compute.py", "--as-of", "{as_of}", "--new-only"], 5400,
     "features for the new snapshot"),
    (["9_score.py", "--date", "{as_of}", "--save", "--top", "10"], 2700,
     "score and publish picks"),
]


def last_trading_day(today: datetime.date | None = None) -> datetime.date:
    """
    The most recent weekday on or before `today`.

    A Saturday-night run must not ask for a Saturday snapshot: every
    price-derived feature would be reaching back to Friday's close anyway, and
    dating the row Saturday makes the snapshot's own date disagree with the data
    in it. Public holidays are not handled - the staleness checks downstream
    catch a date with no trade behind it.
    """
    d = today or datetime.date.today()
    while d.weekday() > 4:          # 5 = Saturday, 6 = Sunday
        d -= datetime.timedelta(days=1)
    return d


def run_step(args, timeout, label, log=print):
    """Run one script. Returns (ok, seconds, tail_of_output)."""
    cmd = [sys.executable] + args
    log(f"  -> {' '.join(args)}   ({label})")
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd, cwd=QUANT_DIR, timeout=timeout,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
    except subprocess.TimeoutExpired:
        return False, time.perf_counter() - t0, f"timed out after {timeout}s"

    secs = time.perf_counter() - t0
    out = (proc.stdout or "") + (proc.stderr or "")
    tail = "\n".join(l for l in out.strip().splitlines() if l.strip())[-1500:]
    if proc.returncode != 0:
        return False, secs, f"exit {proc.returncode}\n{tail[-800:]}"
    return True, secs, tail


def run_pipeline(as_of: datetime.date | None = None, log=print, dry_run=False):
    """
    Run every step in order, stopping at the first failure.

    Stopping matters: the steps are a chain, and running 9_score.py after
    6_feature_compute.py failed would publish a ranking built on last week's
    features while dating it today. A partial run that stops is recoverable; one
    that carries on quietly is not.

    Returns (ok, summary_lines).
    """
    as_of = as_of or last_trading_day()
    lines = [f"Quant pipeline — as-of {as_of}"]
    log(lines[0])

    total = 0.0
    for args, timeout, label in STEPS:
        args = [a.format(as_of=as_of.isoformat()) for a in args]
        if dry_run:
            log(f"  (dry run) {' '.join(args)}")
            continue
        ok, secs, tail = run_step(args, timeout, label, log)
        total += secs
        if not ok:
            lines.append(f"FAILED at {args[0]} after {secs/60:.1f} min")
            lines.append(tail)
            log(lines[-2])
            log(tail)
            return False, lines
        lines.append(f"  {args[0]:<26} {secs/60:>5.1f} min   {label}")
        log(lines[-1])

    if dry_run:
        return True, lines
    lines.append(f"Done in {total/60:.1f} min")
    log(lines[-1])
    return True, lines


if __name__ == "__main__":
    import argparse
    a = argparse.ArgumentParser()
    a.add_argument("--as-of", default=None,
                   help="snapshot date; defaults to the last weekday")
    a.add_argument("--dry-run", action="store_true")
    g = a.parse_args()
    as_of = datetime.date.fromisoformat(g.as_of) if g.as_of else None
    ok, _ = run_pipeline(as_of, dry_run=g.dry_run)
    sys.exit(0 if ok else 1)
