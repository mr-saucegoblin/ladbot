"""
cdr_list.py — the reviewed list of depositary receipts, read by everything that
needs to know what is not a Canadian company.

Why a checked-in text file and not a database flag or a live API call:

  * FMP's isAdr flag is False for every CIBC CDR. backfill_ticker_flags.py
    classified 751 tickers and found zero, in a universe that was 22% CDRs.
  * country='US' over-captures badly. Brookfield Wealth Solutions, Coeur Mining
    and Ivanhoe Electric are US-domiciled and genuinely TSX-listed.
  * The screen that generated the shortlist (thin Toronto volume against a
    parent-sized market cap) separates the two groups well but not perfectly, so
    a human decided each borderline case and the decisions are recorded here.

A reviewed file is auditable, diffable and survives an API changing its mind.
When the universe is refreshed, regenerate the shortlist and re-review — new CDRs
launch regularly.
"""

import pathlib

EXCLUSIONS_FILE = pathlib.Path(__file__).parent / "cdr_exclusions.txt"


def load_cdr_exclusions(path: pathlib.Path | None = None) -> set[str]:
    """
    Return the excluded tickers as a set.

    Format is one ticker per line, with everything from '#' onward treated as a
    comment — so a fully commented line (the REVIEWED AND KEPT section) is skipped
    entirely, and a trailing '# Apple Inc. (US, 704B)' is ignored.

    Returns an empty set if the file is missing rather than raising, because a
    fresh clone has no reason to have one yet. Callers that care report the count.
    """
    p = path or EXCLUSIONS_FILE
    if not p.exists():
        return set()
    out = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        ticker = line.split("#", 1)[0].strip()
        if ticker:
            out.add(ticker)
    return out
