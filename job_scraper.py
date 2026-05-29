"""
Job scraper and scoring engine.

Fetches from Indeed Canada RSS and Jobbank.gc.ca RSS, scores postings
against a target investment/finance profile, stores in SQLite, and
exposes helpers for Discord delivery.
"""

import html
import os
import re
import sqlite3
import time
import datetime
import feedparser
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/Toronto")
SEP = "━━━━━━━━━━━━━━━━━━━━"

_DB_PATH = os.environ.get("JOB_DB_PATH", os.path.join(os.path.dirname(__file__), "jobs.db"))


# ── DB ────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                company TEXT,
                title TEXT NOT NULL,
                location TEXT,
                comp_text TEXT,
                comp_value INTEGER DEFAULT 0,
                url TEXT UNIQUE NOT NULL,
                description TEXT,
                score INTEGER DEFAULT 0,
                score_reasons TEXT,
                first_seen TEXT DEFAULT (datetime('now')),
                last_seen TEXT DEFAULT (datetime('now')),
                alert_sent INTEGER DEFAULT 0,
                digest_sent INTEGER DEFAULT 0
            )
        """)


def _upsert_job(job: dict) -> bool:
    """Insert job if URL not seen. Returns True if new."""
    with _conn() as conn:
        row = conn.execute("SELECT id FROM job_postings WHERE url = ?", (job["url"],)).fetchone()
        if row:
            conn.execute("UPDATE job_postings SET last_seen = datetime('now') WHERE url = ?", (job["url"],))
            return False
        conn.execute(
            """
            INSERT INTO job_postings
                (source, company, title, location, comp_text, comp_value,
                 url, description, score, score_reasons)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job["source"], job.get("company", ""), job["title"],
                job.get("location", ""), job.get("comp_text", ""),
                job.get("comp_value", 0), job["url"],
                job.get("description", "")[:4000],
                job.get("score", 0), job.get("score_reasons", ""),
            ),
        )
        return True


def get_unalerted_high_priority() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM job_postings WHERE score >= 75 AND alert_sent = 0 ORDER BY score DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def mark_alerts_sent(ids: list[int]):
    if not ids:
        return
    with _conn() as conn:
        conn.executemany("UPDATE job_postings SET alert_sent = 1 WHERE id = ?", [(i,) for i in ids])


def get_digest_jobs() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM job_postings WHERE score >= 50 AND digest_sent = 0 ORDER BY score DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def mark_digest_sent(ids: list[int]):
    if not ids:
        return
    with _conn() as conn:
        conn.executemany("UPDATE job_postings SET digest_sent = 1 WHERE id = ?", [(i,) for i in ids])


# ── Fetching ──────────────────────────────────────────────────────────────────

INDEED_QUERIES = [
    ("director finance", "Canada"),
    ("vice president finance", "Canada"),
    ("director investment", "Canada"),
    ("VP portfolio management", "Canada"),
    ("director capital markets", "Canada"),
    ("director corporate development", "Canada"),
    ("director structured finance", "Canada"),
    ("VP investment management", "Canada"),
    ("senior manager finance", "Canada"),
    ("director credit", "Canada"),
]

JOBBANK_QUERIES = [
    "director finance",
    "vice president finance",
    "director investment",
]


def _strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _extract_comp(text: str) -> tuple[str, int]:
    """Parse first salary mention. Returns (raw_string, annualized_int)."""
    if not text:
        return "", 0
    m = re.search(r"\$([\d,]+)\s*([kK])?", text)
    if not m:
        return "", 0
    raw = m.group(0)
    val = int(m.group(1).replace(",", ""))
    if m.group(2):
        val *= 1000
    # annualize if looks hourly (< 500) or monthly (< 12000)
    if val < 500:
        val *= 2080
    elif val < 12000:
        val *= 12
    return raw, val


def _parse_indeed_entry(entry: object) -> dict:
    title = entry.get("title", "")
    company = ""
    if " - " in title:
        parts = title.rsplit(" - ", 1)
        title, company = parts[0].strip(), parts[1].strip()
    elif " at " in title.lower():
        idx = title.lower().rfind(" at ")
        company = title[idx + 4 :].strip()
        title = title[:idx].strip()

    summary = _strip_html(entry.get("summary", ""))
    comp_text, comp_value = _extract_comp(f"{title} {summary}")

    location = "Canada"
    for tag in entry.get("tags", []):
        if isinstance(tag, dict) and "location" in tag.get("scheme", ""):
            location = tag.get("term", "Canada")
            break

    return {
        "source": "indeed",
        "title": title,
        "company": company,
        "location": location,
        "url": entry.get("link", ""),
        "description": summary[:4000],
        "comp_text": comp_text,
        "comp_value": comp_value,
    }


def fetch_indeed_jobs() -> list[dict]:
    jobs, seen = [], set()
    for query, location in INDEED_QUERIES:
        url = f"https://ca.indeed.com/rss?q={query.replace(' ', '+')}&l={location}"
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
            time.sleep(1.5)
            for entry in feed.entries:
                link = entry.get("link", "")
                if not link or link in seen:
                    continue
                seen.add(link)
                jobs.append(_parse_indeed_entry(entry))
        except Exception as e:
            print(f"[job_scraper] Indeed error ({query}): {e}")
    return jobs


def fetch_jobbank_jobs() -> list[dict]:
    jobs, seen = [], set()
    for query in JOBBANK_QUERIES:
        url = f"https://www.jobbank.gc.ca/jobsearch/rss?searchstring={query.replace(' ', '+')}&locationid=9219"
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
            time.sleep(1)
            for entry in feed.entries:
                link = entry.get("link", "")
                if not link or link in seen:
                    continue
                seen.add(link)
                summary = _strip_html(entry.get("summary", ""))
                comp_text, comp_value = _extract_comp(summary)
                src = entry.get("source") or {}
                company = src.get("title", "") if isinstance(src, dict) else ""
                jobs.append({
                    "source": "jobbank",
                    "title": entry.get("title", ""),
                    "company": company,
                    "location": "Canada",
                    "url": link,
                    "description": summary[:4000],
                    "comp_text": comp_text,
                    "comp_value": comp_value,
                })
        except Exception as e:
            print(f"[job_scraper] Jobbank error ({query}): {e}")
    return jobs


# ── Scoring ───────────────────────────────────────────────────────────────────

_HARD_EXCLUDE = [
    "property manager", "leasing agent", "appraisal technician",
    "intern", "co-op", "coop", "junior", "entry level",
    "coordinator", "receptionist", "administrative assistant",
]
_TITLE_OVERRIDE = ["senior analyst", "principal analyst", "research analyst"]

_SENIOR = [
    "director", "vice president", "vp ", "vp,", "vp-", "managing director",
    "head of", "principal", "partner", "managing partner", "chief",
]
_MID = [" manager ", " manager,", "lead "]

_FUNCTION: dict[str, int] = {
    "investment": 7, "portfolio": 7, "asset management": 7, "fund management": 7,
    "structured finance": 7, "project finance": 7, "infrastructure finance": 7,
    "corporate development": 7, "m&a": 7, "merger": 5, "acquisition": 5,
    "capital markets": 7, "credit": 6, "lending": 5, "debt finance": 6,
    "private equity": 7, "fixed income": 5, "fp&a": 5, "treasury": 4,
}

_LATAM = [
    "latin america", "latam", "emerging markets", "cross-border",
    "brazil", "mexico", "colombia", "peru", "chile", "argentina",
    "international markets", "global markets",
]

_PRIORITY_EMP = [
    "cdpq", "cpp investments", "omers", "teachers'", "ontario teachers",
    "psp investments", "aimco", "bci group", "bci invest",
    "export development canada", "edc ", "findev", "bdc ",
    "cmhc", "brookfield", "northleaf", "harbourvest", "actis",
    "wealthsimple", "koho", "clearco", "ifc ", "idb invest",
]
_SECONDARY_EMP = [
    "scotiabank", "bmo ", "hsbc", "itau", "atkins", "bombardier",
    "gildan", "kinross", "agnico", "lundin", "first quantum",
    "pension", "private equity fund", "infrastructure fund",
]

_SOFT: dict[str, int] = {
    "spanish": 3, "bilingual": 2, "cfa": 3, "chartered financial analyst": 3,
    "remote-first": 2, "work from anywhere": 2, "distributed team": 2,
}


def _is_hard_excluded(title: str) -> bool:
    t = title.lower()
    if any(ov in t for ov in _TITLE_OVERRIDE):
        return False
    return any(excl in t for excl in _HARD_EXCLUDE)


def score_job(job: dict) -> tuple[int, str]:
    title = (job.get("title") or "").lower()
    desc = (job.get("description") or "").lower()
    company = (job.get("company") or "").lower()
    full = f"{title} {desc} {company}"
    reasons, s = [], 0

    # Seniority (25)
    if any(kw in full for kw in _SENIOR):
        s += 25
        reasons.append("senior title")
    elif any(kw in f" {title} " for kw in _MID):
        s += 12

    # Function (20)
    fn_pts, fn_hits = 0, []
    for kw, pts in _FUNCTION.items():
        if kw in full:
            fn_pts += pts
            fn_hits.append(kw)
    s += min(fn_pts, 20)
    if fn_hits:
        reasons.append(f"function: {', '.join(fn_hits[:3])}")

    # LatAm (20)
    latam_hits = [k for k in _LATAM if k in full]
    if latam_hits:
        s += 20
        reasons.append(f"LatAm: {', '.join(latam_hits[:2])}")

    # Employer (15)
    if any(e in company or e in full for e in _PRIORITY_EMP):
        s += 15
        reasons.append("priority employer")
    elif any(e in company or e in full for e in _SECONDARY_EMP):
        s += 8
        reasons.append("relevant employer")

    # Remote (10)
    if any(k in full for k in ["remote", "flexible location", "work from anywhere", "hybrid", "distributed"]):
        s += 10
        reasons.append("remote/flexible")

    # Comp (10)
    comp_val = job.get("comp_value") or 0
    if comp_val >= 160000:
        s += 10
        reasons.append(f"comp ~${comp_val:,}")
    elif comp_val >= 130000:
        s += 5
        reasons.append(f"comp ~${comp_val:,}")

    # Soft boosts
    for kw, pts in _SOFT.items():
        if kw in full:
            s += pts
            reasons.append(kw)

    return min(s, 100), " | ".join(reasons)


# ── Main scrape ───────────────────────────────────────────────────────────────

def run_scrape() -> int:
    """Fetch all sources, score, store. Returns count of new jobs stored (score >= 50)."""
    all_jobs = fetch_indeed_jobs() + fetch_jobbank_jobs()
    new_count = 0
    for job in all_jobs:
        if not job["url"] or _is_hard_excluded(job["title"]):
            continue
        sc, reasons = score_job(job)
        if sc < 50:
            continue
        job["score"] = sc
        job["score_reasons"] = reasons
        if _upsert_job(job):
            new_count += 1
    return new_count


# ── Discord formatting ────────────────────────────────────────────────────────

def format_alert(job: dict) -> str:
    comp = job.get("comp_text") or "Not listed"
    company = job.get("company") or "Unknown"
    return (
        f"🚨 **High Priority Match**\n"
        f"🏢 **{company}** — {job['title']}\n"
        f"📍 {job.get('location', '?')} | 💰 {comp} | ⭐ Score: {job['score']}/100\n"
        f"🔗 {job['url']}\n"
        f"Why: {job.get('score_reasons', '')}"
    )


def format_digest(jobs: list[dict]) -> str:
    if not jobs:
        return ""
    today = datetime.datetime.now(ET).strftime("%B %d, %Y")
    lines = [
        f"📋 **Job Digest — {today}**",
        f"Found {len(jobs)} match{'es' if len(jobs) != 1 else ''}\n",
    ]
    for job in jobs:
        comp = job.get("comp_text") or "Not listed"
        company = job.get("company") or "Unknown"
        lines += [
            SEP,
            f"🏢 **{company}** — {job['title']}",
            f"📍 {job.get('location', '?')} | 💰 {comp} | ⭐ Score: {job['score']}/100",
            f"🔗 {job['url']}",
            f"Why: {job.get('score_reasons', '')}",
        ]
    lines.append(SEP)
    return "\n".join(lines)
