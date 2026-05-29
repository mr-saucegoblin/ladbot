"""
Job scraper and scoring engine.

Fetches from Adzuna Canada API, scores postings against a target
investment/finance profile using Claude, stores in SQLite, and exposes
helpers for Discord delivery.
"""

import os
import re
import time
import datetime
import requests
import sqlite3
import anthropic
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/Toronto")
SEP = "━━━━━━━━━━━━━━━━━━━━"

_DB_PATH = os.environ.get("JOB_DB_PATH", os.path.join(os.path.dirname(__file__), "jobs.db"))

ADZUNA_APP_ID  = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")
ADZUNA_BASE    = "https://api.adzuna.com/v1/api/jobs/ca/search"


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

# 3 broad queries keep us well within Adzuna free tier (250 calls/month)
ADZUNA_QUERIES = [
    "portfolio management",
    "private equity",
    "structured finance",
    "asset management",
    "capital markets",
    "corporate development",
    "investment management",
    "private credit",
    "infrastructure finance",
    "fund management",
    "project finance",
    "credit risk",
    "mergers acquisitions",
    "development finance",
    "fixed income",
]


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
    if val < 500:
        val *= 2080   # hourly → annual
    elif val < 12000:
        val *= 12     # monthly → annual
    return raw, val


def _fetch_adzuna_page(query: str, page: int = 1) -> list[dict]:
    resp = requests.get(
        f"{ADZUNA_BASE}/{page}",
        params={
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "what": query,
            "results_per_page": 50,
            "sort_by": "date",
            "max_days_old": 14,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def fetch_adzuna_jobs() -> list[dict]:
    jobs, seen = [], set()
    for query in ADZUNA_QUERIES:
        try:
            results = _fetch_adzuna_page(query)
            time.sleep(0.5)
            for r in results:
                url = r.get("redirect_url", "")
                if not url or url in seen:
                    continue
                seen.add(url)

                salary_min = r.get("salary_min") or 0
                salary_max = r.get("salary_max") or 0
                comp_value = int((salary_min + salary_max) / 2) if salary_min or salary_max else 0

                # Annualize if hourly (< 500) or monthly (< 12000); discard if still junk
                if 0 < comp_value < 500:
                    comp_value *= 2080
                elif 0 < comp_value < 12000:
                    comp_value *= 12
                if comp_value < 30000:
                    comp_value = 0  # discard implausible values

                comp_text = f"~${comp_value:,}" if comp_value else ""

                desc = r.get("description", "")
                if not comp_value:
                    comp_text, comp_value = _extract_comp(desc)

                location_obj = r.get("location", {})
                location = location_obj.get("display_name", "Canada") if isinstance(location_obj, dict) else "Canada"

                jobs.append({
                    "source": "adzuna",
                    "title": r.get("title", ""),
                    "company": r.get("company", {}).get("display_name", ""),
                    "location": location,
                    "url": url,
                    "description": desc[:4000],
                    "comp_text": comp_text,
                    "comp_value": comp_value,
                })
        except Exception as e:
            print(f"[job_scraper] Adzuna error ({query}): {e}")
    return jobs


def debug_fetch() -> str:
    """Fetch first Adzuna query, report raw results."""
    query = ADZUNA_QUERIES[0]
    try:
        results = _fetch_adzuna_page(query)
        lines = [f"**Adzuna** (`{query}`) — {len(results)} results"]
        for r in results[:5]:
            company = r.get("company", {}).get("display_name", "?")
            lines.append(f"  • {r.get('title', '?')} — {company}")
        return "\n".join(lines)
    except Exception as e:
        return f"**Adzuna** error: {e}"


# ── Candidate profile ────────────────────────────────────────────────────────

CANDIDATE_PROFILE = """
CFA charterholder with ~10 years of finance experience.
Current role: Manager, Finance & Valuations at a major Canadian real estate platform ($5B+ portfolio of private entities and a TSX-listed REIT).
Core skills: DCF modelling, debt MTM/VTB valuation, structured finance, IFRS fair value, cash flow forecasting, covenant monitoring, loan documentation review.
Technical: Python (data science and ML), Bloomberg Terminal, ARGUS Enterprise, advanced Excel.
Languages: English (native), French (conversational), Spanish (B1 — actively developing).
Target roles: Senior finance and investment positions (Director, VP, Senior Manager, or equivalent) at DFIs, pension funds, PE/infrastructure funds, fintechs, and Canadian banks with international mandates.
Not interested in: analyst roles, associate roles (unless senior), pure accounting/audit, HR, marketing, engineering, or software development.
Strong differentiators: CFA designation, debt/structured finance background, real asset valuation expertise, bilingual (EN/FR), developing Spanish.
"""


# ── Filtering ─────────────────────────────────────────────────────────────────

_HARD_EXCLUDE = [
    "intern", "co-op", "coop", "junior", "entry level",
    "receptionist", "administrative assistant",
    "talent acquisition", "marketing specialist", "marketing manager",
    "software engineer", "software developer", "data engineer",
    "bookkeeper", "auditor",
]

_FINANCE_TITLE_KW = [
    "finance", "investment", "portfolio", "capital", "credit", "fund",
    "asset", "banking", "treasury", "equity", "debt", "structured",
    "corporate", "director", "vice president", "vp", "head of",
    "chief", "partner", "principal", "managing", "manager", "analyst",
    "valuation", "lending", "mergers", "acquisition", "private equity",
]


def _is_hard_excluded(title: str) -> bool:
    t = title.lower()
    return any(excl in t for excl in _HARD_EXCLUDE)


def _is_finance_adjacent(job: dict) -> bool:
    """Quick pre-filter: skip anything with no finance signal in title or company."""
    t = (job.get("title") or "").lower()
    c = (job.get("company") or "").lower()
    return any(kw in t or kw in c for kw in _FINANCE_TITLE_KW)


# ── Claude scoring ────────────────────────────────────────────────────────────

def score_job_with_claude(job: dict, client: anthropic.Anthropic) -> tuple[int, str]:
    """Score a job posting 0-100 against the candidate profile using Claude."""
    prompt = (
        f"You are evaluating a job posting for a specific candidate. "
        f"Score the match from 0 to 100 and give a single-line reason.\n\n"
        f"CANDIDATE PROFILE:\n{CANDIDATE_PROFILE}\n\n"
        f"JOB POSTING:\n"
        f"Title: {job.get('title', '')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Location: {job.get('location', '?')}\n"
        f"Description: {(job.get('description') or '')[:1500]}\n\n"
        f"Scoring guide:\n"
        f"75-100: Strong match — seniority, function, and employer type all align well\n"
        f"50-74: Decent match — some alignment but gaps exist\n"
        f"25-49: Weak match — minimal alignment, probably not worth applying\n"
        f"0-24: No match — wrong level, wrong function, or wrong industry\n\n"
        f"Respond in exactly this format (two lines only):\n"
        f"SCORE: [0-100]\n"
        f"REASON: [one sentence]"
    )
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        score_match = re.search(r"SCORE:\s*(\d+)", text)
        reason_match = re.search(r"REASON:\s*(.+)", text)
        score = int(score_match.group(1)) if score_match else 0
        reason = reason_match.group(1).strip() if reason_match else text[:120]
        return min(max(score, 0), 100), reason
    except Exception as e:
        print(f"[job_scraper] Claude scoring error: {e}")
        return 0, "scoring error"


# ── Main scrape ───────────────────────────────────────────────────────────────

def run_scrape(claude_client: anthropic.Anthropic) -> int:
    """Fetch, score with Claude, store. Returns count of new jobs stored (score >= 50)."""
    all_jobs = fetch_adzuna_jobs()
    new_count = 0
    for job in all_jobs:
        if not job["url"]:
            continue
        if _is_hard_excluded(job["title"]):
            continue
        if not _is_finance_adjacent(job):
            continue
        sc, reason = score_job_with_claude(job, claude_client)
        if sc < 50:
            continue
        job["score"] = sc
        job["score_reasons"] = reason
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
        f"🔗 <{job['url']}>\n"
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
    blocks = []
    for job in jobs:
        comp = job.get("comp_text") or "Not listed"
        company = job.get("company") or "Unknown"
        blocks.append(
            f"{SEP}\n"
            f"🏢 **{company}** — {job['title']}\n"
            f"📍 {job.get('location', '?')} | 💰 {comp} | ⭐ Score: {job['score']}/100\n"
            f"🔗 <{job['url']}>\n"
            f"Why: {job.get('score_reasons', '')}"
        )
    blocks.append(SEP)
    return "\n\n".join(lines) + "\n\n" + "\n\n".join(blocks)
