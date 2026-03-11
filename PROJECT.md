# PROJECT.md — Canadian Small Cap Quant Research System

> Paste this file at the start of every Claude Code session to restore full context.

---

## North Star

**Find Canadian small cap stocks positioned to benefit from emerging global trends before the market figures it out.**

This is not a general-purpose stock screener. It is a specific, opinionated system designed to catch a particular type of mispricing: under-researched Canadian small caps where a macro or thematic tailwind is building, but the stock hasn't moved yet.

The system is also a financial media business. Every signal it generates is potential content. The audience (Twitter → newsletter → paid community) is built on the same research the model produces.

---

## Investment Philosophy

Three questions, answered in order:

1. **Is a theme emerging?** (Sentiment model — theme detection via news/filings)
2. **Is this company positioned to benefit?** (Fundamental model — do they have the assets, revenue, or exposure to actually capture the trend?)
3. **Has the market noticed yet?** (Technical model — price/momentum, is it early or already priced in?)

A stock only becomes a candidate when all three align. The goal is not to predict every stock — it's to scan for early, high-conviction setups that the market is still sleeping on.

**Why Canadian small caps specifically:** Lower analyst coverage means mispricings last longer. TSX small caps are structurally under-researched relative to US equivalents. That's the edge.

---

## Stock Universe

- **TSX Composite Index** + **TSX Small Cap Index**
- ~450 stocks total
- **Excludes TSXV** (too speculative, liquidity too thin)
- Ticker format: `SYMBOL.TO` for yFinance, plain `SYMBOL` for FMP

---

## Architecture — How It Works

```
News/Filings (RSS + SEDAR+)
        ↓
  Theme Detection (Claude — existing script)
        ↓
  Sentiment Scores → PostgreSQL
        ↓
Fundamental Data (FMP API / yFinance fallback)
        ↓
  Fundamental Scores → PostgreSQL
        ↓
  Price/Technical Data (FMP / yFinance)
        ↓
  Technical Scores → PostgreSQL
        ↓
  Meta-Model (LightGBM/XGBoost) — combines all three
        ↓
  Ranked Watchlist / Alerts
        ↓
  Discord Bot (alerts + conversational memory)
  Twitter/X Bot (content, Claude-generated copy)
```

---

## Tech Stack

| Layer | Tool | Notes |
|---|---|---|
| Hosting | Railway or Render | Not deployed yet |
| Database | PostgreSQL | Central store for all data + conversation memory |
| Financial data | FMP API | Primary — TSX coverage TBD/needs validation |
| Financial data fallback | yFinance | `.TO` suffix for TSX tickers |
| Filing gaps | SEDAR+ | Manual or scraped |
| News/RSS | Custom RSS scraper | **Already built** |
| Theme detection | Claude API | **Already built** — RSS → Claude → themes |
| Tweet generation | Claude API | **Already built** — themes → tweet copy |
| ML models | LightGBM / XGBoost | One model per signal type + meta-model |
| Brokerage | IBKR (primary), Questrade (backup) | Both accounts exist, ib_insync for IBKR |
| Discord bot | discord.py | Not built yet |
| Twitter/X bot | Not built yet | Deployment/posting not wired up |

**Language:** Python throughout. Keep it simple — working > elegant.

---

## What's Already Built

- [x] RSS news scraper
- [x] Claude integration: news → theme identification
- [x] Claude integration: themes → tweet copy generation
- [ ] Hosting / deployment
- [ ] Twitter/X posting (auto-publish not wired up)
- [ ] PostgreSQL schema and data pipeline
- [ ] FMP coverage validation for TSX tickers
- [ ] Technical/price model
- [ ] Fundamental model
- [ ] Sentiment scoring model (formalized from existing Claude pipeline)
- [ ] Meta-model combining all three
- [ ] Sector/thematic aggregation layer
- [ ] Discord bot

---

## Build Order

Work through these in sequence. Don't skip ahead.

1. **Validate FMP coverage** — pull all ~450 TSX tickers, check which ones FMP actually returns data for. Flag gaps. Decide fallback rules.
2. **PostgreSQL schema** — design tables for: tickers, price history, fundamentals, sentiment scores, themes, model outputs, conversation memory
3. **Data pipeline** — scheduled ingestion of price + fundamental data into Postgres
4. **Technical/price model** — momentum, trend signals. First ML model.
5. **Fundamental model** — filter for companies with real exposure to identified themes
6. **Sentiment model** — formalize existing RSS→Claude pipeline into scored output stored in Postgres
7. **Meta-model** — LightGBM/XGBoost combining all three signal types
8. **Sector/thematic aggregation** — roll up stock-level signals into theme-level views
9. **Discord bot** — alerts + Claude conversational memory backed by Postgres
10. **Twitter/X bot** — wire up existing tweet generation to auto-post

---

## Key Architecture Decisions & Rationale

**Why PostgreSQL over a simpler store?**
Conversation memory for the Discord bot needs relational structure. Price/fundamental data is naturally tabular. One DB for everything keeps ops simple.

**Why LightGBM/XGBoost over deep learning?**
Dataset is ~450 stocks. That's small. Tree-based models handle tabular data well at this scale, are interpretable, and don't need GPUs. Keep it simple.

**Why a meta-model instead of one combined model?**
Each signal type (sentiment, fundamental, technical) has different update frequencies and data formats. Training them separately then combining allows independent iteration. If the sentiment model improves, retrain it without touching the others.

**Why Claude for theme detection instead of a fine-tuned model?**
Speed to market. The existing Claude pipeline already works. Formalize it into scored outputs first, optimize later if cost becomes an issue.

**Why FMP as primary over yFinance?**
FMP has a proper API with rate limits and reliability guarantees. yFinance is scraping under the hood — fine for one-offs, fragile at scale. But FMP's Canadian small cap coverage is unverified — validate before committing.

**Two-stage signal logic:**
Early theme detection (sentiment) catches setups before price moves. Momentum confirmation (technical) provides entry timing. The fundamental layer filters out companies that are thematically adjacent but can't actually benefit. This ordering matters — don't use momentum to find themes, use themes to find momentum opportunities.

---

## Coding Conventions (keep Claude Code consistent)

- **Simple and working first.** No premature abstraction. Get it running, then clean it up.
- **One script per concern.** Don't combine data ingestion with model training with alerting.
- **All secrets in `.env`.** Never hardcode API keys. Use `python-dotenv`.
- **Log everything to stdout** while building. Proper logging later.
- **Ticker format:** Store as `SYMBOL` internally, convert to `SYMBOL.TO` for yFinance calls.
- **Postgres connection:** Use `psycopg2` or `SQLAlchemy`. Keep one shared `db.py` utility.

---

## Business Model Context

The research system and the media business are the same thing. Signals → content → audience → revenue.

Funnel: Free Twitter → Free Newsletter → Paid Substack → Premium Discord community

Brand persona: Anonymous quant, data-driven, Canadian small cap focus, independent (not reflexively contrarian). The Twitter/Discord voice should feel like a sharp, under-the-radar analyst — not a hype account.

---

*Last updated: March 2026*
