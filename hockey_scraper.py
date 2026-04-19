"""
hockey_scraper.py
NHL playoff fantasy stats for the lads' 2026 playoff league.

Scoring: 1pt per skater point (G+A), 1pt per goalie team win, 1pt per goalie team shutout.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SEASON = 20252026
PLAYOFFS = 3
SHEET_ID = "1PWah19k2BwMoahzlcap8pwUwMXsTDUbXndqiDjMyzC4"
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")
PLAYER_ID_CACHE = os.getenv("HOCKEY_PLAYER_ID_CACHE", "hockey_player_ids.json")
SNAPSHOT_FILE = os.getenv("HOCKEY_SNAPSHOT_FILE", "hockey_snapshot.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
ET = ZoneInfo("America/Toronto")
NHL_BASE = "https://api-web.nhle.com/v1"

# All 32 NHL teams — used to build player ID map from rosters
NHL_TEAMS = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH",
    "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SEA", "SJS",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG",
]

# Known name differences between our roster and the NHL API
PLAYER_NAME_CORRECTIONS = {
    "Tim Stutzle": "Tim Stützle",
}

# ── Rosters ───────────────────────────────────────────────────────────────────

ROSTERS = {
    "Los Chupacabruhs": {
        "gm": "Gino",
        "skaters": [
            "Brandon Hagel", "Jason Robertson", "Wyatt Johnston",
            "Valeri Nichushkin", "Bryan Rust", "Ridly Greig",
            "Cale Makar", "Thomas Harley", "Josh Manson", "Jordan Spence",
        ],
        "goalie_team": "PIT",
    },
    "Molson Experts": {
        "gm": "Bullen",
        "skaters": [
            "Mikko Rantanen", "Sebastian Aho", "Andrei Svechnikov",
            "Clayton Keller", "Josh Norris", "Jason Zucker",
            "Rasmus Dahlin", "Shayne Gostisbehere", "Mikhail Sergachev", "MacKenzie Weegar",
        ],
        "goalie_team": "DAL",
    },
    "The Rye Guys": {
        "gm": "Jer",
        "skaters": [
            "Tage Thompson", "Brayden Point", "Cutter Gauthier",
            "Leo Carlsson", "Troy Terry", "Matt Duchene",
            "Quinn Hughes", "Miro Heiskanen", "Jackson LaCombe", "Brandt Clarke",
        ],
        "goalie_team": "BUF",
    },
    "Flaccid Lizurds": {
        "gm": "Lizard",
        "skaters": [
            "Martin Necas", "Seth Jarvis", "Mark Stone",
            "Alex Tuch", "Matt Boldy", "Josh Doan",
            "Alexander Nikishin", "Bowen Byram", "Mattias Samuelsson", "Charle-Edouard D'Astous",
        ],
        "goalie_team": "CAR",
    },
    "Mid Ice Crisis": {
        "gm": "Paul",
        "skaters": [
            "Jack Eichel", "Sidney Crosby", "Dylan Guenther",
            "Anthony Mantha", "Adrian Kempe", "Porter Martone",
            "Jake Sanderson", "Sam Malinski", "Charlie McAvoy", "John Carlson",
        ],
        "goalie_team": "COL",
    },
    "REBORN": {
        "gm": "Elliott",
        "skaters": [
            "Nikita Kucherov", "Mitch Marner", "Brady Tkachuk",
            "Drake Batherson", "Pavel Dorofeyev", "Dylan Cozens",
            "Darren Raddysh", "Artem Zub", "Owen Power", "Jared Spurgeon",
        ],
        "goalie_team": "OTT",
    },
    "Eupi's Revenge": {
        "gm": "Horse",
        "skaters": [
            "Nathan MacKinnon", "Artturi Lehkonen", "Evgeni Malkin",
            "Logan Stankoven", "Nikolaj Ehlers", "Egor Chinakhov",
            "Evan Bouchard", "Sean Walker", "Kris Letang", "K'Andre Miller",
        ],
        "goalie_team": "TBL",
    },
    "Der Meister": {
        "gm": "Kenn",
        "skaters": [
            "Connor McDavid", "Juraj Slafkovsky", "David Pastrnak",
            "Ivan Demidov", "Artemi Panarin", "Rickard Rakell",
            "Lane Hutson", "Mike Matheson", "Darnell Nurse", "Noah Dobson",
        ],
        "goalie_team": "MIN",
    },
    "The Labatt Blues": {
        "gm": "Jon",
        "skaters": [
            "Jake Guentzel", "Brock Nelson", "Anthony Cirelli",
            "Gabriel Landeskog", "Tomas Hertl", "Corey Perry",
            "Shea Theodore", "Rasmus Andersson", "J.J. Moser", "Ryan McDonagh",
        ],
        "goalie_team": "EDM",
    },
    "Jimmy Stus Hot Picks": {
        "gm": "Trav",
        "skaters": [
            "Leon Draisaitl", "Nazem Kadri", "Zach Hyman",
            "Tim Stutzle", "Shane Pinto", "Vasily Podkolzin",
            "Thomas Chabot", "Brock Faber", "Noah Hanifin", "Logan Stanley",
        ],
        "goalie_team": "VGK",
    },
    "Matt's Pesky Puck Hawgz": {
        "gm": "Steckly",
        "skaters": [
            "Nick Suzuki", "Cole Caufield", "Kirill Kaprizov",
            "Ryan Nugent-Hopkins", "Jackson Blake", "Oliver Kapanen",
            "Erik Karlsson", "Brent Burns", "Devon Toews", "Alexandre Carrier",
        ],
        "goalie_team": "MTL",
    },
}

# Precomputed lookups
ALL_SKATERS = {p for r in ROSTERS.values() for p in r["skaters"]}
PLAYER_TO_TEAM = {p: name for name, r in ROSTERS.items() for p in r["skaters"]}
GOALIE_TO_FANTASY = {r["goalie_team"]: name for name, r in ROSTERS.items()}

# Teams that appear in the schedule tab (all 16 pool teams' NHL clubs)
POOL_NHL_TEAMS = {
    "MTL", "EDM", "BOS", "LAK", "PIT", "MIN",
    "CAR", "COL", "TBL", "BUF", "VGK", "OTT",
    "DAL", "ANA", "UTA", "PHI",
}
SCHEDULE_TAB = "Schedule"  # Google Sheet tab name — update if different


# ── NHL API helpers ───────────────────────────────────────────────────────────

def _get(url, params=None, retries=4):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10))
                logger.warning(f"Rate limited, sleeping {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == retries - 1:
                logger.warning(f"GET {url} failed: {e}")
            else:
                time.sleep(2)
    return None


import unicodedata

def _normalize(name):
    """Lowercase, strip accents and punctuation for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Replace both straight and curly apostrophes/quotes
    return (ascii_name.lower()
            .replace(".", "").replace("'", "").replace("\u2019", "").replace("\u2018", "")
            .replace("-", " ").strip())


def build_master_player_map():
    """Fetch all 32 NHL team rosters and return a name→id map."""
    id_map = {}
    for team in NHL_TEAMS:
        roster = _get(f"{NHL_BASE}/roster/{team}/{SEASON}")
        if not roster:
            logger.warning(f"Could not get roster for {team}")
            continue
        for category in ("forwards", "defensemen", "goalies"):
            for player in roster.get(category, []):
                first = player.get("firstName", {}).get("default", "")
                last = player.get("lastName", {}).get("default", "")
                name = f"{first} {last}".strip()
                pid = player.get("id")
                if name and pid:
                    id_map[name] = pid
        time.sleep(0.15)
    logger.info(f"Built master player map: {len(id_map)} players")
    return id_map


def get_player_ids():
    """Return name→id map for all skaters. Builds from rosters on first run."""
    cache = {}
    if os.path.exists(PLAYER_ID_CACHE):
        with open(PLAYER_ID_CACHE) as f:
            cache = json.load(f)

    missing = [p for p in ALL_SKATERS if p not in cache]
    if not missing:
        return cache

    logger.info(f"Building player ID map from NHL rosters...")
    master = build_master_player_map()
    normalized_master = {_normalize(k): v for k, v in master.items()}

    for player in missing:
        # Apply known corrections first
        lookup_name = PLAYER_NAME_CORRECTIONS.get(player, player)
        if lookup_name in master:
            cache[player] = master[lookup_name]
        else:
            # Fuzzy match via normalized name
            match = normalized_master.get(_normalize(lookup_name))
            if match:
                cache[player] = match
            else:
                logger.warning(f"Player not found in any NHL roster: {player}")

    with open(PLAYER_ID_CACHE, "w") as f:
        json.dump(cache, f, indent=2)
    return cache


def get_skater_playoff_stats(player_id):
    data = _get(f"{NHL_BASE}/player/{player_id}/landing")
    if not data:
        return {"pts": 0, "gp": 0, "team": ""}
    for s in data.get("seasonTotals", []):
        if s.get("season") == SEASON and s.get("gameTypeId") == PLAYOFFS:
            return {
                "pts": s.get("points", 0),
                "gp": s.get("gamesPlayed", 0),
                "team": data.get("currentTeamAbbrev", ""),
            }
    return {"pts": 0, "gp": 0, "team": data.get("currentTeamAbbrev", "")}


def get_goalie_playoff_stats(player_id):
    data = _get(f"{NHL_BASE}/player/{player_id}/landing")
    if not data:
        return {"wins": 0, "shutouts": 0, "gp": 0}
    for s in data.get("seasonTotals", []):
        if s.get("season") == SEASON and s.get("gameTypeId") == PLAYOFFS:
            return {
                "wins": s.get("wins", 0),
                "shutouts": s.get("shutouts", 0),
                "gp": s.get("gamesPlayed", 0),
            }
    return {"wins": 0, "shutouts": 0, "gp": 0}


def get_team_goalie_stats(team_abbrev):
    """Aggregate wins + shutouts across all goalies on a team."""
    roster = _get(f"{NHL_BASE}/roster/{team_abbrev}/{SEASON}")
    if not roster:
        return {"pts": 0, "gp": 0}
    wins = shutouts = 0
    max_gp = 0
    for g in roster.get("goalies", []):
        s = get_goalie_playoff_stats(g["id"])
        wins += s["wins"]
        shutouts += s["shutouts"]
        max_gp = max(max_gp, s["gp"])
        time.sleep(0.5)
    return {"pts": wins + shutouts, "gp": max_gp, "wins": wins, "shutouts": shutouts}


# ── Stats aggregation ─────────────────────────────────────────────────────────

def fetch_all_stats(player_ids):
    """Returns {players: {name: stats}, goalies: {name: stats}, fantasy: {team: pts}}"""
    players = {}
    goalies = {}
    fantasy = {name: 0 for name in ROSTERS}

    for team_name, roster in ROSTERS.items():
        for player in roster["skaters"]:
            pid = player_ids.get(player)
            if not pid:
                players[player] = {"pts": 0, "gp": 0, "team": "???"}
                continue
            s = get_skater_playoff_stats(pid)
            players[player] = s
            fantasy[team_name] += s["pts"]
            time.sleep(0.5)

        gt = roster["goalie_team"]
        gs = get_team_goalie_stats(gt)
        goalies[f"{gt} Goalies"] = {**gs, "team": gt}
        fantasy[team_name] += gs["pts"]

    return {"players": players, "goalies": goalies, "fantasy": fantasy}


# ── Schedule tab ──────────────────────────────────────────────────────────────

def fetch_playoff_schedule():
    """Fetch all playoff game dates for pool teams, April 19 – June 30.
    Uses /schedule/{date} which returns a full week — ~11 API calls total."""
    from datetime import date as _date, timedelta
    schedule = {}  # date_str -> set of team abbrevs playing
    current = _date(2026, 4, 19)
    end = _date(2026, 6, 30)
    while current <= end:
        week_key = current.strftime("%Y-%m-%d")
        data = _get(f"{NHL_BASE}/schedule/{week_key}")
        time.sleep(1)
        if data:
            for day in data.get("gameWeek", []):
                day_date = day.get("date")
                if not day_date:
                    continue
                for game in day.get("games", []):
                    if game.get("gameType") != PLAYOFFS:
                        continue
                    away = game.get("awayTeam", {}).get("abbrev", "")
                    home = game.get("homeTeam", {}).get("abbrev", "")
                    if day_date not in schedule:
                        schedule[day_date] = set()
                    if away in POOL_NHL_TEAMS:
                        schedule[day_date].add(away)
                    if home in POOL_NHL_TEAMS:
                        schedule[day_date].add(home)
        current += timedelta(days=7)
    return schedule


def update_schedule_tab():
    """Update the Schedule sheet tab with YES/blank for each team's game days."""
    import gspread.utils
    schedule = fetch_playoff_schedule()
    if not schedule:
        logger.warning("[schedule_tab] No schedule data fetched")
        return

    gc = _get_gspread_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(SCHEDULE_TAB)
    all_values = ws.get_all_values()
    if len(all_values) < 3:
        logger.warning("[schedule_tab] Sheet looks empty")
        return

    date_row = all_values[1]  # sheet row 2
    team_col = [row[0] if row else "" for row in all_values]

    date_to_col = {cell: ci for ci, cell in enumerate(date_row) if cell}
    team_to_row = {
        cell.upper(): ri for ri, cell in enumerate(team_col)
        if cell and cell.upper() in POOL_NHL_TEAMS
    }

    updates = []
    for date_str, col_idx in date_to_col.items():
        playing = schedule.get(date_str, set())
        for team, row_idx in team_to_row.items():
            val = "YES" if team in playing else ""
            current = all_values[row_idx][col_idx] if col_idx < len(all_values[row_idx]) else ""
            if val != current:
                cell = gspread.utils.rowcol_to_a1(row_idx + 1, col_idx + 1)
                updates.append({"range": cell, "values": [[val]]})

    if updates:
        ws.batch_update(updates)
        logger.info(f"[schedule_tab] Updated {len(updates)} cells")
    else:
        logger.info("[schedule_tab] Already up to date")


# ── Google Sheets ─────────────────────────────────────────────────────────────

def _get_gspread_client():
    """Build gspread client from env JSON (Railway) or local file (dev)."""
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    if creds_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(creds_json)
            tmp = f.name
        creds = Credentials.from_service_account_file(tmp, scopes=SCOPES)
        os.unlink(tmp)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def update_sheet(stats):
    gc = _get_gspread_client()
    ws = gc.open_by_key(SHEET_ID).worksheet("AllData")

    rows = [["Player", "Tm", "PTS", "GP"]]
    entries = [(n, s["team"], s["pts"], s["gp"]) for n, s in stats["players"].items()]
    entries += [(n, s["team"], s["pts"], s["gp"]) for n, s in stats["goalies"].items()]
    entries.sort(key=lambda x: -x[2])
    rows.extend(entries)

    ws.clear()
    ws.update(rows, "A1")
    logger.info(f"AllData updated: {len(entries)} rows")


# ── Snapshot (for daily delta) ────────────────────────────────────────────────

def save_snapshot(stats):
    snap = {
        "ts": datetime.now(ET).isoformat(),
        "players": {k: v["pts"] for k, v in stats["players"].items()},
        "goalies": {k: v["pts"] for k, v in stats["goalies"].items()},
        "fantasy": stats["fantasy"],
    }
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(snap, f)


def load_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE) as f:
            return json.load(f)
    return None


def compute_delta(old_snap, new_stats):
    if not old_snap:
        return {"players": {}, "fantasy": {}}

    all_old = {**old_snap.get("players", {}), **old_snap.get("goalies", {})}
    all_new = {
        **{k: v["pts"] for k, v in new_stats["players"].items()},
        **{k: v["pts"] for k, v in new_stats["goalies"].items()},
    }

    player_delta = {n: all_new[n] - all_old.get(n, 0) for n in all_new
                    if all_new[n] - all_old.get(n, 0) > 0}
    fantasy_delta = {t: new_stats["fantasy"][t] - old_snap.get("fantasy", {}).get(t, 0)
                     for t in new_stats["fantasy"]
                     if new_stats["fantasy"][t] - old_snap.get("fantasy", {}).get(t, 0) > 0}

    return {"players": player_delta, "fantasy": fantasy_delta}


# ── Game summaries ────────────────────────────────────────────────────────────

def get_game_summaries(date_str):
    data = _get(f"{NHL_BASE}/score/{date_str}")
    if not data:
        return []
    games = []
    for g in data.get("games", []):
        if g.get("gameType") != PLAYOFFS:
            continue
        away = g.get("awayTeam", {})
        home = g.get("homeTeam", {})
        games.append({
            "away": away.get("abbrev", ""), "away_score": away.get("score", 0),
            "home": home.get("abbrev", ""), "home_score": home.get("score", 0),
        })
    return games


# ── Public API ────────────────────────────────────────────────────────────────

def update_sheet_only():
    """Fetch current stats and update sheet. Used for live updates during games."""
    player_ids = get_player_ids()
    stats = fetch_all_stats(player_ids)
    update_sheet(stats)
    return stats


def morning_recap():
    """Fetch stats, diff against yesterday's snapshot, update sheet, save new snapshot."""
    player_ids = get_player_ids()
    old_snap = load_snapshot()
    stats = fetch_all_stats(player_ids)
    update_sheet(stats)
    delta = compute_delta(old_snap, stats)
    save_snapshot(stats)
    return stats, delta, old_snap


def get_recap_data(stats, delta, old_snap):
    """Return structured recap data for Claude to narrate."""
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    games = get_game_summaries(yesterday)

    # Top scorers yesterday
    top_scorers = []
    for player_name, pts in sorted(delta["players"].items(), key=lambda x: -x[1]):
        fteam = PLAYER_TO_TEAM.get(player_name)
        if not fteam:
            abbrev = player_name.split()[0]
            fteam = GOALIE_TO_FANTASY.get(abbrev)
        if fteam:
            top_scorers.append({
                "player": player_name,
                "pts": pts,
                "fantasy_team": fteam,
                "gm": ROSTERS[fteam]["gm"],
            })

    # Standings + rank changes
    old_fantasy = old_snap.get("fantasy", {}) if old_snap else {}
    old_ranked = {team: i + 1 for i, (team, _) in enumerate(sorted(old_fantasy.items(), key=lambda x: -x[1]))}
    new_ranked = sorted(stats["fantasy"].items(), key=lambda x: -x[1])

    standings = []
    for i, (team, pts) in enumerate(new_ranked):
        prev_rank = old_ranked.get(team, i + 1)
        standings.append({
            "rank": i + 1,
            "prev_rank": prev_rank,
            "rank_change": prev_rank - (i + 1),  # positive = moved up
            "team": team,
            "gm": ROSTERS[team]["gm"],
            "pts": pts,
            "pts_gained": pts - old_fantasy.get(team, 0),
        })

    return {"games": games, "top_scorers": top_scorers, "standings": standings}


def build_discord_recap(stats, delta):
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    games = get_game_summaries(yesterday)

    lines = ["**🏒 PLAYOFF FANTASY HOCKEY — LAST NIGHT**", ""]

    if games:
        lines.append("**Last Night's Games:**")
        for g in games:
            lines.append(f"> {g['away']} **{g['away_score']}** @ {g['home']} **{g['home_score']}**")
        lines.append("")
    else:
        lines.append("*No playoff games last night.*\n")

    if delta["players"]:
        lines.append("**Yesterday's Fantasy Scoring:**")
        by_team = {n: [] for n in ROSTERS}
        for player_name, pts in sorted(delta["players"].items(), key=lambda x: -x[1]):
            fteam = PLAYER_TO_TEAM.get(player_name)
            if not fteam:
                abbrev = player_name.split()[0]
                fteam = GOALIE_TO_FANTASY.get(abbrev)
            if fteam:
                by_team[fteam].append(f"{player_name} +{pts}")

        for fteam, yday_pts in sorted(delta["fantasy"].items(), key=lambda x: -x[1]):
            gm = ROSTERS[fteam]["gm"]
            scorers = ", ".join(by_team[fteam]) or "—"
            lines.append(f"> **{fteam}** ({gm}) +{yday_pts} — {scorers}")
        lines.append("")

    lines.append("**Current Standings:**")
    for i, (fteam, pts) in enumerate(sorted(stats["fantasy"].items(), key=lambda x: -x[1]), 1):
        gm = ROSTERS[fteam]["gm"]
        lines.append(f"> {i}. **{fteam}** ({gm}) — {pts} pts")

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    stats, delta = morning_recap()
    print(build_discord_recap(stats, delta))
