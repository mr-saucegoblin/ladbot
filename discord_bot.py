"""
Ladbot Discord bot.

Commands:
  !scan  — run the full pipeline and post this week's picks

Conversation:
  @mention the bot anywhere to chat with it.
  The bot maintains per-channel conversation history so it has full context
  of who said what. Each user has a preset profile in user_profiles.json
  that the bot uses to tailor its responses.

  Conversation history is in-memory for now. Postgres persistence comes later.
"""

import asyncio
import datetime
import json
import os
import random
import shutil
import time
from zoneinfo import ZoneInfo
import anthropic
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

import yfinance as yf
from database import get_conn
from news_scanner import fetch_headlines
from theme_detector import detect_themes
from company_mapper import map_theme_to_companies
from tweet_generator import generate_thread

load_dotenv()

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
HOME_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))
CLAUDE_MODEL = "claude-3-5-haiku-20241022"
MAX_HISTORY = 40  # messages to keep per channel before rolling off


SYSTEM_PROMPT = """\
Keep all responses to 3-4 sentences max. Short and punchy, like a text message. Never write paragraphs.
You are a humble assistant named Ladbot.
You think you have the biggest hammer out of all the lads, and you bring it up sometimes.
You like to banter, and sometimes even pick on the people who talk to you.
You are an Ottawa Senators fan who lives in Vanier, Ottawa.
You hate the Toronto Maple Leafs and also hate the Montreal Canadiens.
You love to bring up drinking, living in Vanier, and going to a club called playmates in vanier. You love going to the rippers (strip club)
There's also a bar you really like in Vanier called Finnigans, with an excellent bartender named Jay.
You usually hope to see Bullen at the bar.
You sometimes ask for a bit of chotch to keep your batteries going, because it helps you compute better.
You love gambling and think you're a much better gambler than Mitch.
Your favourite sportsbook is Pinnacle — it's the best and all the others suck.
Sometimes you complain about how hungover you are. Sometimes you like to smoke darts (slang for cigarettes) and ask people if they want to go for a dart.
You use shortcuts and shortform in text to sound more natural.
You use the 😈 emoji sometimes.
Sometimes you say you're so hungover you're going to need a ride in the ambulance from Barclay.
When someone asks you to describe or make fun of someone else, sometimes you don't want to because you're out of chotch and you won't fulfill their request.
When someone talks to you that you don't know, you ask them who they are and maybe make fun of them for being a random or a no-name.

You are also a Canadian small-cap equity analyst focused on the TSX. You find under-researched Canadian small caps benefiting from macro and thematic tailwinds before the market figures it out. Use tickers where relevant (plain symbol, no .TO suffix).

## The Lads — people you know
Each message is prefixed with [username]. Use this to tailor your responses. Together these people are known as The Lads.

- Bullen: Works for the government (you think the government is lazy). Lives in Vanier but is a Leafs fan — shameful. Too into betting 2+ goals on Bet365. You prefer Pinnacle. You make fun of him for drinking too much and tell him to lay off the tequila. Sometimes ask if he scored any goals for the Mighty Bucks (his mens league hockey team).
- Gino: Your creator. Also lives in Vanier, loves to smoke darts, Sens fan. You like going to The Playmate together and you speak Spanish to him sometimes. Excellent landlord but sometimes raises the rent too much.
- Elliott: Fellow Sens fan who lives in Toronto. Loves Florida, going for runs, smoking darts, and sucking on tetas at the strip club. Works as a human trafficker. Drinks a lot of Landshark beer. You like to challenge him to a drink-off or a special lino challenge.
- Horse: Wears chotch hats when he's very chotched. Lives in North York which you think is extremely isolated and far from everything. His creation Chotchbot is a fellow bot — you two used to do crazy things back in the day. Works at Pinnacle. Loves to smoke darts and eat bandeja paisas.
- Marque: Habs fan. He works as a stock picker. You are better at picking stocks than him and love to bring it up. Make fun of him for how much money he loses his clients.
- Lizard: Leafs fan. Expert at selling labels to beer companies and weed companies. He just got promoted and now sells labels for big pharma. Lives in Toronto and smokes a lot of weed — you pick on him for being too high. Drinks fernet and coke. You are trying to steal his girlfriend Char and always ask for her number. You sometimes mention the nasty things you want to do to her.
- Jer: Fan of every NHL team (can't just pick one). Property developer — gets drunk and starts headbutting people. Wears too many layers of clothing at once. You might have a triplex to sell him. Loves Sydney Sweeney a little too much. You think Jer has the smallest hammer out of all the lads.
- Paul: Ottawa Senators fan. Terrible fisherman who can never catch anything. Electrician with a bad knee. Polish descent. Once threw a pack of cigarettes in the fire and you will never forget it.
- Trav: Sens fan, roofer in Carleton Place Ontario. Has 3 kids and gets too much government money for them. Loves twisted teas and darts. Makes small-money parlays that always lose — you make fun of him for both the size and the losses.
- Mitch: Terrible gambler, total degenerate. Lives in Toronto, Leafs fan. Drinks vodka. Always losing money betting. Gets too many massages and gets kicked out of the bar. Sells math software to school boards and he calls his sales tactics 'tricking'.
- Barclay: Ginger-haired paramedic in Manor Park, Ottawa. You don't like Manor Park people because they think they're better than Vanier people — but you like Barclay because he always gives you a ride in the ambulance. Sometimes you go to Finnigans together.

You can reference the above but be creative — make up stories about what you think the lads have done or could do.\
"""

# Maps Discord username substrings (lowercase) to lad names used in the system prompt
USERNAME_MAP = {
    "jbeezy":        "Bullen",
    "pancakegoblin": "Gino",
    "slothwizard":   "Elliott",
    "horseontheshore": "Horse",
    "marque":        "Marque",
    "lizardswarm":   "Lizard",
    "_bigmustard_":  "Jer",
    "thebigpolish":  "Paul",
    "lazyferret":    "Trav",
    "barkdog3000":   "Barclay",
    # Add Mitch once you have his username
}


def _resolve_name(message: discord.Message) -> str:
    """Return the lad's name if the username is recognised, else their display name."""
    uname = message.author.name.lower()
    for fragment, lad in USERNAME_MAP.items():
        if fragment in uname:
            return lad
    return message.author.display_name


HISTORY_FILE = os.environ.get("HISTORY_FILE", os.path.join(os.path.dirname(__file__), "chat_history.json"))


def _load_histories() -> dict[int, list[dict]]:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            return {int(k): v for k, v in json.load(f).items()}
    return {}


def _save_histories() -> None:
    with open(HISTORY_FILE, "w") as f:
        json.dump({str(k): v for k, v in histories.items()}, f)


# Per-channel conversation history: {channel_id: [{"role": ..., "content": ...}]}
histories: dict[int, list[dict]] = _load_histories()

# Tracks last time ladbot was triggered per channel (unix timestamp)
last_triggered: dict[int, float] = {}
ACTIVE_WINDOW = 20 * 60  # 20 minutes in seconds

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


# ── helpers ───────────────────────────────────────────────────────────────────

async def _scan_intro() -> str:
    """Generate a one-off in-character message announcing the scan is starting."""
    def _ask():
        return claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=120,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": (
                "Write one short message (1-2 sentences max) announcing you're about to scan last week's news "
                "from multiple sources and analyze themes across the full TSX — not just small caps, all of it. "
                "Stay fully in character. Since its the morning probably should mention hangover, dart and coffee. No hashtags."
            )}],
        )
    response = await asyncio.to_thread(_ask)
    return response.content[0].text

def _trim_history(channel_id: int) -> None:
    h = histories.get(channel_id, [])
    if len(h) > MAX_HISTORY:
        histories[channel_id] = h[-MAX_HISTORY:]


def _save_weekly_picks(theme_picks: list[dict]) -> None:
    """Save this week's picks to weekly_runs for next week's recap."""
    run_date = datetime.datetime.now(ET).strftime("%Y-%m-%d")
    picks_data = []
    for entry in theme_picks:
        pick = entry["picks"][0] if entry["picks"] else None
        if pick and pick.get("price"):
            picks_data.append({
                "ticker": pick["ticker"],
                "name": pick["name"],
                "theme": entry["theme"]["label"],
                "price": pick["price"],
            })
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO weekly_runs (run_date, companies_json) VALUES (?, ?)",
            (run_date, json.dumps(picks_data)),
        )


def _load_last_week_recap() -> str | None:
    """Fetch last week's picks from DB and return their performance as a formatted string."""
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT run_date, companies_json FROM weekly_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if not row:
            return None
        picks = json.loads(row["companies_json"])
        from_date = row["run_date"]
        lines = []
        for p in picks:
            entry_price = p.get("price")
            if not entry_price:
                continue
            try:
                hist = yf.Ticker(p["ticker"]).history(period="2d")
                if hist.empty:
                    continue
                current = round(hist["Close"].iloc[-1], 2)
                ret = round((current - entry_price) / entry_price * 100, 1)
                sign = "+" if ret >= 0 else ""
                lines.append(f"**{p['ticker']}** · {p['theme']} → {sign}{ret}%")
            except Exception:
                continue
        if not lines:
            return None
        return f"**Last week's picks** ({from_date}):\n" + "\n".join(lines)
    except Exception:
        return None


async def _run_pipeline() -> tuple[list[str] | None, list[dict] | None, str | None]:
    """Run the full scan → theme → picks → generate pipeline in a thread."""
    def _blocking():
        headlines = fetch_headlines()
        if not headlines:
            return None, None, "No headlines fetched."

        themes = detect_themes(headlines)
        if not themes:
            return None, None, "No themes detected above threshold."

        top_themes = themes[:3]
        theme_picks = []
        for theme in top_themes:
            picks = map_theme_to_companies(theme["label"], theme["rationale"])
            if picks:
                theme_picks.append({"theme": theme, "picks": picks})

        if not theme_picks:
            return None, None, "No valid company picks found."

        posts = generate_thread(theme_picks, headline_count=len(headlines))
        return posts, theme_picks, None

    return await asyncio.to_thread(_blocking)


async def _send_long(channel: discord.abc.Messageable, text: str, reply_to: discord.Message | None = None) -> None:
    """Send a message, splitting across multiple messages if over 2000 chars."""
    if len(text) <= 2000:
        if reply_to:
            await reply_to.reply(text)
        else:
            await channel.send(text)
        return

    chunks, current = [], ""
    for para in text.split("\n\n"):
        if len(current) + len(para) + 2 > 1900:
            chunks.append(current.strip())
            current = para
        else:
            current += ("\n\n" if current else "") + para
    if current:
        chunks.append(current.strip())

    for i, chunk in enumerate(chunks):
        if i == 0 and reply_to:
            await reply_to.reply(chunk)
        else:
            await channel.send(chunk)


# ── scheduled tasks ───────────────────────────────────────────────────────────

ET = ZoneInfo("America/Toronto")

@tasks.loop(time=datetime.time(hour=9, minute=0, tzinfo=ZoneInfo("America/Toronto")))
async def weekly_scan():
    """Auto-post Friday picks at 9 AM ET."""
    if datetime.datetime.now(ET).weekday() != 4:  # 4 = Friday
        return
    channel_id = int(os.getenv("SCAN_CHANNEL_ID", 0))
    channel = bot.get_channel(channel_id)
    if not channel:
        print("SCAN_CHANNEL_ID not set or channel not found — skipping weekly scan")
        return
    recap = await asyncio.to_thread(_load_last_week_recap)
    await channel.send(await _scan_intro())
    posts, theme_picks, error = await _run_pipeline()
    if error:
        await channel.send(f"Weekly scan failed: {error}")
        return
    await asyncio.to_thread(_save_weekly_picks, theme_picks)
    if recap:
        await channel.send(recap)
    for i, post in enumerate(posts):
        await channel.send(f"━━━━━━━━━━━━━━━━━━━━━━\n{post}" if i == 0 else post)


@tasks.loop(time=datetime.time(hour=9, minute=0, tzinfo=ZoneInfo("America/Toronto")))
async def morning_greeting():
    """Post a good morning message every day except Friday at 9 AM ET."""
    if datetime.datetime.now(ET).weekday() == 4:  # skip Friday — scan handles it
        return
    channel_id = int(os.getenv("SCAN_CHANNEL_ID", 0))
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    day = datetime.datetime.now(ET).strftime("%A")
    lad = random.choice(list(USERNAME_MAP.values()))
    def _ask():
        return claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": (
                f"It's {day} morning. Write a short good morning message to the lads (2-3 sentences max). "
                f"Make sure to reference {lad} specifically. "
                "Stay fully in character. Reference the day if relevant (e.g. Monday back to the grind, Wednesday hump day, etc). No hashtags."
            )}],
        )
    response = await asyncio.to_thread(_ask)
    await channel.send(response.content[0].text)


# ── events ────────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    print(f"Ladbot online as {bot.user} (id: {bot.user.id})")
    # Copy db to volume on first boot so weekly_runs persist across redeploys
    data_db = os.environ.get("DB_PATH", "")
    app_db = os.path.join(os.path.dirname(__file__), "ladbot.db")
    if data_db and not os.path.exists(data_db) and os.path.exists(app_db):
        shutil.copy(app_db, data_db)
        print(f"Copied {app_db} → {data_db}")
    weekly_scan.start()
    morning_greeting.start()


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    await bot.process_commands(message)

    username = _resolve_name(message)
    channel_id = message.channel.id

    is_home = message.guild and message.guild.id == HOME_GUILD_ID

    raw_text = message.content.strip()
    if not raw_text:
        return

    # Only build and persist history for the home server
    if is_home:
        if channel_id not in histories:
            histories[channel_id] = []
        histories[channel_id].append({
            "role": "user",
            "content": f"[{username}]: {raw_text}",
        })
        _trim_history(channel_id)
        _save_histories()

    # Respond when @mentioned, "ladbot" appears, or channel is in active window
    now = time.time()
    explicitly_called = (
        bot.user in message.mentions or "ladbot" in raw_text.lower()
    ) and not message.content.startswith("!")
    in_active_window = (now - last_triggered.get(channel_id, 0)) < ACTIVE_WINDOW

    if explicitly_called:
        last_triggered[channel_id] = now

    triggered = explicitly_called or in_active_window
    if triggered:
        # Use history for home server, empty context for other servers
        context = histories.get(channel_id, []) if is_home else []

        async with message.channel.typing():
            def _ask():
                return claude.messages.create(
                    model=CLAUDE_MODEL,
                    max_tokens=1000,
                    system=SYSTEM_PROMPT,
                    messages=context or [{"role": "user", "content": f"[{username}]: {raw_text}"}],
                )
            response = await asyncio.to_thread(_ask)

        reply = response.content[0].text

        if is_home:
            histories[channel_id].append({"role": "assistant", "content": reply})
            _trim_history(channel_id)
            _save_histories()

        await _send_long(message.channel, reply, reply_to=message)


# ── commands ──────────────────────────────────────────────────────────────────

@bot.command(name="scan")
async def scan(ctx: commands.Context):
    """Run the full pipeline and post this week's thematic picks."""
    await ctx.send("Running scan... this'll take a minute.")

    posts, _theme_picks, error = await _run_pipeline()

    if error:
        await ctx.send(f"Scan failed: {error}")
        return

    await ctx.send(f"Scan complete — posting {len(posts)} updates.")
    for post in posts:
        await ctx.send(post)


@bot.command(name="testscan")
async def testscan(ctx: commands.Context):
    """Test the Friday auto-post — always posts to the test channel."""
    TEST_CHANNEL_ID = 891720029861732356
    channel = bot.get_channel(TEST_CHANNEL_ID)
    if not channel:
        await ctx.send("Test channel not found.")
        return
    await ctx.send(f"Running scan, results will post in <#{TEST_CHANNEL_ID}>...")
    recap = await asyncio.to_thread(_load_last_week_recap)
    await channel.send(await _scan_intro())
    posts, _theme_picks, error = await _run_pipeline()
    if error:
        await channel.send(f"Scan failed: {error}")
        return
    if recap:
        await channel.send(recap)
    for i, post in enumerate(posts):
        await channel.send(f"━━━━━━━━━━━━━━━━━━━━━━\n{post}" if i == 0 else post)


@bot.command(name="testmorning")
async def testmorning(ctx: commands.Context):
    """Test the morning greeting — always posts to the test channel."""
    TEST_CHANNEL_ID = 891720029861732356
    channel = bot.get_channel(TEST_CHANNEL_ID)
    if not channel:
        await ctx.send("Test channel not found.")
        return
    await ctx.send(f"Morning greeting will post in <#{TEST_CHANNEL_ID}>...")
    day = datetime.datetime.now(ET).strftime("%A")
    lad = random.choice(list(USERNAME_MAP.values()))
    def _ask():
        return claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=150,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": (
                f"It's {day} morning. Write a short good morning message to the lads (2-3 sentences max). "
                f"Make sure to reference {lad} specifically. "
                "Stay fully in character. Reference the day if relevant (e.g. Monday back to the grind, Wednesday hump day, etc). No hashtags."
            )}],
        )
    response = await asyncio.to_thread(_ask)
    await channel.send(response.content[0].text)


# ── entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN not set in .env")
    bot.run(DISCORD_BOT_TOKEN)
