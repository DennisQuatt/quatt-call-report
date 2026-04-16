#!/usr/bin/env python3
"""
Quatt Aircall Dashboard Updater — Incremental
- First run: fetches all data from Jan 1 to today, saves cache
- Subsequent runs: only fetches calls since last update (~seconds not minutes)
- Updates dashboard HTML and pushes to GitHub
"""

import json, re, ssl, base64, subprocess, threading, time, os
import urllib.request
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

# ─── Config ──────────────────────────────────────────────────────────────────

def _load_secrets():
    secrets = {}
    p = os.path.join(os.path.dirname(__file__), '.secrets')
    try:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    secrets[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return secrets

_S = _load_secrets()

API_ID    = _S.get('AIRCALL_API_ID', '')
API_TOKEN = _S.get('AIRCALL_API_TOKEN', '')
NUMBER_IDS = [1149598, 1163482, 1143805]

TZ        = ZoneInfo("Europe/Amsterdam")
DASHBOARD = "/Users/dennisquatt/aircall-dashboard/aircall-dashboard.html"
CACHE     = "/Users/dennisquatt/aircall-dashboard/aircall_cache.json"
REPO_DIR  = "/Users/dennisquatt/aircall-dashboard"
SF_CACHE  = "/Users/dennisquatt/aircall-dashboard/snowflake_cache.json"
GITHUB_PAT  = _S.get('GITHUB_PAT', '')
GITHUB_REPO = "github.com/DennisQuatt/quatt-call-report.git"

HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 4, 3), date(2026, 4, 6),
    date(2026, 4, 27), date(2026, 5, 14), date(2026, 5, 25),
    date(2026, 12, 25), date(2026, 12, 26),
}

AGENTS = [
    ("Max Oeberius Kapteijn","S"),("Ray Zitter","S"),("Nathan Pieterse","S"),
    ("Jaleel Ahmad","C"),("Daan Van Wolferen","S"),("Erwin Corten","S"),
    ("Steve Manirakiza","S"),("Jasper Breukelman","C"),("Tijn de Haan","C"),
    ("Jimmy van Kessel","C"),("Sjoerd Verhagen","C"),("Youssef Adou","C"),
    ("Teun Slaghek","S"),("Sophia Van Tuijl","S"),("Rogier van Oort","C"),
    ("Joren Jans","C"),("Thijmen Van Schaik","S"),("Guus Versteeg","C"),
    ("Joris van Oosterhout","C"),("Sjoerd Balvers","C"),("Thomas Koopman","S"),
    ("Rik Treffers","S"),("Mark Munneke","H"),("Ferdinand van der Zee","H"),
    ("Amir Wali","H"),("Jordi de Vos","H"),("Henrico van Tilburg","H"),
    ("Brian Boersma","H"),("Sven van Kampen","H"),("Nicola Gobbi","H"),
    ("Mitchell Huisman","H"),("Koen Bender","H"),("Seth van Die","H"),
    ("Marin Knezovic","H"),("Glenn Verhoof","H"),("Arash Nahas Farmaniyeh","H"),
    ("Marco Willems","H"),("Dennis Jonker","H"),("Vincent Turner","H"),
    ("Jasmine Musicco","H"),("Paul Vermin","H"),("Stefan Hanna","H"),
    ("Nicky Willems","H"),("Ravi van Lent","H"),
]
AGENT_LOOKUP = {n.lower(): n for n, _ in AGENTS}

# ─── API helpers ─────────────────────────────────────────────────────────────

def make_ctx():
    c = ssl.create_default_context()
    c.load_verify_locations('/etc/ssl/cert.pem')
    return c

AUTH = base64.b64encode(f"{API_ID}:{API_TOKEN}".encode()).decode()

def get_json(url, ctx):
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers={"Authorization": f"Basic {AUTH}"})
            with urllib.request.urlopen(req, context=ctx, timeout=45) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(15 * (attempt + 1))
            else:
                raise
        except (TimeoutError, OSError):
            time.sleep(5)
    raise RuntimeError(f"Failed after retries: {url}")

# ─── 1. FETCH ─────────────────────────────────────────────────────────────────

def fetch_range(number_id, from_ts, to_ts):
    """Fetch all calls for one number in a time range, paged."""
    ctx = make_ctx()
    calls = []
    url = (f"https://api.aircall.io/v1/calls"
           f"?number_id={number_id}&from={from_ts}&to={to_ts}&per_page=50&order=asc")
    while url:
        data = get_json(url, ctx)
        calls.extend(data.get('calls', []))
        url = data.get('meta', {}).get('next_page_link')
        time.sleep(0.15)
    return calls

def fetch_window_parallel(from_ts, to_ts):
    """Fetch calls for all 3 numbers in parallel for the given time window."""
    results = {}
    def worker(nid):
        results[nid] = fetch_range(nid, from_ts, to_ts)
    threads = [threading.Thread(target=worker, args=(nid,)) for nid in NUMBER_IDS]
    for t in threads: t.start()
    for t in threads: t.join()
    return [c for nid in NUMBER_IDS for c in results.get(nid, [])]

def fetch_weekly_parallel(from_dt, to_dt):
    """Fetch in weekly chunks (avoids 10k cap), 3 numbers in parallel per chunk."""
    all_calls = []
    cur = from_dt
    while cur < to_dt:
        nxt = min(cur + timedelta(weeks=1), to_dt)
        batch = fetch_window_parallel(int(cur.timestamp()), int(nxt.timestamp()))
        all_calls.extend(batch)
        print(f"  {cur.strftime('%m-%d')} → {nxt.strftime('%m-%d')}: {len(batch)} calls ({len(all_calls)} total)")
        cur = nxt
    return all_calls

# ─── 2. CACHE ─────────────────────────────────────────────────────────────────

def load_cache():
    try:
        with open(CACHE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"calls": [], "last_ts": None}

def save_cache(cache):
    with open(CACHE, 'w') as f:
        json.dump(cache, f)

def get_new_calls(cache):
    """Fetch only calls newer than what's already cached."""
    today    = datetime.now(TZ)
    today_ts = int(today.timestamp())

    if not cache['last_ts']:
        # First run: fetch everything from Jan 1
        print("  First run — fetching full history from Jan 1...")
        from_dt = datetime(today.year, 1, 1, tzinfo=TZ)
        to_dt   = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=TZ)
        new_calls = fetch_weekly_parallel(from_dt, to_dt)
    else:
        # Incremental: fetch from last_ts to now
        last_dt = datetime.fromtimestamp(cache['last_ts'], tz=TZ)
        # Go back 2h to catch any delayed call records
        from_ts = int((last_dt - timedelta(hours=2)).timestamp())
        print(f"  Incremental fetch from {last_dt.strftime('%m-%d %H:%M')}...")
        new_calls = fetch_window_parallel(from_ts, today_ts)
        print(f"  Got {len(new_calls)} new calls")

    return new_calls, today_ts

def merge_cache(cache, new_calls, new_ts):
    """Merge new calls into cache, deduplicate (within-batch and against cache)."""
    seen_ids = {c['id'] for c in cache['calls']}
    added = []
    for c in new_calls:
        if c['id'] not in seen_ids:
            added.append(c)
            seen_ids.add(c['id'])  # prevent within-batch dupes too
    cache['calls'].extend(added)
    cache['last_ts'] = new_ts
    print(f"  Added {len(added)} new unique calls. Total cached: {len(cache['calls'])}")
    return cache

# ─── 3. PROCESS ───────────────────────────────────────────────────────────────

def is_working_day(d):
    return d.weekday() < 5 and date(d.year, d.month, d.day) not in HOLIDAYS_2026

def process_calls(calls):
    per_agent = {name: {} for name, _ in AGENTS}
    for c in calls:
        user = c.get('user')
        if not user:
            continue
        name = AGENT_LOOKUP.get(user.get('name', '').lower())
        if not name:
            continue
        ts = c.get('started_at')
        if not ts:
            continue
        dt = datetime.fromtimestamp(ts, tz=TZ)
        if not is_working_day(dt):
            continue
        day = dt.strftime('%m-%d')
        row = per_agent[name].setdefault(day, [0, 0, 0, 0, 0, 0, 0])
        direction   = c.get('direction', '')
        answered_at = c.get('answered_at')
        ended_at    = c.get('ended_at')
        if direction == 'inbound':
            row[0] += 1
        elif direction == 'outbound':
            row[1] += 1
        if answered_at and ended_at:
            incall = ended_at - answered_at
            if incall >= 0:
                row[3] += incall
                row[4]  = max(row[4], incall)
                row[5] += 1
                if incall > 30:
                    row[6] += 1
    return per_agent

# ─── 4. PERIODS ───────────────────────────────────────────────────────────────

def working_days_in_range(start, end):
    result, cur = [], start
    while cur < end:
        if is_working_day(cur):
            result.append(cur.strftime('%m-%d'))
        cur += timedelta(days=1)
    return result

def calendar_days_in_range(start, end):
    result, cur = [], start
    while cur < end:
        result.append(cur.strftime('%m-%d'))
        cur += timedelta(days=1)
    return result

def fmt(d):
    return d.strftime('%b %-d')

def compute_periods(today_dt):
    today = today_dt.date()
    year  = today.year
    day_abbr = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    yesterday = today - timedelta(days=1)
    monday    = today - timedelta(days=today.weekday())
    last_mon  = monday - timedelta(weeks=1)
    last_sun  = monday - timedelta(days=1)
    thisweek  = working_days_in_range(monday, today + timedelta(days=1))
    lastweek_work = working_days_in_range(last_mon, last_sun + timedelta(days=1))
    lastweek_all  = calendar_days_in_range(last_mon, last_sun + timedelta(days=1))
    march_dates = working_days_in_range(date(year, 3, 1), date(year, 4, 1))
    april_dates = working_days_in_range(date(year, 4, 1), today + timedelta(days=1))
    all_dates   = working_days_in_range(date(year, 1, 1), today + timedelta(days=1))
    tw_desc = f"This Week - {fmt(monday)} to {fmt(today)}" if monday != today else f"This Week - {fmt(today)}"
    periods = [
        {"id":"today",     "label":"Today",      "dates":[today.strftime('%m-%d')],
         "desc": f"Today - {day_abbr[today.weekday()]} {fmt(today)}"},
        {"id":"yesterday", "label":"Yesterday",   "dates":[yesterday.strftime('%m-%d')],
         "desc": f"Yesterday - {day_abbr[yesterday.weekday()]} {fmt(yesterday)}"},
        {"id":"thisweek",  "label":"This Week",   "dates": thisweek,  "desc": tw_desc},
        {"id":"lastweek",  "label":"Last Week",   "dates": lastweek_all,
         "desc": f"Last Week - {fmt(last_mon)} to {fmt(last_sun)}"},
        {"id":"march",     "label":"March",       "dates": march_dates, "desc": f"March {year}"},
        {"id":"april",     "label":"April",       "dates": april_dates, "desc": f"April {year}"},
        {"id":"all",       "label":"All Time",    "dates": all_dates,
         "desc": f"Jan - {fmt(today)}"},
    ]
    return {
        "MARCH": march_dates, "APRIL": april_dates,
        "LASTWEEK": lastweek_work, "THISWEEK": thisweek, "ALL": all_dates,
        "PERIODS": periods,
    }

# ─── 5. PATCH HTML ────────────────────────────────────────────────────────────

def build_D(per_agent):
    result = []
    for name, team in AGENTS:
        days = dict(sorted(per_agent.get(name, {}).items()))
        result.append({"n": name, "t": team, "p": days})
    return result

def patch_html(html, D, pvars, updated_ts):
    sep = (',', ':')
    D_json = json.dumps(D, separators=sep, ensure_ascii=False)
    html = re.sub(r'var D=\[.*\];(?=\n?var MARCH)', D_json.join(['var D=', ';']), html)
    for varname in ('MARCH', 'APRIL', 'LASTWEEK', 'THISWEEK', 'ALL'):
        val = json.dumps(pvars[varname], separators=sep)
        html = re.sub(rf'var {varname}=\[.*?\];', f'var {varname}={val};', html)
    periods_json = json.dumps(pvars['PERIODS'], separators=sep, ensure_ascii=False)
    html = re.sub(r'var PERIODS=\[.*?\];', f'var PERIODS={periods_json};', html)
    # Header badge: "Last updated: DATE TIME"
    html = re.sub(
        r'Last updated: [^<"]+',
        f'Last updated: {updated_ts} · will automatically update every 10min.',
        html
    )
    # Footer source label
    html = re.sub(r'Source: Aircall [A-Za-z]+', 'Source: Aircall API', html)
    html = re.sub(r'· Updated: [^<·"]+', '', html)
    html = html.replace('Source: Aircall API', f'Source: Aircall API · Updated: {updated_ts}')
    return html

# ─── 6. SNOWFLAKE DATA ───────────────────────────────────────────────────────

def load_sf_cache():
    try:
        with open(SF_CACHE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def patch_snowflake_data(html, sf_cache):
    sep = (',', ':')
    for varname, key in (('SF_MEETINGS', 'meetings'),
                         ('SF_QUOTES_SENT', 'quotes_sent'),
                         ('SF_QUOTES_ACCEPTED', 'quotes_accepted'),
                         ('SF_REVENUE', 'revenue')):
        data = sf_cache.get(key, {})
        val  = json.dumps(data, separators=sep, ensure_ascii=False)
        html = re.sub(rf'var {varname}=\{{.*?\}};', f'var {varname}={val};', html, flags=re.DOTALL)
    return html

# ─── 7. GIT PUSH ─────────────────────────────────────────────────────────────

def git_push(ts_label):
    remote = f"https://{GITHUB_PAT}@{GITHUB_REPO}"
    def run(cmd):
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=REPO_DIR)
    run('git add aircall-dashboard.html')
    if run('git diff --cached --quiet').returncode == 0:
        print("  No changes to commit.")
        return
    run(f'git commit -m "Auto-update {ts_label}"')
    run(f'git remote set-url origin {remote}')
    result = run('git push origin main')
    if result.returncode != 0:
        run('git pull origin main --no-rebase')
        result = run('git push origin main')
    if result.returncode == 0:
        print(f"  Pushed to GitHub.")
    else:
        print(f"  Push failed: {result.stderr[:200]}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(TZ)
    ts  = now.strftime('%Y-%m-%d %H:%M')
    print(f"=== Aircall Dashboard Update {ts} ===")

    print("\n[1/4] Fetching from Aircall API...")
    cache = load_cache()
    new_calls, new_ts = get_new_calls(cache)
    cache = merge_cache(cache, new_calls, new_ts)
    save_cache(cache)

    print("\n[2/4] Processing calls...")
    per_agent = process_calls(cache['calls'])
    D = build_D(per_agent)
    print(f"  Agents with data: {sum(1 for a in D if a['p'])}/{len(D)}")

    print("\n[3/4] Patching HTML...")
    pvars    = compute_periods(now)
    sf_cache = load_sf_cache()
    with open(DASHBOARD, encoding='utf-8') as f:
        html = f.read()
    html = patch_html(html, D, pvars, ts)
    html = patch_snowflake_data(html, sf_cache)
    with open(DASHBOARD, 'w', encoding='utf-8') as f:
        f.write(html)
    sf_loaded = bool(sf_cache.get('meetings'))
    print(f"  HTML saved. SF data: {'loaded' if sf_loaded else 'not found (cache missing)'}")

    print("\n[4/4] Pushing to GitHub (auto-approve)...")
    git_push(ts)
    print("\n=== Done ===")

if __name__ == '__main__':
    main()
