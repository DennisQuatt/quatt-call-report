#!/usr/bin/env python3
"""
Refresh data.json from Aircall API + Snowflake.

Required env vars:
  AIRCALL_API_ID, AIRCALL_API_TOKEN
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""
import os, json, requests, snowflake.connector
from datetime import datetime, date, timedelta, timezone

AIRCALL_BASE = 'https://api.aircall.io/v1'
YEAR_START   = date(2026, 1, 1)

ROSTER = {
    "Jaleel Ahmad": "C", "Rogier van Oort": "C", "Joris van Oosterhout": "C",
    "Joren Jans": "C", "Tijn de Haan": "C", "Jasper Breukelman": "C",
    "Sjoerd Balvers": "C", "Sjoerd Verhagen": "C", "Guus Versteeg": "C",
    "Jimmy van Kessel": "C", "Youssef Adou": "C",
    "Daan Van Wolferen": "S", "Teun Slaghek": "S", "Sophia Van Tuijl": "S",
    "Erwin Corten": "S", "Max Oeberius Kapteijn": "S", "Ray Zitter": "S",
    "Thijmen Van Schaik": "S", "Nathan Pieterse": "S", "Thomas Koopman": "S",
    "Rik Treffers": "S", "Steve Manirakiza": "S", "Floris Veenendaal": "S",
    "Lein Overtoom": "S",
    "Paul Vermin": "H", "Amir Wali": "H", "Henrico van Tilburg": "H",
    "Jordi de Vos": "H", "Arash Nahas Farmaniyeh": "H", "Nicola Gobbi": "H",
    "Brian Boersma": "H", "Marin Knezovic": "H", "Ferdinand van der Zee": "H",
    "Sven van Kampen": "H", "Mark Munneke": "H", "Dennis Jonker": "H",
    "Vincent Turner": "H", "Nicky Willems": "H", "Mitchell Huisman": "H",
    "Glenn Verhoof": "H", "Marco Willems": "H", "Koen Bender": "H",
    "Stefan Hanna": "H", "Seth van Die": "H", "Jasmine Musicco": "H",
    "Ravi van Lent": "H", "Kasper Baas": "H", "Tom Heringa": "H",
}


def fetch_aircall_calls():
    api_id    = os.environ['AIRCALL_API_ID']
    api_token = os.environ['AIRCALL_API_TOKEN']
    from_ts   = int(datetime(YEAR_START.year, YEAR_START.month, YEAR_START.day).timestamp())
    to_ts     = int(datetime.now().timestamp())
    calls, page = [], 1
    while True:
        r = requests.get(
            f'{AIRCALL_BASE}/calls',
            auth=(api_id, api_token),
            params={'from': from_ts, 'to': to_ts, 'page': page, 'per_page': 50},
            timeout=30,
        )
        r.raise_for_status()
        data  = r.json()
        batch = data.get('calls', [])
        calls.extend(batch)
        print(f'  Aircall page {page}: {len(batch)} calls (total {len(calls)})')
        if not data.get('meta', {}).get('next_page_link'):
            break
        page += 1
    return calls


def process_aircall(calls):
    data = {}
    for call in calls:
        user = call.get('user') or {}
        name = user.get('name', '')
        if name not in ROSTER:
            continue
        ts = call.get('started_at') or 0
        if not ts:
            continue
        dk    = datetime.fromtimestamp(ts).strftime('%m-%d')
        dur   = call.get('duration') or 0
        status = call.get('status', '')
        is_in  = 1 if call.get('direction') == 'inbound'  else 0
        is_out = 1 if call.get('direction') == 'outbound' else 0
        missed = 1 if status in ('missed', 'voicemail') or dur == 0 else 0
        known  = 1 if dur > 0 else 0
        over30 = 1 if dur >= 30 else 0
        data.setdefault(name, {}).setdefault(dk, [0, 0, 0, 0, 0, 0, 0])
        d = data[name][dk]
        d[0] += is_in; d[1] += is_out; d[2] += missed
        d[3] += dur; d[4] = max(d[4], dur); d[5] += known; d[6] += over30
    return data


def query_snowflake():
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema='MARTS',
    )
    cur = conn.cursor()
    yr  = YEAR_START.strftime('%Y-%m-%d')
    base = ("DWH.MARTS.DIM_DEALS_COMMERCIAL c "
            "JOIN DWH.MARTS.DIM_DEALS_BASE b ON c.DEAL_ID=b.DEAL_ID "
            "JOIN DWH.MARTS.DIM_DEALS_OWNERS o ON b.OWNER_ID=o.OWNER_ID")

    cur.execute(f"SELECT o.DEAL_OWNER,TO_CHAR(c.APPOINTMENT_COMPLETED_DATE,'MM-DD'),COUNT(*) "
                f"FROM {base} WHERE c.APPOINTMENT_COMPLETED_DATE>='{yr}' GROUP BY 1,2")
    meetings = {}
    for name, d, n in cur.fetchall():
        meetings.setdefault(name, {})[d] = int(n)

    cur.execute(f"SELECT o.DEAL_OWNER,TO_CHAR(c.QUOTE_SENT_DATE,'MM-DD'),COUNT(*) "
                f"FROM {base} WHERE c.QUOTE_SENT_DATE>='{yr}' GROUP BY 1,2")
    quotes_sent = {}
    for name, d, n in cur.fetchall():
        quotes_sent.setdefault(name, {})[d] = int(n)

    cur.execute(f"SELECT o.DEAL_OWNER,TO_CHAR(c.QUOTE_SIGNED_DATE,'MM-DD'),COUNT(*),SUM(b.AMOUNT) "
                f"FROM {base} WHERE c.QUOTE_SIGNED_DATE>='{yr}' GROUP BY 1,2")
    quotes_accepted, revenue = {}, {}
    for name, d, qa, rev in cur.fetchall():
        quotes_accepted.setdefault(name, {})[d] = int(qa)
        revenue.setdefault(name, {})[d] = float(rev or 0)

    cur.close()
    conn.close()
    return meetings, quotes_sent, quotes_accepted, revenue


def working_days(start, end):
    r, d = [], start
    while d <= end:
        if d.weekday() < 5:
            r.append(d.strftime('%m-%d'))
        d += timedelta(days=1)
    return r


def build_periods(today):
    MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    DOW = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    yest = today - timedelta(days=1)
    while yest.weekday() >= 5:
        yest -= timedelta(days=1)
    mon   = today - timedelta(days=today.weekday())
    pmon  = mon - timedelta(days=7)
    pfri  = mon - timedelta(days=3)
    ms    = date(today.year, today.month, 1)
    pm_s  = date(today.year, today.month - 1, 1) if today.month > 1 else date(today.year - 1, 12, 1)
    pm_e  = ms - timedelta(days=1)
    return [
        {"id":"today",     "label":"Today",     "dates":working_days(today, today),
         "desc":f"Today - {DOW[today.weekday()]} {MON[today.month-1]} {today.day}"},
        {"id":"yesterday", "label":"Yesterday", "dates":working_days(yest, yest),
         "desc":f"Yesterday - {DOW[yest.weekday()]} {MON[yest.month-1]} {yest.day}"},
        {"id":"thisweek",  "label":"This Week", "dates":working_days(mon, today),
         "desc":f"This Week - {MON[mon.month-1]} {mon.day} to {MON[today.month-1]} {today.day}"},
        {"id":"lastweek",  "label":"Last Week", "dates":working_days(pmon, pfri),
         "desc":f"Last Week - {MON[pmon.month-1]} {pmon.day} to {MON[pfri.month-1]} {pfri.day}"},
        {"id":"thismonth", "label":MON[today.month-1], "dates":working_days(ms, today),
         "desc":f"{MON[today.month-1]} {today.year}"},
        {"id":"lastmonth", "label":MON[pm_s.month-1], "dates":working_days(pm_s, pm_e),
         "desc":f"{MON[pm_s.month-1]} {pm_s.year}"},
        {"id":"all",       "label":"All Time",  "dates":working_days(YEAR_START, today),
         "desc":f"Jan - {MON[today.month-1]} {today.day}"},
    ]


def main():
    today = date.today()
    print('Fetching Aircall...')
    calls_raw = fetch_aircall_calls()
    call_data = process_aircall(calls_raw)
    print(f'  Processed {len(calls_raw)} calls for {len(call_data)} agents')

    print('Querying Snowflake...')
    meetings, quotes_sent, quotes_accepted, revenue = query_snowflake()
    print(f'  meetings={len(meetings)} qs={len(quotes_sent)} qa={len(quotes_accepted)} rev={len(revenue)}')

    D = [{"n": name, "t": team, "p": call_data.get(name, {})} for name, team in ROSTER.items()]

    output = {
        "updated": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "periods": build_periods(today),
        "calls":   D,
        "sf": {
            "meetings":        meetings,
            "quotes_sent":     quotes_sent,
            "quotes_accepted": quotes_accepted,
            "revenue":         revenue,
        },
    }

    out = os.path.join(os.path.dirname(__file__), '..', 'data.json')
    with open(out, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    print(f'data.json written ({os.path.getsize(out):,} bytes)')


if __name__ == '__main__':
    main()
