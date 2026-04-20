#!/usr/bin/env python3
"""
Scoutline — API-Football Pro backend
Run: python3 proxy.py
"""

from http.server import HTTPServer, ThreadingHTTPServer, SimpleHTTPRequestHandler
import urllib.request, urllib.error, ssl as _ssl
import json, os, time, threading, hashlib, atexit, math, concurrent.futures, urllib.parse, re as _re
import datetime as _dt, sqlite3, secrets

# Load .env
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

PORT            = int(os.environ.get('PORT', 8081))

# ── Auth ──────────────────────────────────────────────────────────────────────
AUTH_DB   = os.path.join('data', 'users.db')
SESSIONS  = {}   # token -> {'user': str, 'expires': float}
SESSION_TTL = 7 * 24 * 3600   # 7 days

PW_ITERATIONS = 100_000

def _init_auth_db():
    os.makedirs('data', exist_ok=True)
    con = sqlite3.connect(AUTH_DB)
    con.execute('''CREATE TABLE IF NOT EXISTS users (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        username   TEXT    UNIQUE NOT NULL,
        pw_hash    TEXT    NOT NULL,
        salt       TEXT    NOT NULL,
        iterations INTEGER NOT NULL DEFAULT 200000,
        created    TEXT    NOT NULL DEFAULT (datetime('now'))
    )''')
    # migrate existing rows that lack the iterations column
    try:
        con.execute('ALTER TABLE users ADD COLUMN iterations INTEGER NOT NULL DEFAULT 200000')
    except Exception:
        pass
    con.commit(); con.close()

def _hash_pw(password: str, salt: str, iterations: int) -> str:
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), iterations).hex()

def auth_check_user(username: str, password: str) -> bool:
    try:
        con = sqlite3.connect(AUTH_DB)
        row = con.execute('SELECT pw_hash, salt, iterations FROM users WHERE username=?', (username,)).fetchone()
        con.close()
        if not row: return False
        return secrets.compare_digest(row[0], _hash_pw(password, row[1], row[2]))
    except Exception: return False

def auth_create_user(username: str, password: str):
    salt = secrets.token_hex(32)
    pw_hash = _hash_pw(password, salt, PW_ITERATIONS)
    con = sqlite3.connect(AUTH_DB)
    con.execute('INSERT INTO users (username, pw_hash, salt, iterations) VALUES (?,?,?,?)',
                (username, pw_hash, salt, PW_ITERATIONS))
    con.commit(); con.close()

def session_create(username: str) -> str:
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {'user': username, 'expires': time.time() + SESSION_TTL}
    return token

def session_get(token: str):
    s = SESSIONS.get(token)
    if not s: return None
    if time.time() > s['expires']:
        SESSIONS.pop(token, None); return None
    return s

def _get_session_from_request(handler) -> str | None:
    cookie_header = handler.headers.get('Cookie', '')
    for part in cookie_header.split(';'):
        part = part.strip()
        if part.startswith('scoutline_session='):
            token = part[len('scoutline_session='):]
            if session_get(token): return token
    return None

LOGIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scoutline — Sign in</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=IBM+Plex+Mono:wght@500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Manrope',sans-serif;background:#DED3BF;color:#20251F;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
.card{background:#FFF9EE;border:1px solid rgba(55,44,28,.12);border-radius:16px;padding:44px 40px;width:100%;max-width:400px;box-shadow:0 8px 32px rgba(55,44,28,.12)}
.logo{display:flex;align-items:center;gap:14px;margin-bottom:36px}
.logo-mark{width:44px;height:44px;background:#10896C;border-radius:12px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.logo-text{font-size:21px;font-weight:800;letter-spacing:-.3px;color:#20251F}
.logo-sub{font-size:10px;color:#8A9187;font-family:'IBM Plex Mono',monospace;margin-top:3px;letter-spacing:.5px}
h1{font-size:18px;font-weight:700;margin-bottom:6px;color:#20251F}
.sub{font-size:13px;color:#5A635B;margin-bottom:28px;line-height:1.5}
label{display:block;font-size:11px;font-weight:700;color:#5A635B;margin-bottom:7px;letter-spacing:.5px;text-transform:uppercase}
input{width:100%;height:42px;background:#DBCFBA;border:1px solid rgba(55,44,28,.18);border-radius:10px;padding:0 14px;font-size:14px;font-family:'Manrope',sans-serif;color:#20251F;outline:none;transition:border-color .15s,box-shadow .15s}
input:focus{border-color:#B76D12;box-shadow:0 0 0 3px rgba(183,109,18,.14)}
.field{margin-bottom:18px}
.btn{width:100%;height:44px;background:#B76D12;border:none;border-radius:10px;font-size:14px;font-weight:700;font-family:'Manrope',sans-serif;color:#fff;cursor:pointer;margin-top:8px;transition:opacity .15s;letter-spacing:.1px}
.btn:hover{opacity:.88}
.error{background:#FBE9E6;border:1px solid rgba(184,70,63,.25);border-radius:8px;padding:11px 14px;font-size:13px;color:#B8463F;margin-bottom:20px;display:none;line-height:1.5}
.error.show{display:block}
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <div class="logo-mark">
      <svg viewBox="0 0 24 24" fill="none" width="26" height="26">
        <path d="M12 3L20 8V16L12 21L4 16V8L12 3Z" stroke="#fff" stroke-width="1.5" fill="rgba(255,255,255,.15)"/>
        <circle cx="12" cy="12" r="3.5" fill="#fff" opacity=".8"/>
        <line x1="12" y1="3" x2="12" y2="8.5" stroke="#fff" stroke-width="1.5"/>
        <line x1="20" y1="8" x2="15.2" y2="10.2" stroke="#fff" stroke-width="1.5"/>
        <line x1="20" y1="16" x2="15.2" y2="13.8" stroke="#fff" stroke-width="1.5"/>
        <line x1="12" y1="21" x2="12" y2="15.5" stroke="#fff" stroke-width="1.5"/>
        <line x1="4" y1="16" x2="8.8" y2="13.8" stroke="#fff" stroke-width="1.5"/>
        <line x1="4" y1="8" x2="8.8" y2="10.2" stroke="#fff" stroke-width="1.5"/>
      </svg>
    </div>
    <div>
      <div class="logo-text">Scoutline</div>
      <div class="logo-sub">FOOTBALL INTELLIGENCE</div>
    </div>
  </div>
  <h1>Sign in</h1>
  <p class="sub">Enter your credentials to access the dashboard.</p>
  <div class="error" id="err"></div>
  <form method="POST" action="/login">
    <div class="field">
      <label>Username</label>
      <input type="text" name="username" autocomplete="username" autofocus required>
    </div>
    <div class="field">
      <label>Password</label>
      <input type="password" name="password" autocomplete="current-password" required>
    </div>
    <button class="btn" type="submit">Sign in</button>
  </form>
</div>
<script>
const p=new URLSearchParams(location.search);
if(p.get('err')){const e=document.getElementById('err');e.textContent=p.get('err')==='1'?'Invalid username or password.':'Session expired. Please sign in again.';e.classList.add('show');}
</script>
</body>
</html>
'''
DISK_CACHE_FILE = os.environ.get('CACHE_FILE', 'scoutline_cache.json')
PREDICTION_FILE = os.environ.get('PREDICTION_FILE', os.path.join('data', 'prediction_history.json'))
ODDS_HISTORY_FILE  = os.environ.get('ODDS_HISTORY_FILE',  os.path.join('data', 'odds_history.json'))
CALIBRATION_FILE   = os.environ.get('CALIBRATION_FILE',  os.path.join('data', 'league_calibration.json'))

# ── API-Football Pro (api-sports.io) ─────────────────────────────────────────
APIF_KEY  = (os.environ.get('APIFOOTBALL_KEY')
             or os.environ.get('API_FOOTBALL_KEY')
             or os.environ.get('API_SPORTS_KEY', ''))
APIF_BASE = 'https://v3.football.api-sports.io'
APIF_TEAMSTAT_MATCHES = int(os.environ.get('APIF_TEAMSTAT_MATCHES', 24))

APIF_LEAGUE_MAP = {
    # England
    'PL':  {'id': 39,  'season': 2025}, 'ELC': {'id': 40,  'season': 2025},
    'L1':  {'id': 41,  'season': 2025}, 'L2':  {'id': 42,  'season': 2025},
    # Spain
    'PD':  {'id': 140, 'season': 2025}, 'PD2': {'id': 141, 'season': 2025},
    # Germany
    'BL1': {'id': 78,  'season': 2025}, 'BL2': {'id': 79,  'season': 2025},
    # Italy
    'SA':  {'id': 135, 'season': 2025}, 'SB':  {'id': 136, 'season': 2025},
    # France
    'FL1': {'id': 61,  'season': 2025}, 'FL2': {'id': 62,  'season': 2025},
    # Portugal / Netherlands
    'PPL': {'id': 94,  'season': 2025}, 'DED': {'id': 88,  'season': 2025},
    # Turkey / Scotland / Greece / Belgium / Austria / Poland
    'TSL': {'id': 203, 'season': 2025}, 'SP':  {'id': 179, 'season': 2025},
    'SC1': {'id': 180, 'season': 2025},  # Scottish Championship
    'GL':  {'id': 197, 'season': 2025}, 'BPL': {'id': 144, 'season': 2025},
    'AFL': {'id': 218, 'season': 2025}, 'PEK': {'id': 106, 'season': 2025},
    # Denmark / Switzerland / Russia
    'DSL': {'id': 119, 'season': 2025}, 'SSL': {'id': 207, 'season': 2025},
    'RUS': {'id': 235, 'season': 2025},
    # Nordic (calendar-year seasons)
    'NOR': {'id': 103, 'season': 2026}, 'SWE': {'id': 113, 'season': 2026},
    # UEFA
    'CL':  {'id': 2,   'season': 2025}, 'EL':  {'id': 3,   'season': 2025},
    'ECL': {'id': 877, 'season': 2025},
    # Americas
    'BSA': {'id': 71,  'season': 2026}, 'BSB': {'id': 72,  'season': 2026},
    'MLS': {'id': 253, 'season': 2026}, 'ARG': {'id': 128, 'season': 2026},
    'LMX': {'id': 262, 'season': 2026},
    # Asia / Middle East
    'SPL': {'id': 307, 'season': 2025}, 'JPL': {'id': 98,  'season': 2026},
    'KCL': {'id': 292, 'season': 2026},
    # International
    'WC':  {'id': 1,   'season': 2026}, 'EC':  {'id': 4,   'season': 2024},
}

# ── Odds Providers ────────────────────────────────────────────────────────────
ODDS_API_KEY   = os.environ.get('ODDS_API_KEY', '')
ODDS_API_BASE  = 'https://api.the-odds-api.com/v4'
ODDS_REGION    = 'eu'
ODDS_CACHE_TTL = 1800  # 30 min
ODDS_LAST_ERROR = {}
ODDS_FALLBACK_ENABLED = os.environ.get('ODDS_FALLBACK_ENABLED', '').lower() in ('1', 'true', 'yes')

# ── Understat (free, no key — top-5 EU leagues xG data) ─────────────────────
UNDERSTAT_LEAGUE_MAP = {
    'PL': 'EPL', 'PD': 'La_liga', 'BL1': 'Bundesliga',
    'SA': 'Serie_A', 'FL1': 'Ligue_1',
}
UNDERSTAT_TTL = 21600  # 6 h

def _fetch_understat(comp):
    us_code = UNDERSTAT_LEAGUE_MAP.get(comp)
    if not us_code:
        return {}
    ck = f'understat_{comp}'
    cached = get_cache(ck)
    if cached is not None:
        return cached
    try:
        url = f'https://understat.com/league/{us_code}'
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
            html = resp.read().decode('utf-8')
        m = _re.search(r"var teamsData\s*=\s*JSON\.parse\('(.+?)'\)", html)
        if not m:
            print(f'[Understat] teamsData not found for {comp}'); return {}
        import urllib.parse as _up
        teams_raw = json.loads(_up.unquote(m.group(1)))
        result = {}
        for _tid, td in teams_raw.items():
            name    = td.get('title', '')
            history = td.get('history', [])
            if not history: continue
            def _avg(vals): return round(sum(vals)/len(vals), 4) if vals else None
            all_m  = history
            home_m = [x for x in history if x.get('h_a') == 'h']
            away_m = [x for x in history if x.get('h_a') == 'a']
            def _xg(lst):  return [float(x.get('xG',  0)) for x in lst]
            def _xga(lst): return [float(x.get('xGA', 0)) for x in lst]
            norm = _re.sub(r'[^a-z0-9]', '', name.lower())
            result[norm] = {
                'name': name,
                'xgPg':      _avg(_xg(all_m)),   'xgaPg':      _avg(_xga(all_m)),
                'xgHomePg':  _avg(_xg(home_m)),  'xgaHomePg':  _avg(_xga(home_m)),
                'xgAwayPg':  _avg(_xg(away_m)),  'xgaAwayPg':  _avg(_xga(away_m)),
            }
        set_cache(ck, result, UNDERSTAT_TTL)
        print(f'[Understat] {comp}: loaded {len(result)} teams')
        return result
    except Exception as e:
        print(f'[Understat] {comp} failed: {e}'); return {}

# ── ClubElo (free, no key required) ──────────────────────────────────────────
CLUBELO_BASE    = 'http://api.clubelo.com'
CLUBELO_TTL     = 86400  # 24 h — ratings update weekly

def _fetch_clubelo():
    today = _dt.date.today().isoformat()
    ck = f'clubelo_{today}'
    cached = get_cache(ck)
    if cached is not None:
        return cached
    try:
        url = f'{CLUBELO_BASE}/{today}'
        req = urllib.request.Request(url, headers={'User-Agent': 'Scoutline/1.0'})
        with urllib.request.urlopen(req, timeout=12) as resp:
            text = resp.read().decode('utf-8')
        result = {}
        for line in text.strip().split('\n')[1:]:
            parts = line.split(',')
            if len(parts) < 5: continue
            club, country, level, elo_str = parts[1], parts[2], parts[3], parts[4]
            try:
                norm = _re.sub(r'[^a-z0-9]', '', club.lower())
                result[norm] = {'name': club, 'elo': round(float(elo_str)),
                                'country': country, 'level': int(level or 1)}
            except (ValueError, IndexError):
                continue
        set_cache(ck, result, CLUBELO_TTL)
        print(f'[ClubElo] loaded {len(result)} clubs')
        return result
    except Exception as e:
        print(f'[ClubElo] fetch failed: {e}')
        return {}

# ── Football-Data.org (free tier — for league calibration) ────────────────────
FD_KEY          = os.environ.get('FOOTBALL_DATA_KEY', '')
FD_BASE         = 'https://api.football-data.org/v4'
FD_LEAGUE_MAP   = {
    'PL':'PL','ELC':'ELC','L1':'EL1','L2':'EL2',
    'BL1':'BL1','PD':'PD','SA':'SA','FL1':'FL1',
    'CL':'CL','EL':'EL','DED':'DED','PPL':'PPL','BSA':'BSA',
}
CALIBRATION_SEASONS = [2021, 2022, 2023, 2024]

ODDS_SPORT_KEYS = {
    'PL':  'soccer_epl',             'ELC': 'soccer_efl_champ',
    'L1':  'soccer_england_league1', 'L2':  'soccer_england_league2',
    'PD':  'soccer_spain_la_liga',   'PD2': 'soccer_spain_segunda_division',
    'BL1': 'soccer_germany_bundesliga', 'BL2': 'soccer_germany_bundesliga2',
    'SA':  'soccer_italy_serie_a',   'SB':  'soccer_italy_serie_b',
    'FL1': 'soccer_france_ligue_one','FL2': 'soccer_france_ligue_two',
    'PPL': 'soccer_portugal_primeira_liga',
    'DED': 'soccer_netherlands_eredivisie',
    'TSL': 'soccer_turkey_super_league',
    'SP':  'soccer_spl',
    'SC1': 'soccer_scotland_championship',
    'GL':  'soccer_greece_super_league',
    'BPL': 'soccer_belgium_first_div',
    'AFL': 'soccer_austria_bundesliga',
    'PEK': 'soccer_poland_ekstraklasa',
    'DSL': 'soccer_denmark_superliga',
    'SSL': 'soccer_switzerland_superleague',
    'NOR': 'soccer_norway_eliteserien',
    'SWE': 'soccer_sweden_allsvenskan',
    'RUS': 'soccer_russia_premier_league',
    'CL':  'soccer_uefa_champs_league',
    'EL':  'soccer_uefa_europa_league',
    'ECL': 'soccer_uefa_europa_conference_league',
    'BSA': 'soccer_brazil_campeonato',  'BSB': 'soccer_brazil_serie_b',
    'MLS': 'soccer_usa_mls',
    'ARG': 'soccer_argentina_primera_division',
    'LMX': 'soccer_mexico_ligamx',
    'SPL': 'soccer_saudi_arabia_pro_league',
    'JPL': 'soccer_japan_j_league',
    'KCL': 'soccer_korea_kleague1',
    'WC':  'soccer_fifa_world_cup',
    'EC':  'soccer_uefa_european_championship',
}

# ── Cache ─────────────────────────────────────────────────────────────────────
cache = {}
cache_lock = threading.Lock()
CACHE_TTL = {'standings': 1800, 'fixtures': 300, 'results': 1800, 'teamstats': 21600, 'default': 300}

def _ttl(path):
    if 'teamstats' in path: return CACHE_TTL['teamstats']
    if 'standings' in path: return CACHE_TTL['standings']
    if 'FINISHED'  in path: return CACHE_TTL['results']
    if 'SCHEDULED' in path: return CACHE_TTL['fixtures']
    return CACHE_TTL['default']

def _key(p): return hashlib.md5(p.encode()).hexdigest()

def get_cache(path):
    with cache_lock:
        e = cache.get(_key(path))
        if e and time.time() - e['ts'] < e.get('ttl', _ttl(path)): return e['data']
    return None

def get_stale_cache(path, max_age=86400):
    with cache_lock:
        e = cache.get(_key(path))
        if e and time.time() - e.get('ts', 0) < max_age:
            return e.get('data')
    return None

def set_cache(path, data, ttl=None):
    entry = {'data': data, 'ts': time.time()}
    if ttl is not None:
        entry['ttl'] = ttl
    with cache_lock: cache[_key(path)] = entry

def cache_meta(path):
    with cache_lock:
        e = cache.get(_key(path))
        if not e:
            return {'hit': False}
        ttl = e.get('ttl', _ttl(path))
        age = max(0, time.time() - e.get('ts', 0))
        return {'hit': age < ttl, 'ageSeconds': round(age), 'ttlSeconds': ttl,
                'updatedAt': e.get('ts'), 'expiresInSeconds': max(0, round(ttl - age))}

def delete_cache(path):
    with cache_lock:
        return cache.pop(_key(path), None) is not None

def load_disk_cache():
    if not os.path.exists(DISK_CACHE_FILE): return
    try:
        with open(DISK_CACHE_FILE) as f: saved = json.load(f)
        with cache_lock: cache.update(saved)
        valid = sum(1 for v in saved.values() if time.time() - v.get('ts', 0) < 86400)
        print(f'  [DISK] Loaded {len(saved)} entries ({valid} valid)')
    except Exception as e: print(f'  [DISK] Load failed: {e}')

def save_disk_cache():
    try:
        with cache_lock: to_save = {k: v for k, v in cache.items() if time.time() - v['ts'] < 86400}
        with open(DISK_CACHE_FILE, 'w') as f: json.dump(to_save, f)
        print(f'  [DISK] Saved {len(to_save)} entries')
    except Exception as e: print(f'  [DISK] Save failed: {e}')

# ── Team stats status ─────────────────────────────────────────────────────────
teamstats_status = {}
teamstats_lock   = threading.Lock()
prediction_lock  = threading.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────
def _stat_num(value):
    if value in (None, '', '-'): return None
    if isinstance(value, str):
        v = value.strip().replace('%', '')
        if not v: return None
        try: return float(v)
        except ValueError: return None
    if isinstance(value, (int, float)): return float(value)
    return None

def _fuzzy_match(name, team_dict):
    """Match a team name against a dict of name→stats."""
    if not team_dict: return None
    nl = name.lower().strip()
    if name in team_dict: return team_dict[name]
    for k, v in team_dict.items():
        if k.lower() == nl: return v
    words = {w for w in nl.split() if len(w) > 2}
    best_score, best_v = 0, None
    for k, v in team_dict.items():
        kwords = {w for w in k.lower().split() if len(w) > 2}
        score = len(words & kwords) / max(len(words), len(kwords), 1)
        if score > best_score and score >= 0.5:
            best_score, best_v = score, v
    return best_v

# ── Poisson helpers ───────────────────────────────────────────────────────────
def _poisson_pmf(k, lam):
    if lam <= 0 or k < 0: return 0.0
    return (lam**k * math.exp(-lam)) / math.factorial(k)

def _match_probs(lh, la, max_g=7):
    ph = pd = pa = 0.0
    for i in range(max_g + 1):
        pi = _poisson_pmf(i, lh)
        for j in range(max_g + 1):
            p = pi * _poisson_pmf(j, la)
            if i > j:    ph += p
            elif i == j: pd += p
            else:        pa += p
    tot = ph + pd + pa
    return (ph/tot, pd/tot, pa/tot) if tot else (1/3, 1/3, 1/3)

def _over25_prob(lh, la, max_g=7):
    p = 0.0
    for i in range(max_g + 1):
        pi = _poisson_pmf(i, lh)
        for j in range(max_g + 1):
            if i + j > 2: p += pi * _poisson_pmf(j, la)
    return p

def _dc_draw_prob(lh, la, rho, max_g=7):
    """DC-corrected draw probability for a given rho."""
    def _tau(i, j):
        if i == 0 and j == 0: return 1 - lh * la * rho
        if i == 1 and j == 0: return 1 + la * rho
        if i == 0 and j == 1: return 1 + lh * rho
        if i == 1 and j == 1: return 1 - rho
        return 1.0
    return sum(_poisson_pmf(k, lh) * _poisson_pmf(k, la) * _tau(k, k)
               for k in range(max_g + 1))

# Prediction ledger
def _prediction_path():
    path = PREDICTION_FILE
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    return path

def _load_predictions():
    path = _prediction_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else data.get('predictions', [])
    except Exception as e:
        print(f'  [PRED] Load failed: {e}')
        return []

def _save_predictions(rows):
    path = _prediction_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(rows, f, indent=2)
    except Exception as e:
        print(f'  [PRED] Save failed: {e}')

def _prediction_id(row):
    seed = '|'.join(str(row.get(k, '')) for k in (
        'createdAt', 'competition', 'fixtureId', 'homeTeamId', 'awayTeamId',
        'homeTeam', 'awayTeam', 'model'
    ))
    return hashlib.md5(seed.encode()).hexdigest()[:16]

def _prediction_pick(row):
    probs = row.get('probabilities') or {}
    vals = {'H': probs.get('home') or 0, 'D': probs.get('draw') or 0, 'A': probs.get('away') or 0}
    return max(vals, key=vals.get)

def _norm_team_name(name):
    s = (name or '').lower()
    s = _re.sub(r'\b(fc|afc|cf|sc|st|saint|the)\b', ' ', s)
    s = _re.sub(r'[^a-z0-9]+', ' ', s)
    return ' '.join(w for w in s.split() if w)

def _team_names_match(a, b):
    na, nb = _norm_team_name(a), _norm_team_name(b)
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    aw = {w for w in na.split() if len(w) > 2}
    bw = {w for w in nb.split() if len(w) > 2}
    return bool(aw and bw and len(aw & bw) / max(len(aw), len(bw), 1) >= 0.6)

def _grade_prediction(row, match):
    ft = ((match.get('score') or {}).get('fullTime') or {})
    hg, ag = ft.get('home'), ft.get('away')
    if hg is None or ag is None:
        return False
    actual = 'H' if hg > ag else 'A' if ag > hg else 'D'
    pick = row.get('pick') or _prediction_pick(row)
    probs = row.get('probabilities') or {}
    p = {
        'H': (probs.get('home') or 0) / 100,
        'D': (probs.get('draw') or 0) / 100,
        'A': (probs.get('away') or 0) / 100,
    }
    brier = sum((p[k] - (1 if actual == k else 0)) ** 2 for k in ('H', 'D', 'A'))
    pred_score = row.get('predictedScore') or {}
    row['status'] = 'graded'
    row['actual'] = {'home': hg, 'away': ag, 'outcome': actual, 'utcDate': match.get('utcDate')}
    row['metrics'] = {
        'outcomeCorrect': pick == actual,
        'scoreCorrect': pred_score.get('home') == hg and pred_score.get('away') == ag,
        'brier': round(brier, 4),
    }
    row['gradedAt'] = _dt.datetime.now(_dt.timezone.utc).isoformat()
    return True

def _match_prediction_to_result(row):
    comp = row.get('competition')
    fixture_id = row.get('fixtureId')
    if fixture_id and APIF_KEY:
        data = apif_get('fixtures', {'id': fixture_id}) or []
        matches = _apif_to_matches(data)
        if matches and matches[0].get('status') == 'FINISHED':
            return matches[0]
    if not comp:
        return None
    kickoff_raw = row.get('kickoff') or ''
    created_raw = row.get('createdAt') or ''
    predicted_at = None
    try:
        predicted_at = _dt.datetime.fromisoformat(created_raw.replace('Z', '+00:00'))
        if predicted_at.tzinfo is None:
            predicted_at = predicted_at.replace(tzinfo=_dt.timezone.utc)
    except Exception:
        predicted_at = None
    try:
        kickoff = _dt.datetime.fromisoformat(kickoff_raw.replace('Z', '+00:00'))
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=_dt.timezone.utc)
    except Exception:
        kickoff = None
    data = apif_matches(comp, status='FINISHED') or {}
    hid, aid = row.get('homeTeamId'), row.get('awayTeamId')
    hname = (row.get('homeTeam') or '').lower()
    aname = (row.get('awayTeam') or '').lower()
    for m in data.get('matches', []):
        mh = m.get('homeTeam') or {}; ma = m.get('awayTeam') or {}
        ids_match = hid and aid and mh.get('id') == hid and ma.get('id') == aid
        names_match = hname and aname and _team_names_match(mh.get('name'), hname) and _team_names_match(ma.get('name'), aname)
        if not (ids_match or names_match):
            continue
        if kickoff:
            try:
                played = _dt.datetime.fromisoformat((m.get('utcDate') or '').replace('Z', '+00:00'))
                if played.tzinfo is None:
                    played = played.replace(tzinfo=_dt.timezone.utc)
                if abs((played - kickoff).total_seconds()) > 3 * 86400:
                    continue
            except Exception:
                continue
        elif predicted_at:
            try:
                played = _dt.datetime.fromisoformat((m.get('utcDate') or '').replace('Z', '+00:00'))
                if played.tzinfo is None:
                    played = played.replace(tzinfo=_dt.timezone.utc)
                if played < predicted_at - _dt.timedelta(hours=12):
                    continue
                if played > predicted_at + _dt.timedelta(days=14):
                    continue
            except Exception:
                continue
        return m
    return None

def _score_predictions(rows):
    changed = False
    for row in rows:
        if row.get('status') == 'graded':
            continue
        match = _match_prediction_to_result(row)
        if match and _grade_prediction(row, match):
            changed = True
    return changed

def _prediction_summary(rows):
    graded = [r for r in rows if r.get('status') == 'graded']
    pending = [r for r in rows if r.get('status') != 'graded']
    if not graded:
        return {'total': len(rows), 'graded': 0, 'pending': len(pending),
                'outcomeAccuracy': None, 'scoreAccuracy': None, 'avgBrier': None,
                'calibration': None}
    outcome_ok = sum(1 for r in graded if (r.get('metrics') or {}).get('outcomeCorrect'))
    score_ok = sum(1 for r in graded if (r.get('metrics') or {}).get('scoreCorrect'))
    briers = [(r.get('metrics') or {}).get('brier') for r in graded if (r.get('metrics') or {}).get('brier') is not None]
    cal = {}
    for key, label in [('home', 'H'), ('draw', 'D'), ('away', 'A')]:
        avg_pred = sum(((r.get('probabilities') or {}).get(key) or 0) for r in graded) / len(graded)
        actual_rate = sum(1 for r in graded if ((r.get('actual') or {}).get('outcome') == label)) / len(graded) * 100
        cal[key] = {'avgPred': round(avg_pred, 1), 'actualRate': round(actual_rate, 1),
                    'delta': round(actual_rate - avg_pred, 1)}
    return {'total': len(rows), 'graded': len(graded), 'pending': len(pending),
            'outcomeAccuracy': round(outcome_ok / len(graded) * 100, 1),
            'scoreAccuracy': round(score_ok / len(graded) * 100, 1),
            'avgBrier': round(sum(briers) / len(briers), 4) if briers else None,
            'calibration': cal,
            'tuning': _prediction_tuning(rows)}

def _avg(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None

def _clamp(v, lo, hi):
    return max(lo, min(hi, v))

# ── League calibration (football-data.org historical data) ───────────────────
_calibration_lock     = threading.Lock()
_calibration_building = False

def _load_calibration():
    return _load_json_file(CALIBRATION_FILE, {})

def fetch_fd_season(fd_code, season):
    if not FD_KEY:
        return []
    cache_key = f'/fd_cal/{fd_code}/{season}'
    cached = get_cache(cache_key)
    if cached is not None:
        return cached
    url = f'{FD_BASE}/competitions/{fd_code}/matches?season={season}&status=FINISHED'
    try:
        ctx = _ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={'X-Auth-Token': FD_KEY, 'User-Agent': 'Scoutline/2.0'})
        with urllib.request.urlopen(req, timeout=25, context=ctx) as r:
            raw = json.loads(r.read())
            results = []
            for m in raw.get('matches', []):
                sc = (m.get('score') or {}).get('fullTime') or {}
                hg, ag = sc.get('home'), sc.get('away')
                if hg is not None and ag is not None:
                    try: results.append((int(hg), int(ag)))
                    except (TypeError, ValueError): pass
            set_cache(cache_key, results, ttl=7 * 24 * 3600)
            print(f'  [FD] {fd_code}/{season}: {len(results)} matches')
            return results
    except Exception as e:
        print(f'  [FD] {fd_code}/{season} failed: {e}')
        return []

def build_league_calibration():
    global _calibration_building
    with _calibration_lock:
        if _calibration_building: return
        _calibration_building = True
    try:
        print('  [CAL] Building league calibration from football-data.org...')
        leagues = {}
        for comp, fd_code in FD_LEAGUE_MAP.items():
            all_matches = []
            for season in CALIBRATION_SEASONS:
                all_matches.extend(fetch_fd_season(fd_code, season))
                time.sleep(0.25)
            if len(all_matches) < 50:
                print(f'  [CAL] {comp}: insufficient data ({len(all_matches)} matches)')
                continue
            n      = len(all_matches)
            h_wins = sum(1 for h, a in all_matches if h > a)
            draws  = sum(1 for h, a in all_matches if h == a)
            a_wins = n - h_wins - draws
            avg_hg = sum(h for h, a in all_matches) / n
            avg_ag = sum(a for h, a in all_matches) / n
            over25 = sum(1 for h, a in all_matches if h + a > 2)
            btts   = sum(1 for h, a in all_matches if h > 0 and a > 0)
            # binary-search rho so DC draw rate ≈ historical draw rate
            target_dr = draws / n
            best_rho, best_diff = -0.13, float('inf')
            for rho100 in range(-25, -5):
                rho_c = rho100 / 100.0
                diff  = abs(_dc_draw_prob(avg_hg, avg_ag, rho_c) - target_dr)
                if diff < best_diff:
                    best_diff, best_rho = diff, rho_c
            ha_factor = avg_hg / avg_ag if avg_ag > 0 else 1.1
            leagues[comp] = {
                'n': n,
                'homeWinPct':   round(h_wins / n, 4),
                'drawPct':      round(draws   / n, 4),
                'awayWinPct':   round(a_wins  / n, 4),
                'avgHomeGoals': round(avg_hg, 3),
                'avgAwayGoals': round(avg_ag, 3),
                'avgTotalGoals':round(avg_hg + avg_ag, 3),
                'homeAdvFactor':round(ha_factor, 3),
                'over25Rate':   round(over25 / n, 4),
                'bttsRate':     round(btts   / n, 4),
                'suggestedRho': round(_clamp(best_rho, -0.25, -0.06), 3),
            }
            print(f'  [CAL] {comp}: n={n} homeAdv={ha_factor:.3f} '
                  f'draw={draws/n:.3f} rho={best_rho}')
        if leagues:
            result = {'builtAt': _dt.datetime.now(_dt.timezone.utc).isoformat(), 'leagues': leagues}
            _save_json_file(CALIBRATION_FILE, result)
            print(f'  [CAL] Saved calibration for {len(leagues)} leagues')
            return result
        return {}
    finally:
        with _calibration_lock:
            _calibration_building = False

def _calibration_is_stale():
    cal = _load_calibration()
    if not cal or not cal.get('leagues'):
        return True
    try:
        age = (_dt.datetime.now(_dt.timezone.utc) -
               _dt.datetime.fromisoformat(cal['builtAt'])).total_seconds()
        return age > 7 * 24 * 3600
    except Exception:
        return True

def _prediction_tuning(rows):
    graded = [r for r in rows if r.get('status') == 'graded']

    def build(sample, comp=None, min_ready=20):
        cal_data   = _load_calibration()
        cal_leagues = cal_data.get('leagues', {}) if cal_data else {}
        cal        = cal_leagues.get(comp or '') or {}
        N_PRIOR    = 80  # virtual sample weight for historical prior

        if not sample:
            if cal:
                ha_mult  = round(cal.get('homeAdvFactor', 1.1) / 1.1, 3)
                rho_val  = round(cal.get('suggestedRho', -0.13), 3)
                o25_adj  = round(_clamp((cal.get('over25Rate', 0.52) - 0.52) * 100, -8, 8), 1)
                return {'graded': 0, 'ready': True, 'prior_source': 'historical',
                        'bias': {'home': 0, 'draw': 0, 'away': 0},
                        'homeAdvMultiplier': ha_mult, 'dcRho': rho_val,
                        'oddsWeightScale': 1.0, 'richDataWeightScale': 1.0,
                        'over25Adjustment': o25_adj, 'diagnostics': {}}
            return {'graded': 0, 'ready': False}

        n = len(sample)
        actual_counts = {'H': 0, 'D': 0, 'A': 0}
        pred_sum = {'home': 0.0, 'draw': 0.0, 'away': 0.0}
        over_preds = []
        over_actuals = []
        for r in sample:
            actual = (r.get('actual') or {}).get('outcome')
            if actual in actual_counts:
                actual_counts[actual] += 1
            probs = r.get('probabilities') or {}
            pred_sum['home'] += probs.get('home') or 0
            pred_sum['draw'] += probs.get('draw') or 0
            pred_sum['away'] += probs.get('away') or 0
            markets = r.get('markets') or {}
            actual_score = r.get('actual') or {}
            if markets.get('over25') is not None and actual_score.get('home') is not None and actual_score.get('away') is not None:
                over_preds.append(markets.get('over25'))
                over_actuals.append(100 if (actual_score.get('home') + actual_score.get('away')) > 2 else 0)

        deltas = {
            'home': actual_counts['H'] / n * 100 - pred_sum['home'] / n,
            'draw': actual_counts['D'] / n * 100 - pred_sum['draw'] / n,
            'away': actual_counts['A'] / n * 100 - pred_sum['away'] / n,
        }
        brier_all = _avg([(r.get('metrics') or {}).get('brier') for r in sample])
        odds_rows = [r for r in sample if (r.get('odds') or {}).get('home')]
        no_odds_rows = [r for r in sample if not (r.get('odds') or {}).get('home')]
        brier_odds = _avg([(r.get('metrics') or {}).get('brier') for r in odds_rows])
        brier_no_odds = _avg([(r.get('metrics') or {}).get('brier') for r in no_odds_rows])
        rich_rows = [r for r in sample if (r.get('dataQuality') or {}).get('cls') in ('good', 'warn')]
        est_rows = [r for r in sample if (r.get('dataQuality') or {}).get('cls') == 'weak']
        brier_rich = _avg([(r.get('metrics') or {}).get('brier') for r in rich_rows])
        brier_est = _avg([(r.get('metrics') or {}).get('brier') for r in est_rows])
        over_delta = (_avg(over_actuals) or 0) - (_avg(over_preds) or 0) if over_preds else 0

        # Bayesian blend: prior from historical calibration, obs from predictions
        def blend(prior_val, obs_val):
            if prior_val is None:
                return obs_val
            return (N_PRIOR * prior_val + n * obs_val) / (N_PRIOR + n)

        obs_ha_corr  = 1.0 + (deltas['home'] - deltas['away']) / 1000
        obs_rho      = -0.13 - deltas['draw'] / 400
        prior_ha_corr = cal.get('homeAdvFactor', 1.1) / 1.1 if cal else None
        prior_rho     = cal.get('suggestedRho', -0.13) if cal else None
        prior_o25     = (cal.get('over25Rate', 0.52) - 0.52) * 100 if cal else None

        ha_mult  = round(_clamp(blend(prior_ha_corr, obs_ha_corr), 0.90, 1.15), 3)
        rho_val  = round(_clamp(blend(prior_rho, obs_rho), -0.25, -0.06), 3)
        if over_preds:
            o25_adj = round(_clamp(blend(prior_o25, over_delta / 2), -8, 8), 1)
        elif prior_o25 is not None:
            o25_adj = round(_clamp(prior_o25, -8, 8), 1)
        else:
            o25_adj = 0.0

        return {
            'graded': n,
            'ready': bool(cal) or n >= min_ready,
            'prior_source': 'historical+observed' if cal else 'observed',
            'bias': {
                'home': round(_clamp(deltas['home'] / 500, -0.04, 0.04), 4),
                'draw': round(_clamp(deltas['draw'] / 500, -0.04, 0.04), 4),
                'away': round(_clamp(deltas['away'] / 500, -0.04, 0.04), 4),
            },
            'homeAdvMultiplier': ha_mult,
            'dcRho': rho_val,
            'oddsWeightScale': round(_clamp(1 + ((brier_no_odds or brier_all or 0) - (brier_odds or brier_all or 0)) * 0.35, 0.85, 1.25), 3),
            'richDataWeightScale': round(_clamp(1 + ((brier_est or brier_all or 0) - (brier_rich or brier_all or 0)) * 0.25, 0.9, 1.2), 3),
            'over25Adjustment': o25_adj,
            'diagnostics': {
                'avgBrier': round(brier_all, 4) if brier_all is not None else None,
                'oddsRows': len(odds_rows),
                'richRows': len(rich_rows),
                'over25Rows': len(over_preds),
                'deltas': {k: round(v, 1) for k, v in deltas.items()},
            }
        }

    leagues = {}
    for comp in sorted({r.get('competition') for r in graded if r.get('competition')}):
        sample = [r for r in graded if r.get('competition') == comp]
        leagues[comp] = build(sample, comp=comp, min_ready=25)
    return {'global': build(graded, comp=None, min_ready=15), 'leagues': leagues}

def _data_path(path):
    if os.path.isabs(path):
        return path
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)

def _load_json_file(path, default):
    path = _data_path(path)
    if not os.path.exists(path):
        return default
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f'  [DATA] Load failed {path}: {e}')
        return default

def _save_json_file(path, data):
    path = _data_path(path)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f'  [DATA] Save failed {path}: {e}')

def _odds_game_key(comp, game):
    return '|'.join(str(x or '') for x in (
        comp, game.get('id'), game.get('home'), game.get('away'), game.get('commence_time')
    ))

def _record_odds_snapshot(comp, games):
    if not games:
        return games
    hist = _load_json_file(ODDS_HISTORY_FILE, {})
    now = _dt.datetime.now(_dt.timezone.utc).isoformat()
    out = []
    for game in games:
        g = dict(game)
        key = _odds_game_key(comp, g)
        rows = hist.get(key, [])
        prev = rows[-1] if rows else None
        snap = {
            'ts': now,
            'h': g.get('best_h'), 'd': g.get('best_d'), 'a': g.get('best_a'),
            'o25': g.get('best_o25'), 'u25': g.get('best_u25'),
        }
        movement = {}
        if prev:
            for src_key, label in [('h', 'home'), ('d', 'draw'), ('a', 'away'), ('o25', 'over25'), ('u25', 'under25')]:
                old, new = prev.get(src_key), snap.get(src_key)
                if old and new:
                    movement[label] = round(new - old, 3)
        if movement:
            g['odds_movement'] = movement
        rows.append(snap)
        hist[key] = rows[-20:]
        out.append(g)
    _save_json_file(ODDS_HISTORY_FILE, hist)
    return out

# ── API-Football Pro ──────────────────────────────────────────────────────────
def apif_get(endpoint, params=None):
    """Fetch from API-Football with caching. Returns response list or None."""
    if not APIF_KEY: return None
    if params is None: params = {}
    qs = '&'.join(f'{k}={v}' for k, v in sorted(params.items()))
    cache_key = f'/apif/{endpoint}?{qs}'
    cached = get_cache(cache_key)
    if cached is not None: return cached
    url = f'{APIF_BASE}/{endpoint}?{qs}'
    try:
        ctx = _ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={'x-apisports-key': APIF_KEY, 'User-Agent': 'Scoutline/2.0'})
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            raw = json.loads(r.read())
            errs = raw.get('errors', {})
            if errs and errs != []:
                stale = get_stale_cache(cache_key)
                print(f'  [APIF] {endpoint} error: {errs}{"; using stale cache" if stale is not None else ""}')
                return stale
            resp = raw.get('response', [])
            set_cache(cache_key, resp)
            rem = r.headers.get('x-ratelimit-requests-remaining', '?')
            print(f'  [APIF] {endpoint}({qs[:60]}): {len(resp)} results, rem={rem}')
            return resp
    except Exception as e:
        stale = get_stale_cache(cache_key)
        print(f'  [APIF] {endpoint} failed: {e}{"; using stale cache" if stale is not None else ""}')
        return stale

def _apif_status(short):
    if short in ('NS', 'TBD'):             return 'SCHEDULED'
    if short in ('FT', 'AET', 'PEN'):      return 'FINISHED'
    if short in ('HT',):                   return 'PAUSED'
    if short in ('1H','2H','ET','BT','P'): return 'IN_PLAY'
    if short in ('PST',):                  return 'POSTPONED'
    if short in ('CANC',):                 return 'CANCELLED'
    return 'UNKNOWN'

def _apif_to_matches(raw):
    """Translate API-Football fixtures list → football-data.org matches shape."""
    matches = []
    for fx in (raw or []):
        f = fx.get('fixture', {}); tms = fx.get('teams', {})
        league = fx.get('league') or {}
        gl = fx.get('goals', {}); ht = (fx.get('score') or {}).get('halftime') or {}
        matches.append({
            'id': f.get('id'), 'utcDate': f.get('date', ''),
            'status': _apif_status(f.get('status', {}).get('short', 'NS')),
            'matchday': league.get('round', ''),
            'competition': {'id': league.get('id'), 'name': league.get('name', '')},
            'homeTeam': {'id': tms.get('home',{}).get('id'), 'name': tms.get('home',{}).get('name',''),
                         'shortName': tms.get('home',{}).get('name','')},
            'awayTeam': {'id': tms.get('away',{}).get('id'), 'name': tms.get('away',{}).get('name',''),
                         'shortName': tms.get('away',{}).get('name','')},
            'score': {'fullTime': {'home': gl.get('home'), 'away': gl.get('away')},
                      'halfTime': {'home': ht.get('home'), 'away': ht.get('away')}},
        })
    return matches

def apif_standings(comp):
    """Fetch standings in football-data.org shape."""
    info = APIF_LEAGUE_MAP.get(comp)
    if not info: return None
    data = apif_get('standings', {'league': info['id'], 'season': info['season']})
    if not data: return None
    try: rows = data[0]['league']['standings'][0]
    except (IndexError, KeyError, TypeError): return None
    table = []
    for r in rows:
        table.append({
            'position': r.get('rank'), 'points': r.get('points', 0),
            'team': {'id': r['team']['id'], 'name': r['team']['name'], 'shortName': r['team']['name']},
            'playedGames': r['all']['played'], 'won': r['all']['win'],
            'draw': r['all']['draw'], 'lost': r['all']['lose'],
            'goalsFor': r['all']['goals']['for'], 'goalsAgainst': r['all']['goals']['against'],
            'goalDifference': r.get('goalsDiff', 0), 'form': r.get('form', ''),
        })
    league = data[0].get('league', {})
    return {'standings': [{'table': table}],
            'competition': {'name': league.get('name', comp), 'code': comp},
            'season': {'year': info['season']}}

def apif_matches(comp, status='', date_from=None, date_to=None, home_team=None, away_team=None, limit=None):
    """Fetch fixtures in football-data.org matches shape."""
    info = APIF_LEAGUE_MAP.get(comp)
    if not info: return None
    params = {'league': info['id'], 'season': info['season']}
    if home_team and away_team:
        h2h_params = {'h2h': f'{home_team}-{away_team}'}
        if limit: h2h_params['last'] = limit
        data = apif_get('fixtures/headtohead', h2h_params)
        if data is None: return None
        matches = _apif_to_matches(data)
    elif status == 'SCHEDULED':
        params['next'] = 20
        data = apif_get('fixtures', params)
        if data is None: return None
        matches = [m for m in _apif_to_matches(data) if m['status'] == 'SCHEDULED']
    elif status == 'FINISHED':
        params['status'] = 'FT'
        try:
            params['last'] = max(1, min(100, int(limit or 100)))
        except (TypeError, ValueError):
            params['last'] = 100
        data = apif_get('fixtures', params)
        if not data and params.get('last') != 50:
            fallback_params = dict(params)
            fallback_params['last'] = 50
            data = apif_get('fixtures', fallback_params)
        if data is None: return None
        matches = _apif_to_matches(data)
    else:
        if date_from: params['from'] = date_from
        if date_to:   params['to']   = date_to
        data = apif_get('fixtures', params)
        if data is None: return None
        matches = _apif_to_matches(data)
    for m in matches:
        m['competition'] = {
            **(m.get('competition') or {}),
            'code': comp,
            'requestedLeagueId': info['id'],
        }
    return {'matches': matches, 'count': len(matches),
            'competition': {'code': comp, 'id': info['id']}, 'season': {'year': info['season']}}

def build_teamstats(comp):
    """Build per-team stats from API-Football fixture statistics."""
    cp = f'/teamstats/{comp}'
    existing = get_cache(cp)
    if existing and existing.get('teams'):
        first_team = next(iter((existing.get('teams') or {}).values()), {})
        if existing.get('leagueAverages') and 'cornAllowedHomePg' in first_team:
            with teamstats_lock: teamstats_status[comp] = 'ready'; return
        delete_cache(cp)
        existing = None
    with teamstats_lock:
        if teamstats_status.get(comp) == 'building': return
        teamstats_status[comp] = 'building'

    def worker():
        info = APIF_LEAGUE_MAP.get(comp)
        if not info:
            with teamstats_lock: teamstats_status[comp] = 'unavailable'; return
        print(f'  [TS] {comp}: fetching (league={info["id"]}, season={info["season"]})…')
        std = apif_standings(comp)
        if not std or not std.get('standings'):
            with teamstats_lock: teamstats_status[comp] = 'unavailable'; return
        teams_in_league = ((std.get('standings') or [{}])[0].get('table') or [])
        finished = apif_get('fixtures', {'league': info['id'], 'season': info['season'],
                                         'status': 'FT', 'last': APIF_TEAMSTAT_MATCHES}) or []
        if not finished:
            with teamstats_lock: teamstats_status[comp] = 'unavailable'; return

        per_team = {}
        def ensure_team(tid, tname):
            key = str(tid)
            if key not in per_team:
                per_team[key] = {
                    'name': tname, 'games': 0,
                    'xg': 0.0, 'xga': 0.0, 'xg_n': 0,
                    'sot': 0.0, 'sot_n': 0, 'shots': 0.0, 'shots_n': 0,
                    'corn': 0.0, 'corn_n': 0, 'fouls': 0.0, 'fouls_n': 0,
                    'poss': 0.0, 'poss_n': 0, 'saves': 0.0, 'saves_n': 0,
                    'yc': 0.0, 'yc_n': 0, 'rc': 0.0, 'rc_n': 0,
                    'goals_f': 0.0, 'goals_a': 0.0,
                    'home_games': 0, 'away_games': 0,
                    'home_gf': 0.0, 'home_ga': 0.0,
                    'away_gf': 0.0, 'away_ga': 0.0,
                    'home_xg': 0.0, 'home_xga': 0.0, 'home_xg_n': 0,
                    'away_xg': 0.0, 'away_xga': 0.0, 'away_xg_n': 0,
                    'home_sot': 0.0, 'home_sot_n': 0, 'away_sot': 0.0, 'away_sot_n': 0,
                    'home_corn': 0.0, 'home_corn_n': 0, 'away_corn': 0.0, 'away_corn_n': 0,
                    'home_corna': 0.0, 'home_corna_n': 0, 'away_corna': 0.0, 'away_corna_n': 0,
                    'home_yc': 0.0, 'home_yc_n': 0, 'away_yc': 0.0, 'away_yc_n': 0,
                    'ht_g': 0.0, 'ht_n': 0, 'clean_sheets': 0,
                    'recent': [],
                    'ref_cards': 0.0, 'ref_games': 0,
                }
            return per_team[key]

        referees = {}

        for fx in finished:
            fixture = fx.get('fixture') or {}
            teams   = fx.get('teams')   or {}
            score   = fx.get('score')   or {}
            goals   = fx.get('goals')   or {}
            hid     = (teams.get('home') or {}).get('id')
            aid     = (teams.get('away') or {}).get('id')
            hname   = (teams.get('home') or {}).get('name', '')
            aname   = (teams.get('away') or {}).get('name', '')
            if not hid or not aid: continue
            hentry = ensure_team(hid, hname)
            aentry = ensure_team(aid, aname)
            hentry['games'] += 1; aentry['games'] += 1
            hg = goals.get('home') or 0; ag = goals.get('away') or 0
            hentry['goals_f'] += hg; hentry['goals_a'] += ag
            aentry['goals_f'] += ag; aentry['goals_a'] += hg
            hentry['home_games'] += 1; hentry['home_gf'] += hg; hentry['home_ga'] += ag
            aentry['away_games'] += 1; aentry['away_gf'] += ag; aentry['away_ga'] += hg
            if ag == 0: hentry['clean_sheets'] += 1
            if hg == 0: aentry['clean_sheets'] += 1
            ht = score.get('halftime') or {}
            if ht.get('home') is not None and ht.get('away') is not None:
                hentry['ht_g'] += ht.get('home') or 0; aentry['ht_g'] += ht.get('away') or 0
                hentry['ht_n'] += 1;                   aentry['ht_n'] += 1

            stat_rows = apif_get('fixtures/statistics', {'fixture': fixture.get('id')}) or []
            stats_by_team = {}
            for row in stat_rows:
                tid = str((row.get('team') or {}).get('id') or '')
                stats = {item.get('type'): item.get('value') for item in (row.get('statistics') or [])}
                stats_by_team[tid] = stats

            def apply(entry, own, opp, venue):
                xg   = _stat_num(own.get('expected_goals'))
                xga  = _stat_num(opp.get('expected_goals'))
                sot  = _stat_num(own.get('Shots on Goal'))
                shts = _stat_num(own.get('Total Shots'))
                corn = _stat_num(own.get('Corner Kicks'))
                corna = _stat_num(opp.get('Corner Kicks'))
                foul = _stat_num(own.get('Fouls'))
                poss = _stat_num(own.get('Ball Possession'))
                svs  = _stat_num(own.get('Goalkeeper Saves'))
                yc   = _stat_num(own.get('Yellow Cards'))
                rc   = _stat_num(own.get('Red Cards'))
                if xg   is not None: entry['xg']    += xg;  entry['xg_n']    += 1
                if xga  is not None: entry['xga']   += xga
                if sot  is not None: entry['sot']   += sot; entry['sot_n']   += 1
                if shts is not None: entry['shots'] += shts;entry['shots_n'] += 1
                if corn is not None: entry['corn']  += corn;entry['corn_n']  += 1
                if foul is not None: entry['fouls'] += foul;entry['fouls_n'] += 1
                if poss is not None: entry['poss']  += poss;entry['poss_n']  += 1
                if svs  is not None: entry['saves'] += svs; entry['saves_n'] += 1
                if yc   is not None: entry['yc']    += yc;  entry['yc_n']   += 1
                if rc   is not None: entry['rc']    += rc;  entry['rc_n']   += 1
                if venue == 'home':
                    if xg  is not None: entry['home_xg'] += xg; entry['home_xg_n'] += 1
                    if xga is not None: entry['home_xga'] += xga
                    if sot is not None: entry['home_sot'] += sot; entry['home_sot_n'] += 1
                    if corn is not None: entry['home_corn'] += corn; entry['home_corn_n'] += 1
                    if corna is not None: entry['home_corna'] += corna; entry['home_corna_n'] += 1
                    if yc is not None: entry['home_yc'] += yc; entry['home_yc_n'] += 1
                else:
                    if xg  is not None: entry['away_xg'] += xg; entry['away_xg_n'] += 1
                    if xga is not None: entry['away_xga'] += xga
                    if sot is not None: entry['away_sot'] += sot; entry['away_sot_n'] += 1
                    if corn is not None: entry['away_corn'] += corn; entry['away_corn_n'] += 1
                    if corna is not None: entry['away_corna'] += corna; entry['away_corna_n'] += 1
                    if yc is not None: entry['away_yc'] += yc; entry['away_yc_n'] += 1
                entry['recent'].append({
                    'fixtureId': fixture.get('id'), 'date': fixture.get('date'), 'venue': venue,
                    'gf': hg if venue == 'home' else ag,
                    'ga': ag if venue == 'home' else hg,
                    'xg': xg, 'xga': xga, 'sot': sot, 'shots': shts,
                    'corners': corn, 'cornersAllowed': corna, 'yellowCards': yc, 'redCards': rc,
                })

            apply(hentry, stats_by_team.get(str(hid), {}), stats_by_team.get(str(aid), {}), 'home')
            apply(aentry, stats_by_team.get(str(aid), {}), stats_by_team.get(str(hid), {}), 'away')

            ref = (fixture.get('referee') or '').split(',')[0].strip()
            hy = _stat_num((stats_by_team.get(str(hid), {}) or {}).get('Yellow Cards')) or 0
            ay = _stat_num((stats_by_team.get(str(aid), {}) or {}).get('Yellow Cards')) or 0
            hr = _stat_num((stats_by_team.get(str(hid), {}) or {}).get('Red Cards')) or 0
            ar = _stat_num((stats_by_team.get(str(aid), {}) or {}).get('Red Cards')) or 0
            if ref:
                refs = referees.setdefault(ref, {'games': 0, 'cards': 0.0})
                refs['games'] += 1
                refs['cards'] += hy + ay + hr + ar
                hentry['ref_cards'] += hy + ay + hr + ar; hentry['ref_games'] += 1
                aentry['ref_cards'] += hy + ay + hr + ar; aentry['ref_games'] += 1

        def avg(total, n): return round(total / n, 2) if n else None
        def avg1(total, n): return round(total / n, 1) if n else None
        def recent_avg(items, key, n=5):
            vals = [_stat_num(x.get(key)) for x in sorted(items, key=lambda x: x.get('date') or '', reverse=True)[:n]]
            vals = [v for v in vals if v is not None]
            return round(sum(vals) / len(vals), 2) if vals else None

        summary = {}; name_map = {}
        for row in teams_in_league:
            team = row['team']; tid = str(team['id']); tname = team['name']
            s = per_team.get(tid) or ensure_team(tid, tname)
            g = max(1, s['games'])
            recent = sorted(s.get('recent') or [], key=lambda x: x.get('date') or '', reverse=True)[:5]
            entry = {
                'name':        tname,       'games':      g,
                'xg_pg':       avg(s['xg'],   s['xg_n']),
                'xga_pg':      avg(s['xga'],  s['xg_n']),
                'sotPg':       avg(s['sot'],  s['sot_n']),
                'shotsPg':     avg(s['shots'],s['shots_n']),
                'cornPg':      avg(s['corn'], s['corn_n']),
                'ycardPg':     avg(s['yc'],   s['yc_n']),
                'rcardPg':     avg(s['rc'],   s['rc_n']),
                'cardPg':      avg(s['yc']+s['rc'], g) if (s['yc_n'] or s['rc_n']) else None,
                'foulPg':      avg(s['fouls'],s['fouls_n']),
                'possPg':      avg1(s['poss'],s['poss_n']),
                'savesPg':     avg(s['saves'],s['saves_n']),
                'goals_pg':    round(s['goals_f'] / g, 2),
                'goals_ag_pg': round(s['goals_a'] / g, 2),
                'gfHomePg':    round(s['home_gf'] / s['home_games'], 2) if s['home_games'] else None,
                'gaHomePg':    round(s['home_ga'] / s['home_games'], 2) if s['home_games'] else None,
                'gfAwayPg':    round(s['away_gf'] / s['away_games'], 2) if s['away_games'] else None,
                'gaAwayPg':    round(s['away_ga'] / s['away_games'], 2) if s['away_games'] else None,
                'xgHomePg':    avg(s['home_xg'], s['home_xg_n']),
                'xgaHomePg':   avg(s['home_xga'], s['home_xg_n']),
                'xgAwayPg':    avg(s['away_xg'], s['away_xg_n']),
                'xgaAwayPg':   avg(s['away_xga'], s['away_xg_n']),
                'sotHomePg':   avg(s['home_sot'], s['home_sot_n']),
                'sotAwayPg':   avg(s['away_sot'], s['away_sot_n']),
                'cornHomePg':  avg(s['home_corn'], s['home_corn_n']),
                'cornAwayPg':  avg(s['away_corn'], s['away_corn_n']),
                'cornAllowedHomePg': avg(s['home_corna'], s['home_corna_n']),
                'cornAllowedAwayPg': avg(s['away_corna'], s['away_corna_n']),
                'ycHomePg':    avg(s['home_yc'], s['home_yc_n']),
                'ycAwayPg':    avg(s['away_yc'], s['away_yc_n']),
                'last5': {
                    'games': len(recent),
                    'gfPg': recent_avg(recent, 'gf'),
                    'gaPg': recent_avg(recent, 'ga'),
                    'xgPg': recent_avg(recent, 'xg'),
                    'xgaPg': recent_avg(recent, 'xga'),
                    'sotPg': recent_avg(recent, 'sot'),
                    'cornPg': recent_avg(recent, 'corners'),
                    'cornAllowedPg': recent_avg(recent, 'cornersAllowed'),
                    'ycardPg': recent_avg(recent, 'yellowCards'),
                },
                'refCardPg':   avg(s['ref_cards'], s['ref_games']),
                'htGpg':       avg(s['ht_g'],  s['ht_n']),
                'csRate':      round(s['clean_sheets'] / g * 100, 1),
                'hasStats':    bool(s['xg_n'] or s['sot_n'] or s['corn_n'] or s['yc_n']),
            }
            summary[tid]    = entry
            name_map[tname] = entry
            print(f'  [TS] {comp}: {tname} — xG/g={entry["xg_pg"]}, SoT/g={entry["sotPg"]}, YC/g={entry["ycardPg"]}')

        def avg_entries(key):
            vals = [_stat_num(v.get(key)) for v in summary.values()]
            vals = [v for v in vals if v is not None]
            return round(sum(vals) / len(vals), 2) if vals else None

        finished_count = max(1, len(finished))
        league_averages = {
            'homeGoalsPg': round(sum(((fx.get('goals') or {}).get('home') or 0) for fx in finished) / finished_count, 2),
            'awayGoalsPg': round(sum(((fx.get('goals') or {}).get('away') or 0) for fx in finished) / finished_count, 2),
            'homeConcededPg': round(sum(((fx.get('goals') or {}).get('away') or 0) for fx in finished) / finished_count, 2),
            'awayConcededPg': round(sum(((fx.get('goals') or {}).get('home') or 0) for fx in finished) / finished_count, 2),
            'homeCornersForPg': avg_entries('cornHomePg'),
            'awayCornersForPg': avg_entries('cornAwayPg'),
            'homeCornersAllowedPg': avg_entries('cornAllowedHomePg'),
            'awayCornersAllowedPg': avg_entries('cornAllowedAwayPg'),
            'cardsPg': round(sum(v.get('cards') or 0 for v in referees.values()) / max(1, sum(v.get('games') or 0 for v in referees.values())), 2) if referees else None,
            'matches': len(finished),
        }

        scorers_raw = apif_get('players/topscorers', {'league': info['id'], 'season': info['season']}) or []
        key_scorers = []
        for item in scorers_raw[:20]:
            pl = item.get('player', {}); st = (item.get('statistics') or [{}])[0]
            g  = (st.get('goals') or {}).get('total') or 0
            if g >= 3:
                key_scorers.append({'name': pl.get('name', '?'), 'id': pl.get('id'),
                                    'teamId':   (st.get('team') or {}).get('id'),
                                    'teamName': (st.get('team') or {}).get('name', ''), 'goals': g})

        set_cache(cp, {'competition': comp, 'teams': summary, 'name_map': name_map,
                       'matchesProcessed': len(summary), 'source': 'api-football',
                       'leagueAverages': league_averages,
                       'referees': {k: {'games': v['games'], 'cardsPg': round(v['cards']/max(1, v['games']), 2)}
                                    for k, v in referees.items()},
                       'suspensionRisks': [], 'keyScorers': key_scorers})
        with teamstats_lock: teamstats_status[comp] = 'ready'
        print(f'  [TS] {comp}: done — {len(summary)} teams, {len(key_scorers)} top scorers')
        save_disk_cache()

    threading.Thread(target=worker, daemon=True).start()

# ── Odds Providers ────────────────────────────────────────────────────────────
def _to_odd(value):
    if value in (None, '', '-'): return None
    try:
        odd = float(value)
        return odd if odd > 1 else None
    except (TypeError, ValueError):
        return None

def _best_book(rows, key, odd):
    if not odd: return None
    for row in rows:
        if row.get(key) == odd:
            return row.get('name')
    return None

def _normalize_apif_odds_games(raw_games, fixtures_by_id):
    enriched = []
    for game in (raw_games or []):
        fx = game.get('fixture') or {}
        fixture_id = fx.get('id')
        fixture = fixtures_by_id.get(fixture_id, {})
        home = ((fixture.get('homeTeam') or {}).get('name') or
                fx.get('home') or game.get('home_team') or '')
        away = ((fixture.get('awayTeam') or {}).get('name') or
                fx.get('away') or game.get('away_team') or '')
        if not home or not away:
            continue

        bk_rows = []
        for bk in game.get('bookmakers', []):
            row = {'name': bk.get('name', 'Bookmaker'), 'key': str(bk.get('id', bk.get('name', '')))}
            for bet in bk.get('bets', []):
                bet_name = (bet.get('name') or '').lower()
                bet_id = str(bet.get('id') or '')
                for val in bet.get('values', []):
                    label = str(val.get('value') or '').strip()
                    low = label.lower()
                    odd = _to_odd(val.get('odd'))
                    if not odd:
                        continue
                    if bet_id == '1' or bet_name == 'match winner':
                        if low in ('home', home.lower()):
                            row['h'] = odd
                        elif low == 'draw':
                            row['d'] = odd
                        elif low in ('away', away.lower()):
                            row['a'] = odd
                    elif bet_id == '5' or bet_name == 'goals over/under':
                        if low in ('over 2.5', 'over2.5', 'over 2,5'):
                            row['o25'] = odd
                        elif low in ('under 2.5', 'under2.5', 'under 2,5'):
                            row['u25'] = odd
            if any(row.get(k) for k in ('h', 'd', 'a', 'o25', 'u25')):
                bk_rows.append(row)

        enriched.extend(_enrich_odds_rows({
            'id': fixture_id,
            'commence_time': fx.get('date') or fixture.get('utcDate'),
            'home': home,
            'away': away,
            'bookmakers': bk_rows,
            'source': 'api-football',
        }))
    return enriched

def fetch_apif_odds(comp):
    info = APIF_LEAGUE_MAP.get(comp)
    if not info or not APIF_KEY:
        return []
    cache_key = f'/apif_odds/{comp}/{info["id"]}/{info["season"]}'
    cached = get_cache(cache_key)
    if cached is not None:
        return cached

    fixtures_data = apif_matches(comp, status='SCHEDULED') or {}
    fixtures = fixtures_data.get('matches') or []
    fixtures_by_id = {m.get('id'): m for m in fixtures if m.get('id')}

    raw_games = apif_get('odds', {'league': info['id'], 'season': info['season']}) or []
    games = _normalize_apif_odds_games(raw_games, fixtures_by_id)
    with cache_lock:
        cache[_key(cache_key)] = {'data': games, 'ts': time.time(), 'ttl': ODDS_CACHE_TTL}
    print(f'  [APIF-ODDS] {comp}: {len(games)} games')
    return games

def fetch_odds(sport_key, markets='h2h,totals'):
    ODDS_LAST_ERROR.pop(sport_key, None)
    cache_key = f'/odds_api/{sport_key}/{markets}'
    cached = get_cache(cache_key)
    if cached is not None: return cached
    url = (f'{ODDS_API_BASE}/sports/{sport_key}/odds'
           f'?apiKey={ODDS_API_KEY}&regions={ODDS_REGION}'
           f'&markets={markets}&oddsFormat=decimal')
    try:
        ctx = _ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={'User-Agent': 'Scoutline/2.0'})
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            remaining = r.headers.get('x-requests-remaining', '?')
            used      = r.headers.get('x-requests-used', '?')
            print(f'  [ODDS] {sport_key}: used={used} remaining={remaining}')
            data = json.loads(r.read())
            with cache_lock:
                cache[_key(cache_key)] = {'data': data, 'ts': time.time(), 'ttl': ODDS_CACHE_TTL}
            return data
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', 'ignore')
        try:
            detail = json.loads(body)
            msg = detail.get('message') or detail.get('error_code') or body
            code = detail.get('error_code')
        except Exception:
            msg, code = body or str(e), None
        ODDS_LAST_ERROR[sport_key] = {'status': e.code, 'message': msg, 'code': code}
        print(f'  [ODDS] {sport_key} failed: HTTP {e.code} {msg}')
        return []
    except Exception as e:
        ODDS_LAST_ERROR[sport_key] = {'status': 503, 'message': str(e), 'code': None}
        print(f'  [ODDS] {sport_key} failed: {e}'); return []

def _enrich_odds_rows(game):
    bk_rows = game.get('bookmakers') or []
    hs  = [r['h']   for r in bk_rows if r.get('h')]
    ds  = [r['d']   for r in bk_rows if r.get('d')]
    as_ = [r['a']   for r in bk_rows if r.get('a')]
    o25 = [r['o25'] for r in bk_rows if r.get('o25')]
    u25 = [r['u25'] for r in bk_rows if r.get('u25')]
    odds_h = max(hs)  if hs  else None
    odds_d = max(ds)  if ds  else None
    odds_a = max(as_) if as_ else None
    impl_h = impl_d = impl_a = None
    if odds_h and odds_d and odds_a:
        rh = 1/odds_h; rd = 1/odds_d; ra = 1/odds_a; tot = rh+rd+ra
        impl_h = round(rh/tot*100, 1); impl_d = round(rd/tot*100, 1); impl_a = round(ra/tot*100, 1)
    return [{
        'id': game.get('id'), 'commence_time': game.get('commence_time'),
        'home': game.get('home', ''), 'away': game.get('away', ''),
        'best_h': odds_h, 'best_d': odds_d, 'best_a': odds_a,
        'best_bk_h': _best_book(bk_rows, 'h', odds_h),
        'best_bk_d': _best_book(bk_rows, 'd', odds_d),
        'best_bk_a': _best_book(bk_rows, 'a', odds_a),
        'best_o25': max(o25) if o25 else None, 'best_u25': max(u25) if u25 else None,
        'impl_h': impl_h, 'impl_d': impl_d, 'impl_a': impl_a,
        'bookmakers': bk_rows, 'num_bookmakers': len(bk_rows),
        'source': game.get('source', 'the-odds-api'),
    }]

def _normalize_odds_games(raw_games):
    enriched = []
    for game in (raw_games or []):
        home = game.get('home_team', ''); away = game.get('away_team', '')
        bk_rows = []
        for bk in game.get('bookmakers', []):
            row = {'name': bk['title'], 'key': bk['key']}
            for mkt in bk.get('markets', []):
                if mkt['key'] == 'h2h':
                    for o in mkt['outcomes']:
                        if o['name'] == home:    row['h'] = o['price']
                        elif o['name'] == 'Draw': row['d'] = o['price']
                        else:                    row['a'] = o['price']
                elif mkt['key'] == 'totals':
                    for o in mkt['outcomes']:
                        if o.get('point') == 2.5:
                            if o['name'] == 'Over':  row['o25'] = o['price']
                            if o['name'] == 'Under': row['u25'] = o['price']
            bk_rows.append(row)
        enriched.extend(_enrich_odds_rows({
            'id': game.get('id'), 'commence_time': game.get('commence_time'),
            'home': home, 'away': away, 'bookmakers': bk_rows, 'source': 'the-odds-api',
        }))
    return enriched

def get_normalized_odds(comp):
    games = fetch_apif_odds(comp)
    if games:
        return _record_odds_snapshot(comp, games), 'api-football', None

    sport_key = ODDS_SPORT_KEYS.get(comp)
    if not sport_key:
        return [], 'api-football', {'status': 404, 'message': f'No fallback odds key for {comp}', 'code': None}
    if APIF_KEY and not ODDS_FALLBACK_ENABLED:
        return [], 'api-football', {'status': 404, 'message': 'No API-Football odds found for this league right now. The Odds API fallback is disabled to avoid quota errors; set ODDS_FALLBACK_ENABLED=1 if you want to use it.', 'code': None}
    if not ODDS_API_KEY:
        return [], 'api-football', {'status': 503, 'message': 'No API-Football odds found, and ODDS_API_KEY fallback is not set in .env', 'code': None}

    raw = fetch_odds(sport_key, markets='h2h,totals')
    games = _normalize_odds_games(raw or [])
    err = ODDS_LAST_ERROR.get(sport_key)
    source = 'the-odds-api'
    return _record_odds_snapshot(comp, games), source, err if not games else None

def _advisor_standings_map(comp):
    """Lightweight fallback model data when detailed teamstats are not cached."""
    std = apif_standings(comp)
    rows = ((std or {}).get('standings') or [{}])[0].get('table') or []
    name_map = {}
    for row in rows:
        played = max(1, row.get('playedGames') or 0)
        team = row.get('team') or {}
        entry = {
            'goals_pg': (row.get('goalsFor') or 0) / played,
            'goals_ag_pg': (row.get('goalsAgainst') or 0) / played,
        }
        for name in {team.get('name'), team.get('shortName')}:
            if name:
                name_map[name] = entry
    return name_map

# Leagues the advisor scans by default (all leagues with odds keys)
ADVISOR_LEAGUES = [c for c in APIF_LEAGUE_MAP if c in ODDS_SPORT_KEYS]

# ── HTTP Handler ──────────────────────────────────────────────────────────────
class Handler(SimpleHTTPRequestHandler):
    def _require_auth(self) -> bool:
        if _get_session_from_request(self): return True
        self.send_response(302)
        self.send_header('Location', '/login?err=2')
        self.end_headers(); return False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = parsed.query

        if path == '/health':
            self.send_response(200); self.send_header('Content-Type','text/plain')
            self.end_headers(); self.wfile.write(b'ok'); return

        if path == '/login':
            body = LOGIN_HTML.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers(); self.wfile.write(body); return

        if path == '/logout':
            cookie = self.headers.get('Cookie', '')
            for part in cookie.split(';'):
                part = part.strip()
                if part.startswith('scoutline_session='):
                    SESSIONS.pop(part[len('scoutline_session='):], None)
            self.send_response(302)
            self.send_header('Set-Cookie', 'scoutline_session=; Max-Age=0; Path=/')
            self.send_header('Location', '/login')
            self.end_headers(); return

        if not self._require_auth(): return

        if   path.startswith('/api/'):       self.handle_api(path[4:] + ('?' + qs if qs else ''))
        elif path.startswith('/teamstats/'): self.handle_teamstats(path.split('/')[-1].upper())
        elif path.startswith('/odds/'):      self.handle_odds(path.split('/')[-1].upper())
        elif path.startswith('/schedule/'):  self.handle_schedule(path.split('/')[-1].upper(), qs)
        elif path.startswith('/refresh/'):   self.handle_refresh(path.split('/')[-1].upper())
        elif path.startswith('/players/'):   self.handle_players(path.split('/')[-1].upper())
        elif path.startswith('/injuries/'):  self.handle_injuries(path.split('/')[-1])
        elif path.startswith('/fixture-intel/'): self.handle_fixture_intel(path.split('/')[-1])
        elif path == '/advisor':             self.handle_advisor(qs)
        elif path == '/predictions':         self.handle_predictions(qs)
        elif path == '/calibration':         self.handle_calibration()
        elif path == '/clubelo':             self.send_json(_fetch_clubelo())
        elif path.startswith('/understat/'): self.send_json(_fetch_understat(path.split('/')[-1].upper()))
        elif path == '/config':
            self.send_json({'apif': bool(APIF_KEY),
                            'odds': bool(APIF_KEY or ODDS_API_KEY),
                            'odds_provider': 'api-football' if APIF_KEY else 'the-odds-api',
                            'odds_fallback': ODDS_FALLBACK_ENABLED,
                            'full_data_leagues': list(APIF_LEAGUE_MAP.keys())})
        elif path == '/status':
            with teamstats_lock:
                ts = dict(teamstats_status)
            self.send_json({'ts': ts, 'apif': bool(APIF_KEY),
                            'odds': bool(APIF_KEY or ODDS_API_KEY),
                            'odds_provider': 'api-football' if APIF_KEY else 'the-odds-api',
                            'odds_fallback': ODDS_FALLBACK_ENABLED,
                            'cache_entries': len(cache),
                            'ttl': CACHE_TTL})
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == '/login':
            size = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(size).decode('utf-8')
            params = urllib.parse.parse_qs(body)
            username = params.get('username', [''])[0].strip()
            password = params.get('password', [''])[0]
            if auth_check_user(username, password):
                token = session_create(username)
                self.send_response(302)
                self.send_header('Set-Cookie', f'scoutline_session={token}; Max-Age={SESSION_TTL}; Path=/; HttpOnly; SameSite=Lax')
                self.send_header('Location', '/scoutline.html')
                self.end_headers()
            else:
                self.send_response(302)
                self.send_header('Location', '/login?err=1')
                self.end_headers()
            return

        if not self._require_auth(): return

        if parsed.path != '/predictions':
            self.send_json({'error': f'Unknown route: {parsed.path}'}, 404); return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if size <= 0 or size > 200000:
                self.send_json({'error': 'Invalid prediction payload size'}, 400); return
            payload = json.loads(self.rfile.read(size).decode('utf-8'))
        except Exception as e:
            self.send_json({'error': f'Invalid JSON: {e}'}, 400); return
        self.handle_prediction_create(payload)

    def handle_api(self, api_path):
        if not APIF_KEY:
            self.send_json({'error': 'APIFOOTBALL_KEY not set in .env'}, 503); return
        m = _re.match(r'/competitions/(\w+)/standings', api_path)
        if m:
            result = apif_standings(m.group(1))
            if result: self.send_json(result); return
            self.send_json({'error': f'No standings for {m.group(1)}'}, 404); return
        m = _re.match(r'/competitions/(\w+)/matches', api_path)
        if m:
            comp    = m.group(1)
            qs_str  = api_path.split('?')[1] if '?' in api_path else ''
            qs      = urllib.parse.parse_qs(qs_str)
            result  = apif_matches(comp,
                                   qs.get('status',   [''])[0],
                                   qs.get('dateFrom', [None])[0],
                                   qs.get('dateTo',   [None])[0],
                                   qs.get('homeTeam', [None])[0],
                                   qs.get('awayTeam', [None])[0],
                                   qs.get('limit',    [None])[0])
            if result: self.send_json(result); return
            self.send_json({'error': f'No fixtures for {comp}'}, 404); return
        self.send_json({'error': f'Unknown route: {api_path}'}, 404)

    def handle_prediction_create(self, payload):
        required = ('competition', 'homeTeam', 'awayTeam', 'probabilities', 'predictedScore')
        if not isinstance(payload, dict) or any(k not in payload for k in required):
            self.send_json({'error': 'Missing prediction fields'}, 400); return
        now = _dt.datetime.now(_dt.timezone.utc).isoformat()
        row = {
            'createdAt': payload.get('createdAt') or now,
            'competition': str(payload.get('competition') or '').upper(),
            'fixtureId': payload.get('fixtureId'),
            'kickoff': payload.get('kickoff'),
            'homeTeamId': payload.get('homeTeamId'),
            'awayTeamId': payload.get('awayTeamId'),
            'homeTeam': payload.get('homeTeam'),
            'awayTeam': payload.get('awayTeam'),
            'model': payload.get('model') or 'balanced',
            'homeAdvantage': payload.get('homeAdvantage'),
            'tuning': payload.get('tuning') or {},
            'pick': payload.get('pick'),
            'confidence': payload.get('confidence'),
            'probabilities': payload.get('probabilities') or {},
            'predictedScore': payload.get('predictedScore') or {},
            'markets': payload.get('markets') or {},
            'odds': payload.get('odds') or {},
            'fixtureIntel': payload.get('fixtureIntel'),
            'dataQuality': payload.get('dataQuality') or {},
            'status': 'pending',
        }
        row['id'] = payload.get('id') or _prediction_id(row)
        if not row.get('pick'):
            row['pick'] = _prediction_pick(row)
        with prediction_lock:
            rows = _load_predictions()
            rows.append(row)
            _save_predictions(rows)
        self.send_json({'prediction': row, 'summary': _prediction_summary(rows)}, 201)

    def handle_predictions(self, qs):
        params = urllib.parse.parse_qs(qs or '')
        refresh = params.get('refresh', ['0'])[0].lower() in ('1', 'true', 'yes')
        with prediction_lock:
            rows = _load_predictions()
            if refresh and APIF_KEY:
                if _score_predictions(rows):
                    _save_predictions(rows)
            try:
                limit = int(params.get('limit', [100])[0] or 100)
            except ValueError:
                limit = 100
            limit = max(1, min(500, limit))
            ordered = sorted(rows, key=lambda r: r.get('createdAt', ''), reverse=True)
            self.send_json({'predictions': ordered[:limit], 'summary': _prediction_summary(rows),
                            'file': _prediction_path()})

    def handle_teamstats(self, comp):
        if not APIF_KEY:
            self.send_json({'error': 'APIFOOTBALL_KEY not set', 'teams': {}}, 503); return
        if comp not in APIF_LEAGUE_MAP:
            self.send_json({'status': 'unavailable', 'competition': comp, 'teams': {},
                            'message': f'{comp} not supported'}); return
        cp = f'/teamstats/{comp}'
        c  = get_cache(cp)
        if c and c.get('teams') and c.get('leagueAverages'):
            first_team = next(iter((c.get('teams') or {}).values()), {})
            if 'cornAllowedHomePg' not in first_team:
                delete_cache(cp)
                c = None
        if c and c.get('teams') and c.get('leagueAverages'):
            with teamstats_lock: teamstats_status[comp] = 'ready'
            out = dict(c)
            out['cache'] = cache_meta(cp)
            self.send_json(out); return
        build_teamstats(comp)
        with teamstats_lock: st = teamstats_status.get(comp, 'building')
        self.send_json({'status': st, 'competition': comp, 'teams': {}}, 202)

    def handle_players(self, comp):
        cp = f'/teamstats/{comp}'
        c  = get_cache(cp)
        if not c or not c.get('teams'):
            build_teamstats(comp)
            self.send_json({'status': 'building', 'competition': comp,
                            'suspensionRisks': [], 'keyScorers': []}, 202); return
        self.send_json({'competition': comp,
                        'suspensionRisks': c.get('suspensionRisks', []),
                        'keyScorers':      c.get('keyScorers', [])})

    def handle_injuries(self, fixture_id):
        if not APIF_KEY:
            self.send_json({'error': 'APIFOOTBALL_KEY not set'}); return
        data = apif_get('injuries', {'fixture': fixture_id}) or []
        injured = [{'name':   (i.get('player') or {}).get('name', '?'),
                    'type':   (i.get('player') or {}).get('type', 'Injury'),
                    'reason': (i.get('player') or {}).get('reason', ''),
                    'teamId': (i.get('team')   or {}).get('id')} for i in data]
        self.send_json({'fixture_id': fixture_id, 'injured': injured, 'count': len(injured)})

    def handle_fixture_intel(self, fixture_id):
        if not APIF_KEY:
            self.send_json({'error': 'APIFOOTBALL_KEY not set'}); return
        fixture_rows = apif_get('fixtures', {'id': fixture_id}) or []
        fixture = (fixture_rows[0].get('fixture') if fixture_rows else {}) or {}
        league = (fixture_rows[0].get('league') if fixture_rows else {}) or {}
        referee = (fixture.get('referee') or '').split(',')[0].strip()
        comp = None
        for code, info in APIF_LEAGUE_MAP.items():
            if info.get('id') == league.get('id') and info.get('season') == league.get('season'):
                comp = code
                break
        ref_cards_pg = None
        if comp:
            ts = get_cache(f'/teamstats/{comp}') or get_stale_cache(f'/teamstats/{comp}')
            ref_cards_pg = (((ts or {}).get('referees') or {}).get(referee) or {}).get('cardsPg')
        injuries = apif_get('injuries', {'fixture': fixture_id}) or []
        lineups = apif_get('fixtures/lineups', {'fixture': fixture_id}) or []
        events = apif_get('fixtures/events', {'fixture': fixture_id}) or []
        out_injuries = []
        for item in injuries:
            player = item.get('player') or {}
            out_injuries.append({
                'name': player.get('name', '?'),
                'type': player.get('type') or 'Injury',
                'reason': player.get('reason') or '',
                'teamId': (item.get('team') or {}).get('id'),
                'teamName': (item.get('team') or {}).get('name'),
            })
        lineups_out = []
        for row in lineups:
            team = row.get('team') or {}
            start = row.get('startXI') or []
            subs = row.get('substitutes') or []
            lineups_out.append({
                'teamId': team.get('id'), 'teamName': team.get('name'),
                'formation': row.get('formation'),
                'startXI': len(start), 'substitutes': len(subs),
                'coach': (row.get('coach') or {}).get('name'),
            })
        event_summary = {}
        for ev in events:
            team_id = (ev.get('team') or {}).get('id')
            etype = ev.get('type') or 'Other'
            detail = ev.get('detail') or ''
            row = event_summary.setdefault(str(team_id), {'goals': 0, 'yellowCards': 0, 'redCards': 0, 'penalties': 0})
            if etype == 'Goal':
                row['goals'] += 1
                if 'Penalty' in detail: row['penalties'] += 1
            elif etype == 'Card':
                if 'Red' in detail: row['redCards'] += 1
                elif 'Yellow' in detail: row['yellowCards'] += 1
        self.send_json({'fixture_id': fixture_id, 'referee': referee, 'refereeCardsPg': ref_cards_pg,
                        'competition': comp, 'injuries': out_injuries,
                        'lineups': lineups_out, 'events': event_summary})

    def handle_calibration(self):
        cal = _load_calibration()
        if cal and cal.get('leagues') and not _calibration_is_stale():
            self.send_json(cal); return
        threading.Thread(target=build_league_calibration, daemon=True).start()
        self.send_json(cal or {'leagues': {}, 'building': True})

    def handle_odds(self, comp):
        if comp not in APIF_LEAGUE_MAP:
            self.send_json({'error': f'{comp} is not supported for API-Football odds',
                            'available': list(APIF_LEAGUE_MAP.keys())}, 404); return
        games, source, err = get_normalized_odds(comp)
        if not games and err:
            self.send_json({'error': err['message'], 'error_code': err.get('code'),
                            'competition': comp, 'provider': source,
                            'games': [], 'count': 0}, err.get('status') or 503); return
        info = APIF_LEAGUE_MAP.get(comp) or {}
        self.send_json({'competition': comp, 'provider': source,
                        'sport_key': ODDS_SPORT_KEYS.get(comp),
                        'games': games, 'count': len(games),
                        'cache': cache_meta(f'/apif_odds/{comp}/{info.get("id")}/{info.get("season")}')})

    def handle_refresh(self, comp):
        info = APIF_LEAGUE_MAP.get(comp)
        if not info:
            self.send_json({'error': f'{comp} is not supported'}, 404); return
        removed = []
        paths = [
            f'/teamstats/{comp}',
            f'/apif_odds/{comp}/{info["id"]}/{info["season"]}',
            f'/apif/standings?league={info["id"]}&season={info["season"]}',
            f'/apif/fixtures?league={info["id"]}&next=20&season={info["season"]}',
            f'/apif/fixtures?last=50&league={info["id"]}&season={info["season"]}&status=FT',
            f'/apif/fixtures?last=100&league={info["id"]}&season={info["season"]}&status=FT',
            f'/apif/odds?league={info["id"]}&season={info["season"]}',
        ]
        for p in paths:
            if delete_cache(p):
                removed.append(p)
        with teamstats_lock:
            teamstats_status.pop(comp, None)
        self.send_json({'competition': comp, 'removed': len(removed), 'paths': removed,
                        'message': 'Cache cleared for this league. Reload the tab to fetch fresh data.'})

    def handle_schedule(self, comp, qs):
        if not APIF_KEY:
            self.send_json({'error': 'APIFOOTBALL_KEY not set in .env', 'matches': [], 'buckets': []}, 503); return
        params = urllib.parse.parse_qs(qs or '')
        try:
            days = int(params.get('days', [14])[0])
        except ValueError:
            days = 14
        days = max(1, min(30, days))

        data = apif_matches(comp, status='SCHEDULED')
        if not data:
            self.send_json({'error': f'No schedule for {comp}', 'competition': comp, 'matches': [], 'buckets': []}, 404); return

        now = _dt.datetime.now(_dt.timezone.utc)
        cutoff = now + _dt.timedelta(days=days)
        matches = []
        for m in data.get('matches', []):
            raw_dt = (m.get('utcDate') or '').replace('Z', '+00:00')
            try:
                starts = _dt.datetime.fromisoformat(raw_dt)
                if starts.tzinfo is None:
                    starts = starts.replace(tzinfo=_dt.timezone.utc)
            except Exception:
                continue
            if starts < now or starts > cutoff:
                continue
            day_diff = (starts.date() - now.date()).days
            if day_diff == 0: bucket = 'Today'
            elif day_diff == 1: bucket = 'Tomorrow'
            elif day_diff <= 3: bucket = 'Next 3 days'
            elif day_diff <= 7: bucket = 'This week'
            else: bucket = 'Later'
            item = dict(m)
            item['startsInDays'] = day_diff
            item['bucket'] = bucket
            matches.append(item)

        matches.sort(key=lambda x: x.get('utcDate', ''))
        buckets = []
        for label in ['Today', 'Tomorrow', 'Next 3 days', 'This week', 'Later']:
            rows = [m for m in matches if m.get('bucket') == label]
            if rows:
                buckets.append({'label': label, 'count': len(rows), 'matches': rows})
        self.send_json({'competition': comp, 'days': days, 'count': len(matches),
                        'buckets': buckets, 'matches': matches})

    def handle_advisor(self, qs):
        params   = urllib.parse.parse_qs(qs or '')
        leagues  = [l.strip().upper() for l in
                    params.get('leagues', [','.join(ADVISOR_LEAGUES)])[0].split(',') if l.strip()]
        risk     = params.get('risk', ['balanced'])[0]
        top_n    = int(params.get('top', [30])[0])
        try:
            days = max(1, min(30, int(params.get('days', [3])[0])))
        except ValueError:
            days = 3
        min_edge = {'conservative': 12, 'balanced': 8, 'risky': 5}.get(risk, 8)
        KELLY_CAP = 0.25
        now = _dt.datetime.now(_dt.timezone.utc)
        cutoff = now + _dt.timedelta(days=days)

        if not APIF_KEY and not ODDS_API_KEY:
            self.send_json({'error': 'No odds provider configured — set APIFOOTBALL_KEY first, or ODDS_API_KEY as fallback'}); return
        ODDS_LAST_ERROR.clear()

        def fetch_league(comp):
            games, source, _err = get_normalized_odds(comp)
            return comp, games, source

        league_games = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(fetch_league, c): c for c in leagues}
            for fut in concurrent.futures.as_completed(futs):
                try:
                    comp, games, source = fut.result()
                    league_games[comp] = games
                    print(f'  [ADV] {comp}: {len(games)} games ({source})')
                except Exception as e:
                    print(f'  [ADV] fetch error: {e}')

        bets = []
        for comp, games in league_games.items():
            ts_cache = get_cache(f'/teamstats/{comp}') or {}
            name_map = ts_cache.get('name_map', {})
            if games and not name_map:
                name_map = _advisor_standings_map(comp)
            for game in games:
                if not game.get('impl_h'): continue
                raw_dt = (game.get('commence_time') or '').replace('Z', '+00:00')
                try:
                    starts = _dt.datetime.fromisoformat(raw_dt)
                    if starts.tzinfo is None:
                        starts = starts.replace(tzinfo=_dt.timezone.utc)
                    if starts < now or starts > cutoff:
                        continue
                except Exception:
                    pass
                home  = game['home']; away = game['away']
                hdata = _fuzzy_match(home, name_map)
                adata = _fuzzy_match(away, name_map)
                if not hdata or not adata: continue
                h_xg  = hdata.get('xg_pg')  or (hdata.get('sotPg') or 0)*0.30 or hdata.get('goals_pg')  or 1.2
                h_xga = hdata.get('xga_pg') or hdata.get('goals_ag_pg') or 1.1
                a_xg  = adata.get('xg_pg')  or (adata.get('sotPg') or 0)*0.30 or adata.get('goals_pg')  or 1.0
                a_xga = adata.get('xga_pg') or adata.get('goals_ag_pg') or 1.2
                lh    = max(0.2, ((h_xg + a_xga) / 2) * 1.10)
                la    = max(0.2,  (a_xg + h_xga) / 2)
                ph, pd, pa = _match_probs(lh, la)
                po25  = _over25_prob(lh, la)
                dt    = game.get('commence_time', '')
                outcomes = [
                    ('Home win', game['best_h'], game.get('best_bk_h'), ph, (game['impl_h'] or 0)/100),
                    ('Draw',     game['best_d'], game.get('best_bk_d'), pd, (game['impl_d'] or 0)/100),
                    ('Away win', game['best_a'], game.get('best_bk_a'), pa, (game['impl_a'] or 0)/100),
                ]
                if game.get('best_o25'):
                    impl25 = (1/game['best_o25']) if game['best_o25'] > 1 else 0.5
                    outcomes.append(('Over 2.5', game['best_o25'], None, po25, impl25))
                for label, odds, bk, model_p, impl_p in outcomes:
                    if not odds or odds <= 1 or not model_p or not impl_p: continue
                    edge = round(model_p*100 - impl_p*100, 1)
                    if edge < min_edge: continue
                    ev = model_p*(odds - 1) - (1 - model_p)
                    if ev <= 0: continue
                    b  = odds - 1
                    kf = max(0.0, min(KELLY_CAP, (b*model_p - (1 - model_p)) / b))
                    bets.append({
                        'comp': comp, 'match': f'{home} vs {away}',
                        'home': home, 'away': away, 'date': dt,
                        'label': label, 'odds': odds, 'bk': bk,
                        'modelPct': round(model_p*100, 1), 'implPct': round(impl_p*100, 1),
                        'edge': edge, 'ev': round(ev, 4), 'kellyFrac': round(kf, 4),
                    })
        bets.sort(key=lambda x: -x['ev'])
        if not bets and ODDS_LAST_ERROR:
            first = next(iter(ODDS_LAST_ERROR.values()))
            self.send_json({'error': first['message'], 'error_code': first.get('code'),
                            'bets': [], 'total': 0, 'leagues_scanned': len(league_games),
                            'risk': risk, 'min_edge': min_edge}, first.get('status') or 503); return
        self.send_json({'bets': bets[:top_n], 'total': len(bets),
                        'leagues_scanned': len(league_games), 'risk': risk,
                        'min_edge': min_edge, 'days': days})

    def send_json(self, data, code=200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        try:
            status = args[1] if len(args) > 1 else '?'; p = getattr(self, 'path', None) or '?'
            if   p.startswith('/teamstats/'): print(f'  [TS]   {p}  {status}')
            elif p.startswith('/players/'):   print(f'  [PLYR] {p}  {status}')
            elif p.startswith('/fixture-intel/'): print(f'  [INTEL] {p}  {status}')
            elif p.startswith('/odds/'):      print(f'  [ODDS] {p}  {status}')
            elif p.startswith('/advisor'):    print(f'  [ADV]  {p}  {status}')
            elif p.startswith('/predictions'):print(f'  [PRED] {p}  {status}')
            elif p.startswith('/api/'):       print(f'  [API]  {p[4:]}  {status}')
            elif not p.endswith(('.ico', '.map')): print(f'  [WEB]  {p}  {status}')
        except Exception:
            pass

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    _init_auth_db()
    load_disk_cache(); atexit.register(save_disk_cache)
    apif_status = 'configured' if APIF_KEY else 'missing - set APIFOOTBALL_KEY in .env'
    odds_status = (
        'API-Football primary' + (' + fallback' if ODDS_FALLBACK_ENABLED and ODDS_API_KEY else '')
        if APIF_KEY else ('The Odds API fallback' if ODDS_API_KEY else 'missing odds provider')
    )
    print(f'''
  ============================================================
  Scoutline - API-Football Pro
  ------------------------------------------------------------
  Open:    http://localhost:{PORT}/scoutline.html
  APIF:    {apif_status}
  ODDS:    {odds_status}
  Leagues: {len(APIF_LEAGUE_MAP)} supported
  ============================================================
''')
    def startup():
        time.sleep(2)
        ts = get_cache('/teamstats/PL')
        if ts and ts.get('teams'):
            with teamstats_lock: teamstats_status['PL'] = 'ready'
            print(f'  [TS] PL from disk ({len(ts["teams"])} teams)')
        elif APIF_KEY:
            build_teamstats('PL')
        if FD_KEY and _calibration_is_stale():
            threading.Thread(target=build_league_calibration, daemon=True).start()
        elif not FD_KEY:
            print('  [CAL] FOOTBALL_DATA_KEY not set — skipping calibration build')
    threading.Thread(target=startup, daemon=True).start()
    server = ThreadingHTTPServer(('', PORT), Handler)
    try: server.serve_forever()
    except KeyboardInterrupt: print('\n  Stopped.')

