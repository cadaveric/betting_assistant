"""
MLB Baseball adapter.
Uses pybaseball (wraps Baseball Reference / FanGraphs, free, no key).
pip install pybaseball
"""
import datetime as _dt, math, json, urllib.request

try:
    import pybaseball as pb
    pb.cache.enable()
    MLB_AVAILABLE = True
except ImportError:
    MLB_AVAILABLE = False

def _current_mlb_season():
    """MLB season runs Apr-Oct. Returns current calendar year."""
    return _dt.date.today().year

MLB_SEASON = _current_mlb_season()

LEAGUE_MAP = {
    'MLB': {'id': 'MLB', 'name': 'MLB', 'season': MLB_SEASON},
}

_standings_cache = None
_standings_cache_ts = 0.0
_batting_cache = None
_pitching_cache = None
_official_batting_cache = None
_official_pitching_cache = None
_official_cache_ts = 0.0
_CACHE_TTL = 4 * 3600  # 4 hours

_TEAM_ABBR = {
    'ARI':'Arizona Diamondbacks','ATL':'Atlanta Braves','BAL':'Baltimore Orioles',
    'BOS':'Boston Red Sox','CHC':'Chicago Cubs','CWS':'Chicago White Sox',
    'CIN':'Cincinnati Reds','CLE':'Cleveland Guardians','COL':'Colorado Rockies',
    'DET':'Detroit Tigers','HOU':'Houston Astros','KC':'Kansas City Royals',
    'LAA':'Los Angeles Angels','LAD':'Los Angeles Dodgers','MIA':'Miami Marlins',
    'MIL':'Milwaukee Brewers','MIN':'Minnesota Twins','NYM':'New York Mets',
    'NYY':'New York Yankees','ATH':'Athletics','OAK':'Athletics',
    'PHI':'Philadelphia Phillies','PIT':'Pittsburgh Pirates','SD':'San Diego Padres',
    'SEA':'Seattle Mariners','SF':'San Francisco Giants','STL':'St. Louis Cardinals',
    'TB':'Tampa Bay Rays','TEX':'Texas Rangers','TOR':'Toronto Blue Jays',
    'WSH':'Washington Nationals','WAS':'Washington Nationals',
}

def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def _norm_team(team):
    s = str(team or '').strip()
    return _TEAM_ABBR.get(s.upper(), s)

def _official_team_stats(group):
    """Official MLB Stats API fallback; avoids FanGraphs 403s from pybaseball."""
    global _official_batting_cache, _official_pitching_cache, _official_cache_ts
    import time as _time
    stale = (_time.time() - _official_cache_ts) > _CACHE_TTL
    if group == 'hitting' and _official_batting_cache is not None and not stale:
        return _official_batting_cache
    if group == 'pitching' and _official_pitching_cache is not None and not stale:
        return _official_pitching_cache
    url = f'https://statsapi.mlb.com/api/v1/teams/stats?season={MLB_SEASON}&group={group}&stats=season&sportIds=1'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Scoutline/2.0'})
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = json.loads(r.read())
        out = {}
        for split in ((raw.get('stats') or [{}])[0].get('splits') or []):
            team = split.get('team') or {}
            name = team.get('name') or ''
            stat = split.get('stat') or {}
            if not name:
                continue
            games = max(1, int(_safe_float(stat.get('gamesPlayed'), 1)))
            if group == 'hitting':
                ops = _safe_float(stat.get('ops'), 0.720)
                row = {
                    'avg': round(_safe_float(stat.get('avg'), 0.245), 3),
                    'obp': round(_safe_float(stat.get('obp'), 0.315), 3),
                    'slg': round(_safe_float(stat.get('slg'), 0.405), 3),
                    'ops': round(ops, 3),
                    'runs_pg': round(_safe_float(stat.get('runs'), 0) / games, 2),
                    'hr_pg': round(_safe_float(stat.get('homeRuns'), 0) / games, 2),
                    'wrc_plus': int(max(70, min(135, round(100 * ops / 0.720)))),
                    'source': 'mlb-stats-api',
                }
            else:
                row = {
                    'era': round(_safe_float(stat.get('era'), 4.5), 2),
                    'whip': round(_safe_float(stat.get('whip'), 1.3), 2),
                    'avg_allowed': round(_safe_float(stat.get('avg'), 0.245), 3),
                    'runs_allowed_pg': round(_safe_float(stat.get('runs'), 0) / games, 2),
                    'hr_allowed_pg': round(_safe_float(stat.get('homeRuns'), 0) / games, 2),
                    'source': 'mlb-stats-api',
                }
            out[name] = row
            out[str(team.get('id') or '')] = row
        if group == 'hitting':
            _official_batting_cache = out
        else:
            _official_pitching_cache = out
        _official_cache_ts = _time.time()
        return out
    except Exception as e:
        print(f'  [MLB] official {group} stats error: {e}')
        return {}

def get_standings():
    """Return combined AL + NL standings (refreshed every 4 hours)."""
    global _standings_cache, _standings_cache_ts
    import time as _time
    if _standings_cache is not None and (_time.time() - _standings_cache_ts) < _CACHE_TTL:
        return _standings_cache
    if not MLB_AVAILABLE:
        return []
    try:
        raw = pb.standings(MLB_SEASON)
        result = []
        division_names = [
            'AL East','AL Central','AL West',
            'NL East','NL Central','NL West',
        ]
        for div_idx, div_df in enumerate(raw):
            div_name = division_names[div_idx] if div_idx < len(division_names) else f'Div {div_idx}'
            for _, row in div_df.iterrows():
                team = str(row.get('Tm', '')).strip()
                if not team or team == 'Tm': continue
                w = int(_safe_float(row.get('W', 0)))
                l = int(_safe_float(row.get('L', 0)))
                result.append({
                    'team':   team,
                    'name':   team,
                    'wins':   w,
                    'losses': l,
                    'pct':    round(w / max(1, w+l), 3),
                    'gb':     str(row.get('GB', '–')),
                    'rs_pg':  round(_safe_float(row.get('RS', 0)) / max(1, w+l), 2),
                    'ra_pg':  round(_safe_float(row.get('RA', 0)) / max(1, w+l), 2),
                    'div':    div_name,
                    'conf':   'AL' if div_name.startswith('AL') else 'NL',
                })
        _standings_cache = sorted(result, key=lambda x: -x['pct'])
        _standings_cache_ts = _time.time()
        return _standings_cache
    except Exception as e:
        print(f'  [MLB] standings error: {e}')
        return []

def get_team_batting(team):
    """Return team batting stats (wOBA, wRC+, ISO)."""
    official = _official_team_stats('hitting')
    key = _norm_team(team)
    if official.get(key):
        return official[key]
    global _batting_cache
    if _batting_cache is None and MLB_AVAILABLE:
        try:
            _batting_cache = pb.team_batting(MLB_SEASON)
        except Exception as e:
            print(f'  [MLB] batting error: {e}')
    if _batting_cache is None:
        return {}
    try:
        df = _batting_cache
        row = df[df['Team'] == team]
        if row.empty and key != team:
            row = df[df['Team'] == key]
        if row.empty:
            return {}
        r = row.iloc[0]
        return {
            'woba':    round(_safe_float(r.get('wOBA')), 3),
            'wrc_plus':int(_safe_float(r.get('wRC+', 100))),
            'iso':     round(_safe_float(r.get('ISO')), 3),
            'k_pct':   round(_safe_float(r.get('K%')), 3),
            'bb_pct':  round(_safe_float(r.get('BB%')), 3),
            'avg':     round(_safe_float(r.get('AVG')), 3),
        }
    except Exception as e:
        print(f'  [MLB] team batting {team} error: {e}')
        return {}

def get_team_pitching(team):
    """Return team pitching stats (ERA, WHIP, FIP)."""
    official = _official_team_stats('pitching')
    key = _norm_team(team)
    if official.get(key):
        return official[key]
    global _pitching_cache
    if _pitching_cache is None and MLB_AVAILABLE:
        try:
            _pitching_cache = pb.team_pitching(MLB_SEASON)
        except Exception as e:
            print(f'  [MLB] pitching error: {e}')
    if _pitching_cache is None:
        return {}
    try:
        df = _pitching_cache
        row = df[df['Team'] == team]
        if row.empty and key != team:
            row = df[df['Team'] == key]
        if row.empty:
            return {}
        r = row.iloc[0]
        return {
            'era':  round(_safe_float(r.get('ERA', 4.5)), 2),
            'whip': round(_safe_float(r.get('WHIP', 1.3)), 2),
            'fip':  round(_safe_float(r.get('FIP', 4.5)), 2),
            'k9':   round(_safe_float(r.get('K/9', 8.0)), 1),
            'bb9':  round(_safe_float(r.get('BB/9', 3.0)), 1),
            'hr9':  round(_safe_float(r.get('HR/9', 1.2)), 2),
        }
    except Exception as e:
        print(f'  [MLB] team pitching {team} error: {e}')
        return {}

def predict(home_team, away_team, home_bat, away_bat, home_pitch, away_pitch, home_st=None, away_st=None):
    """
    MLB win probability.
    Combines team wRC+ (offense) vs team ERA (pitching).
    Returns {home_win, away_win, over8_5, home_runs, away_runs}
    """
    lg_era  = 4.5
    lg_wrc  = 100

    h_wrc  = home_bat.get('wrc_plus') or (home_bat.get('ops') and 100 * home_bat.get('ops') / 0.720) or lg_wrc
    a_wrc  = away_bat.get('wrc_plus') or (away_bat.get('ops') and 100 * away_bat.get('ops') / 0.720) or lg_wrc
    h_era  = home_pitch.get('era', lg_era)
    a_era  = away_pitch.get('era', lg_era)

    # Expected runs: team offense quality vs opponent pitching quality
    # League avg ~4.5 runs/game; scale by wRC+ and ERA
    h_base = home_bat.get('runs_pg') or 4.5
    a_base = away_bat.get('runs_pg') or 4.5
    h_runs = ((h_wrc / lg_wrc) * 4.5 * 0.55 + h_base * 0.45) * (lg_era / max(0.1, a_era)) * 1.04
    a_runs = ((a_wrc / lg_wrc) * 4.5 * 0.55 + a_base * 0.45) * (lg_era / max(0.1, h_era))

    margin = h_runs - a_runs
    p_home = 1 / (1 + math.exp(-margin * 0.4))  # flatter sigmoid for baseball
    p_home = min(0.80, max(0.20, p_home))
    total  = round(h_runs + a_runs, 1)
    over_8_5 = _over_runs(h_runs, a_runs, 8.5)

    return {
        'home_win':  round(p_home * 100, 1),
        'away_win':  round((1 - p_home) * 100, 1),
        'home_runs': round(h_runs, 1),
        'away_runs': round(a_runs, 1),
        'total_runs': total,
        'over_8_5':  round(over_8_5 * 100, 1),
        'spread':    round(h_runs - a_runs, 2),
        'betting_markets': _betting_markets(home_team, away_team, h_runs, a_runs, p_home, over_8_5),
    }

def _over_runs(lh, la, line):
    """Negative binomial approximation for over/under runs."""
    from math import exp, factorial
    prob = 0.0
    for h in range(20):
        ph = (lh**h * exp(-lh)) / factorial(h)
        for a in range(20):
            if h + a > line:
                prob += ph * (la**a * exp(-la)) / factorial(a)
    return prob

def _poisson_pmf(k, lam):
    from math import exp, factorial
    return (lam ** k * exp(-lam)) / factorial(k)

def _team_over_prob(lam, line):
    prob = 0.0
    for runs in range(20):
        if runs > line:
            prob += _poisson_pmf(runs, lam)
    return prob

def _runline_prob(lh, la, team, line):
    """Probability that team covers a run line, e.g. home -1.5 or away +1.5."""
    prob = 0.0
    for h in range(20):
        ph = _poisson_pmf(h, lh)
        for a in range(20):
            pa = _poisson_pmf(a, la)
            margin = h - a
            if team == 'home' and margin + line > 0:
                prob += ph * pa
            elif team == 'away' and -margin + line > 0:
                prob += ph * pa
    return prob

def _market_conf(prob_pct):
    edge = abs(prob_pct - 50)
    if prob_pct >= 60:
        return 'strong'
    if prob_pct >= 55:
        return 'lean'
    if edge >= 7:
        return 'watch'
    return 'thin'

def _mk_market(market, pick, prob, line=None, reason=''):
    pct = round(prob * 100, 1)
    return {
        'market': market,
        'pick': pick,
        'line': line,
        'probability': pct,
        'confidence': _market_conf(pct),
        'reason': reason,
    }

def _betting_markets(home, away, h_runs, a_runs, p_home, over_8_5):
    """Translate MLB model outputs into markets a bettor actually recognizes."""
    markets = []
    fav_home = p_home >= 0.5
    fav = home if fav_home else away
    dog = away if fav_home else home
    fav_prob = p_home if fav_home else 1 - p_home
    markets.append(_mk_market(
        'Moneyline',
        fav,
        fav_prob,
        reason='Highest modeled win probability.',
    ))

    fav_cover = _runline_prob(h_runs, a_runs, 'home' if fav_home else 'away', -1.5)
    dog_cover = _runline_prob(h_runs, a_runs, 'away' if fav_home else 'home', 1.5)
    if fav_cover >= 0.52:
        markets.append(_mk_market(
            'Run line',
            f'{fav} -1.5',
            fav_cover,
            '-1.5',
            'Favorite has enough projected run margin to consider the alternate risk/reward market.',
        ))
    else:
        markets.append(_mk_market(
            'Run line',
            f'{dog} +1.5',
            dog_cover,
            '+1.5',
            'Model expects a close game; the safer spread angle is the underdog run cushion.',
        ))

    total_prob = over_8_5 if over_8_5 >= 0.5 else 1 - over_8_5
    markets.append(_mk_market(
        'Total runs',
        'Over 8.5' if over_8_5 >= 0.5 else 'Under 8.5',
        total_prob,
        '8.5',
        f'Projected total is {round(h_runs + a_runs, 1)} runs.',
    ))

    home_o45 = _team_over_prob(h_runs, 4.5)
    away_o45 = _team_over_prob(a_runs, 4.5)
    team_totals = [
        (f'{home} Over 4.5', home_o45, home, '4.5'),
        (f'{home} Under 4.5', 1 - home_o45, home, '4.5'),
        (f'{away} Over 4.5', away_o45, away, '4.5'),
        (f'{away} Under 4.5', 1 - away_o45, away, '4.5'),
    ]
    pick, prob, team, line = max(team_totals, key=lambda x: x[1])
    markets.append(_mk_market(
        'Team total',
        pick,
        prob,
        line,
        f'{team} projected for {round(h_runs if team == home else a_runs, 1)} runs.',
    ))

    return sorted(markets, key=lambda m: m['probability'], reverse=True)

def get_today_games(days=3):
    """Return upcoming MLB games via MLB Stats API (free, official, no key)."""
    import urllib.request, json
    import datetime as _dtl
    games = []
    seen = set()
    today = _dtl.date.today()
    for d in range(days):
        date = (today + _dtl.timedelta(days=d)).isoformat()
        try:
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}&hydrate=team"
            req = urllib.request.Request(url, headers={"User-Agent": "Scoutline/2.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            for day in data.get("dates", []):
                for g in day.get("games", []):
                    gid = g.get("gamePk", "")
                    if gid in seen: continue
                    seen.add(gid)
                    ht = (g.get("teams", {}).get("home", {}).get("team", {}) or {})
                    at = (g.get("teams", {}).get("away", {}).get("team", {}) or {})
                    status = (g.get("status", {}).get("abstractGameState", ""))
                    games.append({
                        "game_id": gid,
                        "home": ht.get("name", ""),
                        "away": at.get("name", ""),
                        "home_abbr": ht.get("abbreviation", ""),
                        "away_abbr": at.get("abbreviation", ""),
                        "kickoff": g.get("gameDate", ""),
                        "gameday": date,
                        "status": status,
                    })
        except Exception as e:
            print(f"  [MLB] schedule {date} error: {e}")
    return sorted(games, key=lambda x: x.get("kickoff", ""))
