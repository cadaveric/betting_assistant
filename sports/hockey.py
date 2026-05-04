"""
NHL Hockey adapter.
Uses the official NHL Stats API (api.nhle.com) — free, no key required.
"""
import urllib.request, json, datetime as _dt, math

NHL_BASE = 'https://api-web.nhle.com/v1'
NHL_SEASON = '20242025'

LEAGUE_MAP = {
    'NHL': {'id': 'NHL', 'name': 'NHL', 'season': NHL_SEASON},
}

def _fetch(path):
    try:
        req = urllib.request.Request(
            f'{NHL_BASE}/{path}',
            headers={'User-Agent': 'Scoutline/2.0'}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  [NHL] fetch {path} error: {e}')
        return None

def get_standings():
    """Return list of team standings."""
    data = _fetch('standings/now')
    if not data:
        return []
    result = []
    for div in data.get('standings', []):
        t = div
        result.append({
            'team_id':   t.get('teamAbbrev', {}).get('default', ''),
            'name':      t.get('teamName', {}).get('default', ''),
            'full_name': t.get('teamCommonName', {}).get('default', ''),
            'abbr':      t.get('teamAbbrev', {}).get('default', ''),
            'wins':      t.get('wins', 0),
            'losses':    t.get('losses', 0),
            'ot_losses': t.get('otLosses', 0),
            'points':    t.get('points', 0),
            'gf':        t.get('goalFor', 0),
            'ga':        t.get('goalAgainst', 0),
            'gf_pg':     round(t.get('goalFor', 0) / max(1, t.get('gamesPlayed', 1)), 2),
            'ga_pg':     round(t.get('goalAgainst', 0) / max(1, t.get('gamesPlayed', 1)), 2),
            'pct':       round(t.get('pointPctg', 0), 3),
            'conf':      t.get('conferenceName', ''),
            'div':       t.get('divisionName', ''),
            'games_played': t.get('gamesPlayed', 0),
            'streak':    t.get('streakCode', ''),
        })
    return sorted(result, key=lambda x: -x['points'])

def get_team_stats(abbr):
    """Return recent form and key metrics for a team."""
    data = _fetch(f'club-stats/{abbr}/now')
    if not data:
        return {}
    skaters = data.get('skaters', [])
    goalies = data.get('goalies', [])
    # Aggregate save percentage from starting goalie
    sv_pct = None
    for g in sorted(goalies, key=lambda x: -x.get('gamesStarted', 0)):
        sv_pct = g.get('savePctg')
        break
    return {
        'sv_pct': sv_pct,
        'abbr': abbr,
    }

def get_team_schedule(abbr, weeks=2):
    """Get recent results to compute form."""
    data = _fetch(f'club-schedule-season/{abbr}/{NHL_SEASON}')
    if not data:
        return []
    games = data.get('games', [])
    today = _dt.date.today()
    cutoff = today - _dt.timedelta(weeks=weeks)
    recent = []
    for g in games:
        raw = g.get('gameDate', '')
        try:
            gd = _dt.date.fromisoformat(raw)
        except Exception:
            continue
        if gd < cutoff or gd >= today:
            continue
        away_score = (g.get('awayTeam') or {}).get('score')
        home_score = (g.get('homeTeam') or {}).get('score')
        team_is_home = (g.get('homeTeam') or {}).get('abbrev') == abbr
        if away_score is None or home_score is None:
            continue
        gf = home_score if team_is_home else away_score
        ga = away_score if team_is_home else home_score
        won = gf > ga
        recent.append({'date': raw, 'gf': gf, 'ga': ga, 'won': won, 'home': team_is_home})
    return sorted(recent, key=lambda x: x['date'], reverse=True)

def get_today_games():
    """Return today's NHL schedule."""
    today = _dt.date.today().isoformat()
    data = _fetch(f'schedule/{today}')
    if not data:
        return []
    result = []
    for gw in data.get('gameWeek', []):
        for g in gw.get('games', []):
            result.append({
                'game_id':    g.get('id'),
                'home':       (g.get('homeTeam') or {}).get('commonName', {}).get('default', ''),
                'away':       (g.get('awayTeam') or {}).get('commonName', {}).get('default', ''),
                'home_abbr':  (g.get('homeTeam') or {}).get('abbrev', ''),
                'away_abbr':  (g.get('awayTeam') or {}).get('abbrev', ''),
                'kickoff':    g.get('startTimeUTC', ''),
                'status':     (g.get('gameState') or ''),
            })
    return result

def predict(home_stats, away_stats, home_form=None, away_form=None):
    """
    Simple NHL win probability.
    Uses goals for/against per game + save percentage + form.
    Returns {'home_win', 'away_win', 'total_goals', 'over_5_5'}
    """
    h_gf = home_stats.get('gf_pg', 3.0)
    h_ga = home_stats.get('ga_pg', 3.0)
    a_gf = away_stats.get('gf_pg', 2.8)
    a_ga = away_stats.get('ga_pg', 3.0)

    # Expected goals via Dixon-Coles-style formula
    lg_avg = 3.05
    lh = max(0.5, (h_gf / lg_avg) * (a_ga / lg_avg) * lg_avg * 1.10)  # 1.10 home ice
    la = max(0.5, (a_gf / lg_avg) * (h_ga / lg_avg) * lg_avg)

    # Poisson 1X2 (no draw in regulation → just H/A for simplicity)
    def poisson_win_prob(lam_h, lam_a, max_g=8):
        from math import exp, factorial
        ph = pa = 0.0
        for i in range(max_g + 1):
            pi = (lam_h**i * exp(-lam_h)) / factorial(i)
            for j in range(max_g + 1):
                pj = (lam_a**j * exp(-lam_a)) / factorial(j)
                if i > j: ph += pi * pj
                elif j > i: pa += pi * pj
        total = ph + pa
        return (ph / total if total else 0.5), (pa / total if total else 0.5)

    ph_reg, pa_reg = poisson_win_prob(lh, la)

    # Form adjustment
    h_form_pct = 0.5
    a_form_pct = 0.5
    if home_form:
        wins = sum(1 for g in home_form[:10] if g['won'])
        h_form_pct = wins / max(1, len(home_form[:10]))
    if away_form:
        wins = sum(1 for g in away_form[:10] if g['won'])
        a_form_pct = wins / max(1, len(away_form[:10]))
    form_adj = (h_form_pct - a_form_pct) * 0.15

    p_home = min(0.85, max(0.15, ph_reg + form_adj))
    p_away = 1 - p_home
    total  = round(lh + la, 2)

    return {
        'home_win':   round(p_home * 100, 1),
        'away_win':   round(p_away * 100, 1),
        'total_goals': total,
        'over_5_5':   round(_over_prob(lh, la, 5.5) * 100, 1),
        'lh': round(lh, 2), 'la': round(la, 2),
    }

def _over_prob(lh, la, line=5.5, max_g=10):
    from math import exp, factorial
    p = 0.0
    for i in range(max_g + 1):
        pi = (lh**i * exp(-lh)) / factorial(i)
        for j in range(max_g + 1):
            if i + j > line:
                p += pi * (la**j * exp(-la)) / factorial(j)
    return p
