"""
MLB Baseball adapter.
Uses pybaseball (wraps Baseball Reference / FanGraphs, free, no key).
pip install pybaseball
"""
import datetime as _dt, math

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
_batting_cache = None
_pitching_cache = None

def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def get_standings():
    """Return combined AL + NL standings."""
    global _standings_cache
    if _standings_cache is not None:
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
        return _standings_cache
    except Exception as e:
        print(f'  [MLB] standings error: {e}')
        return []

def get_team_batting(team):
    """Return team batting stats (wOBA, wRC+, ISO)."""
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

    h_wrc  = home_bat.get('wrc_plus', lg_wrc)
    a_wrc  = away_bat.get('wrc_plus', lg_wrc)
    h_era  = home_pitch.get('era', lg_era)
    a_era  = away_pitch.get('era', lg_era)

    # Expected runs: team offense quality vs opponent pitching quality
    # League avg ~4.5 runs/game; scale by wRC+ and ERA
    h_runs = (h_wrc / lg_wrc) * (lg_era / max(0.1, a_era)) * 4.5 * 1.04  # home park
    a_runs = (a_wrc / lg_wrc) * (lg_era / max(0.1, h_era)) * 4.5

    margin = h_runs - a_runs
    p_home = 1 / (1 + math.exp(-margin * 0.4))  # flatter sigmoid for baseball
    p_home = min(0.80, max(0.20, p_home))
    total  = round(h_runs + a_runs, 1)

    return {
        'home_win':  round(p_home * 100, 1),
        'away_win':  round((1 - p_home) * 100, 1),
        'home_runs': round(h_runs, 1),
        'away_runs': round(a_runs, 1),
        'total_runs': total,
        'over_8_5':  round(_over_runs(h_runs, a_runs, 8.5) * 100, 1),
        'spread':    round(h_runs - a_runs, 2),
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
