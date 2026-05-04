"""
NFL American Football adapter.
Uses nfl_data_py (wraps nflFastR/nflverse data, free, no key).
pip install nfl_data_py
"""
import datetime as _dt, math

try:
    import nfl_data_py as nfl
    NFL_AVAILABLE = True
except ImportError:
    NFL_AVAILABLE = False

NFL_SEASON = 2024

LEAGUE_MAP = {
    'NFL': {'id': 'NFL', 'name': 'NFL', 'season': NFL_SEASON},
}

_team_cache = {}
_schedule_cache = None

def _get_schedule():
    global _schedule_cache
    if _schedule_cache is not None:
        return _schedule_cache
    if not NFL_AVAILABLE:
        return []
    try:
        df = nfl.import_schedules([NFL_SEASON])
        if df is None:
            return []
        # Replace NaN/inf with None so the records are JSON-serialisable
        import math
        def _clean(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v
        _schedule_cache = [{k: _clean(v) for k, v in row.items()} for row in df.to_dict('records')]
        return _schedule_cache
    except Exception as e:
        print(f'  [NFL] schedule error: {e}')
        return []

def get_standings():
    """Build standings from season schedule results."""
    if not NFL_AVAILABLE:
        return []
    try:
        games = _get_schedule()
        records = {}  # team -> {wins, losses, ties, pf, pa}
        for g in games:
            if g.get('game_type') != 'REG':
                continue
            home = g.get('home_team', '')
            away = g.get('away_team', '')
            hs = g.get('home_score')
            as_ = g.get('away_score')
            if hs is None or as_ is None or home == '' or away == '':
                continue
            hs, as_ = int(hs), int(as_)
            for t in [home, away]:
                if t not in records:
                    records[t] = {'team': t, 'wins': 0, 'losses': 0, 'ties': 0, 'pf': 0, 'pa': 0, 'games': 0}
            if hs > as_:
                records[home]['wins'] += 1; records[away]['losses'] += 1
            elif as_ > hs:
                records[away]['wins'] += 1; records[home]['losses'] += 1
            else:
                records[home]['ties'] += 1; records[away]['ties'] += 1
            records[home]['pf'] += hs; records[home]['pa'] += as_; records[home]['games'] += 1
            records[away]['pf'] += as_; records[away]['pa'] += hs; records[away]['games'] += 1

        result = []
        for team, r in records.items():
            g = max(1, r['games'])
            result.append({
                'team':     team,
                'name':     team,
                'wins':     r['wins'],
                'losses':   r['losses'],
                'ties':     r['ties'],
                'pf':       r['pf'],
                'pa':       r['pa'],
                'pf_pg':    round(r['pf'] / g, 1),
                'pa_pg':    round(r['pa'] / g, 1),
                'pct':      round((r['wins'] + 0.5 * r['ties']) / g, 3),
                'games':    g,
                'conf':     _conf(team),
                'div':      _div(team),
            })
        return sorted(result, key=lambda x: -x['pct'])
    except Exception as e:
        print(f'  [NFL] standings error: {e}')
        return []

def get_week_games(week=None):
    """Return games for a specific week or the most recent week."""
    try:
        games = _get_schedule()
        if not games:
            return []
        today = _dt.date.today()
        # Filter to games within the next 7 days or most recent past week
        upcoming = []
        for g in games:
            gd = g.get('gameday', '')
            try:
                d = _dt.date.fromisoformat(gd)
            except Exception:
                continue
            if d >= today and d <= today + _dt.timedelta(days=7):
                upcoming.append(g)
        if not upcoming:
            # Fall back to most recent completed week
            played = [g for g in games if g.get('home_score') is not None
                      and g.get('game_type') == 'REG']
            if played:
                max_wk = max(g.get('week', 0) for g in played)
                upcoming = [g for g in played if g.get('week') == max_wk]
        return sorted(upcoming, key=lambda x: (x.get('gameday',''), x.get('gametime','')))
    except Exception as e:
        print(f'  [NFL] week_games error: {e}')
        return []

def get_team_stats(team):
    """Compute offensive/defensive points per game for a team."""
    try:
        games = _get_schedule()
        pf_list, pa_list = [], []
        for g in games:
            if g.get('game_type') != 'REG':
                continue
            if g.get('home_team') == team and g.get('home_score') is not None:
                pf_list.append(int(g['home_score'])); pa_list.append(int(g['away_score']))
            elif g.get('away_team') == team and g.get('away_score') is not None:
                pf_list.append(int(g['away_score'])); pa_list.append(int(g['home_score']))
        if not pf_list:
            return {}
        recent_pf = pf_list[-8:]; recent_pa = pa_list[-8:]
        wins = sum(1 for pf, pa in zip(pf_list[-8:], pa_list[-8:]) if pf > pa)
        return {
            'pf_pg':    round(sum(pf_list) / len(pf_list), 1),
            'pa_pg':    round(sum(pa_list) / len(pa_list), 1),
            'pf_pg_r8': round(sum(recent_pf) / max(1, len(recent_pf)), 1),
            'pa_pg_r8': round(sum(recent_pa) / max(1, len(recent_pa)), 1),
            'wins_l8':  wins,
            'form_pct': round(wins / max(1, len(recent_pf)), 3),
            'games':    len(pf_list),
        }
    except Exception as e:
        print(f'  [NFL] team_stats {team} error: {e}')
        return {}

def predict(home_team, away_team, home_stats, away_stats):
    """Simple NFL win probability: scoring margin + form + home advantage."""
    lg_avg = 23.0  # NFL points per game approx
    h_off = home_stats.get('pf_pg_r8', home_stats.get('pf_pg', lg_avg))
    h_def = home_stats.get('pa_pg_r8', home_stats.get('pa_pg', lg_avg))
    a_off = away_stats.get('pf_pg_r8', away_stats.get('pf_pg', lg_avg))
    a_def = away_stats.get('pa_pg_r8', away_stats.get('pa_pg', lg_avg))
    # Expected points
    h_exp = (h_off + a_def) / 2 * 1.03  # 3% home advantage in NFL
    a_exp = (a_off + h_def) / 2
    margin = h_exp - a_exp
    # Sigmoid to probability
    p_home = 1 / (1 + math.exp(-margin / 7))
    # Form adjustment
    h_form = home_stats.get('form_pct', 0.5)
    a_form = away_stats.get('form_pct', 0.5)
    form_adj = (h_form - a_form) * 0.10
    p_home = min(0.88, max(0.12, p_home + form_adj))
    return {
        'home_win':   round(p_home * 100, 1),
        'away_win':   round((1 - p_home) * 100, 1),
        'home_pts':   round(h_exp, 1),
        'away_pts':   round(a_exp, 1),
        'spread':     round(h_exp - a_exp, 1),
        'total_pts':  round(h_exp + a_exp, 1),
    }

# Conference/division lookup
_AFC = {'BUF','MIA','NE','NYJ','BAL','CIN','CLE','PIT','HOU','IND','JAX','TEN','DEN','KC','LV','LAC'}
_NFC = {'DAL','NYG','PHI','WAS','CHI','DET','GB','MIN','ATL','CAR','NO','TB','ARI','LA','SEA','SF'}
def _conf(t): return 'AFC' if t in _AFC else 'NFC' if t in _NFC else ''
_DIVS = {
    'BUF':'AFC East','MIA':'AFC East','NE':'AFC East','NYJ':'AFC East',
    'BAL':'AFC North','CIN':'AFC North','CLE':'AFC North','PIT':'AFC North',
    'HOU':'AFC South','IND':'AFC South','JAX':'AFC South','TEN':'AFC South',
    'DEN':'AFC West','KC':'AFC West','LV':'AFC West','LAC':'AFC West',
    'DAL':'NFC East','NYG':'NFC East','PHI':'NFC East','WAS':'NFC East',
    'CHI':'NFC North','DET':'NFC North','GB':'NFC North','MIN':'NFC North',
    'ATL':'NFC South','CAR':'NFC South','NO':'NFC South','TB':'NFC South',
    'ARI':'NFC West','LA':'NFC West','SEA':'NFC West','SF':'NFC West',
}
def _div(t): return _DIVS.get(t, '')
