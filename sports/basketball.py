"""
NBA Basketball adapter.
Uses nba_api (official NBA stats, free, no key required).
pip install nba_api
"""
import datetime as _dt, json, time, math
from functools import lru_cache

try:
    from nba_api.stats.endpoints import (
        leaguestandings, teamgamelog, leaguegamefinder,
        scoreboard, teamestimatedmetrics
    )
    from nba_api.stats.static import teams as nba_teams_static
    NBA_AVAILABLE = True
except ImportError:
    NBA_AVAILABLE = False

NBA_SEASON = '2024-25'

LEAGUE_MAP = {
    'NBA': {'id': '00', 'name': 'NBA', 'season': NBA_SEASON},
}

# ── Elo helpers ───────────────────────────────────────────────────────────────
_elo_store = {}   # team_id -> float

def _elo_expected(ra, rb):
    return 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))

def _elo_update(winner_id, loser_id, k=20):
    rw = _elo_store.get(winner_id, 1500.0)
    rl = _elo_store.get(loser_id,  1500.0)
    exp = _elo_expected(rw, rl)
    _elo_store[winner_id] = rw + k * (1 - exp)
    _elo_store[loser_id]  = rl + k * (0 - (1 - exp))

# ── Data fetching ─────────────────────────────────────────────────────────────
def get_standings():
    """Return list of {team_id, name, wins, losses, pct, conf, div, elo}."""
    if not NBA_AVAILABLE:
        return []
    try:
        time.sleep(0.6)  # nba_api rate limit
        ls = leaguestandings.LeagueStandings(season=NBA_SEASON, league_id='00')
        rows = ls.get_data_frames()[0]
        result = []
        for _, r in rows.iterrows():
            tid = str(r.get('TeamID', ''))
            result.append({
                'team_id':   tid,
                'name':      r.get('TeamName', ''),
                'city':      r.get('TeamCity', ''),
                'full_name': f"{r.get('TeamCity','')} {r.get('TeamName','')}",
                'abbr':      r.get('TeamSlug', '')[:3].upper(),
                'wins':      int(r.get('WINS', 0)),
                'losses':    int(r.get('LOSSES', 0)),
                'pct':       float(r.get('WinPCT', 0)),
                'conf':      r.get('Conference', ''),
                'div':       r.get('Division', ''),
                'streak':    r.get('strCurrentStreak', ''),
                'elo':       round(_elo_store.get(tid, 1500.0)),
            })
        return sorted(result, key=lambda x: -x['pct'])
    except Exception as e:
        print(f'  [NBA] standings error: {e}')
        return []

def get_team_stats(team_id):
    """Return offensive/defensive ratings and form for a team."""
    if not NBA_AVAILABLE:
        return {}
    try:
        time.sleep(0.6)
        gl = teamgamelog.TeamGameLog(
            team_id=team_id, season=NBA_SEASON, season_type_all_star='Regular Season'
        )
        df = gl.get_data_frames()[0]
        if df.empty:
            return {}
        recent = df.head(10)
        pts_pg    = round(float(recent['PTS'].mean()), 1)
        pts_ag    = round(float(recent['PTS'].mean()), 1)   # will be refined with opp
        fg_pct    = round(float(recent['FG_PCT'].mean()), 3)
        fg3_pct   = round(float(recent['FG3_PCT'].mean()), 3)
        reb_pg    = round(float(recent['REB'].mean()), 1)
        ast_pg    = round(float(recent['AST'].mean()), 1)
        tov_pg    = round(float(recent['TOV'].mean()), 1)
        wins_l10  = int((recent['WL'] == 'W').sum())
        form_pct  = round(wins_l10 / max(1, len(recent)), 3)
        home_g    = recent[recent['MATCHUP'].str.contains('vs\\.', na=False)]
        away_g    = recent[recent['MATCHUP'].str.contains('@', na=False)]
        pts_home  = round(float(home_g['PTS'].mean()), 1) if not home_g.empty else pts_pg
        pts_away  = round(float(away_g['PTS'].mean()), 1) if not away_g.empty else pts_pg
        return {
            'pts_pg': pts_pg, 'fg_pct': fg_pct, 'fg3_pct': fg3_pct,
            'reb_pg': reb_pg, 'ast_pg': ast_pg, 'tov_pg': tov_pg,
            'pts_home_pg': pts_home, 'pts_away_pg': pts_away,
            'form_pct': form_pct, 'wins_l10': wins_l10,
            'games_l10': len(recent),
        }
    except Exception as e:
        print(f'  [NBA] team_stats {team_id} error: {e}')
        return {}

def get_today_games():
    """Return today's NBA games with basic info."""
    if not NBA_AVAILABLE:
        return []
    try:
        time.sleep(0.6)
        today = _dt.date.today().strftime('%m/%d/%Y')
        sb = scoreboard.Scoreboard(game_date=today)
        games_df = sb.get_data_frames()[0]
        result = []
        for _, g in games_df.iterrows():
            result.append({
                'game_id':    str(g.get('GAME_ID', '')),
                'home':       g.get('HOME_TEAM_NAME', ''),
                'away':       g.get('VISITOR_TEAM_NAME', ''),
                'home_id':    str(g.get('HOME_TEAM_ID', '')),
                'away_id':    str(g.get('VISITOR_TEAM_ID', '')),
                'status':     g.get('GAME_STATUS_TEXT', ''),
                'home_score': g.get('HOME_TEAM_PTS'),
                'away_score': g.get('VISITOR_TEAM_PTS'),
            })
        return result
    except Exception as e:
        print(f'  [NBA] today_games error: {e}')
        return []

# ── Prediction ────────────────────────────────────────────────────────────────
def predict(home_stats, away_stats, home_elo=1500, away_elo=1500):
    """
    Simple NBA win probability.
    Blends Elo (50%) + scoring margin (30%) + form (20%).
    Returns {'home_win': float, 'away_win': float, 'total_pts': float}
    """
    # Elo component
    elo_exp_h = _elo_expected(home_elo + 50, away_elo)  # +50 home court

    # Scoring-margin component
    h_pts = home_stats.get('pts_home_pg', home_stats.get('pts_pg', 110))
    a_pts = away_stats.get('pts_away_pg', away_stats.get('pts_pg', 110))
    # How much each team outscores league avg (110 pts)
    h_off = (h_pts - 110) / 20   # normalised
    a_off = (a_pts - 110) / 20
    margin_h = 0.5 + (h_off - a_off) * 0.3
    margin_h = max(0.15, min(0.85, margin_h))

    # Form component
    h_form = home_stats.get('form_pct', 0.5)
    a_form = away_stats.get('form_pct', 0.5)
    form_h = 0.5 + (h_form - a_form) * 0.5
    form_h = max(0.2, min(0.8, form_h))

    # Blend
    p_home = round(elo_exp_h * 0.50 + margin_h * 0.30 + form_h * 0.20, 3)
    p_away = round(1 - p_home, 3)

    # Total points estimate
    total = round(h_pts + a_pts, 1)

    return {
        'home_win': round(p_home * 100, 1),
        'away_win': round(p_away * 100, 1),
        'total_pts': total,
        'spread': round((h_pts - a_pts), 1),   # positive = home favoured
    }

def get_upcoming_games(days=7):
    """Return upcoming NBA games for the next N days."""
    if not NBA_AVAILABLE:
        return []
    try:
        time.sleep(0.6)
        from nba_api.stats.endpoints import leaguegamefinder
        # Get recently scheduled games (current season)
        gf = leaguegamefinder.LeagueGameFinder(
            league_id_nullable='00',
            season_nullable=NBA_SEASON,
            season_type_nullable='Regular Season',
        )
        df = gf.get_data_frames()[0]
        today = _dt.date.today()
        cutoff = today + _dt.timedelta(days=days)
        # Filter to future games (no score yet = upcoming)
        upcoming_ids = set()
        upcoming = []
        for _, row in df.iterrows():
            try:
                gd = _dt.datetime.strptime(str(row.get('GAME_DATE','')), '%Y-%m-%dT%H:%M:%S').date()
            except Exception:
                try: gd = _dt.date.fromisoformat(str(row.get('GAME_DATE',''))[:10])
                except: continue
            if gd < today or gd > cutoff: continue
            gid = str(row.get('GAME_ID',''))
            if gid in upcoming_ids: continue
            upcoming_ids.add(gid)
            # Need to pair home and away — look for matching game
            home_row = df[(df['GAME_ID']==row['GAME_ID']) & df['MATCHUP'].str.contains('vs\\.', na=False)]
            away_row = df[(df['GAME_ID']==row['GAME_ID']) & df['MATCHUP'].str.contains('@', na=False)]
            if home_row.empty or away_row.empty:
                continue
            hr = home_row.iloc[0]; ar = away_row.iloc[0]
            upcoming.append({
                'game_id':  gid,
                'home':     str(hr.get('TEAM_NAME','')),
                'away':     str(ar.get('TEAM_NAME','')),
                'home_id':  str(hr.get('TEAM_ID','')),
                'away_id':  str(ar.get('TEAM_ID','')),
                'home_abbr':str(hr.get('TEAM_ABBREVIATION','')),
                'away_abbr':str(ar.get('TEAM_ABBREVIATION','')),
                'gameday':  gd.isoformat(),
                'status':   'Scheduled',
            })
        return sorted(upcoming, key=lambda x: x.get('gameday',''))
    except Exception as e:
        print(f'  [NBA] upcoming_games error: {e}')
        # Fall back to today's games
        return get_today_games()
