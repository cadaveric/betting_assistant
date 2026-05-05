"""
NBA Basketball adapter.
Uses nba_api (official NBA stats, free, no key required).
pip install nba_api
"""
import datetime as _dt, json, time, math
from functools import lru_cache

try:
    from nba_api.stats.endpoints import (
        leaguestandings, teamgamelog, leaguegamefinder, teamestimatedmetrics
    )
    try:
        from nba_api.stats.endpoints import scoreboardv3 as _scoreboard_mod
        _USE_V3 = True
    except ImportError:
        try:
            from nba_api.stats.endpoints import scoreboardv2 as _scoreboard_mod
            _USE_V3 = False
        except ImportError:
            from nba_api.stats.endpoints import scoreboard as _scoreboard_mod
            _USE_V3 = False
    from nba_api.stats.static import teams as nba_teams_static
    NBA_AVAILABLE = True
except ImportError as _e:
    NBA_AVAILABLE = False
    print(f'  [NBA] import failed: {_e}')

def _current_nba_season():
    """NBA season runs Oct-Jun. Dynamically returns the active season string."""
    today = _dt.date.today()
    if today.month >= 10:
        return f"{today.year}-{str(today.year+1)[2:]}"
    return f"{today.year-1}-{str(today.year)[2:]}"

NBA_SEASON = _current_nba_season()

LEAGUE_MAP = {
    'NBA': {'id': '00', 'name': 'NBA', 'season': NBA_SEASON},
}

def _static_standings():
    """Last-resort team list so the NBA predictor remains usable."""
    if not NBA_AVAILABLE:
        return []
    try:
        rows = []
        for t in nba_teams_static.get_teams():
            tid = str(t.get('id', '') or '')
            rows.append({
                'team_id': tid,
                'name': t.get('nickname') or t.get('full_name') or t.get('abbreviation') or '',
                'city': t.get('city') or '',
                'full_name': t.get('full_name') or '',
                'abbr': t.get('abbreviation') or '',
                'wins': 0,
                'losses': 0,
                'pct': 0.0,
                'conf': 'NBA',
                'div': '',
                'streak': '',
                'elo': round(_elo_store.get(tid, 1500.0)),
                'fallback': True,
            })
        return sorted(rows, key=lambda x: x.get('full_name') or x.get('name') or '')
    except Exception as e:
        print(f'  [NBA] static teams fallback error: {e}')
        return []

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
            tid = str(r.get('TeamID', '') or '')
            slug = r.get('TeamSlug', '') or r.get('TeamAbbreviation', '') or ''
            city = str(r.get('TeamCity', '') or '')
            name = str(r.get('TeamName', '') or '')
            result.append({
                'team_id':   tid,
                'name':      name,
                'city':      city,
                'full_name': f"{city} {name}".strip(),
                'abbr':      str(slug)[:3].upper() if slug else name[:3].upper(),
                'wins':      int(r.get('WINS', 0) or 0),
                'losses':    int(r.get('LOSSES', 0) or 0),
                'pct':       round(float(r.get('WinPCT', 0) or 0), 3),
                'conf':      str(r.get('Conference', '') or ''),
                'div':       str(r.get('Division', '') or ''),
                'streak':    str(r.get('strCurrentStreak', '') or ''),
                'elo':       round(_elo_store.get(tid, 1500.0)),
            })
        print(f'  [NBA] Standings loaded: {len(result)} teams ({NBA_SEASON})')
        result = sorted(result, key=lambda x: -x['pct'])
        return result or _static_standings()
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f'  [NBA] standings error: {e}')
        return _static_standings()

# Module-level cache for TeamEstimatedMetrics — fetched once, reused for all teams
_estimated_metrics_cache = None
_estimated_metrics_season = None

def _get_estimated_metrics():
    global _estimated_metrics_cache, _estimated_metrics_season
    if _estimated_metrics_cache is not None and _estimated_metrics_season == NBA_SEASON:
        return _estimated_metrics_cache
    try:
        time.sleep(0.4)
        tm = teamestimatedmetrics.TeamEstimatedMetrics(season=NBA_SEASON, league_id='00')
        _estimated_metrics_cache = tm.get_data_frames()[0]
        _estimated_metrics_season = NBA_SEASON
    except Exception as e:
        print(f'  [NBA] estimated metrics error: {e}')
        _estimated_metrics_cache = None
    return _estimated_metrics_cache

# Module-level team ID→name lookup built from nba_teams_static
_team_id_map = {}

def _build_team_lookup():
    global _team_id_map
    if _team_id_map or not NBA_AVAILABLE:
        return
    try:
        for t in nba_teams_static.get_teams():
            tid = str(t.get('id', ''))
            _team_id_map[tid] = {
                'full_name': t.get('full_name', ''),
                'name':      t.get('nickname', ''),
                'city':      t.get('city', ''),
                'abbr':      t.get('abbreviation', ''),
            }
    except Exception as e:
        print(f'  [NBA] team lookup build error: {e}')

_build_team_lookup()

def _resolve_name(team_id, fallback=''):
    """Look up team name from static data by ID; fall back to passed string."""
    info = _team_id_map.get(str(team_id), {})
    return info.get('full_name') or info.get('name') or fallback or ''

def get_team_stats(team_id):
    """Return offensive/defensive ratings and form for a team."""
    if not NBA_AVAILABLE:
        return {}
    try:
        time.sleep(0.6)
        # Try current season (Playoffs OR Regular Season, whichever has data)
        df = None
        for season_type in ('Playoffs', 'Regular Season'):
            gl = teamgamelog.TeamGameLog(
                team_id=team_id, season=NBA_SEASON,
                season_type_all_star=season_type
            )
            candidate = gl.get_data_frames()[0]
            if not candidate.empty:
                df = candidate
                break
        if df is None or df.empty:
            return {}
        recent = df.head(10)
        pts_pg    = round(float(recent['PTS'].mean()), 1)
        plus_minus = recent['PLUS_MINUS'] if 'PLUS_MINUS' in recent else None
        if plus_minus is not None:
            pts_ag = round(float((recent['PTS'] - plus_minus).mean()), 1)
        else:
            pts_ag = 110.0
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
        pts_allowed_home = round(float((home_g['PTS'] - home_g['PLUS_MINUS']).mean()), 1) if not home_g.empty and 'PLUS_MINUS' in home_g else pts_ag
        pts_allowed_away = round(float((away_g['PTS'] - away_g['PLUS_MINUS']).mean()), 1) if not away_g.empty and 'PLUS_MINUS' in away_g else pts_ag
        est = {}
        try:
            mdf = _get_estimated_metrics()
            erow = mdf[mdf['TEAM_ID'].astype(str) == str(team_id)] if mdf is not None else None
            if not erow.empty:
                er = erow.iloc[0]
                est = {
                    'off_rating': round(float(er.get('E_OFF_RATING', 110) or 110), 1),
                    'def_rating': round(float(er.get('E_DEF_RATING', 110) or 110), 1),
                    'net_rating': round(float(er.get('E_NET_RATING', 0) or 0), 1),
                    'pace': round(float(er.get('E_PACE', 99) or 99), 1),
                }
        except Exception:
            est = {}
        return {
            'pts_pg': pts_pg, 'fg_pct': fg_pct, 'fg3_pct': fg3_pct,
            'reb_pg': reb_pg, 'ast_pg': ast_pg, 'tov_pg': tov_pg,
            'pts_home_pg': pts_home, 'pts_away_pg': pts_away,
            'pts_allowed_pg': pts_ag,
            'pts_allowed_home_pg': pts_allowed_home,
            'pts_allowed_away_pg': pts_allowed_away,
            'form_pct': form_pct, 'wins_l10': wins_l10,
            'games_l10': len(recent),
            **est,
        }
    except Exception as e:
        print(f'  [NBA] team_stats {team_id} error: {e}')
        return {}

def _parse_scoreboard(date_str):
    """Return list of game dicts from ScoreboardV3 (or V2 fallback) for a date."""
    try:
        if _USE_V3:
            sb = _scoreboard_mod.ScoreboardV3(game_date=date_str, league_id='00')
            dfs = sb.get_data_frames()
            # df[1] = game header, df[2] = team info (two rows per game: home+away)
            games_hdr  = dfs[1] if len(dfs) > 1 else None
            teams_df   = dfs[2] if len(dfs) > 2 else None
            if games_hdr is None or games_hdr.empty:
                return []
            result = []
            for _, gh in games_hdr.iterrows():
                gid = str(gh.get('gameId', ''))
                if not gid:
                    continue
                # Find the two team rows for this game
                if teams_df is not None and not teams_df.empty:
                    game_teams = teams_df[teams_df['gameId'] == gid]
                else:
                    game_teams = None
                home_row = away_row = None
                if game_teams is not None and len(game_teams) >= 2:
                    # Determine home/away from the game code (format: date/AWAY@HOME)
                    code = str(gh.get('gameCode', ''))
                    away_code = code.split('/')[-1].split('@')[0].upper() if '/@' in code or '@' in code else ''
                    for _, tr in game_teams.iterrows():
                        tricode = str(tr.get('teamTricode', ''))
                        if tricode == away_code:
                            away_row = tr
                        else:
                            home_row = tr
                    if home_row is None and len(game_teams) == 2:
                        rows = list(game_teams.iterrows())
                        away_row, home_row = rows[0][1], rows[1][1]
                def _tname(row):
                    if row is None: return ''
                    city = str(row.get('teamCity','') or '')
                    name = str(row.get('teamName','') or '')
                    return f"{city} {name}".strip() or str(row.get('teamTricode',''))
                def _tid(row):
                    return str(row.get('teamId','') or '') if row is not None else ''
                h_id = _tid(home_row); a_id = _tid(away_row)
                result.append({
                    'game_id':    gid,
                    'home':       _tname(home_row) or _resolve_name(h_id),
                    'away':       _tname(away_row) or _resolve_name(a_id),
                    'home_id':    h_id,
                    'away_id':    a_id,
                    'home_abbr':  str(home_row.get('teamTricode','') if home_row is not None else ''),
                    'away_abbr':  str(away_row.get('teamTricode','') if away_row is not None else ''),
                    'status':     str(gh.get('gameStatusText','') or ''),
                    'home_score': gh.get('homeTeamScore'),
                    'away_score': gh.get('awayTeamScore'),
                })
            return result
        else:
            # V2 fallback
            sb = _scoreboard_mod.ScoreboardV2(game_date=date_str)
            games_df = sb.get_data_frames()[0]
            result = []
            for _, g in games_df.iterrows():
                h_id = str(g.get('HOME_TEAM_ID','') or ''); a_id = str(g.get('VISITOR_TEAM_ID','') or '')
                result.append({
                    'game_id':    str(g.get('GAME_ID','')),
                    'home':       _resolve_name(h_id, str(g.get('HOME_TEAM_ABBREVIATION','') or '')),
                    'away':       _resolve_name(a_id, str(g.get('VISITOR_TEAM_ABBREVIATION','') or '')),
                    'home_id':    h_id, 'away_id': a_id,
                    'home_abbr':  str(g.get('HOME_TEAM_ABBREVIATION','') or ''),
                    'away_abbr':  str(g.get('VISITOR_TEAM_ABBREVIATION','') or ''),
                    'status':     str(g.get('GAME_STATUS_TEXT','') or ''),
                    'home_score': g.get('HOME_TEAM_PTS'), 'away_score': g.get('VISITOR_TEAM_PTS'),
                })
            return result
    except Exception as e:
        print(f'  [NBA] parse_scoreboard {date_str} error: {e}')
        return []

def get_today_games():
    """Return today's NBA games."""
    if not NBA_AVAILABLE: return []
    time.sleep(0.6)
    return _parse_scoreboard(_dt.date.today().strftime('%m/%d/%Y'))

# ── Prediction ────────────────────────────────────────────────────────────────
def predict(home_stats, away_stats, home_elo=1500, away_elo=1500):
    """
    Simple NBA win probability.
    Blends Elo (50%) + scoring margin (30%) + form (20%).
    Returns {'home_win': float, 'away_win': float, 'total_pts': float}
    """
    # Elo component
    elo_exp_h = _elo_expected(home_elo + 50, away_elo)  # +50 home court

    # Scoring-margin component, using both offense and opponent defense.
    h_pts = home_stats.get('pts_home_pg', home_stats.get('pts_pg', 110))
    a_pts = away_stats.get('pts_away_pg', away_stats.get('pts_pg', 110))
    h_allowed = home_stats.get('pts_allowed_home_pg', home_stats.get('pts_allowed_pg', 110))
    a_allowed = away_stats.get('pts_allowed_away_pg', away_stats.get('pts_allowed_pg', 110))
    h_exp = (h_pts + a_allowed) / 2
    a_exp = (a_pts + h_allowed) / 2
    if home_stats.get('pace') and away_stats.get('pace'):
        pace_scale = max(0.92, min(1.08, ((home_stats['pace'] + away_stats['pace']) / 2) / 99.0))
        h_exp *= pace_scale
        a_exp *= pace_scale
    margin = h_exp - a_exp
    margin_h = 1 / (1 + math.exp(-margin / 9.5))
    margin_h = max(0.15, min(0.85, margin_h))

    # Form component
    h_form = home_stats.get('form_pct', 0.5)
    a_form = away_stats.get('form_pct', 0.5)
    form_h = 0.5 + (h_form - a_form) * 0.5
    form_h = max(0.2, min(0.8, form_h))

    # Blend
    rating_h = None
    if home_stats.get('net_rating') is not None and away_stats.get('net_rating') is not None:
        rating_margin = home_stats.get('net_rating', 0) - away_stats.get('net_rating', 0) + 2.0
        rating_h = max(0.15, min(0.85, 1 / (1 + math.exp(-rating_margin / 10))))
    if rating_h is None:
        p_home = round(elo_exp_h * 0.45 + margin_h * 0.35 + form_h * 0.20, 3)
    else:
        p_home = round(elo_exp_h * 0.35 + margin_h * 0.30 + form_h * 0.15 + rating_h * 0.20, 3)
    p_away = round(1 - p_home, 3)

    # Total points estimate
    total = round(h_exp + a_exp, 1)

    return {
        'home_win': round(p_home * 100, 1),
        'away_win': round(p_away * 100, 1),
        'total_pts': total,
        'home_pts': round(h_exp, 1),
        'away_pts': round(a_exp, 1),
        'spread': round((h_exp - a_exp), 1),   # positive = home favoured
    }

def get_upcoming_games(days=7):
    """Return upcoming NBA games for the next N days using ScoreboardV3."""
    if not NBA_AVAILABLE:
        return []
    try:
        today = _dt.date.today()
        upcoming = []
        seen = set()
        for offset in range(days + 1):
            day = today + _dt.timedelta(days=offset)
            time.sleep(0.35)
            date_str = day.strftime('%m/%d/%Y')
            games = _parse_scoreboard(date_str)
            for g in games:
                gid = g.get('game_id', '')
                if not gid or gid in seen:
                    continue
                seen.add(gid)
                status = str(g.get('status', '') or '')
                if status.lower().startswith('final'):
                    continue
                upcoming.append({**g, 'kickoff': day.isoformat(), 'gameday': day.isoformat()})
        return sorted(upcoming, key=lambda x: x.get('gameday', ''))
    except Exception as e:
        print(f'  [NBA] upcoming_games error: {e}')
        # Fall back to today's games
        return get_today_games()
