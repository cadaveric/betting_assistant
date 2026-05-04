#!/usr/bin/env python3
"""
NBA basketball prediction model trainer.
Downloads historical game logs via nba_api, builds rolling team features,
trains XGBoost (or RandomForest fallback) classifier.
Output: data/nba_model.pkl, data/nba_model_meta.json
"""
import os, json, time, datetime as _dt

DATA_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
MODEL_PATH = os.path.join(DATA_DIR, 'nba_model.pkl')
META_PATH  = os.path.join(DATA_DIR, 'nba_model_meta.json')

FEATURE_NAMES = [
    'h_pts_pg10',   # home team pts/game last 10
    'a_pts_pg10',   # away team pts/game last 10
    'h_pa_pg10',    # home team pts allowed/game last 10
    'a_pa_pg10',    # away team pts allowed/game last 10
    'h_form10',     # home team win fraction last 10
    'a_form10',     # away team win fraction last 10
    'h_fg_pct',     # home FG%
    'a_fg_pct',     # away FG%
    'h_elo',        # home Elo (0-1 normalised)
    'a_elo',        # away Elo
]

def _roll(lst, n, default):
    v = [x for x in lst[-n:] if x is not None]
    return sum(v)/len(v) if v else default

def _elo_exp(ra, rb): return 1.0/(1.0+10.0**((rb-ra)/400.0))

def build_dataset():
    try:
        from nba_api.stats.endpoints import leaguegamefinder
        import numpy as np
    except ImportError as e:
        print(f'  [NBA-ML] Missing: {e}'); return None, None

    seasons = ['2021-22','2022-23','2023-24','2024-25']
    all_games = []
    for s in seasons:
        try:
            time.sleep(1.0)
            gf = leaguegamefinder.LeagueGameFinder(
                season_nullable=s, league_id_nullable='00',
                season_type_nullable='Regular Season'
            )
            df = gf.get_data_frames()[0]
            all_games.append(df)
            print(f'  [NBA-ML] {s}: {len(df)} game-team rows')
        except Exception as e:
            print(f'  [NBA-ML] {s} error: {e}')

    if not all_games:
        return None, None

    import pandas as pd
    df = pd.concat(all_games, ignore_index=True)
    # Each row is one team's view of a game; pair home+away
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    df = df.sort_values('GAME_DATE')
    home_df = df[df['MATCHUP'].str.contains('vs\\.', na=False)].copy()
    away_df = df[df['MATCHUP'].str.contains('@', na=False)].copy()
    # Build per-team rolling history
    team_hist = {}   # team_id -> {pts, pa, fg_pct, results, elo}

    def get_team(tid):
        if tid not in team_hist:
            team_hist[tid] = {'pts':[],'pa':[],'fg':[],'res':[],'elo':1500.0}
        return team_hist[tid]

    # Process games chronologically (home_df gives unique games)
    home_df = home_df.sort_values('GAME_DATE')
    X, y = [], []
    for _, hrow in home_df.iterrows():
        gid = hrow['GAME_ID']
        arows = away_df[away_df['GAME_ID'] == gid]
        if arows.empty: continue
        arow = arows.iloc[0]
        h_id = str(hrow['TEAM_ID']); a_id = str(arow['TEAM_ID'])
        h_pts = hrow.get('PTS'); a_pts = arow.get('PTS')
        h_fg  = hrow.get('FG_PCT'); a_fg  = arow.get('FG_PCT')
        if h_pts is None or a_pts is None: continue
        ht = get_team(h_id); at = get_team(a_id)
        # Build features BEFORE this game
        feat = [
            _roll(ht['pts'], 10, 110), _roll(at['pts'], 10, 108),
            _roll(ht['pa'],  10, 110), _roll(at['pa'],  10, 108),
            _roll(ht['res'], 10, 0.5), _roll(at['res'], 10, 0.5),
            _roll(ht['fg'],  10, 0.46),_roll(at['fg'],  10, 0.46),
            min(1.0, ht['elo']/2000.0), min(1.0, at['elo']/2000.0),
        ]
        label = 1 if h_pts > a_pts else 0
        X.append(feat); y.append(label)
        # Update history
        ht['pts'].append(h_pts); ht['pa'].append(a_pts); ht['fg'].append(h_fg)
        at['pts'].append(a_pts); at['pa'].append(h_pts); at['fg'].append(a_fg)
        ht['res'].append(1 if h_pts>a_pts else 0)
        at['res'].append(1 if a_pts>h_pts else 0)
        # Elo update
        exp_h = _elo_exp(ht['elo']+50, at['elo'])  # +50 home court
        sh    = 1.0 if h_pts>a_pts else 0.0
        ht['elo'] += 20*(sh-exp_h); at['elo'] += 20*((1-sh)-(1-exp_h))

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.int32)

def train():
    try:
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        import numpy as np, joblib
    except ImportError as e:
        print(f'  [NBA-ML] sklearn missing: {e}'); return False

    os.makedirs(DATA_DIR, exist_ok=True)
    print('  [NBA-ML] Downloading historical game data via nba_api…')
    X, y = build_dataset()
    if X is None or len(X) < 200:
        print(f'  [NBA-ML] Insufficient data ({len(X) if X is not None else 0}). Need 200+.')
        return False

    print(f'  [NBA-ML] Training on {len(X)} samples × {len(FEATURE_NAMES)} features…')
    try:
        from xgboost import XGBClassifier
        clf = XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.05,
                            subsample=0.8, colsample_bytree=0.8,
                            eval_metric='logloss', n_jobs=1, random_state=42)
        algo = 'XGBoost'
    except ImportError:
        from sklearn.ensemble import RandomForestClassifier
        clf = RandomForestClassifier(n_estimators=100, max_depth=6,
                                     min_samples_leaf=10, n_jobs=1, random_state=42)
        algo = 'RandomForest'

    model = Pipeline([('scaler', StandardScaler()), ('clf', clf)])
    cv    = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring='accuracy', n_jobs=1)
    print(f'  [NBA-ML] CV accuracy: {scores.mean():.3f} ± {scores.std():.3f}')
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)
    meta = {
        'cv_accuracy': round(float(scores.mean()), 4),
        'cv_std':      round(float(scores.std()),  4),
        'n_train':     int(len(X)),
        'n_features':  len(FEATURE_NAMES),
        'algorithm':   algo,
        'feature_names': FEATURE_NAMES,
        'built': _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    with open(META_PATH, 'w') as f:
        json.dump(meta, f, indent=2)
    print(f'  [NBA-ML] Saved → {MODEL_PATH}')
    return True

if __name__ == '__main__':
    ok = train()
    raise SystemExit(0 if ok else 1)
