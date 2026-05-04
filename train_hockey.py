#!/usr/bin/env python3
"""
NHL hockey prediction model trainer.
Downloads historical game data from api.nhle.com (free, no key).
Output: data/nhl_model.pkl, data/nhl_model_meta.json
"""
import os, json, time, urllib.request, datetime as _dt

DATA_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
MODEL_PATH = os.path.join(DATA_DIR, 'nhl_model.pkl')
META_PATH  = os.path.join(DATA_DIR, 'nhl_model_meta.json')
NHL_BASE   = 'https://api-web.nhle.com/v1'

FEATURE_NAMES = [
    'h_gf_pg10', 'a_gf_pg10',   # goals for per game last 10
    'h_ga_pg10', 'a_ga_pg10',   # goals against per game last 10
    'h_form10',  'a_form10',     # win fraction last 10
    'h_elo', 'a_elo',            # Elo (normalised)
]

_TEAMS = [
    'BOS','BUF','DET','FLA','MTL','OTT','TBL','TOR',  # Atlantic
    'CAR','CBJ','NJD','NYI','NYR','PHI','PIT','WSH',  # Metro
    'ARI','CHI','COL','DAL','MIN','NSH','STL','WPG',  # Central
    'ANA','CGY','EDM','LAK','SJS','SEA','UTA','VAN',  # Pacific
]

def _fetch(path):
    try:
        req = urllib.request.Request(f'{NHL_BASE}/{path}',
                                     headers={'User-Agent': 'ScoutlineML/1.0'})
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  [NHL-ML] fetch {path}: {e}')
        return None

def _roll(lst, n, default):
    v = [x for x in lst[-n:] if x is not None]
    return sum(v)/len(v) if v else default

def _elo_exp(ra, rb): return 1.0/(1.0+10.0**((rb-ra)/400.0))

def build_dataset():
    try:
        import numpy as np
    except ImportError:
        print('  [NHL-ML] numpy missing'); return None, None

    seasons = ['20212022','20222023','20232024','20242025','20252026']
    team_hist = {}   # abbr -> {gf, ga, res, elo}

    def get_team(abbr):
        if abbr not in team_hist:
            team_hist[abbr] = {'gf':[],'ga':[],'res':[],'elo':1500.0}
        return team_hist[abbr]

    X, y = [], []
    for season in seasons:
        all_games = []
        for team in _TEAMS:
            time.sleep(0.3)
            data = _fetch(f'club-schedule-season/{team}/{season}')
            if not data: continue
            for g in data.get('games', []):
                if g.get('gameType') != 2: continue  # regular season only
                gid = g.get('id')
                if not gid: continue
                ht  = (g.get('homeTeam') or {}).get('abbrev','')
                at  = (g.get('awayTeam') or {}).get('abbrev','')
                hg  = (g.get('homeTeam') or {}).get('score')
                ag  = (g.get('awayTeam') or {}).get('score')
                gdate = g.get('gameDate','')
                if ht and at and hg is not None and ag is not None:
                    all_games.append({'id':gid,'home':ht,'away':at,'hg':hg,'ag':ag,'date':gdate})

        # Deduplicate by game ID
        seen = set()
        unique = []
        for g in sorted(all_games, key=lambda x: x['date']):
            if g['id'] not in seen:
                seen.add(g['id']); unique.append(g)

        print(f'  [NHL-ML] {season}: {len(unique)} unique games')
        for g in unique:
            ht_abbr = g['home']; at_abbr = g['away']
            hg = g['hg']; ag = g['ag']
            ht = get_team(ht_abbr); at = get_team(at_abbr)
            # Build features BEFORE this game
            feat = [
                _roll(ht['gf'], 10, 3.0), _roll(at['gf'], 10, 2.8),
                _roll(ht['ga'], 10, 3.0), _roll(at['ga'], 10, 2.8),
                _roll(ht['res'],10, 0.5), _roll(at['res'],10, 0.5),
                min(1.0, ht['elo']/2000.0), min(1.0, at['elo']/2000.0),
            ]
            label = 1 if hg > ag else 0
            X.append(feat); y.append(label)
            # Update
            ht['gf'].append(hg); ht['ga'].append(ag)
            at['gf'].append(ag); at['ga'].append(hg)
            ht['res'].append(1 if hg>ag else 0)
            at['res'].append(1 if ag>hg else 0)
            exp_h = _elo_exp(ht['elo']+50, at['elo'])
            sh    = 1.0 if hg>ag else (0.5 if hg==ag else 0.0)
            ht['elo'] += 20*(sh-exp_h); at['elo'] += 20*((1-sh)-(1-exp_h))

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.int32)

def train():
    try:
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        import numpy as np, joblib
    except ImportError as e:
        print(f'  [NHL-ML] sklearn missing: {e}'); return False

    os.makedirs(DATA_DIR, exist_ok=True)
    print('  [NHL-ML] Downloading historical NHL data via api.nhle.com…')
    X, y = build_dataset()
    if X is None or len(X) < 200:
        print(f'  [NHL-ML] Insufficient data. Need 200+.')
        return False

    print(f'  [NHL-ML] Training on {len(X)} samples × {len(FEATURE_NAMES)} features…')
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
    print(f'  [NHL-ML] CV accuracy: {scores.mean():.3f} ± {scores.std():.3f}')
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
    print(f'  [NHL-ML] Saved → {MODEL_PATH}')
    return True

if __name__ == '__main__':
    ok = train()
    raise SystemExit(0 if ok else 1)
