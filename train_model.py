#!/usr/bin/env python3
"""
Football 1X2 prediction ML model trainer.

Downloads historical match data from football-data.co.uk (no API key)
and trains a GradientBoostingClassifier. Called automatically by proxy.py
on first startup if data/prediction_model.pkl is missing.

Usage: python3 train_model.py
Output: data/prediction_model.pkl, data/prediction_model_meta.json
"""
import os, csv, io, math, json, datetime as _dt, urllib.request

DATA_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
MODEL_PATH = os.path.join(DATA_DIR, 'prediction_model.pkl')
META_PATH  = os.path.join(DATA_DIR, 'prediction_model_meta.json')

# Feature order is a contract between train_model.py and proxy.py _ml_features().
FEATURE_NAMES = [
    'form_h', 'form_a',           # rolling 5-game points fraction (0–1)
    'sot_h',  'sot_a',            # shots on target per game (venue-split)
    'gf_h',   'ga_h',             # home team goals for/against per home game
    'gf_a',   'ga_a',             # away team goals for/against per away game
    'xg_h',   'xg_a',            # expected goals per game (venue-split; fallback=goals)
    'shin_h', 'shin_d', 'shin_a', # Shin-corrected market probs (0.44/0.27/0.29 default)
    'has_odds',                    # 1 if odds available, else 0
    'overround',                   # bookmaker margin (sum 1/odds); 0 if no odds; lower=sharper market
    'season_stage',                # fraction of season elapsed (0–1)
    'league_id',                   # encoded league (0–1 normalized)
    'elo_diff',                    # rolling Elo home−away (raw points; scaler normalises)
]

# football-data.co.uk free CSV sources
FDCO_LEAGUES = {
    'PL':'E0', 'ELC':'E1',
    'BL1':'D1', 'PD':'SP1', 'SA':'I1', 'FL1':'F1',
    'DED':'N1', 'PPL':'P1',
}
# 8 seasons — doubles training data vs the previous 4-season set
FDCO_SEASONS = ['1718', '1819', '1920', '2021', '2122', '2223', '2324', '2425']


def _safe_float(v, default=None):
    if v in (None, '', 'NA', 'N/A', '-', '#N/A'): return default
    try: return float(v)
    except: return default

def _parse_date(s):
    for fmt in ('%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d'):
        try: return _dt.datetime.strptime(s.strip(), fmt).date()
        except: pass
    return None

def _shin(oh, od, oa, z=0.03):
    if not (oh and od and oa and oh > 1 and od > 1 and oa > 1): return None, None, None
    w = [1/oh, 1/od, 1/oa]; W = sum(w); q = [wi/W for wi in w]; p = q[:]
    for _ in range(50):
        S = sum(pi*pi for pi in p); A = z + (1-z)*S
        pn = [math.sqrt(max(0.0, (A*qi - z/3.0) / max(1e-12, 1-z))) for qi in q]
        tot = sum(pn)
        if tot < 1e-10: break
        p = [pi/tot for pi in pn]
    tot = sum(p)
    return tuple(pi/tot for pi in p) if tot > 0 else tuple(q)

def _best_odds(row):
    """Return (oh, od, oa, overround) from sharpest available book.
    Priority: Pinnacle (sharpest) → Bet365 → William Hill → market average."""
    def _try(hk, dk, ak):
        h = _safe_float(row.get(hk)); d = _safe_float(row.get(dk)); a = _safe_float(row.get(ak))
        return (h, d, a) if (h and d and a and h > 1 and d > 1 and a > 1) else None
    t = (_try('PSH','PSD','PSA') or _try('B365H','B365D','B365A') or
         _try('WHH','WHD','WHA') or _try('BbAvH','BbAvD','BbAvA'))
    if t:
        oh, od, oa = t
        return oh, od, oa, round(1/oh + 1/od + 1/oa, 4)
    return None, None, None, 0.0

def _roll(lst, n=5, default=None):
    vals = [v for v in lst[-n:] if v is not None]
    return sum(vals)/len(vals) if vals else default

def _fetch_csv(season, code):
    url = f'https://www.football-data.co.uk/mmz4281/{season}/{code}.csv'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'ScoutlineML/1.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode('latin-1')
    except Exception as e:
        print(f'  [ML] fetch failed {code}/{season}: {e}')
        return None


def _elo_expected(ra, rb):
    return 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))


def build_dataset():
    import numpy as np
    X, y = [], []

    for lg_idx, (comp, code) in enumerate(FDCO_LEAGUES.items()):
        lg_norm = lg_idx / max(1, len(FDCO_LEAGUES) - 1)
        n_ok = 0
        # Elo carries across seasons within the same league
        elo_ratings = {}
        history = {}

        for season in FDCO_SEASONS:
            raw = _fetch_csv(season, code)
            if not raw:
                continue
            reader = csv.DictReader(io.StringIO(raw))
            rows = [r for r in reader
                    if r.get('HomeTeam', '').strip()
                    and r.get('FTR', '').strip() in ('H', 'D', 'A')]
            rows.sort(key=lambda r: _parse_date(r.get('Date', '')) or _dt.date.min)
            if not rows:
                continue

            n_total = max(1, len(rows))

            def get_team(name):
                if name not in history:
                    history[name] = {
                        'res': [],
                        'h_gf': [], 'h_ga': [], 'h_sot': [], 'h_xg': [],
                        'a_gf': [], 'a_ga': [], 'a_sot': [], 'a_xg': [],
                    }
                return history[name]

            for idx, row in enumerate(rows):
                ht = row['HomeTeam'].strip()
                at = row['AwayTeam'].strip()
                ftr = row['FTR'].strip()
                hg  = _safe_float(row.get('FTHG'))
                ag  = _safe_float(row.get('FTAG'))
                hst = _safe_float(row.get('HST'))
                ast = _safe_float(row.get('AST'))
                # xG available from fdco since ~2014; HSxG/ASxG columns
                hxg = _safe_float(row.get('HSxG')) or _safe_float(row.get('xG_H'))
                axg = _safe_float(row.get('ASxG')) or _safe_float(row.get('xG_A'))
                # Pinnacle preferred (sharpest book) over B365
                oh, od, oa, overround = _best_odds(row)

                hd = get_team(ht)
                ad = get_team(at)
                stage = idx / n_total

                # Build features from history BEFORE this match (walk-forward)
                form_h = None
                if hd['res']:
                    last5 = hd['res'][-5:]
                    form_h = sum(last5) / (len(last5) * 3)
                form_a = None
                if ad['res']:
                    last5 = ad['res'][-5:]
                    form_a = sum(last5) / (len(last5) * 3)

                sot_h = _roll(hd['h_sot'], default=4.0)
                sot_a = _roll(ad['a_sot'], default=4.0)
                gf_h  = _roll(hd['h_gf'],  default=1.3)
                ga_h  = _roll(hd['h_ga'],  default=1.1)
                gf_a  = _roll(ad['a_gf'],  default=1.1)
                ga_a  = _roll(ad['a_ga'],  default=1.2)
                # xG falls back to actual goals when not in CSV
                xg_h  = _roll(hd['h_xg'],  default=None) or gf_h
                xg_a  = _roll(ad['a_xg'],  default=None) or gf_a

                elo_h = elo_ratings.get(ht, 1500.0)
                elo_a = elo_ratings.get(at, 1500.0)
                elo_diff = elo_h - elo_a

                sh, sd, sa = _shin(oh, od, oa)
                has_odds = 1.0 if sh is not None else 0.0
                if sh is None:
                    sh, sd, sa = 0.44, 0.27, 0.29

                if form_h is not None and form_a is not None:
                    feat = [form_h, form_a, sot_h, sot_a, gf_h, ga_h, gf_a, ga_a,
                            xg_h, xg_a, sh, sd, sa, has_odds, overround, stage, lg_norm, elo_diff]
                    X.append(feat)
                    y.append({'H': 0, 'D': 1, 'A': 2}[ftr])

                # Update rolling stats and Elo AFTER computing features
                if hg is not None and ag is not None:
                    h_pts = 3 if hg > ag else (1 if hg == ag else 0)
                    a_pts = 3 if ag > hg else (1 if ag == hg else 0)
                    hd['res'].append(h_pts); ad['res'].append(a_pts)
                    hd['h_gf'].append(hg); hd['h_ga'].append(ag); hd['h_sot'].append(hst)
                    hd['h_xg'].append(hxg if hxg is not None else hg)
                    ad['a_gf'].append(ag); ad['a_ga'].append(hg); ad['a_sot'].append(ast)
                    ad['a_xg'].append(axg if axg is not None else ag)
                    # Elo update (K=20, +50 home advantage in expected)
                    exp_h = _elo_expected(elo_h + 50, elo_a)
                    score_h = 1.0 if ftr == 'H' else (0.5 if ftr == 'D' else 0.0)
                    elo_ratings[ht] = elo_h + 20 * (score_h - exp_h)
                    elo_ratings[at] = elo_a + 20 * ((1 - score_h) - (1 - exp_h))

            n_ok += 1

        if n_ok:
            print(f'  [ML] {comp}: {n_ok} seasons  (total rows so far: {len(X)})')

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.int32)


def train():
    try:
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        import numpy as np
        import joblib
    except ImportError as e:
        print(f'  [ML] Missing dependency: {e}')
        print('  [ML] Install with: pip install scikit-learn numpy joblib')
        return False

    os.makedirs(DATA_DIR, exist_ok=True)
    print('  [ML] Downloading historical match data from football-data.co.uk...')
    X, y = build_dataset()

    if len(X) < 500:
        print(f'  [ML] Insufficient training data ({len(X)} samples). Need 500+.')
        return False

    print(f'  [ML] Training on {len(X)} samples × {len(FEATURE_NAMES)} features...')
    try:
        from xgboost import XGBClassifier
        clf = XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            eval_metric='mlogloss', n_jobs=1, random_state=42,
        )
        algo = 'XGBoost'
        print('  [ML] Using XGBoost classifier')
    except ImportError:
        from sklearn.ensemble import RandomForestClassifier
        clf = RandomForestClassifier(
            n_estimators=120, max_depth=7, min_samples_leaf=15,
            n_jobs=1, random_state=42,
        )
        algo = 'RandomForest'
        print('  [ML] XGBoost not available — using RandomForest')
    model = Pipeline([
        ('scaler', StandardScaler()),
        ('clf', clf),
    ])

    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring='accuracy', n_jobs=1)
    print(f'  [ML] CV accuracy: {scores.mean():.3f} ± {scores.std():.3f}')

    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)

    from collections import Counter
    dist = Counter(y.tolist())
    meta = {
        'cv_accuracy': round(float(scores.mean()), 4),
        'cv_std':      round(float(scores.std()),  4),
        'n_train':     int(len(X)),
        'n_features':  len(FEATURE_NAMES),
        'algorithm':   algo,
        'feature_names': FEATURE_NAMES,
        'outcome_dist': {'H': int(dist.get(0, 0)), 'D': int(dist.get(1, 0)), 'A': int(dist.get(2, 0))},
        'built': _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    with open(META_PATH, 'w') as f:
        json.dump(meta, f, indent=2)

    print(f'  [ML] Model saved → {MODEL_PATH}  (accuracy={scores.mean():.3f})')
    return True


if __name__ == '__main__':
    ok = train()
    raise SystemExit(0 if ok else 1)
