"""JSON serialization boundary. Original chart calculations are preserved."""
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

VERDICTS = ['ACCURATE', 'APPROXIMATE', 'INACCURATE']


def clean(value):
    """Convert pandas/numpy scalars and absent values to strict RFC JSON."""
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if hasattr(value, 'item'):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return value


def records(frame):
    return clean(frame.to_dict(orient='records'))


def series(value):
    return clean({'labels': value.index.tolist(), 'values': value.tolist()})


def matrix(frame):
    return clean({'labels': frame.index.tolist(), 'datasets': [
        {'label': str(col), 'data': frame[col].tolist()} for col in frame.columns
    ]})


def write_json(payload, output):
    """Atomic replacement: Flask never sees a half-written snapshot."""
    output = Path(output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(clean(payload), ensure_ascii=False, indent=2, allow_nan=False) + '\n'
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         suffix='.tmp', delete=False) as f:
            tmp = Path(f.name)
            f.write(content)
        os.replace(tmp, output)
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def summaries_from_csv(df, min_claims_user=3):
    """Only used for explicit --csv: same summaries as verify_claim.py."""
    def trust_summary(group_cols):
        scored = df[df["SCORED"]]
        g = scored.groupby(group_cols)["DASHBOARD_VERDICT"]
        t = pd.DataFrame({
            "SCORED_CLAIMS": g.size(),
            "ACCURATE": g.apply(lambda s: (s == "ACCURATE").sum()),
            "APPROXIMATE": g.apply(lambda s: (s == "APPROXIMATE").sum()),
            "INACCURATE": g.apply(lambda s: (s == "INACCURATE").sum()),
        })
        t["TRUST_SCORE"] = ((t["ACCURATE"] + 0.5 * t["APPROXIMATE"]) / t["SCORED_CLAIMS"] * 100).round(1)
        total = df.groupby(group_cols).size().rename("TOTAL_CLAIMS")
        return t.join(total, how="right").fillna(0).reset_index()

    by_sub = trust_summary(["SUBREDDIT"])
    by_user = trust_summary(["AUTHOR"])
    by_user["TRUST_LABEL"] = by_user.apply(
        lambda r: "NOT ENOUGH DATA" if r["SCORED_CLAIMS"] < min_claims_user else
        "RELIABLE" if r["TRUST_SCORE"] >= 70 else "CAUTION" if r["TRUST_SCORE"] >= 40 else "SPREADER",
        axis=1)
    return by_sub, by_user


def export_dashboard(df, by_sub, by_user, output, source, *, counts, checkable,
                     type_counts, by_type_verdict, by_coin_verdict, sub_plot,
                     top_n, min_claims_user):
    # Additive metadata and presentation views; never recompute upstream verdicts.
    dates = pd.to_datetime(df.get('POST_DATE', pd.Series(dtype=str)), errors='coerce')
    coverage = pd.crosstab(df['CLAIM_TYPE'], df['SCORED'].map({True: 'Scored', False: 'Not scored'}))
    payload = {
        'schema_version': 2,
        'meta': {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'source_file': source,
            'summary_source': 'Original trust_summary formula applied to CSV' if source.endswith('.csv') else 'Existing SQLite summary tables',
            'total_claims': len(df), 'scored_claims': int(df['SCORED'].sum()),
            'not_scored_claims': int((~df['SCORED']).sum()),
            'date_start': dates.min().date().isoformat() if dates.notna().any() else None,
            'date_end': dates.max().date().isoformat() if dates.notna().any() else None,
            'missing_dates': int(dates.isna().sum()),
            'min_claims_user': min_claims_user,
            'trust_formula': '(ACCURATE + 0.5 × APPROXIMATE) / SCORED_CLAIMS × 100; rounded to 1 decimal',
            'notes': [
                'Sections 01–03 use the existing results_file1.json / results_file2.json. Sections 04–09 are a separate market-verification cohort; totals need not match.',
                'SCORED is normalized exactly as in overview_charts_new.py: only 1 / true count as scored.',
                'ACCURATE, APPROXIMATE and INACCURATE are the existing market-based classifications, not NLI support probabilities or guarantees of truth.',
                'Unscored claims are excluded from accuracy and trust denominators; missing values remain null, never invented zeros.',
                'Trust is claim-weighted, not weighted by unique posts or users. Small sample sizes can produce extreme scores; see the scatter plot.',
                'User view uses the original minimum of 3 scored claims and the deduplicated lowest/highest 10 selection.',
                'Original extraction, coin/proxy matching, thresholds, market windows and timestamp fallback are unchanged. POST_DATE may use the parent post timestamp when a comment timestamp is unavailable.',
                'No per-subreddit sentiment, topic sentiment or post/comment divergence is inferred from these files.',
            ],
        },
        'overview': {
            'verdict_breakdown': series(counts), 'checkable_share': series(checkable),
            'claim_types': series(type_counts), 'accuracy_by_type': matrix(by_type_verdict),
            'accuracy_by_coin': matrix(by_coin_verdict),
            'trust_by_subreddit': records(sub_plot), 'user_reliability': records(top_n),
        },
        'coverage_by_type': matrix(coverage),
        'status_counts': series(df['STATUS'].value_counts()),
        'subreddit_scores': [
            {'subreddit': r['SUBREDDIT'], 'claims': r['TOTAL_CLAIMS'],
             'scored_claims': r['SCORED_CLAIMS'], 'accurate': r['ACCURATE'],
             'approximate': r['APPROXIMATE'], 'inaccurate': r['INACCURATE'],
             'verification_score': r['TRUST_SCORE'] if r['SCORED_CLAIMS'] > 0 else None}
            for r in records(by_sub)
        ],
        'availability': {'subreddit_sentiment': False, 'top_topics': False, 'divergence': False},
    }
    payload = clean(payload)
    write_json(payload, output)
    return payload


def export_market_charts(df, by_day, prices, output):
    """Serialize all additional plots previously drawn by verify_claim.py.

    Called only after the original verifier has real in-memory market history.
    CSV-only runs do not invent missing daily closing price series.
    """
    payload = json.loads(Path(output).read_text(encoding='utf-8'))
    ct = pd.crosstab(df['CLAIM_TYPE'], df['SCORED'].map({True: 'Scored', False: 'Not scored'}))
    scored_ct = pd.crosstab(df.loc[df['SCORED'], 'CLAIM_TYPE'], df.loc[df['SCORED'], 'DASHBOARD_VERDICT'])
    scored_ct = scored_ct.reindex(columns=['ACCURATE', 'APPROXIMATE', 'INACCURATE'], fill_value=0)
    market = {'checkability_by_type': matrix(ct), 'accuracy_by_type': matrix(scored_ct),
              'weekly': None, 'sentiment_vs_market': None, 'claims_vs_price': {}}
    scored = df[df['SCORED']]
    if not scored.empty:
        wk = pd.crosstab(scored['WEEK'], scored['DASHBOARD_VERDICT'], normalize='index') * 100
        wk = wk.reindex(columns=['ACCURATE', 'APPROXIMATE', 'INACCURATE'], fill_value=0)
        vol = df.groupby('WEEK')['MKT_RANGE_PCT'].mean().reindex(wk.index)
        market['weekly'] = {'accuracy_percent': matrix(wk), 'average_daily_volatility_percent': series(vol)}
    if by_day is not None and len(by_day) > 1:
        market['sentiment_vs_market'] = records(by_day)
    draw_order = ['INACCURATE', 'APPROXIMATE', 'ACCURATE']
    for coin, h in prices.items():
        sub = df[(df['COIN'] == coin) & df['SCORED']]
        if sub.empty:
            continue
        points = {}
        for verdict in draw_order:
            s = sub[sub['DASHBOARD_VERDICT'] == verdict]
            if s.empty:
                continue
            y = h['Close'].reindex(s['POST_DATE']).values
            points[verdict] = [{'x': date, 'y': price} for date, price in zip(s['POST_DATE'], y)]
        daily = sub.groupby([sub['POST_DATE'], 'DASHBOARD_VERDICT']).size().unstack(fill_value=0)
        daily = daily.reindex(columns=['ACCURATE', 'APPROXIMATE', 'INACCURATE'], fill_value=0)
        market['claims_vs_price'][coin] = {'close': series(h['Close']), 'points': points, 'daily_counts': matrix(daily)}
    payload['market_charts'] = clean(market)
    write_json(payload, output)
