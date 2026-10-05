"""Export the original overview chart results as dashboard JSON (no NLP rerun)."""
import argparse
import sqlite3
from pathlib import Path
import pandas as pd
from dashboard_export_new import export_dashboard, summaries_from_csv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIN_CLAIMS_USER = 3


def export_overview(df, by_sub, by_user, output, source):
    df["SCORED"] = df["SCORED"].astype(str).str.strip().str.lower().isin(["1", "true"])
    scored = df[df["SCORED"]]
    counts = scored["DASHBOARD_VERDICT"].value_counts().reindex(
        ["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
    checkable = df["SCORED"].map({True: "Checkable", False: "Not checkable"}).value_counts()
    type_counts = df["CLAIM_TYPE"].value_counts().sort_values()
    by_type_verdict = pd.crosstab(scored["CLAIM_TYPE"], scored["DASHBOARD_VERDICT"])
    by_type_verdict = by_type_verdict.reindex(columns=["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
    by_coin_verdict = pd.crosstab(scored["COIN"], scored["DASHBOARD_VERDICT"])
    by_coin_verdict = by_coin_verdict.reindex(columns=["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
    by_coin_verdict = by_coin_verdict.loc[by_coin_verdict.sum(axis=1).sort_values().index]
    sub_plot = by_sub[by_sub["SCORED_CLAIMS"] > 0].sort_values("TRUST_SCORE")
    qualified = by_user[by_user["SCORED_CLAIMS"] >= MIN_CLAIMS_USER].sort_values("TRUST_SCORE")
    top_n = pd.DataFrame(columns=by_user.columns)
    if not qualified.empty:
        top_n = pd.concat([qualified.head(10), qualified.tail(10)]).drop_duplicates()
    return export_dashboard(
        df, by_sub, by_user, output, source,
        counts=counts, checkable=checkable, type_counts=type_counts,
        by_type_verdict=by_type_verdict, by_coin_verdict=by_coin_verdict,
        sub_plot=sub_plot, top_n=top_n, min_claims_user=MIN_CLAIMS_USER,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument('--db', type=Path, help='Existing verified_claims.db, read-only')
    inputs.add_argument('--csv', type=Path, help='Existing verified_claims.csv; rebuild original summaries')
    parser.add_argument('--output', type=Path, default=PROJECT_ROOT / 'notebooks/Prototype/dashboard_data_new.json')
    args = parser.parse_args()
    source = (args.csv or args.db or Path(__file__).with_name('verified_claims.db')).resolve()
    if not source.is_file():
        parser.error(f'Input does not exist: {source}. Use --db or --csv explicitly.')
    if args.csv:
        df = pd.read_csv(source)
        df['SCORED'] = df['SCORED'].astype(str).str.strip().str.lower().isin(['1', 'true'])
        by_sub, by_user = summaries_from_csv(df, MIN_CLAIMS_USER)
    else:
        with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True) as conn:
            df = pd.read_sql('SELECT * FROM verified_claims', conn)
            by_sub = pd.read_sql('SELECT * FROM summary_by_subreddit', conn)
            by_user = pd.read_sql('SELECT * FROM summary_by_user', conn)
    payload = export_overview(df, by_sub, by_user, args.output, source.name)
    print(f"Saved {args.output.resolve()} ({payload['meta']['total_claims']} claims, "
          f"{payload['meta']['scored_claims']} scored)")


if __name__ == '__main__':
    main()
