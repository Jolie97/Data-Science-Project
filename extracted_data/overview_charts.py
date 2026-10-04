import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# CONFIG - reads the output of verify_claims_step1.py, makes no changes to it
# ============================================================
BASE = r"C:\Users\felij\Downloads\Capstone\Final"
verified_db = BASE + r"\verified_claims.db"

VERDICT_COLORS = {"ACCURATE": "#2e7d32", "APPROXIMATE": "#e08e00", "INACCURATE": "#c62828"}
MIN_CLAIMS_USER = 3

conn = sqlite3.connect(verified_db)
df = pd.read_sql("SELECT * FROM verified_claims", conn)
by_sub = pd.read_sql("SELECT * FROM summary_by_subreddit", conn)
by_user = pd.read_sql("SELECT * FROM summary_by_user", conn)
conn.close()

# SQLite stores booleans as 0/1 integers (or sometimes text) - normalize once
# so every later .map()/filter on this column behaves consistently.
df["SCORED"] = df["SCORED"].astype(str).str.strip().str.lower().isin(["1", "true"])

scored = df[df["SCORED"]]
print(f"Total claims: {len(df)} | Scored (checkable): {len(scored)}")

# ============================================================
# 1. OVERALL VERDICT BREAKDOWN (pie) - the headline chart
# ============================================================
counts = scored["DASHBOARD_VERDICT"].value_counts().reindex(
    ["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
fig, ax = plt.subplots(figsize=(7, 7))
ax.pie(counts, labels=[f"{k} ({v})" for k, v in counts.items()],
       colors=[VERDICT_COLORS[k] for k in counts.index],
       autopct="%1.0f%%", startangle=90, wedgeprops={"edgecolor": "white"})
ax.set_title(f"Overall claim accuracy ({len(scored)} checkable claims)")
plt.tight_layout()
plt.savefig(BASE + r"\overview_verdict_breakdown.png", dpi=150)
plt.close()

# ============================================================
# 2. HOW MANY CLAIMS WERE EVEN CHECKABLE (pie)
# ============================================================
checkable = df["SCORED"].map({True: "Checkable", False: "Not checkable"}).value_counts()
fig, ax = plt.subplots(figsize=(7, 7))
ax.pie(checkable, labels=[f"{k} ({v})" for k, v in checkable.items()],
       colors=["#2e7d32", "#9e9e9e"], autopct="%1.0f%%", startangle=90,
       wedgeprops={"edgecolor": "white"})
ax.set_title(f"How many of the {len(df)} claims could be checked against the market")
plt.tight_layout()
plt.savefig(BASE + r"\overview_checkable_share.png", dpi=150)
plt.close()

# ============================================================
# 3. CLAIM TYPES - overall counts, no dates
# ============================================================
type_counts = df["CLAIM_TYPE"].value_counts().sort_values()
fig, ax = plt.subplots(figsize=(9, 5))
type_counts.plot.barh(ax=ax, color="#3a7bd5")
ax.set_title("Claims by type (overall)")
ax.set_xlabel("Number of claims")
plt.tight_layout()
plt.savefig(BASE + r"\overview_claim_types.png", dpi=150)
plt.close()

# ============================================================
# 4. ACCURACY BY CLAIM TYPE (scored claims only)
# ============================================================
by_type_verdict = pd.crosstab(scored["CLAIM_TYPE"], scored["DASHBOARD_VERDICT"])
by_type_verdict = by_type_verdict.reindex(columns=["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
fig, ax = plt.subplots(figsize=(9, 4.5))
by_type_verdict.plot.barh(stacked=True, ax=ax,
                          color=[VERDICT_COLORS[c] for c in by_type_verdict.columns])
ax.set_title("Accuracy by claim type")
ax.set_xlabel("Number of claims")
plt.tight_layout()
plt.savefig(BASE + r"\overview_accuracy_by_type.png", dpi=150)
plt.close()

# ============================================================
# 5. ACCURACY BY COIN (scored claims only)
# ============================================================
by_coin_verdict = pd.crosstab(scored["COIN"], scored["DASHBOARD_VERDICT"])
by_coin_verdict = by_coin_verdict.reindex(columns=["ACCURATE", "APPROXIMATE", "INACCURATE"], fill_value=0)
by_coin_verdict = by_coin_verdict.loc[by_coin_verdict.sum(axis=1).sort_values().index]
fig, ax = plt.subplots(figsize=(9, 4.5))
by_coin_verdict.plot.barh(stacked=True, ax=ax,
                         color=[VERDICT_COLORS[c] for c in by_coin_verdict.columns])
ax.set_title("Accuracy by coin")
ax.set_xlabel("Number of claims")
plt.tight_layout()
plt.savefig(BASE + r"\overview_accuracy_by_coin.png", dpi=150)
plt.close()

# ============================================================
# 6. TRUST SCORE BY SUBREDDIT
# ============================================================
sub_plot = by_sub[by_sub["SCORED_CLAIMS"] > 0].sort_values("TRUST_SCORE")
if not sub_plot.empty:
    fig, ax = plt.subplots(figsize=(9, max(3, 0.4 * len(sub_plot))))
    bar_colors = ["#2e7d32" if v >= 70 else "#e08e00" if v >= 40 else "#c62828"
                  for v in sub_plot["TRUST_SCORE"]]
    ax.barh(sub_plot["SUBREDDIT"], sub_plot["TRUST_SCORE"], color=bar_colors)
    ax.set_xlabel("Trust score (0-100)")
    ax.set_title("Subreddit trust score (overall)")
    ax.set_xlim(0, 100)
    plt.tight_layout()
    plt.savefig(BASE + r"\overview_trust_by_subreddit.png", dpi=150)
    plt.close()

# ============================================================
# 7. TOP RELIABLE USERS / TOP SPREADERS (users with enough claims)
# ============================================================
qualified = by_user[by_user["SCORED_CLAIMS"] >= MIN_CLAIMS_USER].sort_values("TRUST_SCORE")
if not qualified.empty:
    top_n = pd.concat([qualified.head(10), qualified.tail(10)]).drop_duplicates()
    fig, ax = plt.subplots(figsize=(9, max(4, 0.4 * len(top_n))))
    bar_colors = ["#2e7d32" if v >= 70 else "#e08e00" if v >= 40 else "#c62828"
                  for v in top_n["TRUST_SCORE"]]
    ax.barh(top_n["AUTHOR"], top_n["TRUST_SCORE"], color=bar_colors)
    ax.set_xlabel("Trust score (0-100)")
    ax.set_title(f"Most/least reliable users (min {MIN_CLAIMS_USER} scored claims)")
    ax.set_xlim(0, 100)
    plt.tight_layout()
    plt.savefig(BASE + r"\overview_user_reliability.png", dpi=150)
    plt.close()
else:
    print(f"No users have {MIN_CLAIMS_USER}+ scored claims yet - skipping user reliability chart.")

print("\nDone. Overview charts (no dates) saved in", BASE)
print(" - overview_verdict_breakdown.png")
print(" - overview_checkable_share.png")
print(" - overview_claim_types.png")
print(" - overview_accuracy_by_type.png")
print(" - overview_accuracy_by_coin.png")
print(" - overview_trust_by_subreddit.png")
print(" - overview_user_reliability.png (if enough qualifying users)")