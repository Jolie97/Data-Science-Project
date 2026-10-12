"""
CryptoTruth Quantitative Evaluation Script
Generates Accuracy, Precision, Recall, F1-scores, and Confusion Matrices.
"""

import sys
from pathlib import Path
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

# Project setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from sentiment.analyser import SentimentEngine
from verifier.claim_verifier import ClaimVerifier


def evaluate_sentiment(test_csv_path: Path):
    print("=" * 60)
    print(" 1. EVALUATING SENTIMENT ANALYSIS PIPELINE (VADER / FinBERT)")
    print("=" * 60)

    df = pd.read_csv(test_csv_path)
    engine = SentimentEngine()

    predicted_labels = []
    for _, row in df.iterrows():
        text = f"{row['title']} {str(row.get('description', ''))}"
        score = engine.score_vader(text)["compound"]

        # Mapped thresholds
        if score >= 0.05:
            predicted_labels.append("Bullish")
        elif score <= -0.05:
            predicted_labels.append("Bearish")
        else:
            predicted_labels.append("Neutral")

    df["predicted"] = predicted_labels
    y_true = df["true_sentiment"]
    y_pred = df["predicted"]

    labels = ["Bullish", "Bearish", "Neutral"]
    print(f"Overall Accuracy:  {accuracy_score(y_true, y_pred):.4f}")
    print(
        f"Macro F1-Score:    {f1_score(y_true, y_pred, average='macro', zero_division=0):.4f}"
    )
    print(
        f"Weighted F1-Score: {f1_score(y_true, y_pred, average='weighted', zero_division=0):.4f}\n"
    )

    print("Detailed Classification Report:")
    print(classification_report(y_true, y_pred, target_names=labels, zero_division=0))

    print("Confusion Matrix:")
    print(
        pd.DataFrame(
            confusion_matrix(y_true, y_pred, labels=labels),
            index=labels,
            columns=labels,
        )
    )


def evaluate_claim_verifier(test_csv_path: Path):
    print("\n" + "=" * 60)
    print(" 2. EVALUATING CLAIM VERIFICATION (DeBERTa + ChromaDB)")
    print("=" * 60)

    df = pd.read_csv(test_csv_path)
    verifier = ClaimVerifier()

    predicted_labels = []
    for claim in df["claim_text"]:
        res = verifier.verify_claim(claim)
        predicted_labels.append(res["veracity"])  # SUPPORTED or REFUTED

    df["predicted"] = predicted_labels
    y_true = df["true_veracity"]
    y_pred = df["predicted"]

    labels = ["SUPPORTED", "REFUTED"]
    print(f"Overall Accuracy:  {accuracy_score(y_true, y_pred):.4f}")
    print(
        f"Macro F1-Score:    {f1_score(y_true, y_pred, average='macro', zero_division=0):.4f}"
    )
    print(
        f"Weighted F1-Score: {f1_score(y_true, y_pred, average='weighted', zero_division=0):.4f}\n"
    )

    print("Detailed Classification Report:")
    print(classification_report(y_true, y_pred, target_names=labels, zero_division=0))

    print("Confusion Matrix:")
    print(
        pd.DataFrame(
            confusion_matrix(y_true, y_pred, labels=labels),
            index=labels,
            columns=labels,
        )
    )


if __name__ == "__main__":
    sentiment_test = PROJECT_ROOT / "data" / "evaluation_sentiment_testset.csv"
    claims_test = PROJECT_ROOT / "data" / "evaluation_claims_testset.csv"

    if sentiment_test.exists():
        evaluate_sentiment(sentiment_test)
    else:
        print(f"Missing {sentiment_test}. Create the labeled test set first.")

    if claims_test.exists():
        evaluate_claim_verifier(claims_test)
    else:
        print(f"Missing {claims_test}. Create the labeled test set first.")
