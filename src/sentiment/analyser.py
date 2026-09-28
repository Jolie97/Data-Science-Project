"""Sentiment analysis engine using VADER and FinBERT."""

import logging
from typing import Dict, List, Optional
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Ensure VADER lexicon is available locally
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)


class SentimentEngine:
    def __init__(self, load_finbert: bool = True, device: Optional[str] = None):
        """Initializes VADER and optionally loads FinBERT."""
        # 1. Initialize VADER
        self.vader = SentimentIntensityAnalyzer()

        # Add crypto-specific terminology adjustments
        crypto_lexicon_updates = {
            "bullish": 2.0,
            "bearish": -2.0,
            "moon": 2.5,
            "dump": -2.5,
            "pump": 2.0,
            "hodl": 1.5,
            "rugpull": -3.5,
            "scam": -3.0,
            "rekt": -3.0,
            "fud": -2.0,
        }
        self.vader.lexicon.update(crypto_lexicon_updates)

        # 2. Initialize FinBERT if requested
        self.finbert_loaded = False
        if load_finbert:
            self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
            logging.info("Loading FinBERT on %s...", self.device)
            self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            self.model = AutoModelForSequenceClassification.from_pretrained(
                "ProsusAI/finbert"
            ).to(self.device)
            self.model.eval()
            self.finbert_loaded = True
            logging.info("FinBERT loaded successfully.")

    def score_vader(self, text: str) -> Dict[str, float]:
        """Calculates VADER polarity scores."""
        if not text or not text.strip():
            return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0}
        return self.vader.polarity_scores(text)

    def score_finbert_batch(
        self, texts: List[str], batch_size: int = 16
    ) -> List[Dict[str, float]]:
        """Scores a batch of texts using FinBERT with 512 token truncation."""
        if not self.finbert_loaded:
            raise RuntimeError("FinBERT was not initialized. Set load_finbert=True.")

        results = []
        cleaned_texts = [t if (t and t.strip()) else "neutral" for t in texts]

        for i in range(0, len(cleaned_texts), batch_size):
            batch = cleaned_texts[i : i + batch_size]
            inputs = self.tokenizer(
                batch,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512,
            ).to(self.device)

            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            # FinBERT label mapping: 0 -> positive, 1 -> negative, 2 -> neutral
            for probs in probabilities.cpu().numpy():
                pos, neg, neu = float(probs[0]), float(probs[1]), float(probs[2])
                # Normalized net sentiment score between -1.0 and +1.0
                net_score = pos - neg
                results.append(
                    {
                        "finbert_positive": pos,
                        "finbert_negative": neg,
                        "finbert_neutral": neu,
                        "finbert_net": net_score,
                    }
                )

        return results
