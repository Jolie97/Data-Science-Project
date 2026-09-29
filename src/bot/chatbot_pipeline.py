"""Chatbot pipeline: turns raw text into a sentiment result.
Claim verification will be merged in here once that model exists.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))  # adds src/ to the path

from typing import Dict, Optional
from sentiment.analyser import SentimentEngine

_engine: Optional[SentimentEngine] = None

import logging

#quiet down noisy third-party loggers
for noisy_logger in ("httpx", "huggingface_hub", "urllib3", "filelock"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

def get_engine() -> SentimentEngine:
    """Lazily initialise the engine so importing this module stays cheap."""
    global _engine
    if _engine is None:
        _engine = SentimentEngine(load_finbert=True)
    return _engine


def analyse_text(text: str) -> Dict:
    """Run one piece of text through the sentiment pipeline."""
    engine = get_engine()
    return {
        "text": text,
        "vader": engine.score_vader(text),
        "finbert": engine.score_finbert_batch([text])[0],
    }

if __name__ == "__main__":
    print(analyse_text("US dumps are back. Someone is dumping in big numbers exactly as yesterday. Steady, consistent selling around the same time of the day. Jane Street again? Old wallets coming back online? "))