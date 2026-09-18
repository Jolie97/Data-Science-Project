"""Extracts and decomposes verifiable factual claims from raw Reddit posts."""

import re
import spacy
from typing import List, Dict

# Load lightweight English NLP pipeline
nlp = spacy.load("en_core_web_sm")

# Common crypto tickers and entity patterns
CRYPTO_TICKERS = r"\b(BTC|ETH|SOL|ADA|XRP|DOGE|USDT|USDC|BNB|AVAX|DOT|LINK|MATIC|CLARITY|SEC|CFTC|FED)\b"
NUMERIC_OR_MONEY = (
    r"(\$\d+[\d,]*(\.\d+)?|\b\d+(\.\d+)?%|\b\d+k\b|\b\d+M\b|\b\d+B\b|\b\d{4}\b)"
)

# Subjective opinion/question indicators to filter out
OPINION_PATTERNS = [
    r"^\s*what do you think",
    r"^\s*curious if",
    r"^\s*in my opinion",
    r"^\s*anyone else",
    r"^\s*am i the only one",
    r"\?$",  # Exclude questions
]


def is_opinion_or_question(sentence_text: str) -> bool:
    """Returns True if the sentence is a question or purely conversational filler."""
    text_lower = sentence_text.strip().lower()
    for pattern in OPINION_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    return False


def has_factual_or_numeric_anchor(doc) -> bool:
    """Checks if a sentence contains named entities, monetary values, or crypto tickers."""
    text = doc.text

    # 1. Regex check for crypto tickers or dollar/percentage figures
    if re.search(CRYPTO_TICKERS, text, re.IGNORECASE) and re.search(
        NUMERIC_OR_MONEY, text, re.IGNORECASE
    ):
        return True

    # 2. spaCy Named Entity check (MONEY, ORG, LAW, DATE, PERCENT)
    relevant_entities = {
        "MONEY",
        "ORG",
        "LAW",
        "DATE",
        "PERCENT",
        "CARDINAL",
        "PRODUCT",
    }
    entity_labels = {ent.label_ for ent in doc.ents}

    if relevant_entities.intersection(entity_labels) and len(doc) >= 5:
        return True

    return False


def decompose_post(post_id: str, title: str, description: str) -> List[Dict]:
    """
    Decomposes a Reddit post's title and description into atomic, testable claims.
    """
    claims = []

    # Process Title First (Titles are high-priority claim candidates)
    if title and len(title.strip()) > 10 and not is_opinion_or_question(title):
        title_doc = nlp(title.strip())
        claims.append(
            {
                "post_id": post_id,
                "claim_text": title.strip(),
                "source_segment": "title",
                "has_entities": bool(title_doc.ents),
            }
        )

    # Process Body / Description
    if description and len(description.strip()) > 15:
        desc_doc = nlp(description)
        for sent in desc_doc.sents:
            sent_text = sent.text.strip()

            # Filter noise: must be > 6 words, not a question, and contain verifiable entities
            if len(sent_text.split()) >= 6 and not is_opinion_or_question(sent_text):
                if has_factual_or_numeric_anchor(sent):
                    claims.append(
                        {
                            "post_id": post_id,
                            "claim_text": sent_text,
                            "source_segment": "body",
                            "has_entities": True,
                        }
                    )

    return claims
