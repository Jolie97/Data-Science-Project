"""Populates a persistent ChromaDB vector store with trusted crypto news and facts."""

import hashlib
import logging
from pathlib import Path
from typing import List, Dict
import chromadb
from chromadb.utils import embedding_functions
from datasets import load_dataset
import feedparser

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Setup storage paths matching your repository architecture
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CHROMA_PATH = PROJECT_ROOT / "data" / "processed" / "chroma_db"

RSS_FEEDS = {
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "CoinTelegraph": "https://cointelegraph.com/rss",
    "Decrypt": "https://decrypt.co/feed",
}


def get_vector_collection():
    """Initializes a persistent ChromaDB client with sentence-transformer embeddings."""
    CHROMA_PATH.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_PATH))

    # Use standard open-source dense embeddings (runs locally on CPU/GPU)
    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )

    # get_or_create prevents errors on repeated runs
    return client.get_or_create_collection(
        name="crypto_factual_evidence",
        embedding_function=emb_fn,
        metadata={"hnsw:space": "cosine"},
    )


def ingest_rss_feeds(collection):
    """Fetches and inserts articles from major crypto news RSS feeds."""
    docs, metadatas, ids = [], [], []

    for source_name, url in RSS_FEEDS.items():
        logging.info("Parsing RSS feed from %s...", source_name)
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                published = entry.get("published", "")
                link = entry.get("link", "")

                content = f"{title}. {summary}".strip()
                if not content:
                    continue

                # Deterministic ID to avoid duplicate vectors on subsequent runs
                doc_id = hashlib.sha256(link.encode()).hexdigest()[:16]

                docs.append(content)
                metadatas.append(
                    {
                        "source": source_name,
                        "published_at": published,
                        "url": link,
                        "type": "news_rss",
                    }
                )
                ids.append(f"rss_{doc_id}")
        except Exception as e:
            logging.error("Failed to parse %s: %s", source_name, e)

    if docs:
        collection.upsert(documents=docs, metadatas=metadatas, ids=ids)
        logging.info("Ingested %d articles from RSS feeds.", len(docs))


def ingest_finfact_benchmark(collection, max_samples: int = 500):
    """Ingests vetted claims and evidence from the Hugging Face Fin-Fact benchmark."""
    logging.info("Loading Fin-Fact dataset from Hugging Face...")
    try:
        ds = load_dataset("amanrangapur/Fin-Fact", split="train")
        docs, metadatas, ids = [], [], []

        for idx, row in enumerate(ds):
            if idx >= max_samples:
                break
            claim = row.get("claim", "")
            evidence = row.get("evidence", "")
            label = row.get("label", "")

            text = f"Fact-Checked Claim: {claim} Evidence: {evidence}".strip()
            doc_id = f"finfact_{idx}"

            docs.append(text)
            metadatas.append(
                {
                    "source": "Fin-Fact Benchmark",
                    "label": str(label),
                    "type": "benchmark",
                }
            )
            ids.append(doc_id)

        if docs:
            collection.upsert(documents=docs, metadatas=metadatas, ids=ids)
            logging.info("Ingested %d records from Fin-Fact benchmark.", len(docs))
    except Exception as e:
        logging.warning("Fin-Fact dataset loading skipped: %s", e)


if __name__ == "__main__":
    coll = get_vector_collection()
    ingest_rss_feeds(coll)
    ingest_finfact_benchmark(coll)
    logging.info("Vector database setup complete! Stored at %s", CHROMA_PATH)
