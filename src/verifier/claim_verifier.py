"""NLI verification engine running over ChromaDB evidence retrieval."""

import chromadb
from chromadb.utils import embedding_functions
from pathlib import Path
from transformers import pipeline
import torch

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CHROMA_PATH = PROJECT_ROOT / "data" / "processed" / "chroma_db"


class ClaimVerifier:
    def __init__(self):
        # 1. Connect to ChromaDB
        client = chromadb.PersistentClient(path=str(CHROMA_PATH))
        emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )
        self.collection = client.get_collection(
            name="crypto_factual_evidence", embedding_function=emb_fn
        )

        # 2. Load Zero-Shot NLI cross-encoder model
        device = 0 if torch.cuda.is_available() else -1
        self.nli = pipeline(
            "text-classification",
            model="cross-encoder/nli-deberta-v3-small",
            device=device,
        )

    def retrieve_evidence(self, claim_text: str, top_k: int = 3):
        """Semantic search in ChromaDB for relevant news/evidence."""
        results = self.collection.query(query_texts=[claim_text], n_results=top_k)

        documents = results["documents"][0]
        metadatas = results["metadatas"][0]
        return list(zip(documents, metadatas))

    def verify_claim(self, claim_text: str):
        """Retrieves evidence and scores entailment vs contradiction."""
        evidence_pairs = self.retrieve_evidence(claim_text, top_k=2)

        if not evidence_pairs:
            return {"veracity": "UNVERIFIABLE", "confidence": 0.0, "evidence": None}

        verdict_scores = []
        for evidence_text, meta in evidence_pairs:
            # Format premise and hypothesis for NLI cross-encoder
            res = self.nli(f"{evidence_text} [SEP] {claim_text}")[0]
            # Output labels: 'entailment', 'contradiction', or 'neutral'
            verdict_scores.append(
                {
                    "label": res["label"],
                    "score": float(res["score"]),
                    "evidence_snippet": evidence_text[:200] + "...",
                    "source": meta.get("source", "Unknown"),
                }
            )

        # Select highest confidence classification
        best_verdict = max(verdict_scores, key=lambda x: x["score"])

        label_mapping = {
            "entailment": "SUPPORTED",
            "contradiction": "REFUTED",
            "neutral": "NOT ENOUGH INFO",
        }

        return {
            "veracity": label_mapping.get(
                best_verdict["label"].lower(), "NOT ENOUGH INFO"
            ),
            "confidence": round(best_verdict["score"], 3),
            "evidence": best_verdict["evidence_snippet"],
            "source": best_verdict["source"],
        }
