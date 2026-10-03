import json
import os
import sys

from flask import Flask, jsonify, render_template, request

try:  # picks up COINGECKO key etc. from .env (searches upward from this file)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)

# Locate project directory structure dynamically
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # path to dashboard/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))  # project root
PROTOTYPE_DIR = os.path.join(
    BASE_DIR, "..", "notebooks", "Prototype"
)  # path to notebooks/Prototype/

# Make `src.bot.chatbot_pipeline` importable when running from dashboard/
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ---------------------------------------------------------------------------
# Chatbot pipeline hookup
# ---------------------------------------------------------------------------
# The function in src/bot/chatbot_pipeline.py that takes plain text and returns
# the {"text", "vader", "finbert", "verification"} dict. If yours has a
# different name, put it first in this tuple.
PIPELINE_CANDIDATES = (
    "analyse_text",
    "analyze_text",
    "run_pipeline",
    "analyse",
    "analyze",
    "process_text",
    "process",
    "run",
)
MAX_CHARS = 1000

# Trust-score tuning (placeholder logic until the hybrid trust model exists)
SENTIMENT_PENALTY_MAX = 8  # max points removed for very strong tone
STRONG_TONE_THRESHOLD = 0.6  # |finbert_net| above this shows the caution note


def _load_pipeline():
    """Return the pipeline function, or None if it could not be loaded."""
    try:
        from src.bot import chatbot_pipeline as mod
    except Exception as e:  # noqa: BLE001 - we want to show any import problem
        print(f"[CryptoTruth] Could not import src.bot.chatbot_pipeline: {e!r}")
        print("[CryptoTruth] Claim checker disabled (dashboard still works).")
        return None
    for name in PIPELINE_CANDIDATES:
        fn = getattr(mod, name, None)
        if callable(fn):
            print(f"[CryptoTruth] Using chatbot_pipeline.{name}()")
            return fn
    public = [n for n in dir(mod) if not n.startswith("_") and callable(getattr(mod, n))]
    print(f"[CryptoTruth] No entry point found. Looked for {PIPELINE_CANDIDATES}.")
    print(f"[CryptoTruth] Public callables in module: {public}")
    print("[CryptoTruth] Claim checker disabled (dashboard still works).")
    return None


PIPELINE = _load_pipeline()


def build_display(raw, mode="live"):
    """Turn raw pipeline output into the small, plain-language payload the UI shows.

    All the interpretation lives here so the front end stays dumb and no raw
    model output (VADER/FinBERT probabilities, method names) reaches the user.
    """
    ver = raw.get("verification") or {}
    if isinstance(ver, list):  # in case a post yields several claims: use the first
        ver = ver[0] if ver else {}

    # --- Sentiment: FinBERT net score (-1..+1), VADER compound as fallback ---
    net = (raw.get("finbert") or {}).get("finbert_net")
    if net is None:
        net = (raw.get("vader") or {}).get("compound", 0.0)
    net = max(-1.0, min(1.0, float(net or 0.0)))
    tone = "Bullish" if net >= 0.25 else "Bearish" if net <= -0.25 else "Neutral"
    strong_tone = abs(net) >= STRONG_TONE_THRESHOLD

    # --- Verdict + trust score ---
    veracity = str(ver.get("veracity") or "").strip().upper().replace(" ", "_")
    dev = ver.get("deviation_pct")
    score = label = None

    if veracity in ("SUPPORTED", "REFUTED"):
        if isinstance(dev, (int, float)):
            if dev <= 2:
                score, label = 100 - dev * 2.5, "Accurate"
            elif dev <= 10:
                score, label = 79 - (dev - 2) * 2, "Roughly accurate"
            else:
                score, label = max(5, 55 - (dev - 10)), "Inaccurate"
        elif veracity == "SUPPORTED":
            score, label = 85, "Supported by sources"
        else:
            score, label = 15, "Contradicted by sources"

    if score is not None:
        score = max(0, min(100, round(score - abs(net) * SENTIMENT_PENALTY_MAX)))
        tier = "high" if score >= 80 else "caution" if score >= 60 else "low"
        explanation = ver.get("explanation") or ""
    elif not ver:
        tier, label = "unverified", "No verifiable claim found"
        explanation = (
            "This reads like an opinion rather than a factual claim, so there "
            "is nothing to check. The tone reading below still applies."
        )
    else:
        tier, label = "unverified", "Couldn't verify"
        explanation = ver.get("explanation") or (
            "We couldn't find enough evidence to confirm or refute this claim."
        )

    claim = None
    if isinstance(ver.get("claimed"), (int, float)) and isinstance(
        ver.get("actual"), (int, float)
    ):
        claim = {
            "claimed": ver["claimed"],
            "actual": ver["actual"],
            "deviation_pct": dev,
        }

    return {
        "mode": mode,
        "trust": {"score": score, "tier": tier, "label": label},
        "explanation": explanation,
        "claim": claim,
        "source": ver.get("source"),
        "sentiment": {
            "label": tone,
            "position": round((net + 1) / 2 * 100),
            "strong_tone": strong_tone,
        },
    }


def load_json(filename):
    """Safely load JSON data from the notebooks/Prototype directory."""
    file_path = os.path.join(PROTOTYPE_DIR, filename)
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None
    return None


@app.route("/")
def index():
    # Load data from the specified path
    file1_data = load_json("results_file1.json")
    file2_data = load_json("results_file2.json")
    dashboard_data = load_json("dashboard_data.json") or {}
    # Fallback values if files are missing or unreadable
    if file1_data is None:
        file1_data = {"Bullish": 0, "Bearish": 0, "Neutral": 0}
    if file2_data is None:
        file2_data = {"SUPPORTED": 0, "REFUTED": 0}
    return render_template(
        "index.html",
        file1_data=file1_data,
        file2_data=file2_data,
        dashboard_data=dashboard_data,
    )


@app.route("/api/verify", methods=["POST"])
def api_verify():
    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text", "")).strip()
    if not text:
        return jsonify(error="Please enter a post or claim to check."), 400
    if len(text) > MAX_CHARS:
        return jsonify(error=f"Please keep it under {MAX_CHARS} characters."), 400
    if PIPELINE is None:
        return jsonify(error="The claim checker is temporarily unavailable."), 503
    try:
        raw = PIPELINE(text)
        if isinstance(raw, str):
            raw = json.loads(raw)
        return jsonify(build_display(raw))
    except Exception:
        app.logger.exception("verify failed")
        return jsonify(error="Something went wrong while checking that claim."), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)