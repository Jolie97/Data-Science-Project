import sys
from pathlib import Path
sys.path.append(str(Path.cwd() / "src"))  # adjust if running from elsewhere

from sentiment.analyser import SentimentEngine

engine = SentimentEngine(load_finbert=True)

test_post = "looks like KDA is making a comeback with the community edition fork, currently only listed on Gate ( not available for US and EU cryptobros)"

print(engine.score_vader(test_post))
print(engine.score_finbert_batch([test_post]))