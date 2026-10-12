"""Deterministic synthetic CryptoTruth discussions. Standard library only.

Run from repository root: python scripts/generate_dashboard_mock.py
Never reads or writes dashboard_data_new.json or the original result files.
"""
import argparse
import hashlib
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 731204
START, END = date(2024, 1, 1), date(2025, 12, 31)
COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA']
COMMUNITIES = ['cryptocurrency', 'bitcoin', 'ethereum', 'solana', 'cryptomarkets', 'dogecoin']
TOPICS = ['Market outlook', 'Institutional demand', 'Network adoption', 'Regulation', 'DeFi', 'Memecoins']
# Fictional scenario pulses, not claims about historical events.
SCENARIOS = [
    ('2024-03-15', 'Sample adoption rally', .52, 'Network adoption'),
    ('2024-08-05', 'Sample liquidity shock', -.73, 'Market outlook'),
    ('2024-11-18', 'Sample institutional surge', .69, 'Institutional demand'),
    ('2025-03-10', 'Sample regulation concern', -.58, 'Regulation'),
    ('2025-07-22', 'Sample network upgrade cycle', .57, 'Network adoption'),
    ('2025-10-03', 'Sample speculative rotation', .36, 'Memecoins'),
]


def generate():
    rng = random.Random(SEED)
    rows = []
    biases = dict(zip(COMMUNITIES, [.01, .12, .07, .14, -.06, .2]))
    affinity = {'bitcoin':'BTC','ethereum':'ETH','solana':'SOL','dogecoin':'DOGE'}
    templates = {
        'Market outlook': '{coin} market outlook and trading activity',
        'Institutional demand': 'Institutional interest in {coin}',
        'Network adoption': '{coin} usage and network growth',
        'Regulation': 'Regulation discussion around {coin}',
        'DeFi': '{coin} liquidity and DeFi activity',
        'Memecoins': 'Speculative attention shifts toward {coin}',
    }
    events = []
    for day, title, impact, topic in SCENARIOS:
        d = date.fromisoformat(day)
        events.append({'date':day, 'label':title, 'start':str(d-timedelta(days=14)),
                       'end':str(d+timedelta(days=14)), 'topic':topic, 'synthetic':True})
    def make_record(day, kind, coin, sub, topic, sentiment, parent=None, replies=0):
        # Volatile and extreme-toned discussions get more engagement, on average.
        votes = round(rng.lognormvariate(2.55 if kind=='post' else 1.25, 1.0)
                      * (1+abs(sentiment)*2.5) * (1.4 if coin in ('BTC','ETH') else 1))
        checked = .72 if topic in ('Network adoption','Institutional demand') else .56
        if rng.random() > checked:
            verdict, confidence = 'unverified', None
        else:
            reliable = .79 - .2*abs(sentiment) - (.14 if topic=='Memecoins' else 0)
            verdict = 'supported' if rng.random() < reliable else 'unsupported'
            confidence = round(min(.99, .58 + rng.betavariate(4,2)*.4),3)
        return {'id':f'sample-{len(rows)+1:06d}', 'date':str(day), 'coin':coin,
                'subreddit':sub, 'type':kind, 'parent_id':parent, 'topic':topic,
                'title':('Comment on: ' if kind=='comment' else '')+templates[topic].format(coin=coin),
                'sentiment':round(max(-1,min(1,sentiment)),3), 'verdict':verdict,
                'confidence':confidence, 'upvotes':votes, 'reply_count':replies,
                'engagement':votes+replies}
    day=START
    while day<=END:
        t=(day-START).days
        waves=[impact*math.exp(-((day-date.fromisoformat(d)).days/13)**2/2) for d,_,impact,_ in SCENARIOS]
        event=sum(waves)
        market=.13*math.sin(t/44)+.09*math.sin(t/121)+event
        activity=round(9+7*abs(event)+3*(1+math.sin(t/17))/2+(1 if day.weekday()<5 else -1))
        for _ in range(activity):
            sub=rng.choices(COMMUNITIES,weights=[30,22,15,11,15,7])[0]
            coin=affinity[sub] if sub in affinity and rng.random()<.86 else rng.choices(COINS,weights=[32,24,17,10,10,7])[0]
            idx=COINS.index(coin)
            topic=rng.choices(TOPICS,weights=[28,16,21,13,14,8])[0]
            if coin=='DOGE' and rng.random()<.48: topic='Memecoins'
            peak=max(range(len(waves)),key=lambda k:abs(waves[k]))
            if abs(waves[peak])>.2 and rng.random()<.6: topic=SCENARIOS[peak][3]
            coin_wave=.12*math.sin(t/(31+idx*3)+idx)
            tone=market+biases[sub]+coin_wave+rng.gauss(0,.3)
            count=rng.choices([1,2,3,4],weights=[3,5,3,1])[0]
            post=make_record(day,'post',coin,sub,topic,tone,replies=count)
            rows.append(post)
            for _ in range(count):
                # Replies track the post with a skeptical shift and occasional disagreement.
                reply_tone=post['sentiment']*.55+market*.24-.06+rng.gauss(0,.29)
                if rng.random()<.14: reply_tone=-post['sentiment']*.7+rng.gauss(0,.18)
                rows.append(make_record(day,'comment',coin,sub,topic,reply_tone,parent=post['id']))
        day+=timedelta(days=1)
    encoded=json.dumps(rows,separators=(',',':')).encode()
    return {'_meta':{'schema_version':1,'is_mock':True,'label':'Sample data',
        'note':'Entirely synthetic data for interface demonstration. No real Reddit users, real claims, or historical market events are represented.',
        'seed':SEED,'date_start':str(START),'date_end':str(END),'row_count':len(rows),
        'post_count':sum(r['type']=='post' for r in rows),'comment_count':sum(r['type']=='comment' for r in rows),
        'rows_sha256':hashlib.sha256(encoded).hexdigest(),
        'sentiment_cutoff':.05,'engagement_definition':'upvotes + reply_count',
        'claim_definition':'One simulated claim candidate per record. Unverified means not resolved, not false.',
        'time_bucket':'month','coins':COINS,'subreddits':COMMUNITIES,'topics':TOPICS},
        'events':events,'records':rows}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path(__file__).resolve().parents[1]/'notebooks/Prototype/dashboard_mock_data.json')
    args=parser.parse_args()
    if args.output.name in {'dashboard_data_new.json','dashboard_data.json','results_file1.json','results_file2.json'}:
        parser.error('Refusing to overwrite real dashboard results. Choose a separate mock filename.')
    payload=generate()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8')
    print(f"Saved {len(payload['records']):,} synthetic records to {args.output}")


if __name__=='__main__': main()
