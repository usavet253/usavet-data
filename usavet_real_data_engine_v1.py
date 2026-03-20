import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

OUTPUT_FILE = "usavet_real_data_v1.json"
SOURCE_PLAN_FILE = "source_plan.txt"

HEADERS = {"User-Agent": "USAVET-Index/1.0"}


def clamp(value, low=0, high=100):
    return max(low, min(high, int(round(value))))


def safe_get(url):
    try:
        return requests.get(url, headers=HEADERS, timeout=15)
    except:
        return None


def extract_sources(path):
    urls = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("- http"):
                urls.append(line[2:].strip())
    return urls


def strip_html(html):
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"<[^>]+>", " ", html)

    # FIXED LINES
    html = re.sub(r"&nbsp;", " ", html)
    html = re.sub(r"&amp;", "&", html)
    html = re.sub(r"&lt;", "<", html)
    html = re.sub(r"&gt;", ">", html)

    html = re.sub(r"\s+", " ", html)
    return html.lower()


def count(text, words):
    return sum(text.count(w) for w in words)


KEYWORDS = {
    "housing": ["housing", "rent", "mortgage", "eviction"],
    "cost_of_living": ["inflation", "prices", "cost"],
    "employment": ["job", "jobs", "layoff", "unemployment"],
    "morale": ["stress", "mental", "burnout"],
    "benefits": ["benefits", "va", "claims"],
    "media": ["report", "policy", "hearing"],
}


NEGATIVE = {
    "housing": ["eviction", "homeless"],
    "cost_of_living": ["inflation"],
    "employment": ["layoff"],
    "morale": ["stress"],
    "benefits": ["delay"],
    "media": ["investigation"],
}


def analyze(url):
    r = safe_get(url)
    if not r:
        return None

    text = strip_html(r.text[:300000])

    result = {}
    for k in KEYWORDS:
        base = count(text, KEYWORDS[k])
        neg = count(text, NEGATIVE[k])
        score = 50 + base + (neg * 2)
        result[k] = clamp(score)

    return result


def main():
    urls = extract_sources(SOURCE_PLAN_FILE)

    all_scores = []

    for url in urls:
        data = analyze(url)
        if data:
            all_scores.append(data)

    if not all_scores:
        scores = {k: 40 for k in KEYWORDS}
    else:
        scores = {
            k: clamp(sum(d[k] for d in all_scores) / len(all_scores))
            for k in KEYWORDS
        }

    composite = clamp(sum(scores.values()) / len(scores))

    status = (
        "Severe Pressure" if composite >= 75
        else "Elevated Pressure" if composite >= 60
        else "High Pressure" if composite >= 45
        else "Watch" if composite >= 30
        else "Stable"
    )

    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "status": status,
        "composite_score": composite,
        "scores": scores,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print("SUCCESS:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
