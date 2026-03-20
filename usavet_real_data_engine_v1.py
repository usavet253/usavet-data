import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests


OUTPUT_FILE = "usavet_real_data_v1.json"
SOURCE_PLAN_FILE = "source_plan.txt"
REQUEST_TIMEOUT = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36 USAVET-Index/1.0"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


DOMAIN_KEYWORDS = {
    "housing": [
        "housing",
        "rent",
        "mortgage",
        "eviction",
        "homeless",
        "shelter",
        "foreclosure",
        "affordable housing",
        "barracks",
        "base housing",
    ],
    "cost_of_living": [
        "inflation",
        "price",
        "prices",
        "cost of living",
        "grocery",
        "food cost",
        "gas prices",
        "fuel",
        "utility bills",
        "energy costs",
        "affordability",
    ],
    "employment": [
        "employment",
        "job",
        "jobs",
        "hiring",
        "layoff",
        "layoffs",
        "unemployment",
        "workforce",
        "career",
        "careers",
        "labor market",
    ],
    "morale": [
        "morale",
        "stress",
        "burnout",
        "mental health",
        "suicide",
        "well-being",
        "resilience",
        "readiness",
        "quality of life",
        "community support",
    ],
    "benefits": [
        "benefits",
        "va benefits",
        "claims",
        "disability",
        "tricare",
        "gi bill",
        "caregiver",
        "pension",
        "compensation",
        "eligibility",
    ],
    "media": [
        "investigation",
        "hearing",
        "report",
        "controversy",
        "lawsuit",
        "policy",
        "rule",
        "federal register",
        "congress",
        "oversight",
    ],
}

NEGATIVE_HINTS = {
    "housing": ["eviction", "homeless", "foreclosure", "crisis", "shortage"],
    "cost_of_living": ["inflation", "higher prices", "surge", "shortage", "expensive"],
    "employment": ["layoff", "layoffs", "unemployment", "job cuts", "strike"],
    "morale": ["stress", "burnout", "suicide", "crisis", "fatigue"],
    "benefits": ["delay", "backlog", "denial", "confusion", "shortfall"],
    "media": ["controversy", "lawsuit", "investigation", "oversight", "hearing"],
}

POSITIVE_HINTS = {
    "housing": ["improvement", "expansion", "support", "funding"],
    "cost_of_living": ["relief", "lower prices", "reduction", "support"],
    "employment": ["hiring", "growth", "jobs added", "expansion"],
    "morale": ["support", "resilience", "well-being", "improved"],
    "benefits": ["expanded", "improved", "faster", "streamlined"],
    "media": ["clarified", "resolved", "support", "funding"],
}


def clamp(value, low=0, high=100):
    return max(low, min(high, int(round(value))))


def safe_get(url):
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        return response
    except requests.RequestException:
        return None


def extract_sources(path):
    urls = []
    if not os.path.exists(path):
        return urls

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("- http://") or line.startswith("- https://"):
                urls.append(line[2:].strip())
            elif line.startswith("http://") or line.startswith("https://"):
                urls.append(line)

    return urls


def strip_html(html):
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;|&#160;", " ", html)
    html = re.sub(r"&amp;", "&", html)
    html = re.sub(r"&lt;", "<", html)
    html = re.sub(r"&gt;", ">", " ", html)
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def count_keyword_hits(text, keywords):
    total = 0
    lowered = text.lower()
    for keyword in keywords:
        total += len(re.findall(r"\b" + re.escape(keyword.lower()) + r"\b", lowered))
    return total


def parse_last_modified(response):
    if not response:
        return None

    value = response.headers.get("Last-Modified")
    if not value:
        return None

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def freshness_score(last_modified):
    if not last_modified:
        return 50

    now = datetime.now(timezone.utc)
    age_days = (now - last_modified).days

    if age_days <= 1:
        return 95
    if age_days <= 3:
        return 88
    if age_days <= 7:
        return 80
    if age_days <= 14:
        return 72
    if age_days <= 30:
        return 64
    if age_days <= 60:
        return 56
    if age_days <= 90:
        return 48
    return 40


def analyze_source(url):
    response = safe_get(url)
    if response is None:
        return {
            "url": url,
            "ok": False,
            "status_code": None,
            "freshness": 35,
            "domain_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "negative_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "positive_hits": {k: 0 for k in DOMAIN_KEYWORDS},
        }

    text = strip_html(response.text[:600000])

    domain_hits = {}
    negative_hits = {}
    positive_hits = {}

    for domain, keywords in DOMAIN_KEYWORDS.items():
        domain_hits[domain] = count_keyword_hits(text, keywords)
        negative_hits[domain] = count_keyword_hits(text, NEGATIVE_HINTS[domain])
        positive_hits[domain] = count_keyword_hits(text, POSITIVE_HINTS[domain])

    return {
        "url": url,
        "ok": response.ok,
        "status_code": response.status_code,
        "freshness": freshness_score(parse_last_modified(response)),
        "domain_hits": domain_hits,
        "negative_hits": negative_hits,
        "positive_hits": positive_hits,
    }


def score_domain(source_results, domain_name, baseline):
    if not source_results:
        return baseline

    total_hits = sum(item["domain_hits"][domain_name] for item in source_results)
    negative = sum(item["negative_hits"][domain_name] for item in source_results)
    positive = sum(item["positive_hits"][domain_name] for item in source_results)
    avg_freshness = sum(item["freshness"] for item in source_results) / len(source_results)

    # Pressure score model:
    # higher negative coverage = more pressure
    # positive/supportive coverage reduces pressure
    raw = baseline
    raw += min(total_hits * 1.5, 18)
    raw += min(negative * 2.5, 20)
    raw -= min(positive * 2.0, 12)
    raw += (avg_freshness - 50) * 0.20

    return clamp(raw)


def composite_from_scores(scores):
    ordered = [
        scores["housing"],
        scores["cost_of_living"],
        scores["employment"],
        scores["morale"],
        scores["benefits"],
        scores["media"],
    ]
    return clamp(sum(ordered) / len(ordered))


def status_from_composite(value):
    if value >= 75:
        return "Severe Pressure"
    if value >= 60:
        return "Elevated Pressure"
    if value >= 45:
        return "High Pressure"
    if value >= 30:
        return "Watch"
    return "Stable"


def build_narrative(scores, analyzed_count, total_count):
    notes = []

    highest = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:2]
    lowest = sorted(scores.items(), key=lambda x: x[1])[:1]

    if highest:
        notes.append(
            f"Top pressure domains: {highest[0][0].replace('_', ' ')} ({highest[0][1]})"
        )
    if len(highest) > 1:
        notes.append(
            f"Secondary pressure domain: {highest[1][0].replace('_', ' ')} ({highest[1][1]})"
        )
    if lowest:
        notes.append(
            f"Most stable domain: {lowest[0][0].replace('_', ' ')} ({lowest[0][1]})"
        )

    notes.append(f"Sources analyzed successfully: {analyzed_count} of {total_count}")

    if scores["housing"] >= 60:
        notes.append("Housing strain indicators remain elevated.")
    if scores["cost_of_living"] >= 60:
        notes.append("Affordability and cost-of-living pressure remain elevated.")
    if scores["employment"] >= 60:
        notes.append("Employment and workforce stress signals are elevated.")
    if scores["benefits"] >= 60:
        notes.append("Benefits and claims-related friction is elevated.")
    if scores["morale"] >= 60:
        notes.append("Morale and well-being indicators deserve closer watch.")

    if not notes:
        notes.append("System stable")

    return notes[:6]


def main():
    urls = extract_sources(SOURCE_PLAN_FILE)

    if not urls:
        fallback = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "status": "Stable",
            "composite_score": 35,
            "scores": {
                "housing": 35,
                "cost_of_living": 40,
                "employment": 38,
                "morale": 36,
                "benefits": 34,
                "media": 32,
            },
            "narrative": ["No source URLs found in source_plan.txt"],
            "meta": {
                "source_count": 0,
                "successful_sources": 0,
                "fred_api_key_present": bool(os.getenv("FRED_API_KEY")),
            },
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)

        print(f"Wrote fallback output to {OUTPUT_FILE}")
        return

    source_results = [analyze_source(url) for url in urls]
    successful_sources = sum(1 for x in source_results if x["ok"])

    baselines = {
        "housing": 48,
        "cost_of_living": 52,
        "employment": 46,
        "morale": 44,
        "benefits": 42,
        "media": 40,
    }

    scores = {
        domain: score_domain(source_results, domain, baseline)
        for domain, baseline in baselines.items()
    }

    composite = composite_from_scores(scores)
    status = status_from_composite(composite)
    narrative = build_narrative(scores, successful_sources, len(urls))

    result = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "status": status,
        "composite_score": composite,
        "scores": scores,
        "narrative": narrative,
        "meta": {
            "source_count": len(urls),
            "successful_sources": successful_sources,
            "failed_sources": len(urls) - successful_sources,
            "fred_api_key_present": bool(os.getenv("FRED_API_KEY")),
        },
        "sources": [
            {
                "url": item["url"],
                "ok": item["ok"],
                "status_code": item["status_code"],
                "freshness": item["freshness"],
            }
            for item in source_results
        ],
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Wrote {OUTPUT_FILE}")
    print(json.dumps(result["meta"], indent=2))


if __name__ == "__main__":
    main()
