import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

OUTPUT_FILE = "usavet_real_data_v1.json"
SOURCE_PLAN_FILE = "source_plan.txt"
REQUEST_TIMEOUT = 20

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36 USAVET-Index/2.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


DOMAIN_KEYWORDS = {
    "housing": [
        "housing", "rent", "rents", "mortgage", "eviction", "evictions",
        "homeless", "shelter", "foreclosure", "foreclosures",
        "base housing", "barracks", "housing crisis",
    ],
    "cost_of_living": [
        "inflation", "prices", "price", "cost of living", "affordability",
        "grocery", "groceries", "fuel", "gas prices", "energy costs",
        "utility bills", "household costs", "consumer prices",
    ],
    "employment": [
        "employment", "job", "jobs", "hiring", "layoff", "layoffs",
        "unemployment", "workforce", "career", "careers", "labor market",
        "jobless",
    ],
    "morale": [
        "morale", "stress", "burnout", "mental health", "suicide",
        "well-being", "readiness", "quality of life", "fatigue",
        "community support", "resilience",
    ],
    "benefits": [
        "benefits", "va benefits", "claims", "disability", "tricare",
        "gi bill", "caregiver", "pension", "compensation", "eligibility",
        "backlog", "appeals",
    ],
    "media": [
        "investigation", "hearing", "report", "controversy", "lawsuit",
        "policy", "rule", "federal register", "congress", "oversight",
        "watchdog", "audit",
    ],
}

NEGATIVE_HINTS = {
    "housing": ["eviction", "homeless", "foreclosure", "crisis", "shortage"],
    "cost_of_living": ["inflation", "higher prices", "surge", "expensive", "price spike"],
    "employment": ["layoff", "layoffs", "unemployment", "job cuts", "jobless"],
    "morale": ["stress", "burnout", "suicide", "fatigue", "crisis"],
    "benefits": ["delay", "backlog", "denial", "confusion", "appeal"],
    "media": ["controversy", "lawsuit", "investigation", "oversight", "audit"],
}

POSITIVE_HINTS = {
    "housing": ["funding", "expansion", "support", "construction", "assistance"],
    "cost_of_living": ["relief", "reduction", "support", "lower prices", "stabilized"],
    "employment": ["hiring", "growth", "jobs added", "expansion", "recruiting"],
    "morale": ["support", "resilience", "well-being", "improved", "community"],
    "benefits": ["expanded", "improved", "faster", "streamlined", "approved"],
    "media": ["resolved", "clarified", "support", "funding", "reform"],
}

BASELINES = {
    "housing": 48,
    "cost_of_living": 52,
    "employment": 46,
    "morale": 44,
    "benefits": 42,
    "media": 40,
}

TIER_WEIGHTS = {
    "tier_1": 1.60,
    "tier_2": 1.00,
    "tier_3": 0.60,
    "unknown": 0.80,
}


def clamp(value, low=0, high=100):
    return max(low, min(high, int(round(value))))


def safe_get(url, timeout=REQUEST_TIMEOUT):
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    except requests.RequestException:
        return None


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def extract_sources_with_tiers(path):
    if not os.path.exists(path):
        return []

    current_tier = "unknown"
    items = []

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            lower = line.lower()

            if lower.startswith("tier 1"):
                current_tier = "tier_1"
                continue
            if lower.startswith("tier 2"):
                current_tier = "tier_2"
                continue
            if lower.startswith("tier 3"):
                current_tier = "tier_3"
                continue

            if line.startswith("- http://") or line.startswith("- https://"):
                url = line[2:].strip()
                items.append({"url": url, "tier": current_tier})
            elif line.startswith("http://") or line.startswith("https://"):
                items.append({"url": line, "tier": current_tier})

    return items


def strip_html(html):
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;|&#160;", " ", html)
    html = re.sub(r"&amp;", "&", html)
    html = re.sub(r"&lt;", "<", html)
    html = re.sub(r"&gt;", ">", html)
    html = normalize_whitespace(html)
    return html.lower()


def count_keyword_hits(text, keywords):
    total = 0
    for keyword in keywords:
        pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
        total += len(re.findall(pattern, text))
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


def analyze_source(item):
    url = item["url"]
    tier = item["tier"]
    response = safe_get(url)

    if response is None:
        return {
            "url": url,
            "tier": tier,
            "ok": False,
            "status_code": None,
            "freshness": 35,
            "domain_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "negative_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "positive_hits": {k: 0 for k in DOMAIN_KEYWORDS},
        }

    text = strip_html(response.text[:500000])

    domain_hits = {}
    negative_hits = {}
    positive_hits = {}

    for domain, keywords in DOMAIN_KEYWORDS.items():
        domain_hits[domain] = count_keyword_hits(text, keywords)
        negative_hits[domain] = count_keyword_hits(text, NEGATIVE_HINTS[domain])
        positive_hits[domain] = count_keyword_hits(text, POSITIVE_HINTS[domain])

    return {
        "url": url,
        "tier": tier,
        "ok": response.ok,
        "status_code": response.status_code,
        "freshness": freshness_score(parse_last_modified(response)),
        "domain_hits": domain_hits,
        "negative_hits": negative_hits,
        "positive_hits": positive_hits,
    }


def weighted_average(values_with_weights, default_value):
    if not values_with_weights:
        return default_value

    total_weight = sum(weight for _, weight in values_with_weights)
    if total_weight <= 0:
        return default_value

    weighted_sum = sum(value * weight for value, weight in values_with_weights)
    return weighted_sum / total_weight


def score_domain(source_results, domain_name, baseline):
    values = []

    for item in source_results:
        tier_weight = TIER_WEIGHTS.get(item["tier"], TIER_WEIGHTS["unknown"])
        freshness_weight = item["freshness"] / 100.0
        ok_weight = 1.0 if item["ok"] else 0.35
        weight = tier_weight * max(0.40, freshness_weight) * ok_weight

        hits = item["domain_hits"][domain_name]
        negative = item["negative_hits"][domain_name]
        positive = item["positive_hits"][domain_name]

        score = baseline
        score += min(hits * 1.25, 16)
        score += min(negative * 2.8, 22)
        score -= min(positive * 2.0, 12)
        score += (item["freshness"] - 50) * 0.18

        values.append((clamp(score), weight))

    return clamp(weighted_average(values, baseline))


def fred_get_series_latest(series_id):
    if not FRED_API_KEY:
        return None

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 12,
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None

    observations = data.get("observations", [])
    for obs in observations:
        value = obs.get("value")
        if value not in (None, ".", ""):
            try:
                return float(value)
            except ValueError:
                continue

    return None


def get_fred_signals():
    return {
        "cpi": fred_get_series_latest("CPIAUCSL"),
        "unemployment": fred_get_series_latest("UNRATE"),
        "consumer_sentiment_proxy": fred_get_series_latest("UMCSENT"),
    }


def fred_adjustments(fred):
    adjustments = {
        "housing": 0,
        "cost_of_living": 0,
        "employment": 0,
        "morale": 0,
        "benefits": 0,
        "media": 0,
    }

    cpi = fred.get("cpi")
    unemployment = fred.get("unemployment")
    sentiment = fred.get("consumer_sentiment_proxy")

    if cpi is not None:
        # CPI index level is long-run rising, so use a mild static pressure contribution only
        adjustments["cost_of_living"] += 4
        adjustments["housing"] += 2

    if unemployment is not None:
        if unemployment >= 6.5:
            adjustments["employment"] += 14
            adjustments["morale"] += 8
        elif unemployment >= 5.5:
            adjustments["employment"] += 10
            adjustments["morale"] += 6
        elif unemployment >= 4.5:
            adjustments["employment"] += 6
            adjustments["morale"] += 3
        elif unemployment <= 3.8:
            adjustments["employment"] -= 4

    if sentiment is not None:
        if sentiment < 60:
            adjustments["morale"] += 10
            adjustments["cost_of_living"] += 4
        elif sentiment < 75:
            adjustments["morale"] += 5
            adjustments["cost_of_living"] += 2
        elif sentiment > 90:
            adjustments["morale"] -= 4

    return adjustments


def apply_fred_adjustments(scores, fred):
    adjustments = fred_adjustments(fred)
    adjusted = {}

    for domain, value in scores.items():
        adjusted[domain] = clamp(value + adjustments.get(domain, 0))

    return adjusted, adjustments


def composite_from_scores(scores):
    return clamp(sum(scores.values()) / len(scores))


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


def build_narrative(scores, composite, analyzed_count, total_count, fred):
    notes = []

    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    highest = ordered[:2]
    lowest = ordered[-1]

    notes.append(f"Composite pressure score: {composite}.")
    notes.append(
        f"Primary pressure domain: {highest[0][0].replace('_', ' ')} ({highest[0][1]})."
    )
    if len(highest) > 1:
        notes.append(
            f"Secondary pressure domain: {highest[1][0].replace('_', ' ')} ({highest[1][1]})."
        )

    if lowest:
        notes.append(
            f"Most stable domain: {lowest[0].replace('_', ' ')} ({lowest[1]})."
        )

    notes.append(f"Sources analyzed successfully: {analyzed_count} of {total_count}.")

    if fred.get("unemployment") is not None:
        notes.append(f"FRED unemployment reference: {fred['unemployment']:.1f}%.")

    if fred.get("consumer_sentiment_proxy") is not None:
        notes.append(
            f"Consumer sentiment proxy reference: {fred['consumer_sentiment_proxy']:.1f}."
        )

    return notes[:6]


def build_summary_bullets(scores, composite):
    bullets = []

    if composite >= 75:
        bullets.append("System under severe pressure")
    elif composite >= 60:
        bullets.append("System pressure elevated")
    elif composite >= 45:
        bullets.append("System under high pressure")
    elif composite >= 30:
        bullets.append("System on watch")
    else:
        bullets.append("System stable")

    if scores["housing"] >= 60:
        bullets.append("Housing strain elevated")
    if scores["cost_of_living"] >= 60:
        bullets.append("Affordability pressure elevated")
    if scores["employment"] >= 60:
        bullets.append("Employment stress elevated")
    if scores["morale"] >= 60:
        bullets.append("Morale stress elevated")
    if scores["benefits"] >= 60:
        bullets.append("Benefits friction elevated")
    if scores["media"] >= 60:
        bullets.append("Media and oversight pressure elevated")

    if len(bullets) == 1:
        bullets.append("No major secondary pressure spike detected")

    return bullets[:4]


def load_previous_output(path):
    if not os.path.exists(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def trend_from_previous(previous, current_composite):
    if not previous:
        return {"delta_composite": 0, "direction": "flat"}

    previous_value = previous.get("composite_score")
    if previous_value is None:
        return {"delta_composite": 0, "direction": "flat"}

    delta = current_composite - int(previous_value)
    if delta > 2:
        direction = "up"
    elif delta < -2:
        direction = "down"
    else:
        direction = "flat"

    return {
        "delta_composite": delta,
        "direction": direction,
    }


def main():
    previous = load_previous_output(OUTPUT_FILE)
    source_items = extract_sources_with_tiers(SOURCE_PLAN_FILE)

    if not source_items:
        fallback_scores = {
            "housing": 35,
            "cost_of_living": 40,
            "employment": 38,
            "morale": 36,
            "benefits": 34,
            "media": 32,
        }
        composite = composite_from_scores(fallback_scores)

        fallback = {
            "generated_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
            "status": status_from_composite(composite),
            "composite_score": composite,
            "scores": fallback_scores,
            "narrative": ["No source URLs found in source_plan.txt"],
            "summary": build_summary_bullets(fallback_scores, composite),
            "trend": trend_from_previous(previous, composite),
            "meta": {
                "source_count": 0,
                "successful_sources": 0,
                "fred_api_key_present": bool(FRED_API_KEY),
            },
        }

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)

        print(f"Wrote fallback output to {OUTPUT_FILE}")
        return

    source_results = [analyze_source(item) for item in source_items]
    successful_sources = sum(1 for x in source_results if x["ok"])

    base_scores = {
        domain: score_domain(source_results, domain, baseline)
        for domain, baseline in BASELINES.items()
    }

    fred = get_fred_signals()
    final_scores, fred_domain_adjustments = apply_fred_adjustments(base_scores, fred)

    composite = composite_from_scores(final_scores)
    status = status_from_composite(composite)
    narrative = build_narrative(final_scores, composite, successful_sources, len(source_items), fred)
    summary = build_summary_bullets(final_scores, composite)
    trend = trend_from_previous(previous, composite)

    result = {
        "generated_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "status": status,
        "composite_score": composite,
        "scores": final_scores,
        "narrative": narrative,
        "summary": summary,
        "trend": trend,
        "meta": {
            "source_count": len(source_items),
            "successful_sources": successful_sources,
            "failed_sources": len(source_items) - successful_sources,
            "fred_api_key_present": bool(FRED_API_KEY),
            "tier_counts": {
                "tier_1": sum(1 for s in source_items if s["tier"] == "tier_1"),
                "tier_2": sum(1 for s in source_items if s["tier"] == "tier_2"),
                "tier_3": sum(1 for s in source_items if s["tier"] == "tier_3"),
            },
        },
        "fred": fred,
        "fred_domain_adjustments": fred_domain_adjustments,
        "sources": [
            {
                "url": item["url"],
                "tier": item["tier"],
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
