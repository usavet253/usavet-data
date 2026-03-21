import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

OUTPUT_FILE = "usavet_real_data_v1.json"
HISTORY_FILE = "history.json"
SOURCE_PLAN_FILE = "source_plan.txt"
SOURCE_SIGNAL_MAP_FILE = "source_signal_map.json"
REQUEST_TIMEOUT = 20
HISTORY_LIMIT = 90

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "").strip()

NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"
NEWSAPI_PAGE_SIZE = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36 USAVET-Index/5.0"
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
    "policy": [
        "policy", "rule", "regulation", "congress", "senate", "house",
        "oversight", "federal register", "hearing", "bill", "legislation",
    ],
    "support": [
        "support", "assistance", "resource", "resources", "family support",
        "community support", "navigation", "military one source", "nrd",
    ],
    "general": [
        "veteran", "military family", "service member", "defense", "va",
        "readiness", "community", "support", "policy", "housing",
    ],
}

NEGATIVE_HINTS = {
    "housing": ["eviction", "homeless", "foreclosure", "crisis", "shortage"],
    "cost_of_living": ["inflation", "higher prices", "surge", "expensive", "price spike"],
    "employment": ["layoff", "layoffs", "unemployment", "job cuts", "jobless"],
    "morale": ["stress", "burnout", "suicide", "fatigue", "crisis"],
    "benefits": ["delay", "backlog", "denial", "confusion", "appeal"],
    "media": ["controversy", "lawsuit", "investigation", "oversight", "audit"],
    "policy": ["delay", "blocked", "controversy", "hearing", "oversight"],
    "support": ["shortage", "delay", "strain", "gap", "burden"],
    "general": ["crisis", "strain", "pressure", "delay", "risk"],
}

POSITIVE_HINTS = {
    "housing": ["funding", "expansion", "support", "construction", "assistance"],
    "cost_of_living": ["relief", "reduction", "support", "lower prices", "stabilized"],
    "employment": ["hiring", "growth", "jobs added", "expansion", "recruiting"],
    "morale": ["support", "resilience", "well-being", "improved", "community"],
    "benefits": ["expanded", "improved", "faster", "streamlined", "approved"],
    "media": ["resolved", "clarified", "support", "funding", "reform"],
    "policy": ["passed", "approved", "support", "funding", "reform"],
    "support": ["assistance", "support", "expanded", "resource", "help"],
    "general": ["support", "improved", "stabilized", "funding", "relief"],
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

TYPE_MODIFIERS = {
    "policy": 0.20,
    "operational": 0.15,
    "media": 0.05,
    "sentiment": 0.00,
    "unknown": 0.00,
}

DOMAINS = ["housing", "cost_of_living", "employment", "morale", "benefits", "media"]
MAPPED_DOMAINS = set(DOMAINS + ["policy", "support", "general"])

NEWSAPI_QUERIES = {
    "housing": '"veterans" OR "military families" OR "service members" housing rent mortgage homeless eviction',
    "cost_of_living": '"veterans" OR "military families" OR "service members" affordability inflation prices grocery fuel',
    "employment": '"veterans" OR "military spouses" OR "service members" employment jobs hiring layoffs workforce',
    "morale": '"veterans" OR "military families" OR "service members" morale stress burnout mental health readiness',
    "benefits": '"VA benefits" OR "veterans benefits" OR "claims backlog" OR "GI Bill" OR "TRICARE" OR "disability claims"',
    "media": '"veterans" OR "military families" oversight hearing investigation lawsuit policy congress VA DoD',
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


def load_json_file(path, default_value):
    if not os.path.exists(path):
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_value


def save_json_file(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


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
                items.append({"url": line[2:].strip(), "tier": current_tier})
            elif line.startswith("http://") or line.startswith("https://"):
                items.append({"url": line, "tier": current_tier})

    return items


def load_source_signal_map(path):
    data = load_json_file(path, {})
    if not isinstance(data, dict):
        return {}

    sources = data.get("sources", {})
    if not isinstance(sources, dict):
        return {}

    normalized = {}

    for source_key, source_value in sources.items():
        if not isinstance(source_value, dict):
            continue

        url = str(source_value.get("url", "")).strip()
        tier = str(source_value.get("tier", "unknown")).strip()
        source_type = str(source_value.get("type", "unknown")).strip().lower()
        domain = str(source_value.get("domain", "general")).strip().lower()

        if not url:
            continue

        if isinstance(tier, int):
            tier = f"tier_{tier}"
        elif tier in {"1", "2", "3"}:
            tier = f"tier_{tier}"
        elif not tier.startswith("tier_"):
            tier = "unknown"

        if domain not in MAPPED_DOMAINS:
            domain = "general"

        if source_type not in TYPE_MODIFIERS:
            source_type = "unknown"

        normalized[url.rstrip("/")] = {
            "key": source_key,
            "url": url,
            "tier": tier,
            "type": source_type,
            "domain": domain,
        }

    return normalized


def merge_source_plan_with_map(source_plan_items, source_map):
    merged = []

    for item in source_plan_items:
        url = item["url"].rstrip("/")
        mapped = source_map.get(url)

        if mapped:
            merged.append({
                "key": mapped["key"],
                "url": item["url"],
                "tier": mapped["tier"],
                "type": mapped["type"],
                "domain": mapped["domain"],
            })
        else:
            merged.append({
                "key": url,
                "url": item["url"],
                "tier": item.get("tier", "unknown"),
                "type": "unknown",
                "domain": "general",
            })

    return merged


def strip_html(html):
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;|&#160;", " ", " " + html)
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


def parse_iso_datetime(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
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
    source_type = item.get("type", "unknown")
    mapped_domain = item.get("domain", "general")
    response = safe_get(url)

    if response is None:
        return {
            "key": item.get("key", url),
            "url": url,
            "tier": tier,
            "type": source_type,
            "mapped_domain": mapped_domain,
            "ok": False,
            "status_code": None,
            "freshness": 35,
            "domain_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "negative_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "positive_hits": {k: 0 for k in DOMAIN_KEYWORDS},
            "source_kind": "source_plan",
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
        "key": item.get("key", url),
        "url": url,
        "tier": tier,
        "type": source_type,
        "mapped_domain": mapped_domain,
        "ok": response.ok,
        "status_code": response.status_code,
        "freshness": freshness_score(parse_last_modified(response)),
        "domain_hits": domain_hits,
        "negative_hits": negative_hits,
        "positive_hits": positive_hits,
        "source_kind": "source_plan",
    }


def newsapi_headers():
    return {
        "X-Api-Key": NEWS_API_KEY,
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }


def fetch_newsapi_articles_for_domain(domain_name, query):
    if not NEWS_API_KEY:
        return {"ok": False, "status": "missing_api_key", "articles": []}

    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "searchIn": "title,description",
    }

    try:
        response = requests.get(
            NEWSAPI_ENDPOINT,
            params=params,
            headers=newsapi_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        data = response.json()
    except Exception:
        return {"ok": False, "status": "request_failed", "articles": []}

    if response.status_code != 200 or data.get("status") != "ok":
        return {
            "ok": False,
            "status": data.get("code", f"http_{response.status_code}"),
            "articles": [],
        }

    articles = data.get("articles", [])
    normalized = []

    for article in articles:
        title = article.get("title") or ""
        description = article.get("description") or ""
        source_name = ""
        source = article.get("source")
        if isinstance(source, dict):
            source_name = source.get("name") or ""

        published_at = parse_iso_datetime(article.get("publishedAt"))

        normalized.append({
            "title": title,
            "description": description,
            "source_name": source_name,
            "published_at": published_at,
            "url": article.get("url") or "",
            "domain_name": domain_name,
        })

    return {"ok": True, "status": "ok", "articles": normalized}


def analyze_newsapi_article(article):
    text = normalize_whitespace(
        f"{article.get('title', '')} {article.get('description', '')} {article.get('source_name', '')}"
    ).lower()

    domain_hits = {}
    negative_hits = {}
    positive_hits = {}

    for domain, keywords in DOMAIN_KEYWORDS.items():
        domain_hits[domain] = count_keyword_hits(text, keywords)
        negative_hits[domain] = count_keyword_hits(text, NEGATIVE_HINTS[domain])
        positive_hits[domain] = count_keyword_hits(text, POSITIVE_HINTS[domain])

    freshness = freshness_score(article.get("published_at"))

    return {
        "key": f"newsapi_{article.get('domain_name', 'general')}",
        "url": article.get("url", ""),
        "tier": "tier_2",
        "type": "media",
        "mapped_domain": article.get("domain_name", "general"),
        "ok": True,
        "status_code": 200,
        "freshness": freshness,
        "domain_hits": domain_hits,
        "negative_hits": negative_hits,
        "positive_hits": positive_hits,
        "source_kind": "newsapi",
        "source_name": article.get("source_name", ""),
        "title": article.get("title", ""),
    }


def fetch_and_analyze_newsapi():
    if not NEWS_API_KEY:
        return [], {
            "enabled": False,
            "queries_run": 0,
            "articles_used": 0,
            "errors": ["missing_api_key"],
        }

    results = []
    errors = []
    queries_run = 0

    for domain_name, query in NEWSAPI_QUERIES.items():
        queries_run += 1
        payload = fetch_newsapi_articles_for_domain(domain_name, query)

        if not payload["ok"]:
            errors.append(f"{domain_name}:{payload['status']}")
            continue

        for article in payload["articles"]:
            results.append(analyze_newsapi_article(article))

    meta = {
        "enabled": True,
        "queries_run": queries_run,
        "articles_used": len(results),
        "errors": errors,
    }

    return results, meta


def weighted_average(values_with_weights, default_value):
    if not values_with_weights:
        return default_value

    total_weight = sum(weight for _, weight in values_with_weights)
    if total_weight <= 0:
        return default_value

    weighted_sum = sum(value * weight for value, weight in values_with_weights)
    return weighted_sum / total_weight


def source_weight(item):
    tier_weight = TIER_WEIGHTS.get(item["tier"], TIER_WEIGHTS["unknown"])
    type_modifier = TYPE_MODIFIERS.get(item.get("type", "unknown"), 0.0)
    freshness_weight = item["freshness"] / 100.0
    ok_weight = 1.0 if item["ok"] else 0.35

    return (tier_weight + type_modifier) * max(0.40, freshness_weight) * ok_weight


def mapped_domain_bonus(item, domain_name):
    mapped = item.get("mapped_domain", "general")

    if mapped == domain_name:
        return 10
    if mapped == "general":
        return 3
    if mapped in {"policy", "support"} and domain_name in {"benefits", "morale", "media"}:
        return 4
    return 0


def score_domain(source_results, domain_name, baseline):
    values = []

    for item in source_results:
        weight = source_weight(item)

        hits = item["domain_hits"].get(domain_name, 0)
        negative = item["negative_hits"].get(domain_name, 0)
        positive = item["positive_hits"].get(domain_name, 0)

        score = baseline
        score += min(hits * 1.25, 16)
        score += min(negative * 2.8, 22)
        score -= min(positive * 2.0, 12)
        score += (item["freshness"] - 50) * 0.18
        score += mapped_domain_bonus(item, domain_name)

        if item.get("source_kind") == "newsapi":
            score += 2

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
        adjustments["cost_of_living"] += 4
        adjustments["housing"] += 2

    if unemployment is not None:
        if unemployment >= 6.5:
            adjustments["employment"] -= 14
            adjustments["morale"] -= 8
        elif unemployment >= 5.5:
            adjustments["employment"] -= 10
            adjustments["morale"] -= 6
        elif unemployment >= 4.5:
            adjustments["employment"] -= 6
            adjustments["morale"] -= 3
        elif unemployment <= 3.8:
            adjustments["employment"] += 4

    if sentiment is not None:
        if sentiment < 60:
            adjustments["morale"] -= 10
            adjustments["cost_of_living"] -= 4
        elif sentiment < 75:
            adjustments["morale"] -= 5
            adjustments["cost_of_living"] -= 2
        elif sentiment > 90:
            adjustments["morale"] += 4

    return adjustments


def apply_fred_adjustments(scores, fred):
    adjustments = fred_adjustments(fred)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def composite_from_scores(scores):
    return clamp(sum(scores.values()) / len(scores))


def status_from_composite(value):
    if value >= 80:
        return "Stable"
    if value >= 65:
        return "Watch"
    if value >= 50:
        return "Elevated Pressure"
    return "High Pressure"


def build_summary_bullets(scores, composite):
    bullets = []

    if composite >= 80:
        bullets.append("System stable")
    elif composite >= 65:
        bullets.append("System on watch")
    elif composite >= 50:
        bullets.append("System under elevated pressure")
    else:
        bullets.append("System under high pressure")

    if scores["cost_of_living"] >= 65:
        bullets.append("Affordability pressure at watch level")
    elif scores["cost_of_living"] >= 50:
        bullets.append("Affordability pressure elevated")

    if scores["morale"] >= 65:
        bullets.append("Morale conditions on watch")
    elif scores["morale"] >= 50:
        bullets.append("Morale strain elevated")

    if scores["housing"] >= 65:
        bullets.append("Housing conditions on watch")
    elif scores["housing"] >= 50:
        bullets.append("Housing strain elevated")

    if scores["employment"] >= 65:
        bullets.append("Employment conditions on watch")
    elif scores["employment"] >= 50:
        bullets.append("Employment strain elevated")

    if scores["benefits"] >= 65:
        bullets.append("Benefits friction at watch level")
    elif scores["benefits"] >= 50:
        bullets.append("Benefits friction elevated")

    if scores["media"] >= 65:
        bullets.append("Media and oversight pressure on watch")
    elif scores["media"] >= 50:
        bullets.append("Media and oversight pressure elevated")

    if len(bullets) == 1:
        bullets.append("No major secondary pressure spike detected")

    return bullets[:4]


def build_narrative(scores, composite, analyzed_count, total_count, fred, newsapi_meta):
    notes = []
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    notes.append(f"Composite score: {composite}.")
    notes.append(f"Highest pressure domain: {ordered[0][0].replace('_', ' ')} ({ordered[0][1]}).")
    notes.append(f"Second highest pressure domain: {ordered[1][0].replace('_', ' ')} ({ordered[1][1]}).")
    notes.append(f"Most stable domain: {ordered[-1][0].replace('_', ' ')} ({ordered[-1][1]}).")
    notes.append(f"Sources analyzed successfully: {analyzed_count} of {total_count}.")
    if newsapi_meta.get("enabled"):
        notes.append(f"NewsAPI articles used: {newsapi_meta.get('articles_used', 0)}.")
    if fred.get("unemployment") is not None:
        notes.append(f"FRED unemployment reference: {fred['unemployment']:.1f}%.")

    return notes[:6]


def build_history_entry(timestamp, composite, scores, status):
    return {
        "generated_at": timestamp,
        "date": timestamp[:10],
        "composite_score": composite,
        "status": status,
        "scores": scores,
    }


def update_history(history, entry):
    if not isinstance(history, list):
        history = []

    history.append(entry)

    deduped = []
    seen = set()
    for item in history:
        key = item.get("generated_at")
        if key and key not in seen:
            deduped.append(item)
            seen.add(key)

    return deduped[-HISTORY_LIMIT:]


def get_series(history, key):
    values = []
    for item in history:
        if key == "composite_score":
            value = item.get("composite_score")
        else:
            value = item.get("scores", {}).get(key)
        if isinstance(value, (int, float)):
            values.append(int(round(value)))
    return values


def last_n(values, n):
    return values[-n:] if len(values) >= n else values[:]


def compute_delta(values, lookback=7):
    if len(values) < 2:
        return 0

    window = last_n(values, lookback)
    if len(window) < 2:
        return 0

    return int(round(window[-1] - window[0]))


def movement_symbol(delta):
    if delta >= 2:
        return "up"
    if delta <= -2:
        return "down"
    return "flat"


def movement_arrow(delta):
    if delta >= 2:
        return "▲"
    if delta <= -2:
        return "▼"
    return "→"


def movement_color(delta):
    if delta >= 2:
        return "green"
    if delta <= -2:
        return "red"
    return "yellow"


def sparkline(values, points=14):
    window = last_n(values, points)
    return [int(round(v)) for v in window]


def build_trend_object(values):
    delta_7 = compute_delta(values, 7)
    delta_30 = compute_delta(values, 30)

    return {
        "current": values[-1] if values else None,
        "delta_7": delta_7,
        "delta_30": delta_30,
        "direction": movement_symbol(delta_7),
        "arrow": movement_arrow(delta_7),
        "color": movement_color(delta_7),
        "sparkline": sparkline(values, 14),
    }


def build_display_trends(history):
    output = {
        "composite": build_trend_object(get_series(history, "composite_score"))
    }

    for domain in DOMAINS:
        output[domain] = build_trend_object(get_series(history, domain))

    return output


def fallback_result(history):
    fallback_scores = {
        "housing": 35,
        "cost_of_living": 40,
        "employment": 38,
        "morale": 36,
        "benefits": 34,
        "media": 32,
    }
    composite = composite_from_scores(fallback_scores)
    status = status_from_composite(composite)
    timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    history = update_history(
        history,
        build_history_entry(timestamp, composite, fallback_scores, status)
    )
    save_json_file(HISTORY_FILE, history)

    result = {
        "generated_at": timestamp,
        "status": status,
        "composite_score": composite,
        "scores": fallback_scores,
        "narrative": ["No source URLs found in source_plan.txt"],
        "summary": build_summary_bullets(fallback_scores, composite),
        "display_trends": build_display_trends(history),
        "meta": {
            "source_count": 0,
            "successful_sources": 0,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
        },
    }

    save_json_file(OUTPUT_FILE, result)
    print(f"Wrote fallback output to {OUTPUT_FILE}")


def main():
    source_plan_items = extract_sources_with_tiers(SOURCE_PLAN_FILE)
    source_signal_map = load_source_signal_map(SOURCE_SIGNAL_MAP_FILE)
    source_items = merge_source_plan_with_map(source_plan_items, source_signal_map)
    history = load_json_file(HISTORY_FILE, [])

    if not source_items:
        fallback_result(history)
        return

    source_results = [analyze_source(item) for item in source_items]
    newsapi_results, newsapi_meta = fetch_and_analyze_newsapi()
    all_results = source_results + newsapi_results

    successful_sources = sum(1 for x in source_results if x["ok"])

    base_scores = {
        domain: score_domain(all_results, domain, baseline)
        for domain, baseline in BASELINES.items()
    }

    fred = get_fred_signals()
    final_scores, fred_domain_adjustments = apply_fred_adjustments(base_scores, fred)

    composite = composite_from_scores(final_scores)
    status = status_from_composite(composite)
    summary = build_summary_bullets(final_scores, composite)
    narrative = build_narrative(
        final_scores,
        composite,
        successful_sources,
        len(source_items),
        fred,
        newsapi_meta,
    )

    timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    history = update_history(
        history,
        build_history_entry(timestamp, composite, final_scores, status)
    )
    save_json_file(HISTORY_FILE, history)

    result = {
        "generated_at": timestamp,
        "status": status,
        "composite_score": composite,
        "scores": final_scores,
        "summary": summary,
        "narrative": narrative,
        "display_trends": build_display_trends(history),
        "meta": {
            "source_count": len(source_items),
            "successful_sources": successful_sources,
            "failed_sources": len(source_items) - successful_sources,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
            "tier_counts": {
                "tier_1": sum(1 for s in source_items if s["tier"] == "tier_1"),
                "tier_2": sum(1 for s in source_items if s["tier"] == "tier_2"),
                "tier_3": sum(1 for s in source_items if s["tier"] == "tier_3"),
            },
            "type_counts": {
                "policy": sum(1 for s in source_items if s.get("type") == "policy"),
                "operational": sum(1 for s in source_items if s.get("type") == "operational"),
                "media": sum(1 for s in source_items if s.get("type") == "media"),
                "sentiment": sum(1 for s in source_items if s.get("type") == "sentiment"),
                "unknown": sum(1 for s in source_items if s.get("type") == "unknown"),
            },
            "history_points": len(history),
            "source_signal_map_loaded": bool(source_signal_map),
        },
        "fred": fred,
        "fred_domain_adjustments": fred_domain_adjustments,
        "newsapi": newsapi_meta,
        "sources": [
            {
                "key": item["key"],
                "url": item["url"],
                "tier": item["tier"],
                "type": item.get("type", "unknown"),
                "mapped_domain": item.get("mapped_domain", "general"),
                "ok": item["ok"],
                "status_code": item["status_code"],
                "freshness": item["freshness"],
                "source_kind": item.get("source_kind", "source_plan"),
            }
            for item in source_results
        ],
        "newsapi_sources": [
            {
                "key": item["key"],
                "url": item["url"],
                "tier": item["tier"],
                "type": item.get("type", "unknown"),
                "mapped_domain": item.get("mapped_domain", "general"),
                "freshness": item["freshness"],
                "source_kind": item.get("source_kind", "newsapi"),
                "source_name": item.get("source_name", ""),
                "title": item.get("title", ""),
            }
            for item in newsapi_results[:50]
        ],
    }

    save_json_file(OUTPUT_FILE, result)
    print(f"Wrote {OUTPUT_FILE}")
    print(f"Wrote {HISTORY_FILE}")
    print(json.dumps(result["meta"], indent=2))
    print(json.dumps(result["newsapi"], indent=2))


if __name__ == "__main__":
    main()
