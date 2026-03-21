import json
import os
import re
from datetime import datetime, timedelta, timezone
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
BLS_API_KEY = os.getenv("BLS_API_KEY", "").strip()
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "").strip()
BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()
HUD_API_KEY = os.getenv("HUD_API_KEY", "").strip()
VA_API_KEY = os.getenv("VA_API_KEY", "").strip()
VA_BENEFITS_API_KEY = os.getenv("VA_BENEFITS_API_KEY", "").strip()

NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"
NEWSAPI_PAGE_SIZE = 20
BLS_API_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BEA_API_ENDPOINT = "https://apps.bea.gov/api/data"
FEDERAL_REGISTER_ENDPOINT = "https://www.federalregister.gov/api/v1/documents.json"
VA_FACILITIES_ENDPOINT = "https://api.va.gov/facilities/va"
VA_BENEFITS_REFERENCE_ENDPOINT = "https://api.va.gov/services/benefits-reference-data/v1"
HUD_FMR_LIST_ENDPOINT = "https://www.huduser.gov/hudapi/public/fmr/listMetroAreas"
HUD_IL_LIST_ENDPOINT = "https://www.huduser.gov/hudapi/public/il/listMetroAreas"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36 USAVET-Index/9.0"
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
        "jobless", "military spouse employment", "transition assistance",
    ],
    "morale": [
        "morale", "stress", "burnout", "mental health", "suicide",
        "well-being", "readiness", "quality of life", "fatigue",
        "community support", "resilience", "family strain",
    ],
    "benefits": [
        "benefits", "va benefits", "veterans benefits", "claims", "claim",
        "disability", "tricare", "gi bill", "caregiver", "pension",
        "compensation", "eligibility", "backlog", "appeals",
        "va disability", "benefit payments",
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
        "veteran", "veterans", "military family", "military families",
        "service member", "service members", "defense", "va",
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

NEWS_SOURCE_TIER_RULES = {
    "tier_1": {
        "npr": "media",
        "associated press": "media",
        "ap news": "media",
        "reuters": "media",
        "stars and stripes": "media",
        "military times": "media",
        "military.com": "media",
        "defense.gov": "operational",
        "department of defense": "operational",
        "va news": "operational",
        "u.s. department of veterans affairs": "operational",
        "veterans affairs": "operational",
        "federal register": "policy",
        "congress.gov": "policy",
    },
    "tier_2": {
        "defense news": "media",
        "defensenews": "media",
        "defense one": "media",
        "task & purpose": "media",
        "task and purpose": "media",
        "fox news": "media",
        "new york post": "media",
        "nypost": "media",
        "washington examiner": "media",
        "the hill": "media",
        "newsweek": "media",
        "usa today": "media",
        "kpbs": "media",
        "globenewswire": "media",
        "times of india": "media",
        "soldiersystems.net": "media",
    },
    "tier_3": {
        "substack": "sentiment",
        "raw story": "sentiment",
        "common dreams": "sentiment",
        "free republic": "sentiment",
        "freerepublic.com": "sentiment",
        "blog": "sentiment",
    },
}

TIER3_SENTIMENT_WEIGHT_CAP = 0.55
TIER3_ONE_OFF_PENALTY = -2
TIER3_TWO_SOURCE_CLUSTER_BONUS = 1
TIER3_THREE_PLUS_CLUSTER_BONUS = 3
TIER3_MAX_HITS_PER_ITEM = 2
TIER3_MAX_NEGATIVE_PER_ITEM = 2
TIER3_MAX_POSITIVE_PER_ITEM = 2
NEWS_DOMAIN_RELEVANCE_MIN = 2
NEWS_DOMAIN_NEGATIVE_RELEVANCE_MIN = 1

REGION_DEFINITIONS = [
    {
        "id": "west",
        "name": "West",
        "offset": 2,
        "weights": {
            "composite": 0.55,
            "housing": 0.20,
            "cost_of_living": 0.15,
            "employment": 0.10,
        },
        "drivers": [
            "Housing affordability remains constrained",
            "Employment conditions are uneven",
            "Regional media pressure remains mixed",
        ],
    },
    {
        "id": "south",
        "name": "South",
        "offset": 1,
        "weights": {
            "composite": 0.55,
            "cost_of_living": 0.20,
            "employment": 0.15,
            "benefits": 0.10,
        },
        "drivers": [
            "Household cost pressure remains active",
            "Labor conditions are uneven",
            "Service access pressure remains visible",
        ],
    },
    {
        "id": "midwest",
        "name": "Midwest",
        "offset": 0,
        "weights": {
            "composite": 0.60,
            "employment": 0.20,
            "cost_of_living": 0.10,
            "media": 0.10,
        },
        "drivers": [
            "Affordability pressure remains visible",
            "Employment conditions are stable but softening",
            "Narrative pressure is contained",
        ],
    },
    {
        "id": "northeast",
        "name": "Northeast",
        "offset": -1,
        "weights": {
            "composite": 0.55,
            "housing": 0.20,
            "benefits": 0.15,
            "media": 0.10,
        },
        "drivers": [
            "Housing cost pressure remains active",
            "Benefits and service strain remain visible",
            "Media tone remains mixed",
        ],
    },
]


def clamp(value, low=0, high=100):
    return max(low, min(high, int(round(value))))


def parse_float(value):
    if value in (None, "", "."):
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def safe_get(url, timeout=REQUEST_TIMEOUT, headers=None, params=None):
    try:
        return requests.get(
            url,
            headers=headers or HEADERS,
            params=params,
            timeout=timeout,
            allow_redirects=True,
        )
    except requests.RequestException:
        return None


def safe_post(url, timeout=REQUEST_TIMEOUT, headers=None, json_payload=None, data_payload=None):
    try:
        return requests.post(
            url,
            headers=headers,
            json=json_payload,
            data=data_payload,
            timeout=timeout,
        )
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


def classify_newsapi_source(source_name):
    name = normalize_whitespace((source_name or "")).lower()

    for key, source_type in NEWS_SOURCE_TIER_RULES["tier_1"].items():
        if key in name:
            return "tier_1", source_type

    for key, source_type in NEWS_SOURCE_TIER_RULES["tier_2"].items():
        if key in name:
            return "tier_2", source_type

    for key, source_type in NEWS_SOURCE_TIER_RULES["tier_3"].items():
        if key in name:
            return "tier_3", source_type

    return "tier_3", "sentiment"


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

    response = safe_get(
        NEWSAPI_ENDPOINT,
        timeout=REQUEST_TIMEOUT,
        headers=newsapi_headers(),
        params=params,
    )

    if response is None:
        return {"ok": False, "status": "request_failed", "articles": []}

    try:
        data = response.json()
    except Exception:
        return {"ok": False, "status": "invalid_json", "articles": []}

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
        tier, source_type = classify_newsapi_source(source_name)

        normalized.append({
            "title": title,
            "description": description,
            "source_name": source_name,
            "published_at": published_at,
            "url": article.get("url") or "",
            "domain_name": domain_name,
            "tier": tier,
            "type": source_type,
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
        "tier": article.get("tier", "tier_3"),
        "type": article.get("type", "sentiment"),
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


def news_article_relevant_to_domain(item, domain_name):
    hits = item["domain_hits"].get(domain_name, 0)
    negative = item["negative_hits"].get(domain_name, 0)
    positive = item["positive_hits"].get(domain_name, 0)
    total = hits + negative + positive

    if total >= NEWS_DOMAIN_RELEVANCE_MIN:
        return True

    if negative >= NEWS_DOMAIN_NEGATIVE_RELEVANCE_MIN and hits >= 1:
        return True

    return False


def filter_newsapi_results_by_domain(results):
    kept = []
    dropped = []
    domain_drop_counts = {domain: 0 for domain in DOMAINS}

    for item in results:
        domain_name = item.get("mapped_domain", "general")

        if domain_name in DOMAINS and news_article_relevant_to_domain(item, domain_name):
            kept.append(item)
        else:
            dropped.append(item)
            if domain_name in domain_drop_counts:
                domain_drop_counts[domain_name] += 1

    return kept, dropped, domain_drop_counts


def fetch_and_analyze_newsapi():
    if not NEWS_API_KEY:
        return [], {
            "enabled": False,
            "queries_run": 0,
            "articles_used": 0,
            "articles_kept": 0,
            "articles_dropped": 0,
            "errors": ["missing_api_key"],
            "tier_counts": {"tier_1": 0, "tier_2": 0, "tier_3": 0},
            "type_counts": {"policy": 0, "operational": 0, "media": 0, "sentiment": 0},
            "tier_3_guardrails_enabled": True,
            "domain_relevance_filtering_enabled": True,
            "domain_drop_counts": {domain: 0 for domain in DOMAINS},
        }, []

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

    kept, dropped, domain_drop_counts = filter_newsapi_results_by_domain(results)

    tier_counts = {
        "tier_1": sum(1 for x in kept if x["tier"] == "tier_1"),
        "tier_2": sum(1 for x in kept if x["tier"] == "tier_2"),
        "tier_3": sum(1 for x in kept if x["tier"] == "tier_3"),
    }

    type_counts = {
        "policy": sum(1 for x in kept if x["type"] == "policy"),
        "operational": sum(1 for x in kept if x["type"] == "operational"),
        "media": sum(1 for x in kept if x["type"] == "media"),
        "sentiment": sum(1 for x in kept if x["type"] == "sentiment"),
    }

    meta = {
        "enabled": True,
        "queries_run": queries_run,
        "articles_used": len(results),
        "articles_kept": len(kept),
        "articles_dropped": len(dropped),
        "errors": errors,
        "tier_counts": tier_counts,
        "type_counts": type_counts,
        "tier_3_guardrails_enabled": True,
        "domain_relevance_filtering_enabled": True,
        "domain_drop_counts": domain_drop_counts,
    }

    return kept, meta, dropped


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

    weight = (tier_weight + type_modifier) * max(0.40, freshness_weight) * ok_weight

    if item.get("source_kind") == "newsapi" and item.get("tier") == "tier_3":
        return min(weight, TIER3_SENTIMENT_WEIGHT_CAP)

    return weight


def mapped_domain_bonus(item, domain_name):
    mapped = item.get("mapped_domain", "general")

    if mapped == domain_name:
        return 10
    if mapped == "general":
        return 3
    if mapped in {"policy", "support"} and domain_name in {"benefits", "morale", "media"}:
        return 4
    return 0


def tier3_cluster_adjustment(source_results, domain_name):
    tier3_items = [
        item for item in source_results
        if item.get("source_kind") == "newsapi"
        and item.get("tier") == "tier_3"
        and item.get("mapped_domain") == domain_name
    ]

    count = len(tier3_items)

    if count >= 3:
        return TIER3_THREE_PLUS_CLUSTER_BONUS
    if count == 2:
        return TIER3_TWO_SOURCE_CLUSTER_BONUS
    if count == 1:
        return TIER3_ONE_OFF_PENALTY
    return 0


def score_domain(source_results, domain_name, baseline):
    values = []

    for item in source_results:
        weight = source_weight(item)

        hits = item["domain_hits"].get(domain_name, 0)
        negative = item["negative_hits"].get(domain_name, 0)
        positive = item["positive_hits"].get(domain_name, 0)

        if item.get("source_kind") == "newsapi" and item.get("tier") == "tier_3":
            hits = min(hits, TIER3_MAX_HITS_PER_ITEM)
            negative = min(negative, TIER3_MAX_NEGATIVE_PER_ITEM)
            positive = min(positive, TIER3_MAX_POSITIVE_PER_ITEM)

        score = baseline
        score += min(hits * 1.25, 16)
        score += min(negative * 2.8, 22)
        score -= min(positive * 2.0, 12)
        score += (item["freshness"] - 50) * 0.18
        score += mapped_domain_bonus(item, domain_name)

        if item.get("source_kind") == "newsapi":
            if item["tier"] == "tier_1":
                score += 5
            elif item["tier"] == "tier_2":
                score += 2

        values.append((clamp(score), weight))

    final_value = clamp(weighted_average(values, baseline))
    final_value = clamp(final_value + tier3_cluster_adjustment(source_results, domain_name))
    return final_value


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

    response = safe_get(url, timeout=REQUEST_TIMEOUT, params=params, headers={"User-Agent": HEADERS["User-Agent"]})
    if response is None or not response.ok:
        return None

    try:
        data = response.json()
    except Exception:
        return None

    observations = data.get("observations", [])
    for obs in observations:
        value = parse_float(obs.get("value"))
        if value is not None:
            return value
    return None


def get_fred_signals():
    return {
        "cpi": fred_get_series_latest("CPIAUCSL"),
        "unemployment": fred_get_series_latest("UNRATE"),
        "consumer_sentiment_proxy": fred_get_series_latest("UMCSENT"),
    }


def fred_adjustments(fred):
    adjustments = {domain: 0 for domain in DOMAINS}

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


def bls_get_series(series_id):
    if not BLS_API_KEY:
        return None

    payload = {
        "seriesid": [series_id],
        "registrationkey": BLS_API_KEY,
    }

    response = safe_post(
        BLS_API_ENDPOINT,
        timeout=REQUEST_TIMEOUT,
        headers={"Content-Type": "application/json"},
        json_payload=payload,
    )
    if response is None or not response.ok:
        return None

    try:
        data = response.json()
        series = data["Results"]["series"][0]["data"]
    except Exception:
        return None

    for entry in series:
        value = parse_float(entry.get("value"))
        if value is not None:
            return value

    return None


def get_bls_signals():
    return {
        "unemployment_rate": bls_get_series("LNS14000000"),
        "labor_participation": bls_get_series("LNS11300000"),
    }


def bls_adjustments(bls):
    adjustments = {domain: 0 for domain in DOMAINS}

    unemployment = bls.get("unemployment_rate")
    participation = bls.get("labor_participation")

    if unemployment is not None:
        if unemployment >= 6.5:
            adjustments["employment"] -= 10
            adjustments["morale"] -= 6
        elif unemployment >= 5.5:
            adjustments["employment"] -= 7
            adjustments["morale"] -= 4
        elif unemployment >= 4.5:
            adjustments["employment"] -= 3
            adjustments["morale"] -= 2
        elif unemployment <= 3.8:
            adjustments["employment"] += 3

    if participation is not None:
        if participation < 62.0:
            adjustments["employment"] -= 4
            adjustments["morale"] -= 2
        elif participation > 63.5:
            adjustments["employment"] += 2

    return adjustments


def apply_bls_adjustments(scores, bls):
    adjustments = bls_adjustments(bls)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def census_get_acs_national():
    if not CENSUS_API_KEY:
        return {
            "year": None,
            "median_gross_rent": None,
            "median_home_value": None,
            "ok": False,
        }

    current_year = datetime.now(timezone.utc).year
    years_to_try = list(range(current_year - 1, current_year - 6, -1))

    for year in years_to_try:
        url = f"https://api.census.gov/data/{year}/acs/acs1"
        params = {
            "get": "NAME,B25064_001E,B25077_001E",
            "for": "us:1",
            "key": CENSUS_API_KEY,
        }

        response = safe_get(url, timeout=REQUEST_TIMEOUT, params=params, headers={"User-Agent": HEADERS["User-Agent"]})
        if response is None or not response.ok:
            continue

        try:
            data = response.json()
            if len(data) >= 2:
                row = data[1]
                return {
                    "year": year,
                    "median_gross_rent": parse_float(row[1]),
                    "median_home_value": parse_float(row[2]),
                    "ok": True,
                }
        except Exception:
            continue

    return {
        "year": None,
        "median_gross_rent": None,
        "median_home_value": None,
        "ok": False,
    }


def census_adjustments(census):
    adjustments = {domain: 0 for domain in DOMAINS}
    rent = census.get("median_gross_rent")
    home_value = census.get("median_home_value")

    if rent is not None:
        if rent >= 1800:
            adjustments["cost_of_living"] -= 2
            adjustments["housing"] -= 1
        elif rent >= 1500:
            adjustments["cost_of_living"] -= 1

    if home_value is not None:
        if home_value >= 400000:
            adjustments["housing"] -= 2
            adjustments["cost_of_living"] -= 1
        elif home_value >= 300000:
            adjustments["housing"] -= 1

    return adjustments


def apply_census_adjustments(scores, census):
    adjustments = census_adjustments(census)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def bea_get_signal():
    if not BEA_API_KEY:
        return {
            "personal_income_billions": None,
            "table_name": None,
            "year": None,
            "ok": False,
        }

    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T20100",
        "LineNumber": "1",
        "Frequency": "A",
        "Year": "LAST5",
        "ResultFormat": "json",
    }

    response = safe_get(BEA_API_ENDPOINT, timeout=REQUEST_TIMEOUT, params=params, headers={"User-Agent": HEADERS["User-Agent"]})
    if response is None or not response.ok:
        return {
            "personal_income_billions": None,
            "table_name": "T20100",
            "year": None,
            "ok": False,
        }

    try:
        data = response.json()
        records = data["BEAAPI"]["Results"]["Data"]
        cleaned = []
        for item in records:
            value = parse_float(item.get("DataValue"))
            year = item.get("TimePeriod")
            if value is not None:
                cleaned.append((str(year), value))
        if cleaned:
            cleaned.sort(key=lambda x: x[0], reverse=True)
            latest_year, latest_value = cleaned[0]
            return {
                "personal_income_billions": latest_value,
                "table_name": "T20100",
                "year": latest_year,
                "ok": True,
            }
    except Exception:
        pass

    return {
        "personal_income_billions": None,
        "table_name": "T20100",
        "year": None,
        "ok": False,
    }


def bea_adjustments(bea):
    adjustments = {domain: 0 for domain in DOMAINS}
    personal_income = bea.get("personal_income_billions")

    if personal_income is not None:
        if personal_income >= 25000:
            adjustments["employment"] += 1
            adjustments["morale"] += 1
        elif personal_income <= 18000:
            adjustments["employment"] -= 1
            adjustments["morale"] -= 1

    return adjustments


def apply_bea_adjustments(scores, bea):
    adjustments = bea_adjustments(bea)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def hud_headers():
    if not HUD_API_KEY:
        return None
    return {
        "Authorization": f"Bearer {HUD_API_KEY}",
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }


def recursive_count_items(obj):
    if isinstance(obj, list):
        return len(obj)
    if isinstance(obj, dict):
        for key in ("data", "items", "results", "metroareas"):
            if key in obj and isinstance(obj[key], list):
                return len(obj[key])
            if key in obj and isinstance(obj[key], dict):
                for nested_key in ("metroareas", "items", "results"):
                    if nested_key in obj[key] and isinstance(obj[key][nested_key], list):
                        return len(obj[key][nested_key])
    return 0


def hud_try_endpoint(url):
    headers = hud_headers()
    if not headers:
        return {"ok": False, "count": None, "endpoint": url}

    response = safe_get(url, timeout=REQUEST_TIMEOUT, headers=headers)
    if response is None or not response.ok:
        return {"ok": False, "count": None, "endpoint": url}

    try:
        payload = response.json()
        return {
            "ok": True,
            "count": recursive_count_items(payload),
            "endpoint": url,
        }
    except Exception:
        return {"ok": False, "count": None, "endpoint": url}


def get_hud_signals():
    if not HUD_API_KEY:
        return {
            "fmr_endpoint_ok": False,
            "income_limits_endpoint_ok": False,
            "fmr_area_count": None,
            "income_limits_area_count": None,
            "ok": False,
        }

    fmr_result = hud_try_endpoint(HUD_FMR_LIST_ENDPOINT)
    il_result = hud_try_endpoint(HUD_IL_LIST_ENDPOINT)

    return {
        "fmr_endpoint_ok": fmr_result["ok"],
        "income_limits_endpoint_ok": il_result["ok"],
        "fmr_area_count": fmr_result["count"],
        "income_limits_area_count": il_result["count"],
        "ok": bool(fmr_result["ok"] or il_result["ok"]),
    }


def hud_adjustments(hud):
    adjustments = {domain: 0 for domain in DOMAINS}

    if hud.get("fmr_endpoint_ok") and hud.get("income_limits_endpoint_ok"):
        adjustments["housing"] += 1
    elif hud.get("fmr_endpoint_ok") or hud.get("income_limits_endpoint_ok"):
        adjustments["housing"] += 0

    return adjustments


def apply_hud_adjustments(scores, hud):
    adjustments = hud_adjustments(hud)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def va_facilities_headers():
    if not VA_API_KEY:
        return None
    return {
        "apikey": VA_API_KEY,
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }


def get_va_facilities_signals():
    headers = va_facilities_headers()
    if not headers:
        return {
            "ok": False,
            "facility_count_page_1": None,
            "sample_endpoint": VA_FACILITIES_ENDPOINT,
        }

    params = {"page": 1, "per_page": 100}
    response = safe_get(VA_FACILITIES_ENDPOINT, timeout=REQUEST_TIMEOUT, headers=headers, params=params)
    if response is None or not response.ok:
        return {
            "ok": False,
            "facility_count_page_1": None,
            "sample_endpoint": VA_FACILITIES_ENDPOINT,
        }

    try:
        payload = response.json()
        count = len(payload.get("data", [])) if isinstance(payload.get("data"), list) else None
        return {
            "ok": True,
            "facility_count_page_1": count,
            "sample_endpoint": VA_FACILITIES_ENDPOINT,
        }
    except Exception:
        return {
            "ok": False,
            "facility_count_page_1": None,
            "sample_endpoint": VA_FACILITIES_ENDPOINT,
        }


def va_benefits_headers():
    if not VA_BENEFITS_API_KEY:
        return None
    return {
        "apikey": VA_BENEFITS_API_KEY,
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json",
    }


def get_va_benefits_signals():
    headers = va_benefits_headers()
    if not headers:
        return {
            "ok": False,
            "disabilities_count": None,
            "treatment_centers_count": None,
        }

    disabilities_url = f"{VA_BENEFITS_REFERENCE_ENDPOINT}/disabilities"
    treatment_centers_url = f"{VA_BENEFITS_REFERENCE_ENDPOINT}/treatment-centers"

    disability_response = safe_get(disabilities_url, timeout=REQUEST_TIMEOUT, headers=headers)
    treatment_response = safe_get(treatment_centers_url, timeout=REQUEST_TIMEOUT, headers=headers)

    disabilities_count = None
    treatment_centers_count = None
    ok = False

    if disability_response is not None and disability_response.ok:
        try:
            payload = disability_response.json()
            if isinstance(payload.get("data"), list):
                disabilities_count = len(payload["data"])
                ok = True
        except Exception:
            pass

    if treatment_response is not None and treatment_response.ok:
        try:
            payload = treatment_response.json()
            if isinstance(payload.get("data"), list):
                treatment_centers_count = len(payload["data"])
                ok = True
        except Exception:
            pass

    return {
        "ok": ok,
        "disabilities_count": disabilities_count,
        "treatment_centers_count": treatment_centers_count,
    }


def va_adjustments(va_facilities, va_benefits):
    adjustments = {domain: 0 for domain in DOMAINS}

    if va_facilities.get("ok"):
        adjustments["benefits"] += 1

    if va_benefits.get("ok"):
        adjustments["benefits"] += 1
        adjustments["morale"] += 1

    return adjustments


def apply_va_adjustments(scores, va_facilities, va_benefits):
    adjustments = va_adjustments(va_facilities, va_benefits)
    adjusted = {domain: clamp(value + adjustments.get(domain, 0)) for domain, value in scores.items()}
    return adjusted, adjustments


def get_federal_register_signals():
    date_gte = (datetime.now(timezone.utc) - timedelta(days=14)).date().isoformat()
    params = {
        "conditions[publication_date][gte]": date_gte,
        "conditions[term]": '"veterans" OR "military" OR "VA" OR "service members"',
        "order": "newest",
        "per_page": 20,
    }

    response = safe_get(
        FEDERAL_REGISTER_ENDPOINT,
        timeout=REQUEST_TIMEOUT,
        params=params,
        headers={"User-Agent": HEADERS["User-Agent"], "Accept": "application/json"},
    )

    if response is None or not response.ok:
        return {
            "ok": False,
            "count": None,
            "document_numbers": [],
            "agencies": [],
            "date_gte": date_gte,
        }

    try:
        payload = response.json()
        results = payload.get("results", [])
        agencies = set()

        for item in results:
            for agency in item.get("agencies", []):
                name = agency.get("name")
                if name:
                    agencies.add(name)

        return {
            "ok": True,
            "count": payload.get("count", len(results)),
            "document_numbers": [item.get("document_number") for item in results[:10] if item.get("document_number")],
            "agencies": sorted(list(agencies))[:10],
            "date_gte": date_gte,
        }
    except Exception:
        return {
            "ok": False,
            "count": None,
            "document_numbers": [],
            "agencies": [],
            "date_gte": date_gte,
        }


def federal_register_adjustments(fr):
    adjustments = {domain: 0 for domain in DOMAINS}
    count = fr.get("count")

    if count is None:
        return adjustments

    if count >= 40:
        adjustments["media"] += 4
        adjustments["benefits"] += 2
    elif count >= 20:
        adjustments["media"] += 3
        adjustments["benefits"] += 2
    elif count >= 10:
        adjustments["media"] += 2
        adjustments["benefits"] += 1
    elif count >= 5:
        adjustments["media"] += 1

    return adjustments


def apply_federal_register_adjustments(scores, fr):
    adjustments = federal_register_adjustments(fr)
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


def build_narrative(scores, composite, analyzed_count, total_count, fred, newsapi_meta, bls, fr):
    notes = []
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    notes.append(f"Composite score: {composite}.")
    notes.append(f"Highest pressure domain: {ordered[0][0].replace('_', ' ')} ({ordered[0][1]}).")
    notes.append(f"Second highest pressure domain: {ordered[1][0].replace('_', ' ')} ({ordered[1][1]}).")
    notes.append(f"Most stable domain: {ordered[-1][0].replace('_', ' ')} ({ordered[-1][1]}).")
    notes.append(f"Sources analyzed successfully: {analyzed_count} of {total_count}.")

    if newsapi_meta.get("enabled"):
        notes.append(
            f"NewsAPI kept {newsapi_meta.get('articles_kept', 0)} of "
            f"{newsapi_meta.get('articles_used', 0)} articles after relevance filtering."
        )
    elif fr.get("ok") and fr.get("count") is not None:
        notes.append(f"Federal Register returned {fr.get('count')} relevant documents in the recent monitoring window.")
    elif bls.get("unemployment_rate") is not None or bls.get("labor_participation") is not None:
        notes.append("BLS labor signals were incorporated into employment calibration.")

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


def build_region_summary(region_score, composite_score):
    if region_score >= 65:
        return "Localized pressure remains contained relative to national baseline trends."
    if region_score >= 50:
        return "Localized pressure remains mixed relative to national baseline trends."
    if composite_score >= 50:
        return "Localized pressure remains under heavier strain relative to national baseline trends."
    return "Localized pressure remains under high strain relative to national baseline trends."


def build_region_sparkline(region_score, offset):
    p1 = clamp(region_score - 2 + (1 if offset > 0 else 0))
    p2 = clamp(region_score - 1)
    p3 = clamp(region_score - 1)
    p4 = clamp(region_score)
    p5 = clamp(region_score)
    p6 = clamp(region_score + (1 if offset > 1 else 0))
    return [p1, p2, p3, p4, p5, p6]


def calculate_region_score(definition, composite_score, scores):
    weights = definition["weights"]
    total = 0.0

    for key, weight in weights.items():
        if key == "composite":
            total += composite_score * weight
        else:
            total += scores.get(key, composite_score) * weight

    total += definition.get("offset", 0)
    return clamp(total)


def build_regions(composite_score, scores):
    regions = []

    for definition in REGION_DEFINITIONS:
        region_score = calculate_region_score(definition, composite_score, scores)
        region_status = status_from_composite(region_score)
        region_trend = "flat"

        regions.append({
            "id": definition["id"],
            "name": definition["name"],
            "score": region_score,
            "status": region_status,
            "trend": region_trend,
            "summary": build_region_summary(region_score, composite_score),
            "drivers": definition["drivers"][:3],
            "sparkline": build_region_sparkline(region_score, definition.get("offset", 0)),
        })

    return regions


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
        "summary": build_summary_bullets(fallback_scores, composite),
        "narrative": ["No source URLs found in source_plan.txt"],
        "display_trends": build_display_trends(history),
        "regions": build_regions(composite, fallback_scores),
        "meta": {
            "source_count": 0,
            "successful_sources": 0,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
            "bls_api_key_present": bool(BLS_API_KEY),
            "census_api_key_present": bool(CENSUS_API_KEY),
            "bea_api_key_present": bool(BEA_API_KEY),
            "hud_api_key_present": bool(HUD_API_KEY),
            "va_api_key_present": bool(VA_API_KEY),
            "va_benefits_api_key_present": bool(VA_BENEFITS_API_KEY),
            "federal_register_key_required": False,
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
    newsapi_results, newsapi_meta, dropped_newsapi_results = fetch_and_analyze_newsapi()
    all_results = source_results + newsapi_results

    successful_sources = sum(1 for x in source_results if x["ok"])

    base_scores = {
        domain: score_domain(all_results, domain, baseline)
        for domain, baseline in BASELINES.items()
    }

    fred = get_fred_signals()
    scores_after_fred, fred_domain_adjustments = apply_fred_adjustments(base_scores, fred)

    bls = get_bls_signals()
    scores_after_bls, bls_domain_adjustments = apply_bls_adjustments(scores_after_fred, bls)

    census = census_get_acs_national()
    scores_after_census, census_domain_adjustments = apply_census_adjustments(scores_after_bls, census)

    bea = bea_get_signal()
    scores_after_bea, bea_domain_adjustments = apply_bea_adjustments(scores_after_census, bea)

    hud = get_hud_signals()
    scores_after_hud, hud_domain_adjustments = apply_hud_adjustments(scores_after_bea, hud)

    va_facilities = get_va_facilities_signals()
    va_benefits = get_va_benefits_signals()
    scores_after_va, va_domain_adjustments = apply_va_adjustments(scores_after_hud, va_facilities, va_benefits)

    federal_register = get_federal_register_signals()
    final_scores, federal_register_domain_adjustments = apply_federal_register_adjustments(scores_after_va, federal_register)

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
        bls,
        federal_register,
    )
    regions = build_regions(composite, final_scores)

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
        "regions": regions,
        "meta": {
            "source_count": len(source_items),
            "successful_sources": successful_sources,
            "failed_sources": len(source_items) - successful_sources,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
            "bls_api_key_present": bool(BLS_API_KEY),
            "census_api_key_present": bool(CENSUS_API_KEY),
            "bea_api_key_present": bool(BEA_API_KEY),
            "hud_api_key_present": bool(HUD_API_KEY),
            "va_api_key_present": bool(VA_API_KEY),
            "va_benefits_api_key_present": bool(VA_BENEFITS_API_KEY),
            "federal_register_key_required": False,
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
            "regional_model_enabled": True,
            "regional_regions_count": len(regions),
        },
        "fred": fred,
        "fred_domain_adjustments": fred_domain_adjustments,
        "bls": bls,
        "bls_domain_adjustments": bls_domain_adjustments,
        "census": census,
        "census_domain_adjustments": census_domain_adjustments,
        "bea": bea,
        "bea_domain_adjustments": bea_domain_adjustments,
        "hud": hud,
        "hud_domain_adjustments": hud_domain_adjustments,
        "va_facilities": va_facilities,
        "va_benefits": va_benefits,
        "va_domain_adjustments": va_domain_adjustments,
        "federal_register": federal_register,
        "federal_register_domain_adjustments": federal_register_domain_adjustments,
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
        "newsapi_dropped_sources": [
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
            for item in dropped_newsapi_results[:50]
        ],
    }

    save_json_file(OUTPUT_FILE, result)
    print(f"Wrote {OUTPUT_FILE}")
    print(f"Wrote {HISTORY_FILE}")
    print(json.dumps(result["meta"], indent=2))
    print(json.dumps({
        "newsapi": result["newsapi"],
        "federal_register": result["federal_register"],
        "hud": result["hud"],
        "va_facilities": result["va_facilities"],
        "va_benefits": result["va_benefits"],
    }, indent=2))


if __name__ == "__main__":
    main()
