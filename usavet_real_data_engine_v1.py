import json 
import os
from datetime import datetime, timezone

import requests


OUTPUT_FILE = "usavet_real_data_v1.json"
DAILY_FILE = "daily.json"
HISTORY_FILE = "history.json"

FRED_API_KEY = os.getenv("FRED_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
BLS_API_KEY = os.getenv("BLS_API_KEY")
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
BEA_API_KEY = os.getenv("BEA_API_KEY")
HUD_API_KEY = os.getenv("HUD_API_KEY")

# Existing active keys
VA_API_KEY = os.getenv("VA_API_KEY")
VA_BENEFITS_API_KEY = os.getenv("VA_BENEFITS_API_KEY")

# New sandbox keys take priority if present
VA_FACILITIES_API_KEY = os.getenv("VA_FACILITIES_API_KEY_NEW") or VA_API_KEY
VA_BENEFITS_REFERENCE_API_KEY = (
    os.getenv("VA_BENEFITS_REFERENCE_API_KEY_NEW")
    or VA_BENEFITS_API_KEY
)

# Add this as a GitHub Actions variable in usavet-engine
VA_BENEFITS_REFERENCE_URL = (
    os.getenv("VA_BENEFITS_REFERENCE_URL")
    or os.getenv("VA_BENEFITS_REFERENCE_ENDPOINT")
    or ""
)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def load_json_file(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json_file(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def safe_json(response):
    try:
        return response.json()
    except Exception:
        try:
            return {"raw_text": response.text[:1000]}
        except Exception:
            return {"raw_text": "<unreadable response body>"}


def safe_get(url, headers=None, params=None, timeout=(10, 30)):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        return {
            "ok": True,
            "response": response,
            "exception_type": None,
            "exception_message": None,
        }
    except requests.exceptions.RequestException as e:
        return {
            "ok": False,
            "response": None,
            "exception_type": type(e).__name__,
            "exception_message": str(e),
        }
    except Exception as e:
        return {
            "ok": False,
            "response": None,
            "exception_type": type(e).__name__,
            "exception_message": str(e),
        }


def fetch_fred_unemployment():
    result = {
        "ok": False,
        "series_id": "UNRATE",
        "status_code": None,
        "latest_value": None,
        "latest_date": None,
        "reason": None,
    }

    if not FRED_API_KEY:
        result["reason"] = "FRED_API_KEY missing"
        return result

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "UNRATE",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
    }

    req = safe_get(url, params=params)
    if not req["ok"]:
        result["reason"] = f"{req['exception_type']}: {req['exception_message']}"
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["reason"] = str(payload)[:300]
        return result

    observations = payload.get("observations", [])
    if not observations:
        result["reason"] = "no observations returned"
        return result

    latest = observations[0]
    try:
        result["latest_value"] = float(latest.get("value"))
        result["latest_date"] = latest.get("date")
        result["ok"] = True
    except Exception:
        result["reason"] = f"unexpected FRED payload: {str(latest)[:300]}"

    return result


def fetch_bea():
    result = {
        "ok": False,
        "status_code": None,
        "dataset": "NIPA",
        "table_name": "T10101",
        "records_count": 0,
        "sample_time_periods": [],
        "sample_values": [],
        "reason": None,
    }

    if not BEA_API_KEY:
        result["reason"] = "BEA_API_KEY missing"
        return result

    url = "https://apps.bea.gov/api/data"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T10101",
        "Year": "2025",
        "Frequency": "Q",
        "ResultFormat": "json",
    }

    req = safe_get(url, params=params)
    if not req["ok"]:
        result["reason"] = f"{req['exception_type']}: {req['exception_message']}"
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["reason"] = str(payload)[:300]
        return result

    try:
        records = payload["BEAAPI"]["Results"]["Data"]
        result["records_count"] = len(records)
        result["sample_time_periods"] = [r.get("TimePeriod") for r in records[:5] if r.get("TimePeriod")]
        result["sample_values"] = [r.get("DataValue") for r in records[:5] if r.get("DataValue")]
        result["ok"] = True
    except Exception as e:
        result["reason"] = f"{type(e).__name__}: {e}"

    return result


def fetch_hud():
    result = {
        "ok": False,
        "status_code": None,
        "endpoint": "https://www.huduser.gov/hudapi/public/fmr/listStates",
        "sample_count": None,
        "reason": None,
    }

    if not HUD_API_KEY:
        result["reason"] = "HUD_API_KEY missing"
        return result

    headers = {
        "Authorization": f"Bearer {HUD_API_KEY}",
        "Accept": "application/json",
        "User-Agent": "usavet-data/1.0",
    }

    req = safe_get(result["endpoint"], headers=headers)
    if not req["ok"]:
        result["reason"] = f"{req['exception_type']}: {req['exception_message']}"
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["reason"] = str(payload)[:300]
        return result

    if isinstance(payload, dict):
        data = payload.get("data", [])
    elif isinstance(payload, list):
        data = payload
    else:
        data = []

    result["sample_count"] = len(data) if hasattr(data, "__len__") else None
    result["ok"] = True
    return result


def fetch_va_facilities():
    result = {
        "ok": False,
        "status_code": None,
        "endpoint": "https://api.va.gov/services/va_facilities/v1/facilities",
        "facility_count_page_1": None,
        "reason": None,
        "exception_type": None,
        "exception_message": None,
        "using_new_key": bool(os.getenv("VA_FACILITIES_API_KEY_NEW")),
        "sandbox_mode": bool(os.getenv("VA_FACILITIES_API_KEY_NEW")),
    }

    if not VA_FACILITIES_API_KEY:
        result["reason"] = "VA_FACILITIES_API_KEY missing"
        return result

    headers = {
        "apikey": VA_FACILITIES_API_KEY,
        "Accept": "application/json",
        "User-Agent": "usavet-data/1.0",
    }

    params = {
        "type": "health",
        "state": "WA",
        "page": 1,
        "per_page": 10,
    }

    req = safe_get(result["endpoint"], headers=headers, params=params, timeout=(10, 45))
    if not req["ok"]:
        result["reason"] = "request exception before HTTP response"
        result["exception_type"] = req["exception_type"]
        result["exception_message"] = req["exception_message"]
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if response.ok:
        data = payload.get("data", [])
        result["facility_count_page_1"] = len(data) if isinstance(data, list) else 0
        result["ok"] = True
        result["reason"] = "VA facilities endpoint reachable"
        return result

    result["reason"] = str(payload)[:500]
    return result


def fetch_va_benefits_reference():
    result = {
        "ok": False,
        "status_code": None,
        "endpoint": VA_BENEFITS_REFERENCE_URL or "<missing>",
        "sample_count": 0,
        "top_level_keys": [],
        "reason": None,
        "exception_type": None,
        "exception_message": None,
        "using_new_key": bool(os.getenv("VA_BENEFITS_REFERENCE_API_KEY_NEW")),
        "sandbox_mode": bool(os.getenv("VA_BENEFITS_REFERENCE_API_KEY_NEW")),
    }

    if not VA_BENEFITS_REFERENCE_API_KEY:
        result["reason"] = "VA_BENEFITS_REFERENCE_API_KEY missing"
        return result

    if not VA_BENEFITS_REFERENCE_URL:
        result["reason"] = "VA_BENEFITS_REFERENCE_URL missing"
        return result

    headers = {
        "apikey": VA_BENEFITS_REFERENCE_API_KEY,
        "Accept": "application/json",
        "User-Agent": "usavet-data/1.0",
    }

    req = safe_get(VA_BENEFITS_REFERENCE_URL, headers=headers, timeout=(10, 45))
    if not req["ok"]:
        result["reason"] = "request exception before HTTP response"
        result["exception_type"] = req["exception_type"]
        result["exception_message"] = req["exception_message"]
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["reason"] = str(payload)[:500]
        return result

    if isinstance(payload, dict):
        result["top_level_keys"] = list(payload.keys())[:10]
        if isinstance(payload.get("data"), list):
            result["sample_count"] = len(payload["data"])
        elif isinstance(payload.get("data"), dict):
            result["sample_count"] = len(payload["data"].keys())
        else:
            result["sample_count"] = len(payload.keys())
    elif isinstance(payload, list):
        result["sample_count"] = len(payload)
    else:
        result["sample_count"] = 1

    result["ok"] = True
    result["reason"] = "VA benefits reference endpoint reachable"
    return result


def fetch_federal():
    result = {
        "ok": False,
        "count": 0,
        "status_code": None,
        "reason": None,
        "document_numbers": [],
    }

    url = "https://www.federalregister.gov/api/v1/documents.json"
    params = {
        "per_page": 25,
        "conditions[term]": "veteran OR veterans OR military OR VA OR benefits OR housing",
        "order": "newest",
    }

    req = safe_get(url, params=params)
    if not req["ok"]:
        result["reason"] = f"{req['exception_type']}: {req['exception_message']}"
        return result

    response = req["response"]
    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["reason"] = str(payload)[:300]
        return result

    docs = payload.get("results", [])
    result["count"] = len(docs)
    result["document_numbers"] = [d.get("document_number") for d in docs[:5] if d.get("document_number")]
    result["ok"] = True
    return result


def fetch_news():
    result = {
        "enabled": bool(NEWS_API_KEY),
        "tier_1": 0,
        "tier_2": 0,
        "tier_3": 0,
        "kept": 0,
        "policy_like": 0,
        "used": 0,
        "errors": [],
        "sample_sources": [],
    }

    if not NEWS_API_KEY:
        result["errors"].append("NEWS_API_KEY missing")
        return result

    url = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": NEWS_API_KEY}
    params = {
        "q": "veterans OR military OR VA OR housing OR benefits",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 20,
    }

    req = safe_get(url, headers=headers, params=params)
    if not req["ok"]:
        result["errors"].append(f"{req['exception_type']}: {req['exception_message']}")
        return result

    response = req["response"]
    if not response.ok:
        result["errors"].append(str(safe_json(response))[:300])
        return result

    articles = safe_json(response).get("articles", [])
    result["used"] = len(articles)

    for article in articles:
        source_name = ((article.get("source") or {}).get("name") or "").lower()
        blob = f"{article.get('title', '')} {article.get('description', '')}".lower()

        is_tier_1 = any(
            x in source_name
            for x in [
                "reuters",
                "associated press",
                "ap ",
                "npr",
                "bloomberg",
                "cnbc",
                "wsj",
                "new york times",
                "washington post",
            ]
        )
        is_tier_2 = any(
            x in source_name
            for x in [
                "military",
                "defense",
                "va",
                "federal news network",
                "stars and stripes",
            ]
        )

        if is_tier_1:
            result["tier_1"] += 1
            result["kept"] += 1
        elif is_tier_2:
            result["tier_2"] += 1
            result["kept"] += 1
        else:
            result["tier_3"] += 1

        if any(k in blob for k in ["policy", "rule", "regulation", "bill", "law", "congress"]):
            result["policy_like"] += 1

        if len(result["sample_sources"]) < 5 and source_name:
            result["sample_sources"].append(source_name)

    return result


def score_housing(hud, federal):
    score = 50
    if hud["ok"]:
        score += 20
        if hud.get("sample_count"):
            score += min(hud["sample_count"] // 10, 10)
    if federal["ok"]:
        score += min(federal["count"] // 5, 10)
    return clamp(score)


def score_cost_of_living(bea, fred):
    score = 50
    if bea["ok"]:
        score += 15
        if bea.get("records_count", 0) > 10:
            score += 10
    if fred["ok"]:
        unemployment = fred.get("latest_value")
        if unemployment is not None:
            if unemployment <= 4.0:
                score += 15
            elif unemployment <= 5.0:
                score += 10
            elif unemployment <= 6.0:
                score += 5
            elif unemployment >= 8.0:
                score -= 10
    return clamp(score)


def score_employment(fred, bls_key_present):
    score = 50
    if bls_key_present:
        score += 10
    if fred["ok"]:
        unemployment = fred.get("latest_value")
        if unemployment is not None:
            if unemployment <= 4.0:
                score += 25
            elif unemployment <= 5.0:
                score += 15
            elif unemployment <= 6.0:
                score += 8
            elif unemployment >= 8.0:
                score -= 15
    return clamp(score)


def score_policy(federal, news, va_benefits_reference):
    score = 45
    if federal["ok"]:
        score += min(federal["count"], 25)
    score += min(news.get("policy_like", 0) * 2, 10)

    # Sandbox-safe: only a small additive influence
    if va_benefits_reference["ok"]:
        score += 5

    return clamp(score)


def score_healthcare_access(va_facilities, va_benefits_reference, hud):
    score = 45

    if hud["ok"]:
        score += 10

    if va_facilities["ok"]:
        score += 20
        if va_facilities.get("facility_count_page_1") is not None:
            score += min(va_facilities["facility_count_page_1"], 10)
    elif va_facilities.get("status_code") == 401:
        score -= 5

    # Sandbox-safe: small influence only
    if va_benefits_reference["ok"]:
        score += 10
        if va_benefits_reference.get("sample_count") is not None:
            score += min(int(va_benefits_reference["sample_count"]), 5)

    return clamp(score)


def score_sentiment(news):
    score = 50
    score += min(news.get("tier_1", 0) * 5, 20)
    score += min(news.get("tier_2", 0) * 3, 15)
    if news.get("tier_3", 0) > 10:
        score -= 10
    return clamp(score)


def derive_trend(history, overall_score):
    if len(history) < 3:
        return "stable"
    recent = history[-3:]
    prior_scores = [x.get("overall_score") for x in recent if isinstance(x.get("overall_score"), (int, float))]
    if not prior_scores:
        return "stable"
    avg_prior = sum(prior_scores) / len(prior_scores)
    if overall_score >= avg_prior + 2:
        return "improving"
    if overall_score <= avg_prior - 2:
        return "declining"
    return "stable"


def build():
    history = load_json_file(HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []

    fred = fetch_fred_unemployment()
    bea = fetch_bea()
    hud = fetch_hud()
    va_facilities = fetch_va_facilities()
    va_benefits_reference = fetch_va_benefits_reference()
    federal = fetch_federal()
    news = fetch_news()

    housing_score = score_housing(hud, federal)
    cost_of_living_score = score_cost_of_living(bea, fred)
    employment_score = score_employment(fred, bool(BLS_API_KEY))
    policy_score = score_policy(federal, news, va_benefits_reference)
    healthcare_access_score = score_healthcare_access(va_facilities, va_benefits_reference, hud)
    sentiment_score = score_sentiment(news)

    overall_score = round(
        (housing_score * 0.20)
        + (cost_of_living_score * 0.20)
        + (employment_score * 0.20)
        + (policy_score * 0.15)
        + (healthcare_access_score * 0.15)
        + (sentiment_score * 0.10),
        1,
    )

    trend = derive_trend(history, overall_score)

    successful_sources = 0
    successful_sources += 1 if fred["ok"] else 0
    successful_sources += 1 if bea["ok"] else 0
    successful_sources += 1 if hud["ok"] else 0
    successful_sources += 1 if va_facilities["ok"] else 0
    successful_sources += 1 if va_benefits_reference["ok"] else 0
    successful_sources += 1 if federal["ok"] else 0
    successful_sources += 1 if news["kept"] > 0 else 0
    successful_sources += 1 if bool(BLS_API_KEY) else 0
    successful_sources += 1 if bool(CENSUS_API_KEY) else 0

    source_count = 22

    payload = {
        "generated_at": now_utc(),
        "product": "USAVET.AI Daily Accountability Index",
        "version": "v3-va-sandbox-safe",
        "status": "ok",
        "index": {
            "overall_score": overall_score,
            "status_band": "green" if overall_score >= 70 else "yellow" if overall_score >= 50 else "red",
            "trend": trend,
            "weights": {
                "housing": 0.20,
                "cost_of_living": 0.20,
                "employment": 0.20,
                "policy": 0.15,
                "healthcare_access": 0.15,
                "sentiment": 0.10,
            },
        },
        "categories": {
            "housing": housing_score,
            "cost_of_living": cost_of_living_score,
            "employment": employment_score,
            "policy": policy_score,
            "healthcare_access": healthcare_access_score,
            "sentiment": sentiment_score,
        },
        "signals": {
            "hud": hud,
            "fred_unemployment": fred,
            "bea": bea,
            "va_facilities": va_facilities,
            "va_benefits_reference": va_benefits_reference,
            "federal": federal,
            "news": news,
        },
        "diagnostics": {
            "source_count": source_count,
            "successful_sources": successful_sources,
            "failed_sources": source_count - successful_sources,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
            "bls_api_key_present": bool(BLS_API_KEY),
            "census_api_key_present": bool(CENSUS_API_KEY),
            "bea_api_key_present": bool(BEA_API_KEY),
            "hud_api_key_present": bool(HUD_API_KEY),
            "va_api_key_present": bool(VA_API_KEY),
            "va_benefits_api_key_present": bool(VA_BENEFITS_API_KEY),
            "va_facilities_api_key_new_present": bool(os.getenv("VA_FACILITIES_API_KEY_NEW")),
            "va_benefits_reference_api_key_new_present": bool(os.getenv("VA_BENEFITS_REFERENCE_API_KEY_NEW")),
            "va_benefits_reference_url_present": bool(VA_BENEFITS_REFERENCE_URL),
            "va_facilities_ok": va_facilities["ok"],
            "va_benefits_reference_ok": va_benefits_reference["ok"],
            "va_facilities_sandbox_mode": va_facilities["sandbox_mode"],
            "va_benefits_reference_sandbox_mode": va_benefits_reference["sandbox_mode"],
            "tier_1": news["tier_1"],
            "tier_2": news["tier_2"],
            "tier_3": news["tier_3"],
            "policy_documents": federal["count"],
            "history_points": len(history),
        },
        "summary": {
            "headline": (
                f"USAVET integrated daily index is {overall_score} "
                f"({'GREEN' if overall_score >= 70 else 'YELLOW' if overall_score >= 50 else 'RED'}), "
                f"trend is {trend}."
            ),
            "notes": [
                f"Housing score: {housing_score}",
                f"Cost of living score: {cost_of_living_score}",
                f"Employment score: {employment_score}",
                f"Policy score: {policy_score}",
                f"Healthcare access score: {healthcare_access_score}",
                f"Sentiment score: {sentiment_score}",
                f"VA facilities reachable: {va_facilities['ok']}",
                f"VA benefits reference reachable: {va_benefits_reference['ok']}",
                f"VA facilities using new sandbox key: {va_facilities['using_new_key']}",
                f"VA benefits using new sandbox key: {va_benefits_reference['using_new_key']}",
            ],
        },
    }

    return payload


def main():
    data = build()

    write_json_file(OUTPUT_FILE, data)
    write_json_file(DAILY_FILE, data)

    history = load_json_file(HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []

    history.append(
        {
            "time": data["generated_at"],
            "overall_score": data["index"]["overall_score"],
            "status_band": data["index"]["status_band"],
            "trend": data["index"]["trend"],
            "housing": data["categories"]["housing"],
            "cost_of_living": data["categories"]["cost_of_living"],
            "employment": data["categories"]["employment"],
            "policy": data["categories"]["policy"],
            "healthcare_access": data["categories"]["healthcare_access"],
            "sentiment": data["categories"]["sentiment"],
            "successful_sources": data["diagnostics"]["successful_sources"],
            "va_facilities_ok": data["signals"]["va_facilities"]["ok"],
            "va_benefits_reference_ok": data["signals"]["va_benefits_reference"]["ok"],
            "va_facilities_sandbox_mode": data["signals"]["va_facilities"]["sandbox_mode"],
            "va_benefits_reference_sandbox_mode": data["signals"]["va_benefits_reference"]["sandbox_mode"],
        }
    )

    history = history[-180:]
    write_json_file(HISTORY_FILE, history)

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
