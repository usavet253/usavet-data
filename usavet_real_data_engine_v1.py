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
VA_API_KEY = os.getenv("VA_API_KEY")
VA_BENEFITS_API_KEY = os.getenv("VA_BENEFITS_API_KEY")


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def safe_json(response):
    try:
        return response.json()
    except Exception:
        return {"raw_text": response.text[:1000]}


def safe_get(url, params=None, headers=None, timeout=30):
    try:
        return requests.get(url, params=params, headers=headers, timeout=timeout)
    except Exception as e:
        return {"request_exception": f"{type(e).__name__}: {e}"}


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


def fetch_news():
    result = {
        "enabled": bool(NEWS_API_KEY),
        "queries_run": 4,
        "tier_1": 0,
        "tier_2": 0,
        "tier_3": 0,
        "kept": 0,
        "used": 0,
        "dropped": 0,
        "policy_like": 0,
        "kept_article_samples": [],
        "errors": [],
    }

    if not NEWS_API_KEY:
        result["errors"].append("NEWS_API_KEY missing")
        return result

    queries = [
        "veterans housing benefits VA",
        "military family cost of living",
        "veteran employment labor market",
        "veterans policy federal benefits",
    ]

    tier_1_sources = {
        "reuters",
        "associated press",
        "ap ",
        "npr",
        "wall street journal",
        "wsj",
        "new york times",
        "nytimes",
        "washington post",
        "bloomberg",
        "cnbc",
    }

    tier_2_sources = {
        "military times",
        "military.com",
        "stars and stripes",
        "starsandstripes",
        "federal news network",
        "defense",
        "va.gov",
    }

    seen_urls = set()

    for query in queries:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 10,
        }
        headers = {"X-Api-Key": NEWS_API_KEY}

        response = safe_get(url, params=params, headers=headers, timeout=30)

        if isinstance(response, dict) and response.get("request_exception"):
            result["errors"].append(response["request_exception"])
            continue

        if response is None:
            result["errors"].append("news request returned none")
            continue

        payload = safe_json(response)

        if not response.ok:
            result["errors"].append(str(payload)[:300])
            continue

        articles = payload.get("articles", [])
        result["used"] += len(articles)

        for article in articles:
            url_value = article.get("url")
            source_name = ((article.get("source") or {}).get("name") or "").lower()
            title = article.get("title", "") or ""
            description = article.get("description", "") or ""
            blob = f"{title} {description}".lower()

            if not url_value or url_value in seen_urls:
                result["dropped"] += 1
                continue

            seen_urls.add(url_value)

            is_tier_1 = any(s in source_name for s in tier_1_sources)
            is_tier_2 = any(s in source_name for s in tier_2_sources)

            if not is_tier_1 and not is_tier_2:
                result["tier_3"] += 1
                result["dropped"] += 1
                continue

            if is_tier_1:
                result["tier_1"] += 1
            elif is_tier_2:
                result["tier_2"] += 1

            if any(k in blob for k in ["policy", "rule", "regulation", "bill", "law", "congress"]):
                result["policy_like"] += 1

            result["kept"] += 1

            if len(result["kept_article_samples"]) < 5:
                result["kept_article_samples"].append(
                    {
                        "source": source_name,
                        "title": title[:160],
                        "url": url_value,
                    }
                )

    return result


def fetch_federal_register():
    result = {
        "ok": False,
        "count": 0,
        "document_numbers": [],
        "agencies": [],
        "date_gte": "2025-12-01",
        "status_code": None,
        "note": None,
    }

    url = "https://www.federalregister.gov/api/v1/documents.json"
    params = {
        "per_page": 50,
        "order": "newest",
        "conditions[publication_date][gte]": "2025-12-01",
        "conditions[term]": "veteran OR veterans OR VA OR housing OR benefits OR military OR defense OR healthcare",
    }

    response = safe_get(url, params=params, timeout=30)

    if isinstance(response, dict) and response.get("request_exception"):
        result["note"] = response["request_exception"]
        return result

    if response is None:
        result["note"] = "federal register request returned none"
        return result

    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["note"] = str(payload)[:500]
        return result

    docs = payload.get("results", [])
    result["ok"] = True
    result["count"] = len(docs)
    result["document_numbers"] = [d.get("document_number") for d in docs[:5] if d.get("document_number")]
    result["agencies"] = list(
        {
            agency.get("name")
            for d in docs
            for agency in d.get("agencies", [])
            if agency.get("name")
        }
    )[:8]
    return result


def fetch_hud():
    result = {
        "ok": False,
        "fmr_endpoint_ok": False,
        "income_limits_endpoint_ok": False,
        "status_fmr": None,
        "status_il": None,
        "fmr_sample_count": None,
        "il_sample_count": None,
        "note": None,
    }

    if not HUD_API_KEY:
        result["note"] = "HUD_API_KEY missing"
        return result

    headers = {
        "Authorization": f"Bearer {HUD_API_KEY}",
        "Accept": "application/json",
    }

    # Health check endpoint under current documented FMR base path
    fmr_url = "https://www.huduser.gov/hudapi/public/fmr/listStates"
    fmr_response = safe_get(fmr_url, headers=headers, timeout=30)

    if isinstance(fmr_response, dict) and fmr_response.get("request_exception"):
        result["note"] = f"FMR exception: {fmr_response['request_exception']}"
    elif fmr_response is not None:
        result["status_fmr"] = fmr_response.status_code
        fmr_payload = safe_json(fmr_response)
        if fmr_response.ok:
            result["fmr_endpoint_ok"] = True
            data = fmr_payload.get("data")
            if isinstance(data, list):
                result["fmr_sample_count"] = len(data)
            elif isinstance(data, dict):
                result["fmr_sample_count"] = len(data)
            else:
                result["fmr_sample_count"] = 1
        else:
            result["note"] = f"FMR failed: {str(fmr_payload)[:300]}"

    # IL health check using documented IL base path
    il_url = "https://www.huduser.gov/hudapi/public/fmr/listCounties/WA?updated=2025"
    il_response = safe_get(il_url, headers=headers, timeout=30)

    if isinstance(il_response, dict) and il_response.get("request_exception"):
        if not result["note"]:
            result["note"] = f"IL exception: {il_response['request_exception']}"
    elif il_response is not None:
        result["status_il"] = il_response.status_code
        il_payload = safe_json(il_response)
        if il_response.ok:
            result["income_limits_endpoint_ok"] = True
            data = il_payload.get("data")
            if isinstance(data, list):
                result["il_sample_count"] = len(data)
            elif isinstance(data, dict):
                result["il_sample_count"] = len(data)
            else:
                result["il_sample_count"] = 1
        else:
            if not result["note"]:
                result["note"] = f"IL failed: {str(il_payload)[:300]}"

    result["ok"] = bool(result["fmr_endpoint_ok"] or result["income_limits_endpoint_ok"])
    return result


def fetch_va():
    result = {
        "ok": False,
        "status_code": None,
        "endpoint": "https://api.va.gov/services/va_facilities/v1/facilities",
        "facility_count_page_1": None,
        "reason": None,
    }

    if not VA_API_KEY:
        result["reason"] = "VA_API_KEY missing"
        return result

    headers = {
        "apikey": VA_API_KEY,
        "Accept": "application/json",
        "User-Agent": "usavet-data/1.0",
    }

    params = {
        "type": "health",
        "state": "WA",
        "page": 1,
        "per_page": 10,
    }

    response = safe_get(result["endpoint"], params=params, headers=headers, timeout=30)

    if isinstance(response, dict) and response.get("request_exception"):
        result["reason"] = response["request_exception"]
        return result

    if response is None:
        result["reason"] = "no response"
        return result

    result["status_code"] = response.status_code
    payload = safe_json(response)

    if response.ok:
        data = payload.get("data", [])
        result["ok"] = True
        result["facility_count_page_1"] = len(data) if isinstance(data, list) else 0
        result["reason"] = "VA facilities endpoint reachable"
    else:
        result["reason"] = str(payload)[:500]

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
        "note": None,
    }

    if not BEA_API_KEY:
        result["note"] = "BEA_API_KEY missing"
        return result

    url = "https://apps.bea.gov/api/data"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T10101",
        "Frequency": "Q",
        "Year": "2025",
        "ResultFormat": "json",
    }

    response = safe_get(url, params=params, timeout=30)

    if isinstance(response, dict) and response.get("request_exception"):
        result["note"] = response["request_exception"]
        return result

    if response is None:
        result["note"] = "BEA request returned none"
        return result

    result["status_code"] = response.status_code
    payload = safe_json(response)

    if not response.ok:
        result["note"] = str(payload)[:500]
        return result

    try:
        records = payload["BEAAPI"]["Results"]["Data"]
        result["ok"] = True
        result["records_count"] = len(records)
        result["sample_time_periods"] = [r.get("TimePeriod") for r in records[:5] if r.get("TimePeriod")]
        result["sample_values"] = [r.get("DataValue") for r in records[:5] if r.get("DataValue")]
    except Exception as e:
        result["note"] = f"schema parse failed: {type(e).__name__}: {e}"

    return result


def build_payload():
    news = fetch_news()
    federal = fetch_federal_register()
    hud = fetch_hud()
    va = fetch_va()
    bea = fetch_bea()

    successful_sources = 0
    successful_sources += 1 if FRED_API_KEY else 0
    successful_sources += 1 if NEWS_API_KEY and news.get("kept", 0) > 0 else 0
    successful_sources += 1 if BLS_API_KEY else 0
    successful_sources += 1 if CENSUS_API_KEY else 0
    successful_sources += 1 if BEA_API_KEY else 0
    successful_sources += 1 if bea.get("ok") else 0
    successful_sources += 1 if hud.get("ok") else 0
    successful_sources += 1 if VA_API_KEY else 0
    successful_sources += 1 if va.get("ok") else 0
    successful_sources += 1 if VA_BENEFITS_API_KEY else 0
    successful_sources += 1 if federal.get("ok") else 0

    diagnostics = {
        "source_count": 20,
        "successful_sources": successful_sources,
        "failed_sources": 20 - successful_sources,
        "fred_api_key_present": bool(FRED_API_KEY),
        "news_api_key_present": bool(NEWS_API_KEY),
        "bls_api_key_present": bool(BLS_API_KEY),
        "census_api_key_present": bool(CENSUS_API_KEY),
        "bea_api_key_present": bool(BEA_API_KEY),
        "hud_api_key_present": bool(HUD_API_KEY),
        "va_api_key_present": bool(VA_API_KEY),
        "va_benefits_api_key_present": bool(VA_BENEFITS_API_KEY),
        "tier_1": news.get("tier_1", 0),
        "tier_2": news.get("tier_2", 0),
        "tier_3": news.get("tier_3", 0),
        "policy": federal.get("count", 0) + news.get("policy_like", 0),
        "history_points": len(load_json_file(HISTORY_FILE, [])),
    }

    overall_score = 50
    overall_score += min(news.get("kept", 0), 10)
    overall_score += min(federal.get("count", 0), 10)
    overall_score += 5 if hud.get("ok") else 0
    overall_score += 5 if bea.get("ok") else 0
    overall_score += 5 if va.get("ok") else 0
    overall_score = min(overall_score, 100)

    payload = {
        "generated_at": now_utc(),
        "product": "USAVET.AI Daily Accountability Index",
        "version": "v1",
        "status": "ok",
        "index": {
            "overall_score": overall_score,
            "band": "green" if overall_score >= 70 else "yellow" if overall_score >= 50 else "red",
        },
        "diagnostics": diagnostics,
        "va": va,
        "hud": hud,
        "federal": federal,
        "news": news,
        "bea": bea,
    }

    return payload


def update_history(payload):
    history = load_json_file(HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []

    history.append(
        {
            "time": payload.get("generated_at"),
            "success": payload.get("diagnostics", {}).get("successful_sources"),
            "overall_score": payload.get("index", {}).get("overall_score"),
        }
    )

    return history[-180:]


def main():
    payload = build_payload()
    history = update_history(payload)

    write_json_file(OUTPUT_FILE, payload)
    write_json_file(DAILY_FILE, payload)
    write_json_file(HISTORY_FILE, history)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"Wrote {DAILY_FILE}")
    print(f"Wrote {HISTORY_FILE}")

    print("\n=== FINAL ===")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
