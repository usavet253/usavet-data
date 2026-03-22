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


def fetch_va():
    result = {
        "ok": False,
        "status_code": None,
        "endpoint": "https://api.va.gov/services/va_facilities/v1/facilities",
        "facility_count_page_1": None,
        "reason": None,
        "exception_type": None,
        "exception_message": None,
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


def fetch_news():
    result = {
        "enabled": bool(NEWS_API_KEY),
        "tier_1": 0,
        "tier_2": 0,
        "tier_3": 0,
        "kept": 0,
        "errors": [],
    }

    if not NEWS_API_KEY:
        result["errors"].append("NEWS_API_KEY missing")
        return result

    url = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": NEWS_API_KEY}
    params = {
        "q": "veterans OR military",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 10,
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

    for article in articles:
        src = ((article.get("source") or {}).get("name") or "").lower()

        if any(x in src for x in ["reuters", "associated press", "ap ", "npr", "bloomberg", "cnbc", "wsj", "new york times"]):
            result["tier_1"] += 1
            result["kept"] += 1
        elif any(x in src for x in ["military", "defense", "va", "federal news network", "stars and stripes"]):
            result["tier_2"] += 1
            result["kept"] += 1
        else:
            result["tier_3"] += 1

    return result


def fetch_federal():
    result = {
        "ok": False,
        "count": 0,
        "status_code": None,
        "reason": None,
    }

    url = "https://www.federalregister.gov/api/v1/documents.json"
    params = {
        "per_page": 25,
        "conditions[term]": "veteran OR veterans OR military OR VA OR benefits OR housing",
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
    result["ok"] = True
    return result


def fetch_bea():
    result = {
        "ok": False,
        "status_code": None,
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
        "ResultFormat": "json",
    }

    req = safe_get(url, params=params)

    if not req["ok"]:
        result["reason"] = f"{req['exception_type']}: {req['exception_message']}"
        return result

    response = req["response"]
    result["status_code"] = response.status_code

    if response.ok:
        result["ok"] = True
    else:
        result["reason"] = str(safe_json(response))[:300]

    return result


def build():
    hud = fetch_hud()
    va = fetch_va()
    news = fetch_news()
    federal = fetch_federal()
    bea = fetch_bea()

    success = 0
    success += 1 if hud["ok"] else 0
    success += 1 if va["ok"] else 0
    success += 1 if news["kept"] > 0 else 0
    success += 1 if federal["ok"] else 0
    success += 1 if bea["ok"] else 0
    success += 1 if bool(FRED_API_KEY) else 0
    success += 1 if bool(BLS_API_KEY) else 0
    success += 1 if bool(CENSUS_API_KEY) else 0
    success += 1 if bool(VA_BENEFITS_API_KEY) else 0

    payload = {
        "generated_at": now_utc(),
        "diagnostics": {
            "successful_sources": success,
            "failed_sources": 20 - success,
            "fred_api_key_present": bool(FRED_API_KEY),
            "news_api_key_present": bool(NEWS_API_KEY),
            "bls_api_key_present": bool(BLS_API_KEY),
            "census_api_key_present": bool(CENSUS_API_KEY),
            "bea_api_key_present": bool(BEA_API_KEY),
            "hud_api_key_present": bool(HUD_API_KEY),
            "va_api_key_present": bool(VA_API_KEY),
            "va_benefits_api_key_present": bool(VA_BENEFITS_API_KEY),
            "tier_1": news["tier_1"],
            "tier_2": news["tier_2"],
            "tier_3": news["tier_3"],
            "policy": federal["count"],
            "history_points": len(load_json_file(HISTORY_FILE, [])),
        },
        "hud": hud,
        "va": va,
        "bea": bea,
        "news": news,
        "federal": federal,
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
            "success": data["diagnostics"]["successful_sources"],
        }
    )

    history = history[-180:]
    write_json_file(HISTORY_FILE, history)

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
