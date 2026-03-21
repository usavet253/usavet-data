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

def now():
    return datetime.now(timezone.utc).isoformat()

def safe_get(url, headers=None, params=None):
    try:
        return requests.get(url, headers=headers, params=params, timeout=30)
    except:
        return None

# ---------------- HUD ----------------
def fetch_hud():
    result = {"ok": False}

    if not HUD_API_KEY:
        return result

    url = "https://www.huduser.gov/hudapi/public/fmr/listStates"
    headers = {"Authorization": f"Bearer {HUD_API_KEY}"}

    r = safe_get(url, headers=headers)

    if not r or not r.ok:
        return result

    result["ok"] = True
    return result

# ---------------- VA ----------------
def fetch_va():
    result = {"ok": False, "reason": None}

    if not VA_API_KEY:
        result["reason"] = "missing key"
        return result

    url = "https://api.va.gov/services/va_facilities/v1/facilities"
    headers = {"apikey": VA_API_KEY}

    r = safe_get(url, headers=headers)

    if not r:
        result["reason"] = "no response"
        return result

    if r.ok:
        result["ok"] = True
    else:
        result["reason"] = r.text[:200]

    return result

# ---------------- NEWS ----------------
def fetch_news():
    result = {"tier_1": 0, "tier_2": 0, "kept": 0}

    if not NEWS_API_KEY:
        return result

    url = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": NEWS_API_KEY}
    params = {"q": "veterans OR military", "pageSize": 10}

    r = safe_get(url, headers=headers, params=params)

    if not r or not r.ok:
        return result

    articles = r.json().get("articles", [])

    for a in articles:
        src = (a.get("source", {}).get("name") or "").lower()

        if any(x in src for x in ["reuters", "ap", "npr", "bloomberg"]):
            result["tier_1"] += 1
            result["kept"] += 1
        elif any(x in src for x in ["military", "defense", "va"]):
            result["tier_2"] += 1
            result["kept"] += 1

    return result

# ---------------- FEDERAL ----------------
def fetch_federal():
    url = "https://www.federalregister.gov/api/v1/documents.json"
    params = {"per_page": 25, "conditions[term]": "veteran OR military"}

    r = safe_get(url, params=params)

    if not r:
        return {"ok": False, "count": 0}

    data = r.json()
    return {"ok": True, "count": len(data.get("results", []))}

# ---------------- BEA ----------------
def fetch_bea():
    if not BEA_API_KEY:
        return {"ok": False}

    url = "https://apps.bea.gov/api/data"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T10101",
        "Year": "2025",
        "ResultFormat": "json"
    }

    r = safe_get(url, params=params)

    if not r or not r.ok:
        return {"ok": False}

    return {"ok": True}

# ---------------- BUILD ----------------
def build():
    hud = fetch_hud()
    va = fetch_va()
    news = fetch_news()
    fed = fetch_federal()
    bea = fetch_bea()

    success = 0
    success += 1 if hud["ok"] else 0
    success += 1 if va["ok"] else 0
    success += 1 if news["kept"] > 0 else 0
    success += 1 if fed["ok"] else 0
    success += 1 if bea["ok"] else 0
    success += 1 if FRED_API_KEY else 0
    success += 1 if BLS_API_KEY else 0
    success += 1 if CENSUS_API_KEY else 0

    return {
        "generated_at": now(),
        "diagnostics": {
            "successful_sources": success,
            "failed_sources": 20 - success,
            "tier_1": news["tier_1"],
            "tier_2": news["tier_2"],
            "policy": fed["count"]
        },
        "hud": hud,
        "va": va,
        "bea": bea,
        "news": news,
        "federal": fed
    }

# ---------------- MAIN ----------------
def main():
    data = build()

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)

    with open(DAILY_FILE, "w") as f:
        json.dump(data, f, indent=2)

    history = []
    if os.path.exists(HISTORY_FILE):
        try:
            history = json.load(open(HISTORY_FILE))
        except:
            history = []

    history.append({
        "time": data["generated_at"],
        "success": data["diagnostics"]["successful_sources"]
    })

    history = history[-180:]

    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)

    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    main()
