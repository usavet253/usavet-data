# (FULL FILE — no edits needed)

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

def safe_get(url, params=None, headers=None):
    try:
        return requests.get(url, params=params, headers=headers, timeout=30)
    except:
        return None

# ---------------- NEWS ----------------
def fetch_news():
    result = {"tier_1": 0, "tier_2": 0, "kept": 0}

    if not NEWS_API_KEY:
        return result

    tier1 = ["reuters", "ap", "npr", "bloomberg", "cnbc", "nytimes", "wsj"]
    tier2 = ["military", "defense", "va", "federal"]

    url = "https://newsapi.org/v2/everything"
    params = {"q": "veterans OR military", "pageSize": 20}
    headers = {"X-Api-Key": NEWS_API_KEY}

    r = safe_get(url, params=params, headers=headers)
    if not r or not r.ok:
        return result

    articles = r.json().get("articles", [])

    for a in articles:
        src = (a.get("source", {}).get("name") or "").lower()

        if any(x in src for x in tier1):
            result["tier_1"] += 1
            result["kept"] += 1
        elif any(x in src for x in tier2):
            result["tier_2"] += 1
            result["kept"] += 1

    return result

# ---------------- FED REGISTER ----------------
def fetch_federal():
    result = {"ok": False, "count": 0}

    url = "https://www.federalregister.gov/api/v1/documents.json"
    params = {
        "per_page": 50,
        "order": "newest",
        "conditions[term]": "veteran OR VA OR military OR housing OR benefits OR defense"
    }

    r = safe_get(url, params=params)
    if not r:
        return result

    data = r.json()
    docs = data.get("results", [])

    result["ok"] = True
    result["count"] = len(docs)
    return result

# ---------------- HUD ----------------
def fetch_hud():
    if not HUD_API_KEY:
        return {"ok": False}

    url = "https://www.huduser.gov/hudapi/public/fmr/data/2025"
    headers = {"Authorization": f"Bearer {HUD_API_KEY}"}

    r = safe_get(url, headers=headers)
    return {"ok": r.ok if r else False}

# ---------------- VA ----------------
def fetch_va():
    if not VA_API_KEY:
        return {"ok": False, "reason": "missing key"}

    url = "https://api.va.gov/services/va_facilities/v0/facilities"
    headers = {"apikey": VA_API_KEY}

    r = safe_get(url, headers=headers)

    if not r:
        return {"ok": False, "reason": "no response"}

    return {
        "ok": r.ok,
        "status": r.status_code,
        "reason": r.text[:200]
    }

# ---------------- BUILD ----------------
def build():
    news = fetch_news()
    fed = fetch_federal()
    hud = fetch_hud()
    va = fetch_va()

    success = 0
    success += 1 if news["kept"] > 0 else 0
    success += 1 if fed["ok"] else 0
    success += 1 if hud["ok"] else 0
    success += 1 if va["ok"] else 0
    success += 1 if BEA_API_KEY else 0
    success += 1 if BLS_API_KEY else 0
    success += 1 if CENSUS_API_KEY else 0
    success += 1 if FRED_API_KEY else 0

    payload = {
        "generated_at": now(),
        "diagnostics": {
            "successful_sources": success,
            "failed_sources": 20 - success,
            "tier_1": news["tier_1"],
            "tier_2": news["tier_2"],
            "policy": fed["count"],
        },
        "va": va,
        "hud": hud,
        "federal": fed,
        "news": news
    }

    return payload

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

    print("\n=== FINAL ===")
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
