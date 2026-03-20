import requests
from datetime import datetime

# -------------------------
# CONFIG
# -------------------------
FRED_API_KEY = "f9850438b30dc86c085d6980a9088d6e"  # <-- replace this

def get_fred_series(series_id):
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2
    }

    response = requests.get(url, params=params)
    data = response.json()

    observations = data.get("observations", [])
    if len(observations) < 2:
        return None, None

    latest = float(observations[0]["value"])
    previous = float(observations[1]["value"])

    return latest, previous


# -------------------------
# FETCH REAL DATA
# -------------------------
cpi, cpi_prev = get_fred_series("CPIAUCSL")
unrate, unrate_prev = get_fred_series("UNRATE")

# -------------------------
# SCORING LOGIC
# -------------------------
def score_affordability(cpi):
    if cpi is None:
        return 50
    if cpi > 310:
        return 40
    elif cpi > 300:
        return 60
    else:
        return 80


def score_employment(unrate):
    if unrate is None:
        return 50
    if unrate > 6:
        return 40
    elif unrate > 4:
        return 60
    else:
        return 80


affordability_score = score_affordability(cpi)
employment_score = score_employment(unrate)

housing_score = 50
morale_score = 55
benefits_score = 50
media_score = 50

composite = int(
    (housing_score +
     affordability_score +
     employment_score +
     morale_score +
     benefits_score +
     media_score) / 6
)

# -------------------------
# STATUS
# -------------------------
if composite < 40:
    status = "High Pressure"
elif composite < 60:
    status = "Moderate Pressure"
else:
    status = "Stable"

# -------------------------
# NARRATIVE
# -------------------------
narrative = [
    f"CPI index level is {cpi}, previous {cpi_prev}.",
    f"Unemployment rate is {unrate}%, previous {unrate_prev}%.",
    f"Composite score is {composite}/100 indicating {status.lower()}."
]

# -------------------------
# OUTPUT
# -------------------------
output = {
    "timestamp": datetime.utcnow().isoformat(),
    "composite_score": composite,
    "status": status,
    "scores": {
        "housing_affordability": housing_score,
        "cost_of_living": affordability_score,
        "employment": employment_score,
        "health_wellbeing": morale_score,
        "benefits_processing": benefits_score,
        "media_environment": media_score
    },
    "narrative": narrative,
    "sources": {
        "cpi": "FRED CPIAUCSL",
        "unemployment": "FRED UNRATE"
    }
}

# save file
import json
with open("usavet_real_data_v1.json", "w") as f:
    json.dump(output, f, indent=2)

print("Real data file generated")
