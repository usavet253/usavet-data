import json
import os
from datetime import datetime, timezone

import requests

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
OUTPUT_FILE = "usavet_real_data_v1.json"


# -----------------------
# UTIL
# -----------------------

def clamp(value: float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def get_fred_series(series_id: str):
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY is not set")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    observations = data.get("observations", [])

    if len(observations) < 2:
        raise ValueError(f"Not enough data for {series_id}")

    latest = float(observations[0]["value"])
    previous = float(observations[1]["value"])

    return latest, previous


# -----------------------
# SCORING FUNCTIONS
# -----------------------

def score_affordability(cpi_latest, cpi_previous):
    change = cpi_latest - cpi_previous

    score = 75
    score -= max(0, (cpi_latest - 295) * 0.6)
    score -= max(0, change * 30)

    if change > 0:
        score -= 6
    elif change < 0:
        score += 4

    return clamp(score, 10, 100)


def score_employment(unrate_latest, unrate_previous):
    change = unrate_latest - unrate_previous

    score = 88
    score -= max(0, (unrate_latest - 3.5) * 20)

    if change > 0:
        score -= min(change * 75, 18)
    elif change < 0:
        score += min(abs(change) * 50, 10)

    return clamp(score)


def score_interest_rate(fed_rate):
    score = 92
    score -= fed_rate * 13
    return clamp(score)


def score_wages(wage_latest, wage_previous):
    change = wage_latest - wage_previous
    score = 58 + (change * 22)
    return clamp(score)


def score_sentiment(sentiment):
    return clamp(sentiment)


# -----------------------
# 🚨 NEW: HOUSING LEADING SIGNAL
# -----------------------

def score_housing_leading(case_shiller_latest, case_shiller_prev, mortgage_rate):
    price_change = case_shiller_latest - case_shiller_prev

    score = 70

    # Home price acceleration (bad if rising too fast)
    if price_change > 0:
        score -= min(price_change * 25, 20)

    # Mortgage pressure (VERY strong signal)
    score -= mortgage_rate * 6

    # nonlinear stress trigger
    if mortgage_rate > 6:
        score -= 10

    return clamp(score, 5, 100)


# -----------------------
# DERIVED SYSTEMS
# -----------------------

def score_morale(sentiment, wages, employment):
    score = sentiment * 0.5 + wages * 0.2 + employment * 0.3
    return clamp(score)


def score_benefits(unrate_latest, unrate_previous, sentiment):
    change = unrate_latest - unrate_previous

    score = 65
    score -= max(0, (unrate_latest - 4.0) * 10)
    score -= max(0, change * 60)

    if sentiment < 70:
        score -= (70 - sentiment) * 0.35

    return clamp(score)


def score_media(cpi_latest, cpi_previous, unrate_latest, unrate_previous, sentiment):
    cpi_change = cpi_latest - cpi_previous
    unrate_change = unrate_latest - unrate_previous

    pressure = 40
    pressure += min(cpi_change * 80, 20)
    pressure += min(unrate_change * 80, 20)
    pressure += max(0, (70 - sentiment) * 0.4)

    return clamp(100 - pressure, 15, 100)


# -----------------------
# COMPOSITE
# -----------------------

def composite_score(scores):
    return clamp(
        scores["cost_of_living"] * 0.18 +
        scores["employment"] * 0.18 +
        scores["interest_rates"] * 0.14 +
        scores["housing_affordability"] * 0.14 +  # ↑ increased weight
        scores["wage_growth"] * 0.10 +
        scores["consumer_sentiment"] * 0.08 +
        scores["health_wellbeing"] * 0.07 +
        scores["benefits_processing"] * 0.06 +
        scores["media_environment"] * 0.05
    )


def status(score):
    if score >= 72:
        return "Stable"
    if score >= 58:
        return "Watchful"
    if score >= 42:
        return "Moderate Pressure"
    return "High Pressure"


# -----------------------
# MAIN
# -----------------------

def main():

    # Core economic
    cpi_latest, cpi_prev = get_fred_series("CPIAUCSL")
    un_latest, un_prev = get_fred_series("UNRATE")
    fed_rate, _ = get_fred_series("FEDFUNDS")
    wage_latest, wage_prev = get_fred_series("CES0500000003")
    sentiment, _ = get_fred_series("UMCSENT")

    # 🚨 NEW housing data
    housing_price, housing_prev = get_fred_series("CSUSHPISA")
    mortgage_rate, _ = get_fred_series("MORTGAGE30US")

    # Scores
    affordability = score_affordability(cpi_latest, cpi_prev)
    employment = score_employment(un_latest, un_prev)
    interest = score_interest_rate(fed_rate)
    wages = score_wages(wage_latest, wage_prev)
    sentiment_score = score_sentiment(sentiment)

    housing = score_housing_leading(housing_price, housing_prev, mortgage_rate)

    morale = score_morale(sentiment_score, wages, employment)
    benefits = score_benefits(un_latest, un_prev, sentiment)
    media = score_media(cpi_latest, cpi_prev, un_latest, un_prev, sentiment)

    scores = {
        "housing_affordability": housing,
        "cost_of_living": affordability,
        "employment": employment,
        "interest_rates": interest,
        "wage_growth": wages,
        "consumer_sentiment": sentiment_score,
        "health_wellbeing": morale,
        "benefits_processing": benefits,
        "media_environment": media,
    }

    comp = composite_score(scores)

    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_status": "live",
        "composite_score": comp,
        "status": status(comp),
        "scores": scores,
        "sources": {
            "cpi": "FRED CPIAUCSL",
            "unemployment": "FRED UNRATE",
            "fed_funds_rate": "FRED FEDFUNDS",
            "wages": "FRED CES0500000003",
            "sentiment": "FRED UMCSENT",
            "housing_prices": "FRED CSUSHPISA",
            "mortgage_rate": "FRED MORTGAGE30US"
        }
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print("System updated with housing leading indicator")


if __name__ == "__main__":
    main()
