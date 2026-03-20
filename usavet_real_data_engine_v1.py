import json
import os
from datetime import datetime, timezone

import requests


FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
OUTPUT_FILE = "usavet_real_data_v1.json"


def load_previous_output():
    if not os.path.exists(OUTPUT_FILE):
        return None
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


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
        raise ValueError(f"Not enough observations returned for {series_id}")

    latest_raw = observations[0].get("value")
    previous_raw = observations[1].get("value")

    if latest_raw in (None, ".", "") or previous_raw in (None, ".", ""):
        raise ValueError(f"Invalid observation values returned for {series_id}")

    latest = float(latest_raw)
    previous = float(previous_raw)

    return latest, previous


def clamp(value: float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def score_affordability(cpi_latest: float, cpi_previous: float) -> int:
    change = cpi_latest - cpi_previous

    score = 80
    score -= max(0, (cpi_latest - 300.0) * 0.9)
    score -= max(0, change * 40.0)

    if change > 0:
        score -= 8
    elif change < 0:
        score += 5

    return clamp(score)


def score_employment(unrate_latest: float, unrate_previous: float) -> int:
    change = unrate_latest - unrate_previous

    score = 85
    score -= max(0, (unrate_latest - 3.5) * 18.0)

    if change > 0:
        score -= min(change * 60.0, 15.0)
    elif change < 0:
        score += min(abs(change) * 40.0, 8.0)

    return clamp(score)


def score_housing(affordability_score: int, employment_score: int) -> int:
    return clamp((affordability_score * 0.7) + (employment_score * 0.3))


def score_morale(affordability_score: int, employment_score: int, housing_score: int) -> int:
    return clamp(
        affordability_score * 0.35
        + employment_score * 0.35
        + housing_score * 0.30
    )


def score_benefits() -> int:
    return 50


def score_media() -> int:
    return 50


def composite_score(scores: dict) -> int:
    return clamp(
        scores["housing_affordability"] * 0.20
        + scores["cost_of_living"] * 0.25
        + scores["employment"] * 0.25
        + scores["health_wellbeing"] * 0.10
        + scores["benefits_processing"] * 0.10
        + scores["media_environment"] * 0.10
    )


def status_from_score(score: int) -> str:
    if score >= 70:
        return "Stable"
    if score >= 55:
        return "Watchful"
    if score >= 40:
        return "Moderate Pressure"
    return "High Pressure"


def direction_label(latest: float, previous: float, inverse_good: bool = False) -> str:
    if latest == previous:
        return "stable"
    improving = latest < previous if inverse_good else latest > previous
    return "improving" if improving else "deteriorating"


def build_narrative(cpi_latest, cpi_previous, unrate_latest, unrate_previous, composite, status):
    cpi_direction = direction_label(cpi_latest, cpi_previous, inverse_good=True)
    unrate_direction = direction_label(unrate_latest, unrate_previous, inverse_good=True)

    return [
        f"CPI index level is {cpi_latest:.2f}, previous {cpi_previous:.2f}, indicating affordability conditions are {cpi_direction}.",
        f"Unemployment rate is {unrate_latest:.1f}%, previous {unrate_previous:.1f}%, indicating labor conditions are {unrate_direction}.",
        f"Composite score is {composite}/100 indicating {status.lower()}."
    ]


def build_output(cpi_latest, cpi_previous, unrate_latest, unrate_previous, data_status, error_message=None):
    affordability_score = score_affordability(cpi_latest, cpi_previous)
    employment_score = score_employment(unrate_latest, unrate_previous)
    housing_score = score_housing(affordability_score, employment_score)
    morale_score = score_morale(affordability_score, employment_score, housing_score)
    benefits_score = score_benefits()
    media_score = score_media()

    scores = {
        "housing_affordability": housing_score,
        "cost_of_living": affordability_score,
        "employment": employment_score,
        "health_wellbeing": morale_score,
        "benefits_processing": benefits_score,
        "media_environment": media_score,
    }

    composite = composite_score(scores)
    status = status_from_score(composite)

    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_status": data_status,
        "composite_score": composite,
        "status": status,
        "scores": scores,
        "narrative": build_narrative(
            cpi_latest,
            cpi_previous,
            unrate_latest,
            unrate_previous,
            composite,
            status,
        ),
        "sources": {
            "cpi": "FRED CPIAUCSL",
            "unemployment": "FRED UNRATE",
        },
        "raw_inputs": {
            "cpi_latest": cpi_latest,
            "cpi_previous": cpi_previous,
            "unemployment_latest": unrate_latest,
            "unemployment_previous": unrate_previous,
        },
    }

    if error_message:
        output["warning"] = error_message

    return output


def main():
    previous = load_previous_output()

    try:
        cpi_latest, cpi_previous = get_fred_series("CPIAUCSL")
        unrate_latest, unrate_previous = get_fred_series("UNRATE")
        output = build_output(
            cpi_latest,
            cpi_previous,
            unrate_latest,
            unrate_previous,
            data_status="live"
        )

    except Exception as e:
        if previous and "raw_inputs" in previous:
            raw = previous["raw_inputs"]
            output = build_output(
                raw["cpi_latest"],
                raw["cpi_previous"],
                raw["unemployment_latest"],
                raw["unemployment_previous"],
                data_status="fallback_cached",
                error_message=str(e),
            )
        else:
            raise

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("Real data file generated")


if __name__ == "__main__":
    main()
