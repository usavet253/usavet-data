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

    score = 78
    score -= max(0, (cpi_latest - 295.0) * 1.1)
    score -= max(0, change * 55.0)

    if change > 0:
        score -= 10
    elif change < 0:
        score += 6

    return clamp(score)


def score_employment(unrate_latest: float, unrate_previous: float) -> int:
    change = unrate_latest - unrate_previous

    score = 88
    score -= max(0, (unrate_latest - 3.5) * 20.0)

    if change > 0:
        score -= min(change * 75.0, 18.0)
    elif change < 0:
        score += min(abs(change) * 50.0, 10.0)

    return clamp(score)


def score_interest_rate(fed_rate: float) -> int:
    score = 92
    score -= fed_rate * 13.0
    return clamp(score)


def score_wages(wage_latest: float, wage_previous: float) -> int:
    change = wage_latest - wage_previous

    score = 58
    score += change * 22.0

    return clamp(score)


def score_sentiment(sentiment: float) -> int:
    return clamp(sentiment)


def score_housing(affordability_score: int, employment_score: int, interest_score: int) -> int:
    score = (
        affordability_score * 0.55
        + employment_score * 0.15
        + interest_score * 0.30
    )
    return clamp(score)


def score_morale(sentiment_score: int, wage_score: int, employment_score: int) -> int:
    score = (
        sentiment_score * 0.50
        + wage_score * 0.20
        + employment_score * 0.30
    )
    return clamp(score)


def score_benefits_signal(unrate_latest: float, unrate_previous: float, sentiment: float) -> int:
    """
    Signal-detection proxy until live VA backlog source is added.
    Rising unemployment and weak sentiment increase expected strain on systems.
    """
    change = unrate_latest - unrate_previous

    score = 65
    score -= max(0, (unrate_latest - 4.0) * 10.0)
    score -= max(0, change * 60.0)

    if sentiment < 70:
        score -= (70 - sentiment) * 0.35

    return clamp(score)


def score_media_signal(
    cpi_latest: float,
    cpi_previous: float,
    unrate_latest: float,
    unrate_previous: float,
    sentiment: float
) -> int:
    """
    Signal-detection media proxy:
    the more deterioration in inflation + labor + sentiment,
    the more likely issue intensity and narrative pressure rise.
    """
    cpi_change = cpi_latest - cpi_previous
    unrate_change = unrate_latest - unrate_previous

    pressure = 50.0
    pressure += max(0, cpi_change * 120.0)
    pressure += max(0, unrate_change * 120.0)
    pressure += max(0, (70.0 - sentiment) * 0.5)

    score = 100.0 - pressure
    return clamp(score)


def composite_score(scores: dict) -> int:
    return clamp(
        scores["cost_of_living"] * 0.18
        + scores["employment"] * 0.18
        + scores["interest_rates"] * 0.14
        + scores["wage_growth"] * 0.12
        + scores["consumer_sentiment"] * 0.10
        + scores["housing_affordability"] * 0.10
        + scores["health_wellbeing"] * 0.08
        + scores["benefits_processing"] * 0.05
        + scores["media_environment"] * 0.05
    )


def status_from_score(score: int) -> str:
    if score >= 72:
        return "Stable"
    if score >= 58:
        return "Watchful"
    if score >= 42:
        return "Moderate Pressure"
    return "High Pressure"


def direction_label(latest: float, previous: float, inverse_good: bool = False) -> str:
    if latest == previous:
        return "stable"

    improving = latest < previous if inverse_good else latest > previous
    return "improving" if improving else "deteriorating"


def build_narrative(
    cpi_latest,
    cpi_previous,
    unrate_latest,
    unrate_previous,
    fed_rate,
    wage_latest,
    wage_previous,
    sentiment,
    benefits_score,
    media_score,
    composite,
    status,
):
    cpi_direction = direction_label(cpi_latest, cpi_previous, inverse_good=True)
    unrate_direction = direction_label(unrate_latest, unrate_previous, inverse_good=True)
    wage_direction = direction_label(wage_latest, wage_previous, inverse_good=False)

    benefits_signal = "elevated friction" if benefits_score < 45 else "contained friction"
    media_signal = "high narrative pressure" if media_score < 45 else "contained narrative pressure"

    return [
        f"CPI index level is {cpi_latest:.2f}, previous {cpi_previous:.2f}, indicating affordability conditions are {cpi_direction}.",
        f"Unemployment rate is {unrate_latest:.1f}%, previous {unrate_previous:.1f}%, indicating labor conditions are {unrate_direction}.",
        f"Average hourly earnings index is {wage_latest:.2f}, previous {wage_previous:.2f}, indicating wage conditions are {wage_direction}.",
        f"Federal funds rate is {fed_rate:.2f} and consumer sentiment is {sentiment:.1f}.",
        f"Benefits signal suggests {benefits_signal}; media signal suggests {media_signal}.",
        f"Composite score is {composite}/100 indicating {status.lower()}."
    ]


def build_output(
    cpi_latest,
    cpi_previous,
    unrate_latest,
    unrate_previous,
    fed_rate,
    wage_latest,
    wage_previous,
    sentiment,
    data_status,
    error_message=None,
):
    affordability_score = score_affordability(cpi_latest, cpi_previous)
    employment_score = score_employment(unrate_latest, unrate_previous)
    interest_score = score_interest_rate(fed_rate)
    wage_score = score_wages(wage_latest, wage_previous)
    sentiment_score = score_sentiment(sentiment)

    housing_score = score_housing(affordability_score, employment_score, interest_score)
    morale_score = score_morale(sentiment_score, wage_score, employment_score)

    benefits_score = score_benefits_signal(unrate_latest, unrate_previous, sentiment)
    media_score = score_media_signal(
        cpi_latest,
        cpi_previous,
        unrate_latest,
        unrate_previous,
        sentiment
    )

    scores = {
        "housing_affordability": housing_score,
        "cost_of_living": affordability_score,
        "employment": employment_score,
        "interest_rates": interest_score,
        "wage_growth": wage_score,
        "consumer_sentiment": sentiment_score,
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
            fed_rate,
            wage_latest,
            wage_previous,
            sentiment,
            benefits_score,
            media_score,
            composite,
            status,
        ),
        "sources": {
            "cpi": "FRED CPIAUCSL",
            "unemployment": "FRED UNRATE",
            "fed_funds_rate": "FRED FEDFUNDS",
            "hourly_earnings": "FRED CES0500000003",
            "consumer_sentiment": "FRED UMCSENT",
            "benefits_processing": "proxy from labor + sentiment stress model",
            "media_environment": "proxy from inflation + labor + sentiment pressure model",
        },
        "raw_inputs": {
            "cpi_latest": cpi_latest,
            "cpi_previous": cpi_previous,
            "unemployment_latest": unrate_latest,
            "unemployment_previous": unrate_previous,
            "fed_rate": fed_rate,
            "wage_latest": wage_latest,
            "wage_previous": wage_previous,
            "sentiment": sentiment,
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
        fed_rate, _ = get_fred_series("FEDFUNDS")
        wage_latest, wage_previous = get_fred_series("CES0500000003")
        sentiment, _ = get_fred_series("UMCSENT")

        output = build_output(
            cpi_latest,
            cpi_previous,
            unrate_latest,
            unrate_previous,
            fed_rate,
            wage_latest,
            wage_previous,
            sentiment,
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
                raw.get("fed_rate", 5.0),
                raw.get("wage_latest", 1.0),
                raw.get("wage_previous", 1.0),
                raw.get("sentiment", 65.0),
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
