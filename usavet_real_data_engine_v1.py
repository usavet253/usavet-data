\
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional

import requests


BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


@dataclass
class SeriesMetric:
    series_id: str
    label: str
    latest_period: str
    latest_value: float
    prev_period: Optional[str]
    prev_value: Optional[float]
    yoy_period: Optional[str]
    yoy_value: Optional[float]
    mom_change: Optional[float]
    yoy_change: Optional[float]
    unit: str


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def safe_round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def month_token(item: dict) -> str:
    return f"{item['year']}-{item['period'][1:]}"


def fetch_bls_series(series_ids: List[str]) -> Dict[str, List[dict]]:
    # BLS public API v2 supports GET for single-series and POST for multiple series.
    # We use POST for a compact v1 engine and ask for 3 years of history.
    payload = {"seriesid": series_ids, "startyear": str(datetime.utcnow().year - 3), "endyear": str(datetime.utcnow().year)}
    resp = requests.post(BLS_API, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    status = data.get("status")
    if status != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS request failed: {status} | {data.get('message')}")
    results = {}
    for series in data["Results"]["series"]:
        cleaned = []
        for item in series["data"]:
            period = item.get("period", "")
            if not period.startswith("M"):
                continue
            if item.get("value") in (None, ""):
                continue
            cleaned.append(item)
        # API usually returns reverse chronological order already
        results[series["seriesID"]] = cleaned
    return results


def build_metric(series_id: str, label: str, unit: str, items: List[dict]) -> SeriesMetric:
    if len(items) < 2:
        raise RuntimeError(f"Not enough observations for {series_id}")

    latest = items[0]
    prev = items[1] if len(items) > 1 else None

    latest_value = float(latest["value"])
    prev_value = float(prev["value"]) if prev else None
    mom_change = (latest_value - prev_value) if prev_value is not None else None

    yoy_item = None
    latest_month = latest["period"]
    latest_year = int(latest["year"])
    for item in items[1:]:
        if item["period"] == latest_month and int(item["year"]) == latest_year - 1:
            yoy_item = item
            break

    yoy_value = float(yoy_item["value"]) if yoy_item else None
    yoy_change = None
    if yoy_value not in (None, 0):
        yoy_change = ((latest_value - yoy_value) / yoy_value) * 100.0 if unit == "index" else (latest_value - yoy_value)

    return SeriesMetric(
        series_id=series_id,
        label=label,
        latest_period=month_token(latest),
        latest_value=latest_value,
        prev_period=month_token(prev) if prev else None,
        prev_value=prev_value,
        yoy_period=month_token(yoy_item) if yoy_item else None,
        yoy_value=yoy_value,
        mom_change=mom_change,
        yoy_change=yoy_change,
        unit=unit,
    )


def score_cost_of_living(cpi_yoy: float, cpi_mom: float) -> int:
    pressure = (cpi_yoy * 14.0) + (max(cpi_mom, 0.0) * 90.0)
    return int(round(clamp(100.0 - pressure)))


def score_housing(shelter_yoy: float, unemployment_rate: float) -> int:
    pressure = (shelter_yoy * 15.0) + (unemployment_rate * 4.0)
    return int(round(clamp(100.0 - pressure)))


def score_employment(unemployment_rate: float, payroll_change_thousands: float) -> int:
    raw = 78.0 - (unemployment_rate * 8.0) + (payroll_change_thousands / 18.0)
    return int(round(clamp(raw)))


def score_healthcare(medical_yoy: float, cpi_yoy: float) -> int:
    pressure = (medical_yoy * 10.0) + (max(cpi_yoy - 2.0, 0.0) * 4.0)
    return int(round(clamp(100.0 - pressure)))


def score_morale(employment_score: int, cost_score: int, housing_score: int) -> int:
    raw = (employment_score * 0.45) + (cost_score * 0.30) + (housing_score * 0.25)
    return int(round(clamp(raw)))


def weighted_composite(scores: Dict[str, int]) -> int:
    weights = {
        "housing_stability": 0.22,
        "cost_of_living_pressure": 0.24,
        "employment_opportunity": 0.24,
        "healthcare_system_signals": 0.15,
        "veteran_morale_community": 0.15,
    }
    total = sum(scores[k] * w for k, w in weights.items())
    return int(round(clamp(total)))


def status_from_score(score: int) -> str:
    if score >= 75:
        return "Stable"
    if score >= 60:
        return "Watchful"
    if score >= 45:
        return "Elevated Pressure"
    return "High Pressure"


def trend_from_change(change: Optional[float], positive_is_good: bool = True, epsilon: float = 0.05) -> str:
    if change is None:
        return "Flat"
    val = change if positive_is_good else -change
    if val > epsilon:
        return "Improving"
    if val < -epsilon:
        return "Deteriorating"
    return "Flat"


def build_narrative(metrics: Dict[str, SeriesMetric], scores: Dict[str, int], composite: int) -> str:
    cpi = metrics["cpi_all_items"]
    unemp = metrics["unemployment_rate"]
    payroll = metrics["nonfarm_payrolls"]
    shelter = metrics["cpi_shelter"]
    med = metrics["cpi_medical_services"]

    composite_status = status_from_score(composite)
    employment_trend = trend_from_change(payroll.mom_change or 0.0, positive_is_good=True)
    price_trend = trend_from_change(cpi.mom_change or 0.0, positive_is_good=False)

    return (
        f"Composite conditions are {composite_status.lower()} at {composite}/100. "
        f"Headline CPI was {safe_round(cpi.yoy_change, 1)}% year over year in {cpi.latest_period}, "
        f"with shelter at {safe_round(shelter.yoy_change, 1)}% and medical care services at {safe_round(med.yoy_change, 1)}%. "
        f"The unemployment rate stood at {safe_round(unemp.latest_value, 1)}%, while total nonfarm payrolls changed by "
        f"{safe_round(payroll.mom_change, 0)} thousand month over month. "
        f"Employment is {employment_trend.lower()} and price pressure is {price_trend.lower()}, leaving household conditions "
        f"stable only where labor resilience offsets housing and medical-cost drag."
    )


def build_engine_output() -> dict:
    # v1 real-data engine uses public BLS series without requiring local API keys.
    series_map = {
        "cpi_all_items": ("CUUR0000SA0", "CPI-U All Items", "index"),
        "cpi_shelter": ("CUSR0000SAH1", "CPI-U Shelter", "index"),
        "cpi_medical_services": ("CUSR0000SAM2", "CPI-U Medical Care Services", "index"),
        "unemployment_rate": ("LNS14000000", "Unemployment Rate", "percent"),
        "nonfarm_payrolls": ("CES0000000001", "All Employees, Total Nonfarm", "thousands"),
    }
    raw = fetch_bls_series([v[0] for v in series_map.values()])

    metrics: Dict[str, SeriesMetric] = {}
    for key, (series_id, label, unit) in series_map.items():
        metrics[key] = build_metric(series_id, label, unit, raw[series_id])

    cpi_yoy = metrics["cpi_all_items"].yoy_change
    cpi_mom = metrics["cpi_all_items"].mom_change
    shelter_yoy = metrics["cpi_shelter"].yoy_change
    medical_yoy = metrics["cpi_medical_services"].yoy_change
    unemployment_rate = metrics["unemployment_rate"].latest_value
    payroll_mom = metrics["nonfarm_payrolls"].mom_change

    if None in (cpi_yoy, cpi_mom, shelter_yoy, medical_yoy, unemployment_rate, payroll_mom):
        raise RuntimeError("One or more required live metrics could not be calculated.")

    scores = {
        "housing_stability": score_housing(shelter_yoy, unemployment_rate),
        "cost_of_living_pressure": score_cost_of_living(cpi_yoy, cpi_mom),
        "employment_opportunity": score_employment(unemployment_rate, payroll_mom),
        "healthcare_system_signals": score_healthcare(medical_yoy, cpi_yoy),
    }
    scores["veteran_morale_community"] = score_morale(
        scores["employment_opportunity"],
        scores["cost_of_living_pressure"],
        scores["housing_stability"],
    )
    composite = weighted_composite(scores)

    output = {
        "engine_version": "v1",
        "run_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_family": "BLS Public Data API v2",
        "metrics": {k: asdict(v) for k, v in metrics.items()},
        "scores": scores,
        "composite_score": composite,
        "status": status_from_score(composite),
        "narrative": build_narrative(metrics, scores, composite),
        "source_index": [
            {"name": "BLS Public Data API v2", "url": BLS_API},
            {"name": "Series: CPI-U All Items", "series_id": "CUUR0000SA0"},
            {"name": "Series: CPI-U Shelter", "series_id": "CUSR0000SAH1"},
            {"name": "Series: CPI-U Medical Care Services", "series_id": "CUSR0000SAM2"},
            {"name": "Series: Unemployment Rate", "series_id": "LNS14000000"},
            {"name": "Series: All Employees, Total Nonfarm", "series_id": "CES0000000001"},
        ],
    }
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="USAVET real data engine v1")
    parser.add_argument("--output", default="usavet_real_data_v1.json", help="Output JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_engine_output()
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
