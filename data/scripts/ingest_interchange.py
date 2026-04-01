"""
Ingest PJM interchange (imports/exports) data.
Source: EIA API v2 — electricity/rto/region-data (Total Interchange)
Outputs: data/processed/interchange.json (Ultan-compliant)

Strategy: Fetch total interchange for one representative week per quarter,
then annualize. For neighbor detail, fetch interchange-data for one week per year.
"""

import json
import urllib.request
import time
from datetime import date
from pathlib import Path

OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)

API_KEY = "knd9FkLaYpfSr0rJZemHK7mMzIS2GjntMZcIS1qq"
BASE_URL = "https://api.eia.gov/v2"


def fetch_eia(endpoint, params):
    """Fetch from EIA API v2."""
    param_str = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}/{endpoint}?api_key={API_KEY}&{param_str}"
    req = urllib.request.Request(url, headers={"User-Agent": "PJM-Dashboard/1.0"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_week_data(year, month, day, metric_type):
    """Fetch one week of hourly PJM data for a given metric."""
    end_day = min(day + 6, 28)  # Stay within month
    params = {
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": "PJM",
        "facets[type][]": metric_type,
        "start": f"{year}-{month:02d}-{day:02d}T00",
        "end": f"{year}-{month:02d}-{end_day:02d}T23",
        "length": "5000",
    }
    resp = fetch_eia("electricity/rto/region-data/data", params)
    data = resp.get("response", {}).get("data", [])
    return data


def fetch_neighbor_week(year, month, day):
    """Fetch one week of neighbor-level interchange."""
    end_day = min(day + 6, 28)
    start = f"{year}-{month:02d}-{day:02d}T00"
    end = f"{year}-{month:02d}-{end_day:02d}T23"

    # Exports from PJM
    resp1 = fetch_eia("electricity/rto/interchange-data/data", {
        "frequency": "hourly",
        "data[0]": "value",
        "facets[fromba][]": "PJM",
        "start": start,
        "end": end,
        "length": "5000",
    })
    exports = resp1.get("response", {}).get("data", [])
    time.sleep(0.5)

    # Imports to PJM
    resp2 = fetch_eia("electricity/rto/interchange-data/data", {
        "frequency": "hourly",
        "data[0]": "value",
        "facets[toba][]": "PJM",
        "start": start,
        "end": end,
        "length": "5000",
    })
    imports = resp2.get("response", {}).get("data", [])

    return exports, imports


def main():
    # Sample weeks: mid-month of each quarter
    sample_weeks = [
        (1, 10),   # Q1: January
        (4, 10),   # Q2: April
        (7, 10),   # Q3: July
        (10, 10),  # Q4: October
    ]

    annual_summary = []

    for year in range(2020, 2026):
        print(f"  {year}...")
        quarterly_avg_mwh = []

        for month, day in sample_weeks:
            if year == 2025 and month > 3:
                continue  # Don't fetch future months

            # Total interchange
            ti_data = fetch_week_data(year, month, day, "TI")
            ti_sum = sum(float(r["value"]) for r in ti_data if r.get("value"))
            ti_hours = len(ti_data)

            # Demand
            d_data = fetch_week_data(year, month, day, "D")
            d_sum = sum(float(r["value"]) for r in d_data if r.get("value"))

            # Net generation
            ng_data = fetch_week_data(year, month, day, "NG")
            ng_sum = sum(float(r["value"]) for r in ng_data if r.get("value"))

            if ti_hours > 0:
                hourly_avg_ti = ti_sum / ti_hours
                quarterly_avg_mwh.append({
                    "quarter_sample": f"Q{sample_weeks.index((month, day)) + 1}",
                    "interchange_avg_mwh_per_hour": round(hourly_avg_ti, 1),
                    "demand_week_mwh": round(d_sum),
                    "generation_week_mwh": round(ng_sum),
                    "interchange_week_mwh": round(ti_sum),
                    "hours_sampled": ti_hours,
                })

            time.sleep(0.5)

        # Annualize from quarterly samples
        if quarterly_avg_mwh:
            avg_hourly = sum(q["interchange_avg_mwh_per_hour"] for q in quarterly_avg_mwh) / len(quarterly_avg_mwh)
            est_annual = avg_hourly * 8760

            annual_summary.append({
                "year": year,
                "est_annual_interchange_mwh": round(est_annual),
                "avg_hourly_interchange_mwh": round(avg_hourly, 1),
                "quarterly_samples": quarterly_avg_mwh,
                "note": "[ESTIMATE] Annualized from {} quarterly week-long samples".format(
                    len(quarterly_avg_mwh)
                ),
            })

    # Neighbor detail for most recent complete year
    print("  Neighbor detail (2024 sample week)...")
    exports, imports = fetch_neighbor_week(2024, 7, 10)

    neighbors = {}
    for r in exports:
        partner = r.get("toba", "Unknown")
        val = float(r.get("value", 0)) if r.get("value") else 0
        if partner not in neighbors:
            neighbors[partner] = {"export_mwh": 0, "import_mwh": 0}
        neighbors[partner]["export_mwh"] += val

    for r in imports:
        partner = r.get("fromba", "Unknown")
        val = float(r.get("value", 0)) if r.get("value") else 0
        if partner not in neighbors:
            neighbors[partner] = {"export_mwh": 0, "import_mwh": 0}
        neighbors[partner]["import_mwh"] += val

    neighbor_list = []
    for partner, flows in sorted(neighbors.items(), key=lambda x: -(x[1]["import_mwh"] + x[1]["export_mwh"])):
        neighbor_list.append({
            "partner": partner,
            "sample_period": "2024-07-10 to 2024-07-16",
            "export_mwh": round(flows["export_mwh"]),
            "import_mwh": round(flows["import_mwh"]),
            "net_mwh": round(flows["import_mwh"] - flows["export_mwh"]),
        })

    output = {
        "source_url": "https://api.eia.gov/v2/electricity/rto/region-data",
        "source_description": "EIA Form 930 — Hourly Electric Grid Monitor. "
                              "PJM total interchange, demand, and generation. "
                              "Annualized from quarterly week-long samples.",
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM Interconnection, LLC",
        "citation": "U.S. Energy Information Administration. 'Hourly Electric Grid Monitor.' "
                    "Form EIA-930. Accessed {}.".format(date.today().strftime("%B %d, %Y")),
        "known_gaps": [
            "[ESTIMATE]: Annual totals extrapolated from 4 sample weeks per year (mid-quarter). "
            "Actual annual totals may differ due to seasonal variation.",
            "Neighbor-level detail sampled from July 2024 only — winter patterns may differ.",
            "2025 data limited to Q1 sample only.",
        ],
        "notes": "Positive interchange = net imports into PJM. "
                 "Interchange ≈ Demand - Net Generation. "
                 "Annualization multiplies average hourly value by 8,760 hours.",
        "units": "MWh",
        "annual_estimates": annual_summary,
        "neighbor_detail": neighbor_list,
    }

    out_path = OUT_DIR / "interchange.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWrote interchange data to {out_path}")
    for yr in annual_summary:
        direction = "net importer" if yr["est_annual_interchange_mwh"] > 0 else "net exporter"
        print(f"  {yr['year']}: {yr['est_annual_interchange_mwh']:,.0f} MWh ({direction}) [ESTIMATE]")


if __name__ == "__main__":
    main()
