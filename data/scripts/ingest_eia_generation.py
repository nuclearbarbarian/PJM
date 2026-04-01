"""
Ingest EIA electricity generation data for PJM states.
Source: EIA API v2 — electricity/facility-fuel (annual, state-level aggregates)
Outputs: data/processed/generation.json (Ultan-compliant)

Pulls net generation by energy source for PJM states, 2020–2024.
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

PJM_STATES = ["PA", "NJ", "DE", "MD", "VA", "WV", "OH", "IN", "IL", "MI", "KY", "NC", "TN", "DC"]

# Map EIA fuel codes to display categories
FUEL_MAP = {
    "NG": "Natural Gas", "COL": "Coal", "NUC": "Nuclear", "SUN": "Solar",
    "WND": "Wind", "WAT": "Hydro", "PET": "Petroleum", "OTH": "Other",
    "GEO": "Geothermal", "BIO": "Biomass", "WAS": "Waste",
    "ALL": "All Sources", "TSN": "All Sources",
    # More granular codes
    "BIT": "Coal", "SUB": "Coal", "LIG": "Coal", "RC": "Coal",
    "DFO": "Petroleum", "RFO": "Petroleum", "KER": "Petroleum",
    "WDS": "Biomass", "MLG": "Biomass", "OBG": "Biomass",
    "LFG": "Biomass", "OBS": "Biomass", "BLQ": "Biomass",
    "AB": "Biomass", "MSW": "Waste", "OG": "Other Gas",
    "BFG": "Other Gas", "SGC": "Coal", "PC": "Petroleum",
    "WC": "Coal", "TDF": "Other", "MWH": "Storage",
    "PUR": "Other", "WH": "Other", "SLW": "Other",
}


def fetch_eia_data(endpoint, params):
    """Fetch data from EIA API v2."""
    param_str = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}/{endpoint}?api_key={API_KEY}&{param_str}"
    req = urllib.request.Request(url, headers={"User-Agent": "PJM-Dashboard/1.0"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_state_generation():
    """Fetch annual generation by state and energy source, 2020-2024."""
    all_data = []

    for state in PJM_STATES:
        print(f"  Fetching {state}...")
        offset = 0
        while True:
            params = {
                "frequency": "annual",
                "data[0]": "generation",
                "facets[state][]": state,
                "facets[primeMover][]": "ALL",
                "start": "2020",
                "end": "2024",
                "length": "5000",
                "offset": str(offset),
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
            }

            resp = fetch_eia_data("electricity/facility-fuel/data", params)
            data = resp.get("response", {}).get("data", [])
            total = int(resp.get("response", {}).get("total", 0))

            if not data:
                break

            all_data.extend(data)
            offset += len(data)
            if offset >= total:
                break

            time.sleep(0.5)  # Be polite to the API

        time.sleep(0.3)

    return all_data


def aggregate_generation(raw_data):
    """Aggregate plant-level generation into state/year/fuel summaries."""
    # Aggregate: {(year, fuel_category): total_mwh}
    by_year_fuel = {}
    by_year_state_fuel = {}

    for record in raw_data:
        year = record.get("period")
        fuel_code = record.get("fuel2002", "")
        state = record.get("state", "")
        gen = record.get("generation")

        # Skip the "ALL" fuel total rows to avoid double-counting
        if fuel_code == "ALL":
            continue

        if gen is None or year is None:
            continue

        try:
            gen = float(gen)
        except (ValueError, TypeError):
            continue

        fuel_cat = FUEL_MAP.get(fuel_code, "Other")

        key = (year, fuel_cat)
        by_year_fuel[key] = by_year_fuel.get(key, 0) + gen

        key2 = (year, state, fuel_cat)
        by_year_state_fuel[key2] = by_year_state_fuel.get(key2, 0) + gen

    # Reshape into year-keyed summary
    years = sorted(set(k[0] for k in by_year_fuel.keys()))
    fuel_cats = sorted(set(k[1] for k in by_year_fuel.keys()))

    summary_by_year = {}
    for year in years:
        year_data = {}
        for fuel in fuel_cats:
            val = by_year_fuel.get((year, fuel), 0)
            if val != 0:
                year_data[fuel] = round(val)
        summary_by_year[year] = year_data

    return summary_by_year, by_year_state_fuel


def main():
    print("Fetching EIA generation data for PJM states, 2020-2024...")
    raw_data = fetch_state_generation()
    print(f"  Total records fetched: {len(raw_data)}")

    summary_by_year, by_year_state_fuel = aggregate_generation(raw_data)

    # State-level detail
    state_detail = {}
    for (year, state, fuel), gen in by_year_state_fuel.items():
        if state not in state_detail:
            state_detail[state] = {}
        if year not in state_detail[state]:
            state_detail[state][year] = {}
        state_detail[state][year][fuel] = round(gen)

    output = {
        "source_url": "https://api.eia.gov/v2/electricity/facility-fuel",
        "source_description": "EIA Form 923 — Net generation by energy source, annual, "
                              "for PJM states (PA, NJ, DE, MD, VA, WV, OH, IN, IL, MI, KY, NC, TN, DC)",
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM-adjacent states (includes non-PJM portions of partial states)",
        "citation": "U.S. Energy Information Administration. 'Electric Power Operations for "
                    "Individual Power Plants.' Form EIA-923. Accessed {}.".format(
                        date.today().strftime("%B %d, %Y")
                    ),
        "known_gaps": [
            "Data covers full states, not just PJM portions. IL, MI, IN, KY, NC, TN include "
            "non-PJM generation. This overstates PJM totals for partial states.",
            "2024 data may be preliminary depending on EIA release schedule.",
            "Plant-level fuel codes aggregated to categories — see FUEL_MAP in source script.",
        ],
        "notes": "Generation in MWh. Negative values indicate net consumption (e.g., pumped storage). "
                 "State-level aggregation is an approximation of PJM footprint. "
                 "For precise PJM-only totals, spatial clipping at the plant level would be needed.",
        "units": "MWh",
        "summary_by_year": summary_by_year,
        "state_detail": state_detail,
    }

    out_path = OUT_DIR / "generation.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nWrote generation data to {out_path}")
    for year, fuels in sorted(summary_by_year.items()):
        total = sum(fuels.values())
        print(f"  {year}: {total:,.0f} MWh total")
        for fuel, gen in sorted(fuels.items(), key=lambda x: -x[1])[:5]:
            print(f"    {fuel}: {gen:,.0f} MWh")


if __name__ == "__main__":
    main()
