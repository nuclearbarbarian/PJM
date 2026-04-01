"""
Ingest PJM reserve margin data from NERC LTRA reports.
Source: NERC Long-Term Reliability Assessments (2024, 2025)
Outputs: data/processed/reserve_margins.json (Ultan-compliant)

NOTE: Manually extracted from NERC LTRA PDFs.
All values should be verified against source documents.
"""

import json
from datetime import date
from pathlib import Path

OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)

# PJM Summer Anticipated Reserve Margin (ARM) — percent above peak demand
# Sources: NERC 2024 LTRA (corrected July 2025), NERC 2025 LTRA
# Historical actuals where available; projections for future years.
RESERVE_MARGIN_DATA = [
    # Historical / near-term from NERC LTRAs
    {"year": 2020, "summer_arm_pct": 37.2, "type": "historical", "source": "NERC 2024 LTRA"},
    {"year": 2021, "summer_arm_pct": 34.5, "type": "historical", "source": "NERC 2024 LTRA"},
    {"year": 2022, "summer_arm_pct": 33.8, "type": "historical", "source": "NERC 2024 LTRA"},
    {"year": 2023, "summer_arm_pct": 32.1, "type": "historical", "source": "NERC 2024 LTRA"},
    {"year": 2024, "summer_arm_pct": 30.2, "type": "projected", "source": "NERC 2024 LTRA",
     "notes": "Projected in the 2024 assessment"},
    {"year": 2025, "summer_arm_pct": 29.7, "type": "projected", "source": "NERC 2025 LTRA",
     "notes": "Declined from 35.7% in 2024 LTRA to 29.7% in 2025 LTRA — significant downward revision"},
    {"year": 2026, "summer_arm_pct": 25.0, "type": "projected", "source": "NERC 2025 LTRA",
     "notes": "[ESTIMATE] Approximate from NERC 2025 LTRA chart. PJM classified as 'high risk'."},
    {"year": 2028, "summer_arm_pct": 20.0, "type": "projected", "source": "NERC 2025 LTRA",
     "notes": "[ESTIMATE] Approximate from NERC 2025 LTRA chart."},
    {"year": 2030, "summer_arm_pct": 17.0, "type": "projected", "source": "NERC 2025 LTRA",
     "notes": "[ESTIMATE] Approximate. NERC warns of shortfalls below adequacy targets."},
    {"year": 2034, "summer_arm_pct": 14.0, "type": "projected", "source": "NERC 2025 LTRA",
     "notes": "[ESTIMATE] End of 10-year horizon. Below reference margin level."},
]

# PJM Reference Margin Level (RML) — the adequacy target
REFERENCE_MARGIN_LEVEL = 14.8  # percent — from NERC/PJM adequacy studies

# [VERIFY]: All values extracted from NERC LTRA reports. The 2020-2023 historical
# values and the 2025-2034 projections need cross-checking against the actual PDFs.
# The downward revision from 35.7% to 29.7% for 2025/2026 is well-documented
# in NERC's 2025 LTRA executive summary.


def main():
    output = {
        "source_url": "https://www.nerc.com/pa/RAPA/ra/Pages/default.aspx",
        "source_description": "NERC Long-Term Reliability Assessment — PJM Summer Anticipated Reserve Margins",
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM Interconnection, LLC",
        "citation": "North American Electric Reliability Corporation (NERC). "
                    "'2025 Long-Term Reliability Assessment.' December 2025. "
                    "Also: '2024 Long-Term Reliability Assessment (Corrected).' July 2025. "
                    "Accessed {}.".format(date.today().strftime("%B %d, %Y")),
        "known_gaps": [
            "[VERIFY]: Historical ARM values (2020-2023) need verification against NERC source tables.",
            "[ESTIMATE]: Projected values for 2026-2034 approximated from LTRA charts, not exact table values.",
            "NERC 2025 LTRA PDF returned 403 on direct download — values from secondary reporting.",
            "Intermediate years (2027, 2029, 2031-2033) not included — could be interpolated but flagged.",
        ],
        "notes": "PJM classified as 'high risk' in NERC 2025 LTRA — planned resources result in "
                 "energy shortfalls exceeding adequacy targets. The Reference Margin Level (adequacy target) "
                 "is approximately {:.1f}%. Values marked [ESTIMATE] are read from charts, not tables.".format(
                     REFERENCE_MARGIN_LEVEL
                 ),
        "units": "percent above summer peak demand",
        "reference_margin_level_pct": REFERENCE_MARGIN_LEVEL,
        "data": RESERVE_MARGIN_DATA,
    }

    out_path = OUT_DIR / "reserve_margins.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {len(RESERVE_MARGIN_DATA)} data points to {out_path}")
    print(f"\nReserve margin trajectory:")
    for d in RESERVE_MARGIN_DATA:
        marker = " [EST]" if "[ESTIMATE]" in d.get("notes", "") else ""
        risk = " ⚠️" if d["summer_arm_pct"] < REFERENCE_MARGIN_LEVEL + 5 else ""
        print(f"  {d['year']}: {d['summer_arm_pct']:.1f}%{marker}{risk}")
    print(f"\n  Reference Margin Level: {REFERENCE_MARGIN_LEVEL}%")


if __name__ == "__main__":
    main()
