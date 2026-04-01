"""
Ingest planned/proposed PJM transmission routes from ArcGIS Online sources.
Sources: Piedmont Environmental Council's PJM RTEP data portal (public ArcGIS layers).
Outputs: data/processed/planned_transmission.geojson (Ultan-compliant metadata + GeoJSON)

Combines:
  - PJM RTEP Window 3 Selected Routes (Oct 2023) — approved competitive routes
  - PJM 2024 Window 1 Routes — newer competitive proposals
  - JFY 765kV Corridor (primary route) — data center load-driven project

Cross-referenced against PJM's official RTEP project list (projectCostUpgrades.xml,
15,306 projects). See metadata.cross_check for results.
"""

import json
from datetime import date
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "raw"
OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)


def load_and_tag(filepath, source_tag, default_props=None):
    """Load a GeoJSON file and tag each feature with its source."""
    with open(filepath) as f:
        data = json.load(f)

    features = []
    for feat in data.get("features", []):
        if not feat.get("geometry"):
            continue
        props = feat.get("properties", {})
        props["_source"] = source_tag
        if default_props:
            for k, v in default_props.items():
                if k not in props or not props[k]:
                    props[k] = v
        features.append({
            "type": "Feature",
            "geometry": feat["geometry"],
            "properties": props,
        })
    return features


def main():
    all_features = []

    # 1. RTEP Window 3 Selected Routes (2023)
    f = RAW_DIR / "pjm_selected_transmission_routes_2023.geojson"
    if f.exists():
        feats = load_and_tag(f, "RTEP Window 3 (2023)")
        print(f"RTEP Window 3 Selected: {len(feats)} features")
        all_features.extend(feats)

    # 2. PJM 2024 Window 1 Routes
    f = RAW_DIR / "pjm_2024_window1_routes.geojson"
    if f.exists():
        feats = load_and_tag(f, "RTEP Window 1 (2024)")
        print(f"RTEP 2024 Window 1: {len(feats)} features")
        all_features.extend(feats)

    # 3. Proposed New and Upgraded Transmission Lines (broader set, 26 proposals)
    f = RAW_DIR / "proposed_new_upgraded_transmission_lines.geojson"
    if f.exists():
        feats = load_and_tag(f, "PEC Proposed Lines")
        # Normalize property names for consistency
        for feat in feats:
            p = feat["properties"]
            if "Descriptio" in p and "Type" not in p:
                p["Type"] = p["Descriptio"]
        print(f"PEC Proposed Lines: {len(feats)} features")
        # Deduplicate: skip segments already covered by RTEP Window 3
        # (Window 3 has more precise selected routes for the same proposals)
        rtep_proposals = set()
        for feat in all_features:
            prop = feat["properties"].get("Proposal", "")
            if prop:
                rtep_proposals.add(prop.strip())
        new_feats = []
        skipped = 0
        for feat in feats:
            prop = feat["properties"].get("Proposal", "").strip()
            if prop in rtep_proposals:
                skipped += 1
            else:
                new_feats.append(feat)
        print(f"  Deduped: kept {len(new_feats)}, skipped {skipped} (already in RTEP Window 3)")
        all_features.extend(new_feats)

    # 4. JFY 765kV Corridor (primary route only)
    f = RAW_DIR / "jfy_corridor1.geojson"
    if f.exists():
        feats = load_and_tag(f, "JFY 765kV (VL Transmission)", {
            "Proposal": "Joshua Falls-Yeat 765kV",
            "Type": "Greenfield",
        })
        print(f"JFY 765kV: {len(feats)} features")
        all_features.extend(feats)

    print(f"\nTotal planned routes: {len(all_features)}")

    # Categorize
    by_source = {}
    for feat in all_features:
        src = feat["properties"].get("_source", "Unknown")
        by_source[src] = by_source.get(src, 0) + 1

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source_urls": [
                "https://services3.arcgis.com/mTaShYKffyWc5uRb/arcgis/rest/services/Selected_Transmission_Line_Routes_PJM_Oct_31_2023/FeatureServer/21",
                "https://services3.arcgis.com/mTaShYKffyWc5uRb/arcgis/rest/services/PJM_2024_Window_1/FeatureServer/2",
                "https://services3.arcgis.com/mTaShYKffyWc5uRb/arcgis/rest/services/Proposed_New_and_Upgraded_Transmission_Lines/FeatureServer/23",
                "https://services3.arcgis.com/mTaShYKffyWc5uRb/arcgis/rest/services/JFY_CorridorVariations/FeatureServer/0",
            ],
            "source_description": (
                "PJM RTEP competitive transmission routes from Piedmont Environmental Council's "
                "public ArcGIS data portal, plus VL Transmission's JFY 765kV corridor. "
                "These represent proposed and selected routes from PJM's Regional Transmission "
                "Expansion Plan competitive windows."
            ),
            "source_affiliation": (
                "Piedmont Environmental Council (PEC) — regional environmental advocacy "
                "organization in Virginia's Piedmont region. Route geometries are PEC's "
                "digitizations from PJM developer filings, not PJM-published data. PJM does "
                "not publish route geometries publicly (classified as CEII under FERC regulations)."
            ),
            "cross_check": {
                "method": (
                    "Cross-referenced PEC proposal names against PJM's official RTEP project list "
                    "(projectCostUpgrades.xml, 15,306 total projects, 221 in b3800/Window 3 series). "
                    "PEC uses '[TO] [number]' format (e.g., 'Dominion 692'); PJM uses '2022-W3-[number]'."
                ),
                "confirmed_proposals": {
                    "Dominion 692": "2022-W3-692: 28 upgrades, $1,114M, status EP/UC",
                    "Dominion 711": "2022-W3-711: 47 upgrades, $842M, status EP",
                    "Exelon 660": "2022-W3-660: 16 upgrades, $698M, status UC (BGE/PEPCO)",
                    "FE 837": "2022-W3-837: 23 upgrades, $388M, status EP/UC/Cancelled (APS/ME)",
                    "Dominion 967": "2022-W3-967: 13 upgrades, $183M, status EP",
                    "Dominion 516": "2022-W3-516: 13 upgrades, $83M, status EP",
                    "Dominion 74": "2022-W3-74: 1 upgrade, $32M, status EP",
                },
                "unmatched_proposals": (
                    "18 PEC proposals have no matching b3800 RTEP project: these are competing "
                    "proposals that were NOT selected by PJM (e.g., Nextera, PSEG, Transource, "
                    "LS Power entries). They represent real submissions to the competitive window "
                    "but are now superseded by the selected winners."
                ),
                "result": "7 of 25 PEC proposals confirmed as selected PJM RTEP projects ($3,340M total)",
                "pjm_source_url": "https://www.pjm.com/pjmfiles/media/planning/projectConstruction-data/projectCostUpgrades.xml",
                "teac_whitepaper": (
                    "PJM. 'TEAC Board Whitepaper.' December 5, 2023. Contains full b3800 project "
                    "listing with Upgrade IDs, descriptions, costs, and transmission owners."
                ),
                "check_date": "2026-03-30",
            },
            "access_date": date.today().isoformat(),
            "jurisdiction": "PJM Interconnection, LLC",
            "citation": (
                "Piedmont Environmental Council. 'PJM Selected Transmission Line Routes' and "
                "'PJM 2024 Window 1.' ArcGIS Online. VL Transmission. 'JFY Corridor Variations.' "
                f"Accessed {date.today().strftime('%B %d, %Y')}. Cross-referenced against PJM "
                "Interconnection. 'Project Construction Data.' projectCostUpgrades.xml. "
                f"Accessed {date.today().strftime('%B %d, %Y')}."
            ),
            "known_gaps": [
                "[VERIFY: geometry approximate, 7/25 proposals confirmed via PJM RTEP] Route "
                "geometries are PEC digitizations from developer filings, not surveyed centerlines. "
                "PJM does not publish route GIS data (CEII). 7 of 25 PEC proposals match confirmed "
                "PJM RTEP Window 3 selected projects; 18 are competing proposals not selected.",
                "RTEP Window 3 data is from Oct 2023; project statuses current as of March 2026 "
                "(196 in Engineering & Procurement, 21 Under Construction, 3 Cancelled).",
                "Window 3 total: $5.14B across 221 upgrade components, predominantly Dominion "
                "Virginia territory (133 of 221 upgrades) driven by data center load growth.",
            ],
            "notes": (
                "These planned routes complement the existing HIFLD backbone transmission layer. "
                "The JFY 765kV project is driven by Virginia data center load growth and directly "
                "connects to the demand narrative. RTEP competitive windows are PJM's process for "
                "soliciting transmission solutions to identified reliability needs."
            ),
            "summary": {
                "total_routes": len(all_features),
                "by_source": by_source,
            },
        },
        "features": all_features,
    }

    out_path = OUT_DIR / "planned_transmission.geojson"
    with open(out_path, "w") as f:
        json.dump(geojson, f)

    size_kb = out_path.stat().st_size / 1e3
    print(f"\nWrote {len(all_features)} routes to {out_path} ({size_kb:.1f} KB)")
    print(f"By source: {by_source}")


if __name__ == "__main__":
    main()
