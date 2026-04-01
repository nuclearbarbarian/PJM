"""
Ingest DOE National Interest Electric Transmission Corridor (NIETC) boundaries.
Source: Piedmont Environmental Council's ArcGIS data portal (digitized from DOE
preliminary list maps, May 2024).
Outputs: data/processed/nietc_corridors.geojson (Ultan-compliant metadata + GeoJSON)

The NIETC designation identifies geographic areas where new transmission is needed
to address reliability, resilience, or clean energy goals. DOE published a preliminary
list of 10 potential NIETCs on May 8, 2024 (89 FR 39804). The Mid-Atlantic corridor
overlaps PJM's footprint. As of March 2026, DOE has advanced three corridors to Phase 3
(Tribal Energy Access, Southwestern Grid Connector, Lake Erie-Canada) but the
Mid-Atlantic corridor has NOT yet been advanced to Phase 3.

DOE has NOT published official GIS boundaries for the Mid-Atlantic corridor.
The geometry here is PEC's digitization from DOE's preliminary list maps.
An independent digitization by the National Audubon Society (ArcGIS item
49441b164db549759ff9aa2a2804c0e4) covers ~3,100 sq km vs PEC's ~26,000 sq km
for the same corridor — a significant discrepancy indicating boundary uncertainty.
"""

import json
from datetime import date
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "raw"
OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)


def main():
    src = RAW_DIR / "nietc_corridors.geojson"
    if not src.exists():
        print(f"[SKIP] {src} not found")
        return

    with open(src) as f:
        data = json.load(f)

    features = [f for f in data["features"] if f.get("geometry")]
    print(f"NIETC corridors: {len(features)} features")

    # Simplify properties
    clean_features = []
    for feat in features:
        clean_features.append({
            "type": "Feature",
            "geometry": feat["geometry"],
            "properties": {
                "name": "Mid-Atlantic National Interest Electric Transmission Corridor",
                "designation": "DOE Potential NIETC (May 2024 preliminary list)",
                "status": "Preliminary — not yet advanced to Phase 3",
            },
        })

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source_url": "https://services3.arcgis.com/mTaShYKffyWc5uRb/arcgis/rest/services/Proposed_National_Interest_Electric_Transmission_Corridor__NIETC_/FeatureServer/9",
            "source_description": (
                "Mid-Atlantic potential NIETC boundary, digitized by the Piedmont Environmental "
                "Council (PEC) from DOE's preliminary list maps (May 2024). PEC is a regional "
                "environmental advocacy organization in Virginia's Piedmont region. DOE has not "
                "published official GIS boundaries for this corridor."
            ),
            "source_affiliation": (
                "Piedmont Environmental Council (PEC) — regional environmental advocacy "
                "organization. GIS data created by PEC staff (watsun31), not DOE."
            ),
            "cross_check": {
                "audubon_comparison": (
                    "An independent digitization by the National Audubon Society "
                    "(jon.belak@audubon.org, ArcGIS item 49441b164db549759ff9aa2a2804c0e4, "
                    "published June 2024) covers ~3,100 sq km for the Mid-Atlantic corridor. "
                    "PEC's polygon covers ~26,000 sq km — roughly 8x larger. This discrepancy "
                    "indicates significant uncertainty in corridor boundary interpretation."
                ),
                "audubon_service_url": (
                    "https://services1.arcgis.com/lDFzr3JyGEn5Eymu/arcgis/rest/services/"
                    "NIETC_Corridors_Detailed/FeatureServer/1"
                ),
                "federal_register_notice": (
                    "89 FR 39804 (May 10, 2024): Notice of Availability of Preliminary List "
                    "of Potential National Interest Electric Transmission Corridors. Maps in "
                    "notice described as 'rough approximations.' No machine-readable GIS "
                    "boundaries published by DOE for the preliminary list."
                ),
                "phase3_status": (
                    "As of Dec 16, 2024, DOE advanced 3 of 10 corridors to Phase 3: Tribal "
                    "Energy Access, Southwestern Grid Connector, Lake Erie-Canada. The "
                    "Mid-Atlantic corridor was NOT among them."
                ),
                "check_date": "2026-03-30",
            },
            "access_date": date.today().isoformat(),
            "jurisdiction": "U.S. Department of Energy",
            "citation": (
                "U.S. Department of Energy. 'Preliminary List of Potential National Interest "
                "Electric Transmission Corridors.' 89 FR 39804 (May 10, 2024). Boundary "
                "digitized by Piedmont Environmental Council from DOE preliminary maps. "
                f"Accessed {date.today().strftime('%B %d, %Y')}."
            ),
            "known_gaps": [
                "[VERIFY: boundary approximate, no official DOE GIS] Corridor boundary is PEC's "
                "interpretation of DOE preliminary maps. An independent Audubon digitization "
                "produces a polygon ~8x smaller, indicating significant boundary uncertainty.",
                "Mid-Atlantic corridor has not advanced to Phase 3 and may not receive final "
                "designation. Treat as indicative of DOE-identified need area, not a siting boundary.",
                "NIETC designation does not guarantee project approval — it enables FERC backstop "
                "siting authority under FPA Section 216.",
            ],
            "notes": (
                "The Mid-Atlantic NIETC corridor reflects DOE's finding that this region faces "
                "acute transmission capacity needs driven by data center load growth, renewable "
                "energy integration, and grid resilience requirements. The corridor overlaps "
                "PJM's footprint in WV, VA, MD, and PA."
            ),
            "summary": {
                "total_corridors": len(clean_features),
            },
        },
        "features": clean_features,
    }

    out_path = OUT_DIR / "nietc_corridors.geojson"
    with open(out_path, "w") as f:
        json.dump(geojson, f)

    size_kb = out_path.stat().st_size / 1e3
    print(f"Wrote {len(clean_features)} corridors to {out_path} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
