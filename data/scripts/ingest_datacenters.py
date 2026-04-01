"""
DEPRECATED — Replaced by ingest_datacenters_im3.py (March 2026).
This script is retained for rollback only. Do not run unless reverting
the IM3 upgrade. Running this will overwrite datacenters.json with the
older 576-count raw OSM dataset.

Original purpose:
Ingest data center locations from OpenStreetMap (via Overpass API).
Source: OSM tags telecom=data_center, building=data_center, industrial=data_centre
Clips to PJM footprint using HIFLD boundary polygon.
Outputs: data/processed/datacenters.json (Ultan-compliant)
"""

import json
from datetime import date
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "raw"
OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)


def point_in_polygon(px, py, polygon_coords):
    """Ray-casting algorithm for point-in-polygon test."""
    n = len(polygon_coords)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon_coords[i]
        xj, yj = polygon_coords[j]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_multipolygon(px, py, multipolygon):
    """Test if point is inside a GeoJSON MultiPolygon geometry."""
    for polygon in multipolygon:
        if point_in_polygon(px, py, polygon[0]):
            in_hole = False
            for hole in polygon[1:]:
                if point_in_polygon(px, py, hole):
                    in_hole = True
                    break
            if not in_hole:
                return True
    return False


def filter_boundary_exclaves(coords):
    """Remove small IMPA municipal utility exclaves in Indiana."""
    filtered = []
    for poly in coords:
        ring = poly[0]
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        area = (max(lons) - min(lons)) * (max(lats) - min(lats))
        cx = (min(lons) + max(lons)) / 2
        cy = (min(lats) + max(lats)) / 2
        if area >= 1 or not (cx < -86.1 and 38 < cy < 41):
            filtered.append(poly)
    print(f"  Boundary filter: {len(coords)} → {len(filtered)} polygons")
    return filtered


def main():
    # Load PJM boundary
    print("Loading PJM boundary...")
    with open(RAW_DIR / "hifld_planning_areas.geojson") as f:
        boundary = json.load(f)
    pjm_coords = filter_boundary_exclaves(boundary["features"][0]["geometry"]["coordinates"])

    # Load data centers
    print("Loading data centers...")
    with open(RAW_DIR / "data_centers_osm.geojson") as f:
        dc_geojson = json.load(f)

    print(f"Total US data centers: {len(dc_geojson['features'])}")

    # Clip to PJM
    clipped = []
    for feat in dc_geojson["features"]:
        lon, lat = feat["geometry"]["coordinates"]
        if point_in_multipolygon(lon, lat, pjm_coords):
            props = feat["properties"]
            clipped.append({
                "name": props.get("name") or "Unnamed",
                "operator": props.get("operator") or "",
                "osm_id": props.get("osm_id"),
                "osm_type": props.get("osm_type"),
                "city": props.get("addr_city") or "",
                "state": props.get("addr_state") or "",
                "lat": lat,
                "lon": lon,
            })

    print(f"Inside PJM: {len(clipped)}")

    # Summary
    operators = {}
    for dc in clipped:
        op = dc["operator"] or "Unknown"
        operators[op] = operators.get(op, 0) + 1

    states = {}
    for dc in clipped:
        # Try to determine state from coordinates if not tagged
        st = dc["state"] or "Untagged"
        states[st] = states.get(st, 0) + 1

    output = {
        "source_url": "https://overpass-api.de/api/interpreter",
        "source_description": "OpenStreetMap data centers (tags: telecom=data_center, "
                              "building=data_center, industrial=data_centre) via Overpass API, "
                              "spatially clipped to PJM footprint.",
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM Interconnection, LLC",
        "citation": "OpenStreetMap contributors. Data centers extracted via Overpass API. "
                    "Licensed under ODbL. Accessed {}.".format(
                        date.today().strftime("%B %d, %Y")
                    ),
        "known_gaps": [
            "[GAP: ~80% coverage for pre-2024 major facilities] OSM coverage is crowd-sourced. "
            "A March 2026 cross-check against 28 known hyperscale campuses found: 16 of 20 major "
            "operators have OSM presence. Key gaps: CoreWeave (0 facilities — Kenilworth NJ and "
            "Lancaster PA campuses totaling $7.8B investment absent), Yondr (0 tagged by name), "
            "STACK Infrastructure (2 facilities vs 360+ MW announced). Newer AI-era buildouts "
            "(2024-2025 announcements) are structurally absent.",
            "No power demand (MW) data available from OSM. Facility size not included.",
            "Some facilities may be misclassified (e.g., colocation vs. enterprise vs. hyperscale).",
            "addr:state tag is missing for most features — state attribution would require "
            "reverse geocoding or Census boundary join.",
        ],
        "completeness_audit": {
            "date": "2026-03-30",
            "method": (
                "Compiled list of 28 major hyperscale data center campuses in PJM footprint "
                "from public reporting (Data Center Frontier, DCD, Dgtl Infra, Dominion filings, "
                "JLARC Dec 2024 study). Cross-checked operator presence in OSM data."
            ),
            "operator_coverage": {
                "present_in_osm": [
                    "Amazon Web Services (160 facilities)",
                    "Digital Realty (37)", "Equinix (28)", "Google (19)",
                    "Meta (15)", "CyrusOne (12)", "NTT (12)", "Microsoft (8)",
                    "Vantage (7)", "CloudHQ (6)", "Aligned (6)",
                    "PowerHouse (4)", "STACK (2)", "QTS (1 tagged by name, 31 by operator)",
                ],
                "absent_from_osm": [
                    "CoreWeave (0 — $6B Lancaster PA campus, $1.8B Kenilworth NJ campus)",
                    "Yondr (0 tagged by name — 96 MW Loudoun campus)",
                ],
            },
            "geographic_coverage": {
                "northern_virginia": "283 facilities (strong coverage, Ashburn/Sterling corridor well-mapped)",
                "new_albany_ohio": "48 facilities (good coverage, Meta/Google/AWS/QTS present)",
                "new_jersey": "20 facilities (moderate — CoreWeave Kenilworth absent)",
                "pennsylvania": "6 facilities (weak — CoreWeave Lancaster absent)",
            },
            "estimate": (
                "~80% coverage for pre-2024 major facilities by operator count. "
                "Coverage drops significantly for facilities announced/under construction "
                "2024-2025. OSM structurally lags construction by 12-24 months."
            ),
        },
        "notes": "Same underlying data source as the IM3 Open Source Data Center Atlas (PNNL), "
                 "but without IM3's post-processing (area calculation, FIPS attribution). "
                 "Treat as indicative of data center clustering patterns, not a complete census. "
                 "Pending IM3 account approval — once available, IM3 atlas (peer-reviewed, DOI: "
                 "10.5281/zenodo.10886228) should be used as base layer to upgrade from crowd-sourced "
                 "to peer-reviewed per Ultan source hierarchy.",
        "summary": {
            "total_in_pjm": len(clipped),
            "top_operators": dict(sorted(operators.items(), key=lambda x: -x[1])[:15]),
            "by_state_tag": dict(sorted(states.items(), key=lambda x: -x[1])),
        },
        "data": clipped,
    }

    out_path = OUT_DIR / "datacenters.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWrote {len(clipped)} data centers to {out_path}")
    print(f"Top operators: {dict(sorted(operators.items(), key=lambda x: -x[1])[:10])}")


if __name__ == "__main__":
    main()
