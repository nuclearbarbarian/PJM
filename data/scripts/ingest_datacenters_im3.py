"""
Ingest data center locations from IM3 Open Source Data Center Atlas (PNNL).
Source: DOI 10.57931/2550666, hosted at data.msdlive.org
Peer-reviewed post-processing of OpenStreetMap data by Pacific Northwest
National Laboratory (IM3 group). Adds building footprint area (sqft),
county name attribution, and campus aggregation not available in raw OSM.

Uses building layer (1,040 facilities with sqft) as primary source,
supplemented by point layer (93 facilities) for coverage.
Campus layer skipped to avoid double-counting aggregated buildings.

Clips to PJM footprint using HIFLD boundary polygon.
Outputs: data/processed/datacenters.json (Ultan-compliant)

Replaces: ingest_datacenters.py (raw OSM via Overpass API)
"""

import json
import sqlite3
from datetime import date
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "raw"
OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)

GPKG_PATH = RAW_DIR / "im3_open_source_data_center_atlas.gpkg"


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


def load_im3_data():
    """Read building and point layers from GeoPackage via sqlite3."""
    conn = sqlite3.connect(str(GPKG_PATH))
    try:
        cur = conn.cursor()

        facilities = []

        # Building layer — primary source (has sqft)
        cur.execute("""
            SELECT name, operator, state_abb, county, sqft, lon, lat, id
            FROM building
        """)
        for row in cur.fetchall():
            name, operator, state, county, sqft, lon, lat, im3_id = row
            facilities.append({
                "name": name or "Unnamed",
                "operator": operator or "",
                "state": state or "",
                "county": county or "",
                "sqft": round(sqft) if sqft is not None else None,
                "lat": lat,
                "lon": lon,
                "im3_id": im3_id or "",
                "im3_layer": "building",
            })

        building_count = len(facilities)
        print(f"  Building layer: {building_count} facilities")

        # Point layer — supplement (no sqft, but captures some extras)
        cur.execute("""
            SELECT name, operator, state_abb, county, sqft, lon, lat, id
            FROM point
        """)
        point_candidates = []
        for row in cur.fetchall():
            name, operator, state, county, sqft, lon, lat, im3_id = row
            point_candidates.append({
                "name": name or "Unnamed",
                "operator": operator or "",
                "state": state or "",
                "county": county or "",
                "sqft": round(sqft) if sqft is not None else None,
                "lat": lat,
                "lon": lon,
                "im3_id": im3_id or "",
                "im3_layer": "point",
            })

        # Deduplicate: skip point entries within ~0.002° (~200m) of any existing facility
        added_points = 0
        for pt in point_candidates:
            too_close = False
            for b in facilities:
                if abs(pt["lat"] - b["lat"]) < 0.002 and abs(pt["lon"] - b["lon"]) < 0.002:
                    too_close = True
                    break
            if not too_close:
                facilities.append(pt)
                added_points += 1

        print(f"  Point layer: {len(point_candidates)} candidates, {added_points} added (rest deduplicated)")

    finally:
        conn.close()
    return facilities


def main():
    # Load PJM boundary
    print("Loading PJM boundary...")
    with open(RAW_DIR / "hifld_planning_areas.geojson") as f:
        boundary = json.load(f)
    pjm_coords = filter_boundary_exclaves(boundary["features"][0]["geometry"]["coordinates"])

    # Load IM3 data
    print("Loading IM3 data center atlas...")
    facilities = load_im3_data()
    print(f"Total US facilities: {len(facilities)}")

    # Clip to PJM
    print("Clipping to PJM footprint...")
    clipped = []
    for dc in facilities:
        if point_in_multipolygon(dc["lon"], dc["lat"], pjm_coords):
            clipped.append(dc)

    print(f"Inside PJM: {len(clipped)}")

    # Summary stats
    operators = {}
    for dc in clipped:
        op = dc["operator"] or "Unknown"
        operators[op] = operators.get(op, 0) + 1

    states = {}
    for dc in clipped:
        st = dc["state"] or "Untagged"
        states[st] = states.get(st, 0) + 1

    counties = {}
    for dc in clipped:
        if dc["county"]:
            counties[dc["county"]] = counties.get(dc["county"], 0) + 1

    sqft_count = sum(1 for dc in clipped if dc["sqft"])
    total_sqft = sum(dc["sqft"] for dc in clipped if dc["sqft"])
    by_layer = {}
    for dc in clipped:
        by_layer[dc["im3_layer"]] = by_layer.get(dc["im3_layer"], 0) + 1

    output = {
        "source_url": "https://data.msdlive.org/records/65g71-a4731",
        "source_description": (
            "IM3 Open Source Data Center Atlas (PNNL). Peer-reviewed post-processing "
            "of OpenStreetMap data by Pacific Northwest National Laboratory. Adds building "
            "footprint area (sqft), county name attribution, and facility classification "
            "not available in raw OSM. Building layer (primary) supplemented by point layer."
        ),
        "source_affiliation": (
            "Pacific Northwest National Laboratory (PNNL), IM3 project. "
            "US Department of Energy national laboratory. Peer-reviewed dataset."
        ),
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM Interconnection, LLC",
        "citation": (
            "Kramer, Anna; McGraw, Julie; Vernon, Chris. "
            "\"IM3 Open Source Data Center Atlas.\" "
            "MultiSector Dynamics - Living, Intuitive, Value-adding, Environment (MSD-LIVE). "
            "DOI: 10.57931/2550666. Accessed {}.".format(
                date.today().strftime("%B %d, %Y")
            )
        ),
        "known_gaps": [
            "[GAP: ~80% coverage for pre-2024 major facilities] Same underlying OSM data "
            "as previous version, but with PNNL post-processing (sqft, county names). "
            "Key gaps persist: CoreWeave (0 facilities — Kenilworth NJ and Lancaster PA "
            "campuses totaling $7.8B investment absent), Yondr (0 tagged by name), "
            "STACK Infrastructure (2 facilities vs 360+ MW announced). "
            "Newer AI-era buildouts (2024-2025) structurally absent.",
            "No power demand (MW) data in this dataset. Sqft available for building-layer "
            "facilities only ({} of {} have sqft).".format(sqft_count, len(clipped)),
            "Some facilities may be misclassified (colocation vs. enterprise vs. hyperscale).",
        ],
        "completeness_audit": {
            "date": "2026-03-30",
            "method": (
                "Compiled list of 28 major hyperscale data center campuses in PJM footprint "
                "from public reporting (Data Center Frontier, DCD, Dgtl Infra, Dominion filings, "
                "JLARC Dec 2024 study). Cross-checked operator presence in data."
            ),
            "operator_coverage": {
                "present": [
                    "Amazon Web Services", "Digital Realty", "Equinix", "Google",
                    "Meta", "CyrusOne", "NTT", "Microsoft", "Vantage", "CloudHQ",
                    "Aligned", "PowerHouse", "STACK (2 facilities)", "QTS",
                ],
                "absent": [
                    "CoreWeave (0 — $6B Lancaster PA campus, $1.8B Kenilworth NJ campus)",
                    "Yondr (0 tagged by name — 96 MW Loudoun campus)",
                ],
            },
            "geographic_coverage": dict(sorted(states.items(), key=lambda x: -x[1])),
            "estimate": (
                "~80% coverage for pre-2024 major facilities by operator count. "
                "Upgrade from OSM to IM3 adds: sqft for {} facilities, county name "
                "attribution for all, peer-reviewed DOI provenance.".format(sqft_count)
            ),
        },
        "upgrade_notes": (
            "Replaced raw OSM/Overpass source with IM3 atlas (DOI: 10.57931/2550666). "
            "Same underlying data but peer-reviewed provenance (moves from crowd-sourced "
            "to peer-reviewed post-processing in Ultan source hierarchy). "
            "Added fields: sqft, county, im3_id, im3_layer. "
            "Removed fields: city (was addr_city from OSM, mostly empty), osm_id, osm_type. "
            "Previous OSM script retained as ingest_datacenters.py."
        ),
        "summary": {
            "total_in_pjm": len(clipped),
            "by_layer": by_layer,
            "sqft_coverage": f"{sqft_count}/{len(clipped)} facilities have sqft",
            "total_sqft": round(total_sqft) if total_sqft else 0,
            "top_operators": dict(sorted(operators.items(), key=lambda x: -x[1])[:15]),
            "by_state": dict(sorted(states.items(), key=lambda x: -x[1])),
            "top_counties": dict(sorted(counties.items(), key=lambda x: -x[1])[:15]),
        },
        "data": clipped,
    }

    out_path = OUT_DIR / "datacenters.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWrote {len(clipped)} data centers to {out_path}")
    print(f"By layer: {by_layer}")
    print(f"Sqft coverage: {sqft_count}/{len(clipped)}")
    print(f"Top operators: {dict(sorted(operators.items(), key=lambda x: -x[1])[:10])}")
    print(f"Top states: {dict(sorted(states.items(), key=lambda x: -x[1])[:10])}")
    print(f"Top counties: {dict(sorted(counties.items(), key=lambda x: -x[1])[:10])}")


if __name__ == "__main__":
    main()
