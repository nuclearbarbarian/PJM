"""
Ingest and spatially clip power plants to PJM footprint.
Source: HIFLD Power Plants (ArcGIS REST API, from EIA-860)
Clips: Plants filtered by state, then spatially clipped to PJM boundary polygon.
Outputs: data/processed/plants.json (Ultan-compliant)
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
        # polygon is a list of rings; first ring is exterior
        if point_in_polygon(px, py, polygon[0]):
            # Check if inside any holes
            in_hole = False
            for hole in polygon[1:]:
                if point_in_polygon(px, py, hole):
                    in_hole = True
                    break
            if not in_hole:
                return True
    return False


def filter_boundary_exclaves(coords):
    """Remove small IMPA municipal utility exclaves in Indiana that are
    geographically inside MISO territory but attributed to PJM by HIFLD.
    At Indiana latitudes (38-41°N), the main PJM body's western edge is ~-86.02°.
    Small polygons centered west of -86.1° in this band are exclaves."""
    filtered = []
    for poly in coords:
        ring = poly[0]
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        lon_range = max(lons) - min(lons)
        lat_range = max(lats) - min(lats)
        area = lon_range * lat_range
        cx = (min(lons) + max(lons)) / 2
        cy = (min(lats) + max(lats)) / 2
        # Keep large polygons (main body, ComEd/IL block)
        if area >= 1:
            filtered.append(poly)
        # Drop small polygons in Indiana west of PJM's main body
        elif cx < -86.1 and 38 < cy < 41:
            continue
        else:
            filtered.append(poly)
    print(f"  Boundary filter: {len(coords)} → {len(filtered)} polygons "
          f"(removed {len(coords) - len(filtered)} Indiana exclaves)")
    return filtered


def main():
    # Load PJM boundary
    print("Loading PJM boundary...")
    with open(RAW_DIR / "hifld_planning_areas.geojson") as f:
        boundary = json.load(f)

    pjm_geom = boundary["features"][0]["geometry"]
    assert pjm_geom["type"] == "MultiPolygon"
    pjm_coords = filter_boundary_exclaves(pjm_geom["coordinates"])

    # Load power plants
    print("Loading power plants...")
    with open(RAW_DIR / "hifld_power_plants_pjm_states.geojson") as f:
        plants_geojson = json.load(f)

    # Spatial clip
    print(f"Clipping {len(plants_geojson['features'])} plants to PJM footprint...")
    clipped = []
    skipped_no_coords = 0

    for feat in plants_geojson["features"]:
        props = feat.get("properties", {})
        geom = feat.get("geometry")

        if not geom or geom["type"] != "Point":
            skipped_no_coords += 1
            continue

        lon, lat = geom["coordinates"]

        if point_in_multipolygon(lon, lat, pjm_coords):
            # Normalize fuel types for cleaner categorization
            fuel = props.get("PRIM_FUEL", "Unknown")
            fuel_category = categorize_fuel(fuel)

            def clean_val(v):
                """Treat -999999 sentinel values as null."""
                if v is None or v == -999999 or v == "-999999":
                    return None
                return v

            # HIFLD/EIA operator names lag corporate restructurings.
            # Remap stale names to current operators with dates and sources.
            OPERATOR_REMAPS = {
                # Exelon spun off all competitive gen to Constellation Energy,
                # Feb 2, 2022. Source: exeloncorp.com/newsroom
                "EXELON NUCLEAR": "CONSTELLATION ENERGY",
                "EXELON POWER": "CONSTELLATION ENERGY",
                # FirstEnergy Gen → Energy Harbor (Feb 2020) → acquired by
                # Vistra Corp (Mar 1, 2024). Source: investor.vistracorp.com
                "FIRSTENERGY GENERATION CORP": "VISTRA CORP",
                # Edison Mission Energy sold to NRG Energy, closed Mar 2014.
                # Source: powermag.com
                "MIDWEST GENERATIONS EME LLC": "NRG ENERGY",
                # PSEG sold fossil fleet to ArcLight/Parkway Generation,
                # closed Feb 18, 2022 (NJ/MD assets). Source: nj.pseg.com
                "PSEG FOSSIL LLC": "PARKWAY GENERATION",
            }
            operator = props.get("OPERATOR") or ""
            operator = OPERATOR_REMAPS.get(operator, operator)

            plant = {
                "name": props.get("NAME"),
                "state": props.get("STATE"),
                "operator": operator,
                "fuel_raw": fuel,
                "fuel_category": fuel_category,
                "status": props.get("STATUS"),
                "nameplate_mw": clean_val(props.get("OPER_CAP")),
                "summer_cap_mw": clean_val(props.get("SUMMER_CAP")),
                "winter_cap_mw": clean_val(props.get("WINTER_CAP")),
                "net_gen_mwh": clean_val(props.get("NET_GEN")),
                "capacity_factor": clean_val(props.get("CAP_FACTOR")),
                "gen_units": props.get("GEN_UNITS"),
                "lat": lat,
                "lon": lon,
            }
            clipped.append(plant)

    print(f"  Clipped: {len(clipped)} plants inside PJM")
    print(f"  Excluded: {len(plants_geojson['features']) - len(clipped) - skipped_no_coords} outside PJM")
    print(f"  Skipped (no coords): {skipped_no_coords}")

    # Summary
    fuel_breakdown = {}
    total_mw = 0
    for p in clipped:
        cat = p["fuel_category"]
        mw = p.get("nameplate_mw") or 0
        fuel_breakdown[cat] = fuel_breakdown.get(cat, 0) + 1
        total_mw += mw

    fuel_mw = {}
    for p in clipped:
        cat = p["fuel_category"]
        mw = p.get("nameplate_mw") or 0
        fuel_mw[cat] = fuel_mw.get(cat, 0) + mw

    output = {
        "source_url": "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Power_Plants/FeatureServer/0",
        "source_description": "HIFLD Power Plants (derived from EIA-860), spatially clipped to PJM footprint",
        "access_date": date.today().isoformat(),
        "jurisdiction": "PJM Interconnection, LLC",
        "citation": "Homeland Infrastructure Foundation-Level Data (HIFLD). 'Power Plants.' "
                    "U.S. Department of Homeland Security, CISA. Accessed {}.".format(
                        date.today().strftime("%B %d, %Y")
                    ),
        "known_gaps": [
            "HIFLD data vintage may lag EIA-860 by 6-12 months.",
            "Spatial clipping uses PJM Electric Planning Area boundary — some edge cases may be misclassified.",
            "Plants in partial-PJM states (IL, MI, IN, KY, NC, TN) included only if coordinates fall within PJM polygon.",
            "Small plants (<1 MW) may be missing from EIA-860 source data.",
        ],
        "notes": "Plants filtered from 14 PJM-adjacent states, then spatially clipped using "
                 "HIFLD Electric Planning Areas PJM polygon via ray-casting algorithm.",
        "summary": {
            "total_plants": len(clipped),
            "total_nameplate_mw": round(total_mw, 1),
            "plants_by_fuel": dict(sorted(fuel_breakdown.items(), key=lambda x: -x[1])),
            "mw_by_fuel": {k: round(v, 1) for k, v in sorted(fuel_mw.items(), key=lambda x: -x[1])},
        },
        "data": clipped,
    }

    out_path = OUT_DIR / "plants.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nWrote {len(clipped)} plants to {out_path}")
    print(f"Total nameplate: {round(total_mw, 1)} MW")
    print(f"By fuel: {fuel_breakdown}")
    print(f"MW by fuel: {fuel_mw}")


def categorize_fuel(fuel_code):
    """Map HIFLD PRIM_FUEL codes to display categories."""
    mapping = {
        "NG": "Natural Gas", "BIT": "Coal", "SUB": "Coal", "LIG": "Coal",
        "RC": "Coal", "WC": "Coal", "SGC": "Coal",
        "NUC": "Nuclear",
        "SUN": "Solar",
        "WND": "Wind",
        "WAT": "Hydro",
        "DFO": "Oil", "RFO": "Oil", "KER": "Oil", "PC": "Oil",
        "WDS": "Biomass", "OBS": "Biomass", "BLQ": "Biomass", "AB": "Biomass",
        "MSW": "Biomass", "OBG": "Biomass", "LFG": "Biomass", "WC": "Coal",
        "MWH": "Storage",
        "GEO": "Geothermal",
        "PUR": "Purchased Steam",
        "OG": "Other Gas",
        "BFG": "Other Gas",
        "TDF": "Other",
        "OTH": "Other",
        "SLW": "Other",
        "WH": "Other",
        "NOT AVAILABLE": "Unknown",
    }
    return mapping.get(fuel_code, "Other")


if __name__ == "__main__":
    main()
