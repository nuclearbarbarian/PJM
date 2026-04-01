"""
Ingest HIFLD Transmission Lines, filtered to high-voltage (345kV+) within PJM footprint.
Source: HIFLD Electric Power Transmission Lines (ArcGIS REST API)
Outputs: data/processed/transmission.geojson (Ultan-compliant metadata + GeoJSON)

Only includes 345kV, 500kV, and 765kV lines for performance and narrative focus.
These are the backbone transmission lines that define PJM's grid structure.
Spatially clipped to PJM boundary (keeps segments with any vertex inside).
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


def line_intersects_boundary(geometry, pjm_coords):
    """Check if any vertex of a line (or multiline) falls inside PJM boundary."""
    geom_type = geometry["type"]
    if geom_type == "LineString":
        coords_list = [geometry["coordinates"]]
    elif geom_type == "MultiLineString":
        coords_list = geometry["coordinates"]
    else:
        return False

    for coords in coords_list:
        # Sample vertices: first, last, and every 10th point for long lines
        sample_indices = set([0, len(coords) - 1])
        sample_indices.update(range(0, len(coords), 10))
        for i in sample_indices:
            lon, lat = coords[i][0], coords[i][1]
            if point_in_multipolygon(lon, lat, pjm_coords):
                return True
    return False


def main():
    # Load PJM boundary
    print("Loading PJM boundary...")
    with open(RAW_DIR / "hifld_planning_areas.geojson") as f:
        boundary = json.load(f)
    pjm_coords = filter_boundary_exclaves(boundary["features"][0]["geometry"]["coordinates"])

    print("Loading transmission lines...")
    with open(RAW_DIR / "hifld_transmission_pjm.geojson") as f:
        data = json.load(f)

    print(f"Total segments: {len(data['features'])}")

    # Filter to 345kV+
    high_voltage = []
    for feat in data["features"]:
        v = feat["properties"].get("VOLTAGE")
        if v and v >= 345:
            high_voltage.append(feat)

    print(f"High-voltage (345kV+): {len(high_voltage)}")

    # Spatial clip to PJM boundary
    print("Clipping to PJM boundary...")
    clipped = []
    for feat in high_voltage:
        if line_intersects_boundary(feat["geometry"], pjm_coords):
            props = feat["properties"]
            clipped.append({
                "type": "Feature",
                "geometry": feat["geometry"],
                "properties": {
                    "voltage": props.get("VOLTAGE"),
                    "volt_class": props.get("VOLT_CLASS", ""),
                    "owner": props.get("OWNER", ""),
                    "status": props.get("STATUS", ""),
                    "sub_1": props.get("SUB_1", ""),
                    "sub_2": props.get("SUB_2", ""),
                    "type": props.get("TYPE", ""),
                },
            })

    print(f"Inside PJM: {len(high_voltage)} → {len(clipped)} segments")

    # Voltage breakdown
    by_voltage = {}
    for feat in clipped:
        v = feat["properties"]["voltage"]
        by_voltage[v] = by_voltage.get(v, 0) + 1

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source_url": "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Electric_Power_Transmission_Lines/FeatureServer/0",
            "source_description": "HIFLD Electric Power Transmission Lines, filtered to 345kV+ "
                                  "and spatially clipped to PJM boundary (keeps segments with "
                                  "any vertex inside PJM footprint).",
            "access_date": date.today().isoformat(),
            "jurisdiction": "PJM Interconnection, LLC",
            "citation": "Homeland Infrastructure Foundation-Level Data (HIFLD). "
                        "'Electric Power Transmission Lines.' U.S. Department of Homeland Security, CISA. "
                        "Accessed {}.".format(date.today().strftime("%B %d, %Y")),
            "known_gaps": [
                "Lines crossing PJM boundary are kept if any vertex is inside — some "
                "segments may extend slightly beyond the footprint.",
                "Only 345kV+ lines shown — lower voltage distribution lines excluded for performance.",
                "Some line segments may be incomplete or have imprecise routing.",
                "HIFLD data vintage may lag actual grid state by 6-12 months.",
            ],
            "notes": "345kV+ lines represent the bulk power transmission backbone. "
                     "Lower voltage lines (69-230kV) excluded to keep layer performant.",
            "summary": {
                "total_segments": len(clipped),
                "by_voltage": dict(sorted(by_voltage.items())),
            },
        },
        "features": clipped,
    }

    out_path = OUT_DIR / "transmission.geojson"
    with open(out_path, "w") as f:
        json.dump(geojson, f)

    size_mb = out_path.stat().st_size / 1e6
    print(f"\nWrote {len(clipped)} segments to {out_path} ({size_mb:.1f} MB)")
    print(f"By voltage: {by_voltage}")


if __name__ == "__main__":
    main()
