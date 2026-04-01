"""
Ingest HIFLD Transmission Lines, filtered to high-voltage (345kV+) within PJM footprint.
Source: HIFLD Electric Power Transmission Lines (ArcGIS REST API)
Outputs: data/processed/transmission.geojson (Ultan-compliant metadata + GeoJSON)

Only includes 345kV, 500kV, and 765kV lines for performance and narrative focus.
These are the backbone transmission lines that define PJM's grid structure.
"""

import json
from datetime import date
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "raw"
OUT_DIR = Path(__file__).parent.parent / "processed"
OUT_DIR.mkdir(exist_ok=True)


def main():
    print("Loading transmission lines...")
    with open(RAW_DIR / "hifld_transmission_pjm.geojson") as f:
        data = json.load(f)

    print(f"Total segments: {len(data['features'])}")

    # Filter to 345kV+
    high_voltage = []
    for feat in data["features"]:
        v = feat["properties"].get("VOLTAGE")
        if v and v >= 345:
            props = feat["properties"]
            high_voltage.append({
                "type": "Feature",
                "geometry": feat["geometry"],
                "properties": {
                    "voltage": v,
                    "volt_class": props.get("VOLT_CLASS", ""),
                    "owner": props.get("OWNER", ""),
                    "status": props.get("STATUS", ""),
                    "sub_1": props.get("SUB_1", ""),
                    "sub_2": props.get("SUB_2", ""),
                    "type": props.get("TYPE", ""),
                },
            })

    print(f"High-voltage (345kV+): {len(high_voltage)}")

    # Voltage breakdown
    by_voltage = {}
    for feat in high_voltage:
        v = feat["properties"]["voltage"]
        by_voltage[v] = by_voltage.get(v, 0) + 1

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source_url": "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Electric_Power_Transmission_Lines/FeatureServer/0",
            "source_description": "HIFLD Electric Power Transmission Lines, filtered to 345kV+ "
                                  "within PJM bounding box (xmin=-90, ymin=35, xmax=-73, ymax=43)",
            "access_date": date.today().isoformat(),
            "jurisdiction": "PJM region (bounding box, not spatially clipped to exact boundary)",
            "citation": "Homeland Infrastructure Foundation-Level Data (HIFLD). "
                        "'Electric Power Transmission Lines.' U.S. Department of Homeland Security, CISA. "
                        "Accessed {}.".format(date.today().strftime("%B %d, %Y")),
            "known_gaps": [
                "Bounding box filter includes some non-PJM lines in border areas.",
                "Only 345kV+ lines shown — lower voltage distribution lines excluded for performance.",
                "Some line segments may be incomplete or have imprecise routing.",
                "HIFLD data vintage may lag actual grid state by 6-12 months.",
            ],
            "notes": "345kV+ lines represent the bulk power transmission backbone. "
                     "Lower voltage lines (69-230kV) excluded to keep layer performant (~1.2K vs ~25K segments).",
            "summary": {
                "total_segments": len(high_voltage),
                "by_voltage": dict(sorted(by_voltage.items())),
            },
        },
        "features": high_voltage,
    }

    out_path = OUT_DIR / "transmission.geojson"
    with open(out_path, "w") as f:
        json.dump(geojson, f)

    size_mb = out_path.stat().st_size / 1e6
    print(f"\nWrote {len(high_voltage)} segments to {out_path} ({size_mb:.1f} MB)")
    print(f"By voltage: {by_voltage}")


if __name__ == "__main__":
    main()
