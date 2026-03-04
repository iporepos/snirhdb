import argparse
import json
import sqlite3
from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd


def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"folder_data", "folder_output"}
    missing = required_keys - config.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    return config



def main(config_path: Path) -> None:
    # Load configuration
    config = load_config(config_path)

    folder_data = Path(config["folder_data"]).resolve()

    # Ensure directories exist
    if not folder_data.exists():
        raise NotADirectoryError(f"Data folder does not exist: {folder_data}")


    db_file = folder_data / "HIDRO.sqlite"

    print(f"Initializing database at: {db_file}")

    # Connect to SQLite
    conn = sqlite3.connect(db_file)

    try:

        stations_df = pd.read_sql("""
            SELECT s.*
            FROM stations s
            WHERE EXISTS (
                SELECT 1
                FROM timeseries t
                WHERE t.station_id = s.station_id
            )
        """, conn)

        gdf = gpd.GeoDataFrame(
            stations_df,
            geometry=gpd.points_from_xy(stations_df["lon"], stations_df["lat"]),
            crs="EPSG:4674"  # WGS84
        )

        f = Path(config["folder_output"]) / "HIDRO.gpkg"
        gdf.to_file(f, layer="index", driver="GPKG")
        print(f"Index saved to {f}")

    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Get status from the database"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to configuration JSON file."
    )

    args = parser.parse_args()

    try:
        main(args.config)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)