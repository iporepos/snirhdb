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

    required_keys = {"file_stations", "folder_data"}
    missing = required_keys - config.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    return config


def main(config_path: Path) -> None:

    # Load configuration
    config = load_config(config_path)

    folder_data = Path(config["folder_data"]).resolve()
    file_stations = Path(config["file_stations"]).resolve()

    # Ensure resources exist
    if not folder_data.exists():
        raise NotADirectoryError(f"Data folder does not exist: {folder_data}")
    if not file_stations.exists():
        raise NotADirectoryError(f"File stations does not exist: {file_stations}")

    # Load CSV File
    gdf = gpd.read_file(file_stations)



    # Filter Only Required Columns
    columns_needed = [
        "CodigoEstacao", "Nome", "TipoEstacao", "Longitude", "Latitude",
        "BaciaCodigo", "SubBaciaCodigo", "RioCodigo", "UFCodigo", "MunicipioCodigo",
        "ResponsavelCodigo", "ResponsavelUnidade",
        "OperadoraCodigo", "OperadoraUnidade",
        "CodigoAdicional", "Altitude", "AreaDrenagem"
    ]

    # Ensure the CSV has all required columns
    df = gdf[columns_needed]

    print(df.info())

    # Rename Columns to Match SQLite Table
    df = df.rename(columns={
        "CodigoEstacao": "station_id",
        "Nome": "name",
        "TipoEstacao": "station_type",
        "Longitude": "lon",
        "Latitude": "lat",
        "BaciaCodigo": "basin_id",
        "SubBaciaCodigo": "sub_basin_id",
        "RioCodigo": "river_id",
        "UFCodigo": "state_id",
        "MunicipioCodigo": "municipality_id",
        "ResponsavelCodigo": "responsible_id",
        "ResponsavelUnidade": "responsible_unit",
        "OperadoraCodigo": "operator_id",
        "OperadoraUnidade": "operator_unit",
        "CodigoAdicional": "additional_code",
        "Altitude": "altitude",
        "AreaDrenagem": "drainage_area"
    })

    # Connect to SQLite
    conn = sqlite3.connect(folder_data / "HIDRO.sqlite")
    cursor = conn.cursor()

    # Insert Data into SQLite
    try:
        df.to_sql("stations", conn, if_exists="append", index=False)
        print("Stations Data Imported Successfully!")
    finally:
        # Close Connection
        conn.close()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Setup hydro SQLite database loading stations metadata."
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