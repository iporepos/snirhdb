import argparse
import json
import sqlite3
from pathlib import Path
import sys
import pandas as pd


def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"folder_data"}
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
        # List all tables
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        print("Tables:\n", tables)
        print("Stations:\n")
        stations = pd.read_sql("SELECT * FROM stations", conn)
        print(stations.info())
        print(stations)

        '''print("Time Series:\n")
        timeseries = pd.read_sql("SELECT * FROM timeseries", conn)
        print(timeseries.info())
        print(timeseries)'''
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