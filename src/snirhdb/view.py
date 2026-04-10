import argparse
import json
import sqlite3
from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt

def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"folder_data", "view_filter", "folder_output"}
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
        query = "SELECT * FROM timeseries {}".format(config["view_filter"])
        df = pd.read_sql(query, conn)
        df["date"] = pd.to_datetime(df["date"])
        print(df.info())
        df = df.sort_values(by="date")
        print(df)
        plt.plot(df["date"], df["value"], )

        if config["view_save"] == 1:
            f = Path(config["folder_output"]) / "view.svg"
            plt.savefig(f)
            plt.close()
        else:
            plt.show()

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