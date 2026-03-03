import argparse
import json
import sqlite3
from pathlib import Path
import sys


def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"folder_data", "folder_sql"}
    missing = required_keys - config.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    return config


def execute_sql_file(cursor, conn, filepath: Path) -> None:
    """
    Execute a full SQL script file.
    """
    if not filepath.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")

    with filepath.open("r", encoding="utf-8") as file:
        sql_script = file.read()

    cursor.executescript(sql_script)
    conn.commit()


def main(config_path: Path) -> None:
    # Load configuration
    config = load_config(config_path)

    folder_data = Path(config["folder_data"]).resolve()
    folder_sql = Path(config["folder_sql"]).resolve()

    # Ensure directories exist
    if not folder_data.exists():
        raise NotADirectoryError(f"Data folder does not exist: {folder_data}")
    if not folder_sql.exists():
        raise NotADirectoryError(f"SQL folder does not exist: {folder_sql}")

    db_file = folder_data / "HIDRO.sqlite"

    print(f"Initializing database at: {db_file}")

    # Connect to SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        execute_sql_file(cursor, conn, folder_sql / "create_tables.sql")
        execute_sql_file(cursor, conn, folder_sql / "populate_metadata.sql")
        print("Database and metadata initialized successfully.")
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Initialize hydro SQLite database using SQL scripts defined in config JSON."
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