import argparse
import json
import sqlite3
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
import sys
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

# Define Base URL
BASE_URL = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica"

def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"folder_data", "fetch_filter"}
    missing = required_keys - config.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    return config


# Function to Parse XML and Insert into SQLite
def parse_and_insert(parser, station, tipo_dados, nivel_consistencia, conn):
    prefix_dict = {1: "Cota",
                   2: "Chuva",
                   3: "Vazao"}
    prefix = prefix_dict[tipo_dados]

    records = []
    for serie in parser:
        # Extract Year and Month from DataHora
        base_date = pd.to_datetime(serie.findtext("DataHora"))
        year, month = base_date.year, base_date.month

        # declare method
        method = None

        # Detect Method Field Dynamically
        for elem in serie:
            if "Metodo" in elem.tag or "Tipo" in elem.tag:
                method = elem.text
                break

                # Loop Through Daily Values
        for day in range(1, 32):
            value = serie.findtext(f"{prefix}{day:02}")
            status = serie.findtext(f"{prefix}{day:02}Status")

            if value:
                records.append((station, tipo_dados, nivel_consistencia,
                                f"{year}-{month:02d}-{day:02d}", value, status, method))

    # Sort records by date before inserting
    if records:
        records.sort(key=lambda x: x[3])  # Sorting by 'date' column (index 3)
        #print(f"Period of Records: {records[0][3]} to {records[-1][3]}")

        # Insert Records in Bulk to SQLite
        with conn:
            conn.executemany("""
                INSERT OR IGNORE INTO timeseries (station_id, type_id, consistency_id, date, value, status, method_id)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, records)


def main(config_path: Path) -> None:

    # Load configuration
    config = load_config(config_path)

    folder_data = Path(config["folder_data"]).resolve()
    fetch_filter = config["fetch_filter"]

    # Ensure resources exist
    if not folder_data.exists():
        raise NotADirectoryError(f"Data folder does not exist: {folder_data}")

    # SQLite Database File
    db_file = folder_data / "HIDRO.sqlite"
    conn = sqlite3.connect(db_file)

    # GET ALL STATIONS IDS
    fetch_query = f"SELECT * FROM stations {fetch_filter}"
    stations_df = pd.read_sql(fetch_query, conn)
    stations = list(stations_df["station_id"])

    print("Query:\n")
    print(fetch_filter)
    print("\n")
    print(stations_df)

    total_steps = len(stations) * 2 * 2

    with tqdm(total=total_steps, desc="Fetching series", unit="req") as pbar:

        for station in stations:

            for tipo_dados in [1, 3]:

                for nivel_consistencia in [1, 2]:

                    params = {
                        "codEstacao": str(station),
                        "dataInicio": "01/01/1900",
                        "dataFim": "",
                        "tipoDados": str(tipo_dados),
                        "nivelConsistencia": str(nivel_consistencia)
                    }

                    try:
                        response = requests.get(BASE_URL, params=params)

                        if response.status_code == 200:
                            root = ET.fromstring(response.text)
                            parser = root.findall(".//SerieHistorica")

                            if parser:
                                parse_and_insert(
                                    parser,
                                    station,
                                    tipo_dados,
                                    nivel_consistencia,
                                    conn
                                )

                        # always advance progress bar
                        pbar.update(1)

                    except requests.exceptions.RequestException:
                        pbar.update(1)

    '''
    # Fetch and Process Data for Each Station
    for station in stations:
        print(f"Station: {station}")

        for tipo_dados in [1, 3]:  # Iterate Over Types

            for nivel_consistencia in [1, 2]:  # Iterate Over Consistency Levels

                params = {
                    "codEstacao": str(station),
                    "dataInicio": "01/01/1900",
                    "dataFim": "",
                    "tipoDados": str(tipo_dados),
                    "nivelConsistencia": str(nivel_consistencia)
                }

                try:
                    response = requests.get(BASE_URL, params=params)

                    if response.status_code == 200:
                        root = ET.fromstring(response.text)
                        parser = root.findall(".//SerieHistorica")

                        if not parser:
                            print(f"No data found for station {station}")
                        else:
                            print(
                                f"Processing station {station}: - Type: {tipo_dados}, Consistency: {nivel_consistencia} \n URL: {response.url}")
                            parse_and_insert(parser, station, tipo_dados, nivel_consistencia, conn)
                    else:
                        print(
                            f"Failed for station {station} - Tipo: {tipo_dados}, Consistency: {nivel_consistencia} - Status Code: {response.status_code}")

                except requests.exceptions.RequestException as e:
                    print(f"Request error for station {station}: {e}")
    '''

    # Close SQLite Connection
    conn.close()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Fetch time series data from the API"
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