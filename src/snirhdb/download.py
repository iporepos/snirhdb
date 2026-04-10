import argparse
import json
import pprint
import time
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
import sys
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from datetime import datetime

FIELDS_STATIONS = [
    "CodigoEstacao",
    "Nome",
    "TipoEstacao",
    "TipoEstacaoCodigo",
    "UF",
    "UFCodigo"
]

DATA_TYPES = {
    "Flu": [1, 3],
    "Plu": [2]
}
DATA_LEVELS = [1, 2]

def load_config(config_path: Path) -> dict:
    """
    Load and validate JSON configuration file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required_keys = {"api_url", "folder_data", "fetch_filter"}
    missing = required_keys - config.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {missing}")

    return config

def load_stations(config):
    file_stations = config["file_stations"]
    gdf = gpd.read_file(file_stations)

    gdf = gdf[FIELDS_STATIONS].copy()

    station_uf = config["download_uf"]
    station_type = DATA_TYPES[config["download_type"]]

    s_query = f"UFCodigo == {station_uf} and TipoEstacaoCodigo == {station_type}"
    s_query = f"UFCodigo == {station_uf} and TipoEstacaoCodigo == {station_type}"
    gdf = gdf.query(s_query).copy().reset_index(drop=True)

    print("Stations:")
    print(gdf.info())
    print(gdf.head())

    return list(gdf["CodigoEstacao"])

def download_station(parameters):

    def _save(fo, text):
        with open(fo, 'w') as file:
            file.write(text)


    api_url = parameters["api_url"]
    station_id = parameters["station_id"]
    data_type = parameters["data_type"]
    data_level = parameters["data_level"]
    start = parameters["start"]
    end = parameters["end"]
    fo = parameters["fo"]

    params = {
        "codEstacao": str(station_id),
        "dataInicio": f"01/01/{start}",
        "dataFim": f"01/01/{end}",
        "tipoDados": str(data_type),
        "nivelConsistencia": str(data_level)
    }

    try:

        response = requests.get(api_url, params=params)

        if response.status_code == 200:

            root = ET.fromstring(response.text)
            parser = root.findall(".//SerieHistorica")

            if parser:
                with open(fo, 'w') as file:
                    file.write(response.text)
                _save(fo, response.text)
                # print(f"--- Data saved.\n")
                return
            else:
                _save(fo, "Not Found")
                # print(f"--- Data not found in the period.\n")
                return
        else:
            _save(fo, "Not Found")
            # print(f"--- Data not found.\n")
            return

    except requests.exceptions.RequestException:
        _save(fo, "Not Found")
        return



def main(config_path: Path) -> None:

    # Load configuration
    config = load_config(config_path)

    start = config["download_start"]
    end = config["download_end"]

    folder_data = Path(config["folder_data"]).resolve()
    fetch_filter = config["fetch_filter"]

    # Ensure resources exist
    if not folder_data.exists():
        raise NotADirectoryError(f"Data folder does not exist: {folder_data}")

    stations = load_stations(config)
    #stations = stations[:4]


    fo_dir = Path(config["folder_output"]) / config["download_uf"]
    fo_dir.mkdir(exist_ok=True)

    existing_files = [f.name for f in fo_dir.glob("*.xml")]

    parameters = {
        "api_url": config["api_url"],
        "folder_output": fo_dir,
    }

    # -------------------------------------------------
    # MAIN LOOP
    data_types = DATA_TYPES[config["download_type"]]

    total_steps = len(stations) * len(data_types) * len(DATA_LEVELS)

    with tqdm(total=total_steps, desc="Downloading", unit="req") as pbar:
        for station in stations:
            for data_type in data_types:
                for data_level in DATA_LEVELS:

                    name = f"ANA_HIDROCONV_{station}_D{data_type}L{data_level}_X_{start}U{end}.xml"
                    fo = fo_dir / name

                    if name not in set(existing_files):

                        parameters.update({
                            "station_id": station,
                            "data_type": data_type,
                            "data_level": data_level,
                            "start": start,
                            "end": end,
                            "fo": fo
                        })
                        # call download
                        download_station(parameters=parameters)

                    pbar.update(1)







if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download data from the API"
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