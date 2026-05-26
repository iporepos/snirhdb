"""
ANA HidroConv Data Downloader
==============================

A command-line tool for bulk downloading hydrological time-series data from
Brazil's National Water Agency (ANA) HidroConv API. It reads a JSON
configuration file, resolves the target monitoring stations from a GeoPackage
or Shapefile of ANA stations, and downloads raw XML responses for every
combination of station × data type × consistency level within a given date
range, saving each response as an individual file.

Usage
-----
    python download_ana.py --config path/to/config.json

Configuration file (JSON)
--------------------------
The following keys are required:

    api_url         (str)  – Base URL of the ANA HidroConv REST endpoint.
    folder_data     (str)  – Root directory that must already exist on disk;
                             used to validate the environment before any
                             downloads begin.
    folder_output   (str)  – Root directory where downloaded XML files are
                             written. One sub-folder per UF code is created
                             automatically (e.g. ``<folder_output>/43/``).
    file_stations   (str)  – Path to the vector file (GeoPackage / Shapefile)
                             containing ANA station metadata. Must include the
                             fields: CodigoEstacao, Nome, TipoEstacao,
                             TipoEstacaoCodigo, UF, UFCodigo.
    fetch_filter    (any)  – Reserved filter value (read by the loader but not
                             yet applied in the download loop).
    download_type   (str)  – Either ``"Flu"`` (fluviometric) or ``"Plu"``
                             (pluviometric). Controls which tipoDados codes and
                             station types are requested:
                               • "Flu" → tipoDados 1 and 3, TipoEstacaoCodigo [1, 3]
                               • "Plu" → tipoDados 2,       TipoEstacaoCodigo [2]
    download_uf     (list) – List of UF numeric codes (as strings or ints) to
                             process, e.g. ``["43", "42"]``.
    download_start  (int)  – First year of the requested period (inclusive).
                             Sent to the API as ``01/01/<download_start>``.
    download_end    (int)  – Last year of the requested period (inclusive).
                             Sent to the API as ``01/01/<download_end>``.

Output files
------------
Each successful request produces one XML file named according to the pattern::

    ANA_HIDROCONV_<CodigoEstacao>_D<tipoDados>L<nivelConsistencia>_X_<start>U<end>.xml

Files whose names already exist in the output directory are skipped, making
repeated runs resumable without re-downloading completed data.

Files that contain no ``<SerieHistorica>`` elements are saved with the content
``"Not Found"`` to mark the station/period as intentionally empty and prevent
future re-attempts. Files that could not be retrieved after all retries are
*not* created, so the next run will attempt them again automatically.

Retry / resilience strategy
----------------------------
Each HTTP request is attempted up to **3 times** with exponential back-off
(2 s → 4 s). Timeouts are set to 20 s (connect) / 600 s (read) to accommodate
slow ANA responses. HTTP 400 / 404 responses are treated as definitive and are
not retried.

Dependencies
------------
    requests, geopandas, pandas, tqdm, xml.etree.ElementTree (stdlib)

Notes
-----
* Progress is displayed via a ``tqdm`` bar that counts individual HTTP
  requests (stations × data types × consistency levels).
* The ``download_station_simple`` function is a lighter alternative without
  retry logic; it is retained for reference but not called by ``main()``.
"""

import argparse
import json
import pprint
import time
from itertools import dropwhile
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

def load_stations(config, uf_id):
    file_stations = config["file_stations"]
    station_type = DATA_TYPES[config["download_type"]]
    station_uf = int(uf_id)

    gdf = gpd.read_file(file_stations)
    gdf = gdf[FIELDS_STATIONS].copy()

    s_query = f"UFCodigo == {station_uf} and TipoEstacaoCodigo == {station_type}"
    gdf = gdf.query(s_query).copy().reset_index(drop=True)

    print("Stations:")
    print(gdf.info())
    print(gdf.head())

    return list(gdf["CodigoEstacao"])

def download_station_simple(parameters):

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

def download_station(parameters):
    def _save(fo, text):
        with open(fo, 'w', encoding='utf-8') as file:
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

    max_retries = 3
    base_delay = 2  # seconds to wait before retrying

    for attempt in range(max_retries):
        try:
            # Added timeout: (connect_timeout, read_timeout)
            # seconds to connect, seconds to receive data
            response = requests.get(api_url, params=params, timeout=(20, 600))

            if response.status_code == 200:
                try:
                    root = ET.fromstring(response.text)
                    parser = root.findall(".//SerieHistorica")

                    if parser:
                        _save(fo, response.text)
                        return  # Success, exit the function
                    else:
                        _save(fo, "Not Found")
                        return  # Clean empty response, exit the function

                except ET.ParseError:
                    # Sometimes APIs return 200 OK but send an HTML error page instead of XML
                    pass

            elif response.status_code in [400, 404]:
                # Definitive bad requests, no point in retrying
                _save(fo, "Not Found")
                return

            # If it's a 500, 502, 503, 504 server error, it will skip to the retry logic below

        except requests.exceptions.Timeout:
            pass  # Caught a timeout, will retry
        except requests.exceptions.RequestException:
            pass  # Caught a connection error, will retry

        # If we reach here, the request failed. Wait and retry.
        if attempt < max_retries - 1:
            time.sleep(base_delay * (2 ** attempt))  # Exponential backoff: 2s, 4s...

    # If the loop finishes without returning, all retries failed.
    # We DO NOT save a "Not Found" file here.
    # By leaving the file uncreated, the main loop will try to download it again the next time you run the script.
    # Optionally, you can log the failure:
    # print(f"\nFailed to download {station_id} after {max_retries} attempts.")

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


    uf_ids = config["download_uf"]

    for uf in uf_ids:
        print(f"\n>>> UF: {uf}")

        stations = load_stations(config, uf_id=uf)

        fo_dir = Path(config["folder_output"]) / uf
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