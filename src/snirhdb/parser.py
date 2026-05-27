"""
ANA HidroConv XML → CSV Parser
================================

Location: src/snirhdb/parser.py

Parses a single ANA HidroConv XML file into a long-format daily time series
CSV. Supports all three data types:

    D1  – Stage level   (Cota01…Cota31)       units: cm
    D2  – Precipitation (Chuva01…Chuva31)     units: mm
    D3  – Streamflow    (Vazao01…Vazao31)      units: m3s

The data type is detected automatically from the XML content.
"Not Found" sentinel files produce an empty file.

Output format
-------------
The CSV has a metadata preamble followed by the data table.

Preamble (comment lines starting with ``#``):

    # variable: stage          ← "stage" / "streamflow" / "precipitation"
    # units: cm                ← "cm" / "m3s" / "mm"
    # site_code: 39971000
    # tier: 2
    # is_daily_avg: 1
    # method: 1
    # source: ANA HidroWeb API

Field mapping from ANA Portuguese names:

    ANA field              → output key / notes
    ----------------------   ----------------------------------
    EstacaoCodigo          → site_code  (preamble)
    NivelConsistencia      → tier  (per row — varies monthly)
    MediaDiaria            → is_daily_avg  (preamble)
    TipoMedicaoCotas       → method   (D1, preamble)
    TipoMedicaoChuvas      → method   (D2, preamble)
    MetodoObtencaoVazoes   → method   (D3, preamble)
    DataHora + day offset  → datetime (YYYY-MM-DD)
    CotaNN / ChuvaNN /
      VazaoNN              → value
    CotaNNStatus / …       → value_status

Data table (semicolon-separated):

    datetime;tier;status;value

    datetime  – ISO-8601 date of the individual day (YYYY-MM-DD).
    value     – daily measurement. Empty cell when absent in the XML.
    status    – ANA daily quality flag. Empty cell when absent.
    tier      – NivelConsistencia for that month (1 = raw, 2 = revised).
                Varies per row since a station can mix consistency levels.

Monthly aggregate fields (Maxima, Minima, Media, Total, etc.) are discarded.
Missing day-values are preserved as empty cells (no imputation, no row dropping).
Days beyond the actual month length (e.g. Feb 29–31) are dropped.
Day slots absent from the XML entirely produce no row.

Preamble metadata is taken from the first <SerieHistorica> element.

Separator : semicolon (;)
Encoding  : utf-8-sig  (UTF-8 with BOM, Excel-compatible)

Usage
-----
    python src/snirhdb/parser.py <input_xml> <output_csv>

Example
-------
    python src/snirhdb/parser.py bench/ANA_HIDROCONV_39971000_D1L2_X_2018U2023.xml \\
                                  bench/ANA_HIDROCONV_39971000_D1L2_X_2018U2023.csv

Runner integration
------------------
    import glob
    from pathlib import Path
    from src.snirhdb.parser import parse

    def main():
        for f in glob.glob("./bench/*.xml"):
            fi = Path(f)
            parse(fi, fi.parent / f"{fi.stem}.csv")
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, dict] = {
    "Cota": {
        "label":        "D1",
        "variable":     "stage",
        "units":        "cm",
        "method_field": "TipoMedicaoCotas",
        "daily_prefix": "Cota",
        "legend": [
            "# ---",
            "# tier         : 1 = Raw, 2 = Revised",
            "# is_daily_avg : 0 = Instantaneous, 1 = Daily average",
            "# method       : 1 = Staff gauge, 2 = Limnigraph, 3 = Data logger, 5 = SMS staff gauge",
            "# status       : 0 = Blank, 1 = Observed, 2 = Estimated, 3 = Dubious, 4 = Dry gauge",
        ],
    },
    "Chuva": {
        "label":        "D2",
        "variable":     "precipitation",
        "units":        "mm",
        "method_field": "TipoMedicaoChuvas",
        "daily_prefix": "Chuva",
        "legend": [
            "# ---",
            "# tier         : 1 = Raw, 2 = Revised",
            "# method       : 1 = Rain gauge, 2 = Pluviograph, 3 = Data logger",
            "# status       : 0 = Blank, 1 = Observed, 2 = Estimated, 3 = Dubious, 4 = Accumulated",
        ],
    },
    "Vazao": {
        "label":        "D3",
        "variable":     "streamflow",
        "units":        "m3s",
        "method_field": "MetodoObtencaoVazoes",
        "daily_prefix": "Vazao",
        "legend": [
            "# ---",
            "# tier         : 1 = Raw, 2 = Revised",
            "# is_daily_avg : 0 = Instantaneous, 1 = Daily average",
            "# method       : 1 = Rating curve, 2 = Flow transfer, 3 = Flow summation, 4 = ADCP",
            "# status       : 0 = Blank, 1 = Observed, 2 = Estimated, 3 = Dubious, 4 = Dry gauge",
        ],
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_not_found(path: Path) -> bool:
    try:
        return path.read_text(encoding="utf-8", errors="ignore").strip() == "Not Found"
    except OSError:
        return False


FILENAME_TYPE = {"D1": "Cota", "D2": "Chuva", "D3": "Vazao"}


def _detect_schema_from_filename(path: Path) -> dict | None:
    """Fallback: parse the data type from the filename (e.g. _D1L1_)."""
    for code, prefix in FILENAME_TYPE.items():
        if f"_{code}" in path.stem.upper():
            return SCHEMAS[prefix]
    return None


def _detect_schema(root: ET.Element) -> dict | None:
    first = root.find(".//SerieHistorica")
    if first is None:
        return None
    tags = {child.tag for child in first}
    for prefix, schema in SCHEMAS.items():
        if any(t.startswith(prefix) for t in tags):
            return schema
    return None


def _preamble(first_elem: ET.Element, schema: dict) -> str:
    get = lambda tag: (first_elem.findtext(tag) or "").strip()
    lines = [
        f"# variable: {schema['variable']}",
        f"# units: {schema['units']}",
        f"# site_code: {get('EstacaoCodigo')}",
        f"# is_daily_avg: {get('MediaDiaria')}",
        f"# method: {get(schema['method_field'])}",
        "# source: ANA HidroWeb API",
    ] + schema["legend"]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Core parse function
# ---------------------------------------------------------------------------

def parse(input_path: Path, output_path: Path) -> bool:
    """
    Parse one HidroConv XML file and write a long-format CSV with preamble.

    Parameters
    ----------
    input_path:  Path to the source ``.xml`` file.
    output_path: Destination ``.csv`` path (parent dirs created if needed).

    Returns
    -------
    True  if the file contained data and the CSV was written.
    False if the file was a "Not Found" sentinel or had no records (skipped).
    """
    if _is_not_found(input_path):
        print(f"  [skip] Not Found sentinel: {input_path.name}")
        return False

    tree = ET.parse(input_path)
    root = tree.getroot()
    elements = root.findall(".//SerieHistorica")

    if not elements:
        print(f"  [skip] no records: {input_path.name}")
        return False

    schema = _detect_schema(root) or _detect_schema_from_filename(input_path)
    if schema is None:
        print(f"  [skip] could not detect data type: {input_path.name}")
        return False

    prefix = schema["daily_prefix"]
    rows = []

    for elem in elements:
        lookup = {child.tag: child.text for child in elem}

        try:
            month_start = pd.to_datetime(lookup.get("DataHora"))
        except Exception:
            month_start = None

        for day in range(1, 32):
            val_key = f"{prefix}{day:02d}"
            sta_key = f"{prefix}{day:02d}Status"

            if val_key not in lookup and sta_key not in lookup:
                continue

            date_str = None
            if month_start is not None:
                try:
                    date = month_start + pd.DateOffset(days=day - 1)
                    if date.month != month_start.month:
                        continue
                    date_str = date.strftime("%Y-%m-%d")
                except Exception:
                    pass

            rows.append({
                "datetime": date_str,
                "value":    lookup.get(val_key),
                "status":   lookup.get(sta_key),
                "tier":     lookup.get("NivelConsistencia"),
            })

    df = pd.DataFrame(rows, columns=["datetime", "value", "status", "tier"])
    df["value"]  = pd.to_numeric(df["value"],  errors="coerce").astype("float32")
    df["status"] = pd.to_numeric(df["status"], errors="coerce").fillna(3).astype(float).apply(lambda x: x if x <= 4 else 0).astype("Int8")
    df["tier"]   = pd.to_numeric(df["tier"],   errors="coerce").fillna(1).astype("Int8")

    if df["value"].isna().all():
        print(f"  [skip] no values in records: {input_path.name}")
        return False

    df = df[["datetime", "tier", "status", "value"]]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8-sig") as fh:
        fh.write(_preamble(elements[0], schema))
        df.to_csv(fh, sep=";", index=False)

    print(f"[{schema['label']}] {len(df)} daily rows → {output_path}")
    return True


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: parser.py <input_xml> <output_csv>", file=sys.stderr)
        sys.exit(1)

    input_path  = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    if not input_path.exists():
        print(f"ERROR: file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        parse(input_path, output_path)
    except ET.ParseError as exc:
        print(f"ERROR: malformed XML in {input_path.name}: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()