# snirhdb

Code supporting the building of SNIRH database


# Workflow

## Configuration file

The scripts work via a JSON config file like this:

```json
{
  "folder_data": "path/to/data",
  "folder_sql": "path/to/snirhdb/sql",
  "file_stations": "path/to/data/stations.gpkg",
  "fetch_filter": "WHERE station_type = 'Fluviométrica' AND state_id = 24",
  "view_filter": "WHERE station_id = 14840000 AND type_id = 1"
}
```

So all scripts must be called like this:

```bash
python "path/to/initialize.py" --config "path/to/config.json"
```

## Initialization

Create the `HIDRO.sqlite` file

```bash
python "path/to/initialize.py" --config "path/to/config.json"
```

## Setup

This requires an input geopackage with stations metadata
listed in `file_stations`:

```bash
python "path/to/setup.py" --config "path/to/config.json"
```

## Status

Get a status from the database:

```bash
python "path/to/status.py" --config "path/to/config.json"
```

## Fetch

This retrieves data via the API. Use the `fetch_filter` configuration
for custom query.

```bash
python "path/to/fetch.py" --config "path/to/config.json"
```

## View 

Use the `view_filter` for querying the desired station data.

```bash
python "path/to/view.py" --config "path/to/config.json"
```
