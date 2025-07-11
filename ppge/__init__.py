#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import json
import itertools
import csv
from typing import Iterator, Dict, Any
from enum import Enum

import geomet.wkt

from . import pyshp


class GeometryFormat(Enum):
    """Enumeration of supported geometry formats."""

    WKT = "WKT"
    GEOJSON = "GeoJSON"


def _get_sample_rows(
    rows: Iterator[Dict[str, Any]], max_sample: int
) -> tuple[list[Dict[str, Any]], Iterator[Dict[str, Any]]]:
    """
    Get sample rows for schema detection while preserving the original iterator.

    Args:
        rows: Original row iterator
        max_sample: Maximum number of rows to sample

    Returns:
        Tuple of (sample_rows, full_iterator)
    """
    # Create two iterators from the original one
    rows1, rows2 = itertools.tee(rows)

    # Get sample rows using islice
    sample_rows = list(itertools.islice(rows1, max_sample))

    return sample_rows, rows2


def _get_geometry_column_name(existing_columns: set) -> str:
    """
    Determine the name for the geometry column, avoiding conflicts.

    Args:
        existing_columns: Set of existing column names

    Returns:
        str: Name for the geometry column
    """
    if "geometry" not in existing_columns:
        return "geometry"
    elif "WKT" not in existing_columns:
        return "WKT"
    else:
        # Find a unique name by appending numbers
        counter = 1
        while f"geometry_{counter}" in existing_columns:
            counter += 1
        return f"geometry_{counter}"


def export_to_shapefile_from_rows(
    rows: Iterator[Dict[str, Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to Shapefile format.

    Args:
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output Shapefile (without extension)
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Get sample rows for schema detection
    sample_rows, full_rows = _get_sample_rows(rows, 100)
    if not sample_rows:
        return

    with pyshp.Writer(f"{output_path}.shp", shapeType=5) as shp:
        # Add fields for all non-geometry columns
        for key, value in sample_rows[0].items():
            if key != geom_key:
                if isinstance(value, str):
                    shp.field(key, "C")
                elif isinstance(value, int):
                    shp.field(key, "N")
                elif isinstance(value, float):
                    shp.field(key, "F")
                else:
                    shp.field(key, "C")  # Default to character field

        # Export all rows
        for row in full_rows:
            # Extract geometry
            geometry = row[geom_key]

            if geom_format == GeometryFormat.WKT:
                # Parse WKT
                coords = geomet.wkt.loads(geometry)["coordinates"]
            else:  # GeoJSON
                coords = json.loads(geometry)["coordinates"]

            # Create record with all non-geometry fields
            record = {k: v for k, v in row.items() if k != geom_key}
            shp.record(**record)
            shp.poly(coords)

    # Write projection file
    with open(f"{output_path}.prj", "w") as prj:
        prj.write(
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        )


def export_to_geojson_from_rows(
    rows: Iterator[Dict[str, Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to GeoJSON format.

    Args:
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output GeoJSON file
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Get sample rows for schema detection
    sample_rows, full_rows = _get_sample_rows(rows, 100)
    if not sample_rows:
        return

    # Initialize GeoJSON FeatureCollection
    geojson = {"type": "FeatureCollection", "features": []}

    # Export all rows
    for row in full_rows:
        # Extract geometry
        geometry = row[geom_key]

        # Convert WKT to GeoJSON if needed
        if geom_format == GeometryFormat.WKT:
            if isinstance(geometry, str):
                # Convert WKT to GeoJSON
                geometry = geomet.wkt.loads(geometry)
            else:
                # Already a dict, assume it's GeoJSON
                geometry = geometry
        else:  # GeoJSON
            if isinstance(geometry, str):
                geometry = json.loads(geometry)

        # Create properties dict with all non-geometry fields
        properties = {k: v for k, v in row.items() if k != geom_key}

        # Create feature
        feature = {"type": "Feature", "geometry": geometry, "properties": properties}

        geojson["features"].append(feature)

    # Write GeoJSON file
    with open(output_path, "w") as f:
        json.dump(geojson, f, indent=2)


def export_to_csv_from_rows(
    rows: Iterator[Dict[str, Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to CSV format with WKT geometry column.

    Args:
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output CSV file
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Get sample rows for schema detection
    sample_rows, full_rows = _get_sample_rows(rows, 100)
    if not sample_rows:
        return

    # Determine geometry column name
    existing_columns = set(sample_rows[0].keys())
    geometry_column = _get_geometry_column_name(existing_columns)

    # Prepare fieldnames for CSV writer
    fieldnames = []
    for key in sample_rows[0].keys():
        if key != geom_key:
            fieldnames.append(key)
    fieldnames.append(geometry_column)

    # Write CSV file
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Export all rows
        for row in full_rows:
            # Extract geometry
            geometry = row[geom_key]

            # Convert GeoJSON to WKT if needed
            if geom_format == GeometryFormat.GEOJSON:
                if isinstance(geometry, str):
                    geometry = json.loads(geometry)
                # Convert GeoJSON to WKT
                geometry = geomet.wkt.dumps(geometry)
            else:  # WKT
                if not isinstance(geometry, str):
                    # Convert dict to WKT if needed
                    geometry = geomet.wkt.dumps(geometry)

            # Create row dict with all non-geometry fields plus geometry
            csv_row = {k: v for k, v in row.items() if k != geom_key}
            csv_row[geometry_column] = geometry

            writer.writerow(csv_row)


def process_bigquery_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process BigQuery row iterator and export to Shapefile.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process Snowflake row iterator and export to Shapefile.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "GEOM", GeometryFormat.GEOJSON)


def process_bigquery_rows_to_geojson(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process BigQuery row iterator and export to GeoJSON.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output GeoJSON file
    """
    export_to_geojson_from_rows(rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_geojson(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process Snowflake row iterator and export to GeoJSON.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output GeoJSON file
    """
    export_to_geojson_from_rows(rows, output_path, "GEOM", GeometryFormat.GEOJSON)


def process_bigquery_rows_to_csv(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process BigQuery row iterator and export to CSV with WKT geometry.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output CSV file
    """
    export_to_csv_from_rows(rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_csv(
    rows: Iterator[Dict[str, Any]], output_path: str
) -> None:
    """
    Process Snowflake row iterator and export to CSV with WKT geometry.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output CSV file
    """
    export_to_csv_from_rows(rows, output_path, "GEOM", GeometryFormat.GEOJSON)
