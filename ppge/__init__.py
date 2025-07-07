#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import geomet
import geopackage
import shapefile
import shapely
import json
import itertools
from typing import Iterator, Dict, Any
from enum import Enum


class GeometryFormat(Enum):
    """Enumeration of supported geometry formats."""

    WKT = "WKT"
    GEOJSON = "GeoJSON"


def _get_sample_rows(
    rows: Iterator[Dict[str, Any]], max_sample: int = 100
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

    sample_rows = []
    row_count = 0

    # Get sample rows from the first iterator
    for row in rows1:
        sample_rows.append(row)
        row_count += 1

        if row_count >= max_sample:
            break

    return sample_rows, rows2


def export_to_geopackage_from_rows(
    rows: Iterator[Dict[str, Any]],
    output_path: str,
    table_name: str = "geodata",
    geom_key: str = "geom",
    geom_format: GeometryFormat = GeometryFormat.WKT,
) -> None:
    """
    Export row iterator to GeoPackage format.

    Args:
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output GeoPackage file
        table_name: Name of the table within the GeoPackage
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Get sample rows for schema detection
    sample_rows, full_rows = _get_sample_rows(rows)
    if not sample_rows:
        return

    # Create fields dict excluding geometry field
    fields = {}
    for key, value in sample_rows[0].items():
        if key != geom_key:
            if isinstance(value, str):
                fields[key] = "TEXT"
            elif isinstance(value, (int, float)):
                fields[key] = "REAL"
            else:
                fields[key] = "TEXT"  # Default to TEXT for other types

    with geopackage.GeoPackage(output_path) as gpkg:
        tbl = gpkg.create(
            table_name,
            fields=fields,
            wkid=4326,
            geometry_type="POLYGON",
            overwrite=True,
        )

        # Export all rows
        for row in full_rows:
            # Extract geometry
            geometry = row[geom_key]

            # Convert GeoJSON string to dictionary if needed
            if geom_format == GeometryFormat.GEOJSON and isinstance(geometry, str):
                geometry = json.loads(geometry)

            # Create data dict with all non-geometry fields
            data = {k: v for k, v in row.items() if k != geom_key}
            data["Shape"] = geometry

            tbl.insert(data, geom_format=geom_format.value)


def export_to_shapefile_from_rows(
    rows: Iterator[Dict[str, Any]],
    output_path: str,
    geom_key: str = "geom",
    geom_format: GeometryFormat = GeometryFormat.WKT,
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
    sample_rows, full_rows = _get_sample_rows(rows)
    if not sample_rows:
        return

    with shapefile.Writer(f"{output_path}.shp", shapeType=5) as shp:
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
                coords = geomet.wkt.loads(geometry)["coordinates"]
            else:  # GeoJSON
                coords = shapely.from_geojson(geometry).__geo_interface__["coordinates"]

            # Create record with all non-geometry fields
            record = {k: v for k, v in row.items() if k != geom_key}
            shp.record(**record)
            shp.poly(coords)

    # Write projection file
    with open(f"{output_path}.prj", "w") as prj:
        prj.write(
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        )


def process_bigquery_rows_to_geopackage(
    rows: Iterator[Dict[str, Any]], output_path: str, table_name: str = "geodata"
) -> None:
    """
    Process BigQuery row iterator and export to GeoPackage.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(
        rows, output_path, table_name, "geom", GeometryFormat.WKT
    )


def process_snowflake_rows_to_geopackage(
    rows: Iterator[Dict[str, Any]], output_path: str, table_name: str = "geodata"
) -> None:
    """
    Process Snowflake row iterator and export to GeoPackage.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(
        rows, output_path, table_name, "GEOM", GeometryFormat.GEOJSON
    )


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
