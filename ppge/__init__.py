#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import csv
import dataclasses
import enum
import itertools
import json
import typing

import geomet.wkt

from . import pyshp


class FieldType(enum.Enum):
    """Enumeration of schema field types."""

    INT = "int"
    FLOAT = "float"
    STR = "str"
    BYTES = "bytes"
    BOOL = "bool"
    GEOM = "geom"
    GEOG = "geog"


class GeometryFormat(enum.Enum):
    """Enumeration of supported geometry formats."""

    WKT = "WKT"
    GEOJSON = "GeoJSON"


@dataclasses.dataclass
class Field:
    name: str
    type: FieldType | str
    nullable: bool


def _get_sample_rows(
    rows: typing.Iterator[dict[str, typing.Any]], max_sample: int
) -> tuple[list[dict[str, typing.Any]], typing.Iterator[dict[str, typing.Any]]]:
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
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to Shapefile format using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output Shapefile (without extension)
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    with pyshp.Writer(f"{output_path}.shp", shapeType=5) as shp:
        # Add fields for all non-geometry columns from schema
        for field in schema:
            if field.name != geom_key:
                if field.type == FieldType.STR:
                    shp.field(field.name, "C")
                elif field.type == FieldType.INT:
                    shp.field(field.name, "N")
                elif field.type == FieldType.FLOAT:
                    shp.field(field.name, "F")
                elif field.type == FieldType.BOOL:
                    shp.field(field.name, "L")
                else:
                    shp.field(field.name, "C")  # Default to character field
        # Export all rows
        for row in rows:
            geometry = row[geom_key]
            if geom_format == GeometryFormat.WKT:
                coords = geomet.wkt.loads(geometry)["coordinates"]
            else:  # GeoJSON
                coords = json.loads(geometry)["coordinates"]
            record = {k: v for k, v in row.items() if k != geom_key}
            shp.record(**record)
            shp.poly(coords)
    with open(f"{output_path}.prj", "w") as prj:
        prj.write(
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        )


def export_to_geojson_from_rows(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to GeoJSON format using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output GeoJSON file
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    geojson = {"type": "FeatureCollection", "features": []}
    for row in rows:
        geometry = row[geom_key]
        if geom_format == GeometryFormat.WKT:
            if isinstance(geometry, str):
                geometry = geomet.wkt.loads(geometry)
        else:  # GeoJSON
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
        properties = {k: v for k, v in row.items() if k != geom_key}
        feature = {"type": "Feature", "geometry": geometry, "properties": properties}
        geojson["features"].append(feature)
    with open(output_path, "w") as f:
        json.dump(geojson, f, indent=2)


def export_to_csv_from_rows(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    output_path: str,
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to CSV format with WKT geometry column using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output CSV file
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Determine geometry column name
    existing_columns = {field.name for field in schema}
    geometry_column = _get_geometry_column_name(existing_columns)
    # Prepare fieldnames for CSV writer
    fieldnames = [field.name for field in schema if field.name != geom_key]
    fieldnames.append(geometry_column)
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            geometry = row[geom_key]
            if geom_format == GeometryFormat.GEOJSON:
                if isinstance(geometry, str):
                    geometry = json.loads(geometry)
                geometry = geomet.wkt.dumps(geometry)
            else:  # WKT
                if not isinstance(geometry, str):
                    geometry = geomet.wkt.dumps(geometry)
            csv_row = {k: v for k, v in row.items() if k != geom_key}
            csv_row[geometry_column] = geometry
            writer.writerow(csv_row)


def process_bigquery_rows_to_shapefile(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process BigQuery row iterator and export to Shapefile.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(schema, rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_shapefile(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process Snowflake row iterator and export to Shapefile.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(
        schema, rows, output_path, "GEOM", GeometryFormat.GEOJSON
    )


def process_bigquery_rows_to_geojson(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process BigQuery row iterator and export to GeoJSON.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output GeoJSON file
    """
    export_to_geojson_from_rows(schema, rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_geojson(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process Snowflake row iterator and export to GeoJSON.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output GeoJSON file
    """
    export_to_geojson_from_rows(
        schema, rows, output_path, "GEOM", GeometryFormat.GEOJSON
    )


def process_bigquery_rows_to_csv(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process BigQuery row iterator and export to CSV with WKT geometry.

    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output CSV file
    """
    export_to_csv_from_rows(schema, rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_csv(
    rows: typing.Iterator[dict[str, typing.Any]], output_path: str, schema: list[Field]
) -> None:
    """
    Process Snowflake row iterator and export to CSV with WKT geometry.

    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output CSV file
    """
    export_to_csv_from_rows(schema, rows, output_path, "GEOM", GeometryFormat.GEOJSON)
