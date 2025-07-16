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
    def get_converter(field_type):
        if field_type == FieldType.INT:
            return int
        elif field_type == FieldType.FLOAT:
            return float
        elif field_type == FieldType.STR:
            return str
        elif field_type == FieldType.BOOL:
            return bool
        elif field_type == FieldType.BYTES:
            return bytes
        else:
            return lambda x: x

    with pyshp.Writer(f"{output_path}.shp", shapeType=5) as shp:
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
                    shp.field(field.name, "C")
        for row in rows:
            geometry = row[geom_key]
            if geom_format == GeometryFormat.WKT:
                coords = geomet.wkt.loads(geometry)["coordinates"]
            else:
                coords = json.loads(geometry)["coordinates"]
            record = {}
            for field in schema:
                if field.name == geom_key:
                    continue
                value = row.get(field.name)
                if value is None:
                    if not field.nullable:
                        raise ValueError(
                            f"Field '{field.name}' is not nullable but value is None"
                        )
                    record[field.name] = None
                else:
                    try:
                        record[field.name] = get_converter(field.type)(value)
                    except Exception as e:
                        raise ValueError(f"Field '{field.name}' conversion error: {e}")
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
    def get_converter(field_type):
        if field_type == FieldType.INT:
            return int
        elif field_type == FieldType.FLOAT:
            return float
        elif field_type == FieldType.STR:
            return str
        elif field_type == FieldType.BOOL:
            return bool
        elif field_type == FieldType.BYTES:
            return bytes
        else:
            return lambda x: x

    geojson = {"type": "FeatureCollection", "features": []}
    for row in rows:
        geometry = row[geom_key]
        if geom_format == GeometryFormat.WKT:
            if isinstance(geometry, str):
                geometry = geomet.wkt.loads(geometry)
        else:
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
        properties = {}
        for field in schema:
            if field.name == geom_key:
                continue
            value = row.get(field.name)
            if value is None:
                if not field.nullable:
                    raise ValueError(
                        f"Field '{field.name}' is not nullable but value is None"
                    )
                properties[field.name] = None
            else:
                try:
                    properties[field.name] = get_converter(field.type)(value)
                except Exception as e:
                    raise ValueError(f"Field '{field.name}' conversion error: {e}")
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
    def get_converter(field_type):
        if field_type == FieldType.INT:
            return int
        elif field_type == FieldType.FLOAT:
            return float
        elif field_type == FieldType.STR:
            return str
        elif field_type == FieldType.BOOL:
            return bool
        elif field_type == FieldType.BYTES:
            return bytes
        else:
            return lambda x: x

    existing_columns = {field.name for field in schema}
    geometry_column = _get_geometry_column_name(existing_columns)
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
            else:
                if not isinstance(geometry, str):
                    geometry = geomet.wkt.dumps(geometry)
            csv_row = {}
            for field in schema:
                if field.name == geom_key:
                    continue
                value = row.get(field.name)
                if value is None:
                    if not field.nullable:
                        raise ValueError(
                            f"Field '{field.name}' is not nullable but value is None"
                        )
                    csv_row[field.name] = None
                else:
                    try:
                        csv_row[field.name] = get_converter(field.type)(value)
                    except Exception as e:
                        raise ValueError(f"Field '{field.name}' conversion error: {e}")
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
