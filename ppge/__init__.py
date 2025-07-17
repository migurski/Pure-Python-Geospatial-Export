#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import csv
import dataclasses
import enum
import io
import json
import typing

from .geomet import wkt
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
    shapetype: (
        typing.Literal[
            pyshp.NULL,
            pyshp.POINT,
            pyshp.POLYLINE,
            pyshp.POLYGON,
            pyshp.MULTIPOINT,
            pyshp.POINTZ,
            pyshp.POLYLINEZ,
            pyshp.POLYGONZ,
            pyshp.MULTIPOINTZ,
            pyshp.POINTM,
            pyshp.POLYLINEM,
            pyshp.POLYGONM,
            pyshp.MULTIPOINTM,
            pyshp.MULTIPATCH,
        ]
        | None
    )


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


def _get_record_converter(schema: list[Field]) -> dict[str, typing.Callable]:
    """Create a mapping of names to conversion functions for schema fields"""
    converter = {}

    def _complain_if_null(name: str, value: typing.Any) -> typing.Any:
        if value is None:
            raise ValueError(f"Field '{name}' is not nullable but value is None")
        return value

    for field in schema:
        if field.type is FieldType.INT:
            converter[field.name] = int
        elif field.type is FieldType.FLOAT:
            converter[field.name] = float
        elif field.type is FieldType.STR:
            converter[field.name] = str
        elif field.type is FieldType.BOOL:
            converter[field.name] = bool
        elif field.type is FieldType.BYTES:
            converter[field.name] = bytes
        else:
            converter[field.name] = lambda x: x

        if not field.nullable:
            _cv = converter[field.name]
            converter[field.name] = lambda val: _complain_if_null(field.name, _cv(val))

    return converter


def export_to_shapefile_from_rows(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    shp: typing.IO[bytes],
    shx: typing.IO[bytes],
    dbf: typing.IO[bytes],
    prj: typing.IO[bytes],
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to Shapefile format using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        shp, shx, dbf: Writable bytes file-like objects for .shp, .shx, .dbf
        prj: Writable bytes file-like object for .prj
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    converter = _get_record_converter(schema)
    gfield = [f for f in schema if f.type in (FieldType.GEOM, FieldType.GEOG)][0]
    with pyshp.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=gfield.shapetype) as shpfile:
        for field in schema:
            if field.name != geom_key:
                if field.type == FieldType.STR:
                    shpfile.field(field.name, "C")
                elif field.type == FieldType.INT:
                    shpfile.field(field.name, "N")
                elif field.type == FieldType.FLOAT:
                    shpfile.field(field.name, "F")
                elif field.type == FieldType.BOOL:
                    shpfile.field(field.name, "L")
                else:
                    shpfile.field(field.name, "C")
        for row in rows:
            geometry = row[geom_key]
            if geom_format == GeometryFormat.WKT:
                shape = wkt.loads(geometry)
            else:
                shape = json.loads(geometry)
            record = {}
            for field in schema:
                if field.name == geom_key:
                    continue
                try:
                    record[field.name] = converter[field.name](row.get(field.name))
                except Exception as e:
                    raise ValueError(f"Field '{field.name}' conversion error: {e}")
            shpfile.record(**record)
            shpfile.shape(shape)
    # Write projection file
    prj.write(
        b'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
    )


def export_to_geojson_from_rows(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    geojsonfile: typing.IO[bytes],
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to GeoJSON format using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        geojsonfile: Writable bytes file-like object
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    converter = _get_record_converter(schema)
    geojson = {"type": "FeatureCollection", "features": []}
    for row in rows:
        geometry = row[geom_key]
        if geom_format == GeometryFormat.WKT:
            if isinstance(geometry, str):
                geometry = wkt.loads(geometry)
        else:
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
        properties = {}
        for field in schema:
            if field.name == geom_key:
                continue
            try:
                properties[field.name] = converter[field.name](row.get(field.name))
            except Exception as e:
                raise ValueError(f"Field '{field.name}' conversion error: {e}")
        feature = {"type": "Feature", "geometry": geometry, "properties": properties}
        geojson["features"].append(feature)
    textfile = io.TextIOWrapper(geojsonfile, encoding="utf-8")
    json.dump(geojson, textfile, indent=2)
    textfile.flush()
    textfile.detach()  # Prevent closing the underlying BytesIO buffer


def export_to_csv_from_rows(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    csvfile: typing.IO[bytes],
    geom_key: str,
    geom_format: GeometryFormat,
) -> None:
    """
    Export row iterator to CSV format with WKT geometry column using provided schema.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with geometry and other data
        csvfile: Writable bytes file-like object
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    converter = _get_record_converter(schema)
    existing_columns = {field.name for field in schema}
    geometry_column = _get_geometry_column_name(existing_columns)
    fieldnames = [field.name for field in schema if field.name != geom_key]
    fieldnames.append(geometry_column)
    # csvfile is a bytes file-like object, so wrap it for text
    textfile = io.TextIOWrapper(csvfile, encoding="utf-8", newline="")
    writer = csv.DictWriter(textfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        geometry = row[geom_key]
        if geom_format == GeometryFormat.GEOJSON:
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
            geometry = wkt.dumps(geometry)
        else:
            if not isinstance(geometry, str):
                geometry = wkt.dumps(geometry)
        csv_row = {}
        for field in schema:
            if field.name == geom_key:
                continue
            try:
                csv_row[field.name] = converter[field.name](row.get(field.name))
            except Exception as e:
                raise ValueError(f"Field '{field.name}' conversion error: {e}")
        csv_row[geometry_column] = geometry
        writer.writerow(csv_row)
    textfile.flush()
    textfile.detach()  # Prevent closing the underlying BytesIO buffer


def process_bigquery_rows_to_shapefile(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    shp: typing.IO[bytes],
    shx: typing.IO[bytes],
    dbf: typing.IO[bytes],
    prj: typing.IO[bytes],
) -> None:
    """
    Process BigQuery row iterator and export to Shapefile.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'geom' and other fields
        shp, shx, dbf: Writable bytes file-like objects for .shp, .shx, .dbf
        prj: Writable bytes file-like object for .prj
    """
    export_to_shapefile_from_rows(
        schema, rows, shp, shx, dbf, prj, "geom", GeometryFormat.WKT
    )


def process_snowflake_rows_to_shapefile(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    shp: typing.IO[bytes],
    shx: typing.IO[bytes],
    dbf: typing.IO[bytes],
    prj: typing.IO[bytes],
) -> None:
    """
    Process Snowflake row iterator and export to Shapefile.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        shp, shx, dbf: Writable bytes file-like objects for .shp, .shx, .dbf
        prj: Writable bytes file-like object for .prj
    """
    export_to_shapefile_from_rows(
        schema, rows, shp, shx, dbf, prj, "GEOM", GeometryFormat.GEOJSON
    )


def process_bigquery_rows_to_geojson(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    geojsonfile: typing.IO[bytes],
) -> None:
    """
    Process BigQuery row iterator and export to GeoJSON.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'geom' and other fields
        geojsonfile: Writable bytes file-like object
    """
    export_to_geojson_from_rows(schema, rows, geojsonfile, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_geojson(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    geojsonfile: typing.IO[bytes],
) -> None:
    """
    Process Snowflake row iterator and export to GeoJSON.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        geojsonfile: Writable bytes file-like object
    """
    export_to_geojson_from_rows(
        schema, rows, geojsonfile, "GEOM", GeometryFormat.GEOJSON
    )


def process_bigquery_rows_to_csv(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    csvfile: typing.IO[bytes],
) -> None:
    """
    Process BigQuery row iterator and export to CSV with WKT geometry.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'geom' and other fields
        csvfile: Writable bytes file-like object
    """
    export_to_csv_from_rows(schema, rows, csvfile, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_csv(
    schema: list[Field],
    rows: typing.Iterator[dict[str, typing.Any]],
    csvfile: typing.IO[bytes],
) -> None:
    """
    Process Snowflake row iterator and export to CSV with WKT geometry.
    Args:
        schema: List of Field instances defining output fields
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        csvfile: Writable bytes file-like object
    """
    export_to_csv_from_rows(schema, rows, csvfile, "GEOM", GeometryFormat.GEOJSON)
