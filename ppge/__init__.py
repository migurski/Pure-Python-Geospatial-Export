#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import csv
import dataclasses
import enum
import io
import itertools
import json
import typing
import zipfile

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


def combine_shapefile_parts(
    basename: str,
    zip_buffer: typing.IO[bytes],
    shp: typing.IO[bytes],
    shx: typing.IO[bytes],
    dbf: typing.IO[bytes],
    prj: typing.IO[bytes],
) -> None:
    """
    Combine shapefile parts into a zip archive.

    Args:
        basename: Base name for the shapefile (without extension)
        zip_buffer: Writable bytes file-like object for the zip archive
        shp: Readable bytes file-like object containing .shp data
        shx: Readable bytes file-like object containing .shx data
        dbf: Readable bytes file-like object containing .dbf data
        prj: Readable bytes file-like object containing .prj data
    """
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_archive:
        # Reset buffers to beginning and stream each part
        for buffer_obj, extension in [
            (shp, ".shp"),
            (shx, ".shx"),
            (dbf, ".dbf"),
            (prj, ".prj"),
        ]:
            # Create ZipInfo for the file
            zinfo = zipfile.ZipInfo(f"{basename}{extension}")
            zinfo.compress_type = zipfile.ZIP_DEFLATED

            # Stream the data in chunks
            with zip_archive.open(zinfo, "w") as zip_file:
                while True:
                    chunk = buffer_obj.read(10 * 1024 * 1024)  # 10MB chunks
                    if not chunk:
                        break
                    zip_file.write(chunk)


def _parse_geometry_safely(geometry, geom_format: GeometryFormat) -> dict | None:
    """
    Safely parse geometry data from WKT or GeoJSON format.

    Args:
        geometry: Geometry data in WKT or GeoJSON format (string or dict)
        geom_format: Format of the geometry data

    Returns:
        dict: Parsed geometry dictionary, or None if parsing fails
    """
    if geometry is None:
        return None

    try:
        if geom_format == GeometryFormat.WKT:
            if isinstance(geometry, str):
                return wkt.loads(geometry)
            return geometry  # Already parsed
        else:
            if isinstance(geometry, str):
                return json.loads(geometry)
            return geometry  # Already parsed
    except (json.JSONDecodeError, TypeError, ValueError):
        # Return None for any parsing errors - this will result in null geometry
        return None


def _determine_shapetype_from_geometry(geometry, geom_format: GeometryFormat) -> int:
    """
    Determine the shapetype from geometry data.

    Args:
        geometry: Geometry data in WKT or GeoJSON format
        geom_format: Format of the geometry data

    Returns:
        int: pyshp shapetype constant
    """
    parsed_geometry = _parse_geometry_safely(geometry, geom_format)
    if parsed_geometry is None:
        return pyshp.NULL

    geom_type = parsed_geometry.get("type", "").upper()

    if geom_type in ("POINT", "MULTIPOINT"):
        return pyshp.MULTIPOINT
    elif geom_type in ("LINESTRING", "MULTILINESTRING"):
        return pyshp.POLYLINE
    elif geom_type in ("POLYGON", "MULTIPOLYGON"):
        return pyshp.POLYGON
    else:
        return pyshp.NULL


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
    # Create a buffer for rows we need to inspect to determine shapetype
    shapetype, inspected_rows, remaining_rows = pyshp.NULL, [], iter(rows)

    # Iterate until we find a non-null geometry to determine shapetype
    for row in remaining_rows:
        inspected_rows.append(row)
        geometry = row.get(geom_key)
        if geometry is not None:
            shapetype = _determine_shapetype_from_geometry(geometry, geom_format)
            break

    # Chain together inspected rows with remaining rows
    all_rows = itertools.chain(inspected_rows, remaining_rows)

    converter = _get_record_converter(schema)
    with pyshp.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapetype) as shpfile:
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
        for row in all_rows:
            geometry = row[geom_key]
            shape = _parse_geometry_safely(geometry, geom_format)

            # Check if geometry type matches the determined shapetype
            if shape is not None:
                shape_type = _determine_shapetype_from_geometry(geometry, geom_format)
                if shape_type != shapetype:
                    # Skip geometries that don't match the determined type
                    continue

            record = {}
            for field in schema:
                if field.name == geom_key:
                    continue
                try:
                    record[field.name] = converter[field.name](row.get(field.name))
                except Exception as e:
                    raise ValueError(f"Field '{field.name}' conversion error: {e}")
            shpfile.record(**record)
            if shape is not None:
                if shape["type"] == "Point":
                    # Shapefile distinguishes between single and multi points
                    shape["type"] = "Multi" + shape["type"]
                    shape["coordinates"] = [shape["coordinates"]]
                shpfile.shape(shape)
            else:
                shpfile.null()
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
        geometry = _parse_geometry_safely(geometry, geom_format)
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
        parsed_geometry = _parse_geometry_safely(geometry, geom_format)
        if parsed_geometry is not None:
            geometry = wkt.dumps(parsed_geometry)
        else:
            geometry = None  # Will result in empty geometry field
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
