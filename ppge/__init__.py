#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import geomet
import geopackage
import shapefile
import shapely
import json
from typing import Iterator, Dict, Any
from enum import Enum


class GeometryFormat(Enum):
    """Enumeration of supported geometry formats."""
    WKT = "WKT"
    GEOJSON = "GeoJSON"


def export_to_geopackage_from_rows(
    rows: Iterator[Dict[str, Any]], 
    output_path: str, 
    table_name: str = "geodata",
    geom_key: str = "geom",
    geom_format: GeometryFormat = GeometryFormat.WKT
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
    # Get sample row to determine fields
    sample_rows = list(rows)
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

        for row in sample_rows:
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
    geom_format: GeometryFormat = GeometryFormat.WKT
) -> None:
    """
    Export row iterator to Shapefile format.
    
    Args:
        rows: Iterator yielding dictionaries with geometry and other data
        output_path: Path for the output Shapefile (without extension)
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data (WKT or GeoJSON)
    """
    # Get sample row to determine fields
    sample_rows = list(rows)
    if not sample_rows:
        return
    
    with shapefile.Writer(f"{output_path}.shp", shapeType=5) as shp:
        # Add fields for all non-geometry columns
        for key, value in sample_rows[0].items():
            if key != geom_key:
                if isinstance(value, str):
                    shp.field(key, 'C')
                elif isinstance(value, int):
                    shp.field(key, 'N')
                elif isinstance(value, float):
                    shp.field(key, 'F')
                else:
                    shp.field(key, 'C')  # Default to character field

        for row in sample_rows:
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
    rows: Iterator[Dict[str, Any]], 
    output_path: str, 
    table_name: str = "geodata"
) -> None:
    """
    Process BigQuery row iterator and export to GeoPackage.
    
    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(rows, output_path, table_name, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_geopackage(
    rows: Iterator[Dict[str, Any]], 
    output_path: str, 
    table_name: str = "geodata"
) -> None:
    """
    Process Snowflake row iterator and export to GeoPackage.
    
    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(rows, output_path, table_name, "GEOM", GeometryFormat.GEOJSON)


def process_bigquery_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], 
    output_path: str
) -> None:
    """
    Process BigQuery row iterator and export to Shapefile.
    
    Args:
        rows: Iterator yielding dictionaries with 'geom' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "geom", GeometryFormat.WKT)


def process_snowflake_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], 
    output_path: str
) -> None:
    """
    Process Snowflake row iterator and export to Shapefile.
    
    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and other fields
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "GEOM", GeometryFormat.GEOJSON)
