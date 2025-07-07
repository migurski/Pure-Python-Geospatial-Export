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


def export_to_geopackage_from_rows(
    rows: Iterator[Dict[str, Any]], 
    output_path: str, 
    table_name: str = "geodata",
    name_key: str = "name",
    geom_key: str = "geom",
    geom_format: str = "WKT"
) -> None:
    """
    Export row iterator to GeoPackage format.
    
    Args:
        rows: Iterator yielding dictionaries with name and geometry data
        output_path: Path for the output GeoPackage file
        table_name: Name of the table within the GeoPackage
        name_key: Key for the name field in the row dictionary
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data ("WKT" or "GeoJSON")
    """
    with geopackage.GeoPackage(output_path) as gpkg:
        tbl = gpkg.create(
            table_name,
            fields={"name": "TEXT"},
            wkid=4326,
            geometry_type="POLYGON",
            overwrite=True,
        )

        for row in rows:
            name = row[name_key]
            geometry = row[geom_key]
            
            # Convert GeoJSON string to dictionary if needed
            if geom_format == "GeoJSON" and isinstance(geometry, str):
                geometry = json.loads(geometry)
            
            tbl.insert({"name": name, "Shape": geometry}, geom_format=geom_format)


def export_to_shapefile_from_rows(
    rows: Iterator[Dict[str, Any]], 
    output_path: str,
    name_key: str = "name",
    geom_key: str = "geom",
    geom_format: str = "WKT"
) -> None:
    """
    Export row iterator to Shapefile format.
    
    Args:
        rows: Iterator yielding dictionaries with name and geometry data
        output_path: Path for the output Shapefile (without extension)
        name_key: Key for the name field in the row dictionary
        geom_key: Key for the geometry field in the row dictionary
        geom_format: Format of geometry data ("WKT" or "GeoJSON")
    """
    with shapefile.Writer(f"{output_path}.shp", shapeType=5) as shp:
        shp.field("name", "C")

        for row in rows:
            name = row[name_key]
            geometry = row[geom_key]
            
            if geom_format == "WKT":
                coords = geomet.wkt.loads(geometry)["coordinates"]
            else:  # GeoJSON
                coords = shapely.from_geojson(geometry).__geo_interface__["coordinates"]
            
            shp.record(name=name)
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
        rows: Iterator yielding dictionaries with 'geom' and 'name' keys
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(rows, output_path, table_name, "name", "geom", "WKT")


def process_snowflake_rows_to_geopackage(
    rows: Iterator[Dict[str, Any]], 
    output_path: str, 
    table_name: str = "geodata"
) -> None:
    """
    Process Snowflake row iterator and export to GeoPackage.
    
    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and 'NAME' keys
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    export_to_geopackage_from_rows(rows, output_path, table_name, "NAME", "GEOM", "GeoJSON")


def process_bigquery_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], 
    output_path: str
) -> None:
    """
    Process BigQuery row iterator and export to Shapefile.
    
    Args:
        rows: Iterator yielding dictionaries with 'geom' and 'name' keys
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "name", "geom", "WKT")


def process_snowflake_rows_to_shapefile(
    rows: Iterator[Dict[str, Any]], 
    output_path: str
) -> None:
    """
    Process Snowflake row iterator and export to Shapefile.
    
    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and 'NAME' keys
        output_path: Path for output Shapefile (without extension)
    """
    export_to_shapefile_from_rows(rows, output_path, "NAME", "GEOM", "GeoJSON")
