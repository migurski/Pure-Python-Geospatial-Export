#!/usr/bin/env python3
"""
Pure Python Geospatial Export (PPGE) module for converting CSV data to various geospatial formats.
"""

import geomet
import geopackage
import geopandas
import pandas
import shapefile
import shapely
import json
from typing import Iterator, Dict, Any


def create_geodataframe_from_bigquery_rows(rows: Iterator[Dict[str, Any]]) -> geopandas.GeoDataFrame:
    """
    Create a GeoDataFrame from BigQuery row iterator with WKT geometry.
    
    Args:
        rows: Iterator yielding dictionaries with 'geom' and 'name' keys
        
    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with proper CRS
    """
    data = []
    geometries = []
    
    for row in rows:
        data.append({"name": row["name"]})
        geometries.append(row["geom"])
    
    gs = geopandas.GeoSeries.from_wkt(geometries)
    gdf = geopandas.GeoDataFrame(data, geometry=gs, crs="EPSG:4326")
    return gdf


def create_geodataframe_from_snowflake_rows(rows: Iterator[Dict[str, Any]]) -> geopandas.GeoDataFrame:
    """
    Create a GeoDataFrame from Snowflake row iterator with GeoJSON geometry.
    
    Args:
        rows: Iterator yielding dictionaries with 'GEOM' and 'NAME' keys
        
    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with proper CRS
    """
    data = []
    geometries = []
    
    for row in rows:
        data.append({"NAME": row["NAME"]})
        geometries.append(shapely.from_geojson(row["GEOM"]))
    
    gs = geopandas.GeoSeries(geometries)
    gdf = geopandas.GeoDataFrame(data, geometry=gs, crs="EPSG:4326")
    return gdf


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


# Legacy functions for backward compatibility
def create_geodataframe_from_bigquery(df: pandas.DataFrame) -> geopandas.GeoDataFrame:
    """
    Create a GeoDataFrame from BigQuery DataFrame with WKT geometry.
    
    Args:
        df: DataFrame with 'geom' and 'name' columns
        
    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with proper CRS
    """
    gs = geopandas.GeoSeries.from_wkt(df["geom"])
    gdf = geopandas.GeoDataFrame(df[["name"]], geometry=gs, crs="EPSG:4326")
    return gdf


def create_geodataframe_from_snowflake(df: pandas.DataFrame) -> geopandas.GeoDataFrame:
    """
    Create a GeoDataFrame from Snowflake DataFrame with GeoJSON geometry.
    
    Args:
        df: DataFrame with 'GEOM' and 'NAME' columns
        
    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with proper CRS
    """
    gs = geopandas.GeoSeries(df["GEOM"].apply(shapely.from_geojson))
    gdf = geopandas.GeoDataFrame(df[["NAME"]], geometry=gs, crs="EPSG:4326")
    return gdf


def export_to_geopackage(
    gdf: geopandas.GeoDataFrame, output_path: str, table_name: str = "geodata"
) -> None:
    """
    Export GeoDataFrame to GeoPackage format.
    
    Args:
        gdf: GeoDataFrame to export
        output_path: Path for the output GeoPackage file
        table_name: Name of the table within the GeoPackage
    """
    with geopackage.GeoPackage(output_path) as gpkg:
        tbl = gpkg.create(
            table_name,
            fields={"name": "TEXT"},
            wkid=4326,
            geometry_type="POLYGON",
            overwrite=True,
        )

        for idx, row in gdf.iterrows():
            name = row["name"] if "name" in row else row["NAME"]
            wkt = row.geometry.wkt
            tbl.insert({"name": name, "Shape": wkt}, geom_format="WKT")


def export_to_shapefile(gdf: geopandas.GeoDataFrame, output_path: str) -> None:
    """
    Export GeoDataFrame to Shapefile format.
    
    Args:
        gdf: GeoDataFrame to export
        output_path: Path for the output Shapefile (without extension)
    """
    with shapefile.Writer(f"{output_path}.shp", shapeType=5) as shp:
        shp.field("name", "C")

        for idx, row in gdf.iterrows():
            name = row["name"] if "name" in row else row["NAME"]
            shp.record(name=name)
            shp.poly(geomet.wkt.loads(row.geometry.wkt)["coordinates"])

    # Write projection file
    with open(f"{output_path}.prj", "w") as prj:
        prj.write(
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        )


def process_bigquery_to_geopackage(
    df: pandas.DataFrame, output_path: str, table_name: str = "geodata"
) -> None:
    """
    Process BigQuery DataFrame and export to GeoPackage.
    
    Args:
        df: BigQuery DataFrame with 'geom' and 'name' columns
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    gdf = create_geodataframe_from_bigquery(df)
    export_to_geopackage(gdf, output_path, table_name)


def process_snowflake_to_geopackage(
    df: pandas.DataFrame, output_path: str, table_name: str = "geodata"
) -> None:
    """
    Process Snowflake DataFrame and export to GeoPackage.
    
    Args:
        df: Snowflake DataFrame with 'GEOM' and 'NAME' columns
        output_path: Path for output GeoPackage
        table_name: Name of table in GeoPackage
    """
    gdf = create_geodataframe_from_snowflake(df)
    export_to_geopackage(gdf, output_path, table_name)


def process_bigquery_to_shapefile(df: pandas.DataFrame, output_path: str) -> None:
    """
    Process BigQuery DataFrame and export to Shapefile.
    
    Args:
        df: BigQuery DataFrame with 'geom' and 'name' columns
        output_path: Path for output Shapefile (without extension)
    """
    gdf = create_geodataframe_from_bigquery(df)
    export_to_shapefile(gdf, output_path)


def process_snowflake_to_shapefile(df: pandas.DataFrame, output_path: str) -> None:
    """
    Process Snowflake DataFrame and export to Shapefile.
    
    Args:
        df: Snowflake DataFrame with 'GEOM' and 'NAME' columns
        output_path: Path for output Shapefile (without extension)
    """
    gdf = create_geodataframe_from_snowflake(df)
    export_to_shapefile(gdf, output_path)
