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
