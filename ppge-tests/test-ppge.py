#!/usr/bin/env python3
"""
Test cases for Pure Python Geospatial Export (PPGE) module.
"""

import os
import tempfile
import unittest
import pandas
import geopandas

from ppge.geospatial_export import (
    create_geodataframe_from_bigquery,
    create_geodataframe_from_snowflake,
    process_bigquery_to_geopackage,
    process_snowflake_to_geopackage,
    process_bigquery_to_shapefile,
    process_snowflake_to_shapefile,
)


def load_bigquery_csv(csv_path: str) -> pandas.DataFrame:
    """
    Load BigQuery CSV data with WKT geometry into a pandas DataFrame.
    
    Args:
        csv_path: Path to the CSV file containing WKT geometry data
        
    Returns:
        pandas.DataFrame: DataFrame with geometry and name columns
    """
    df = pandas.read_csv(csv_path)
    return df


def load_snowflake_csv(csv_path: str) -> pandas.DataFrame:
    """
    Load Snowflake CSV data with GeoJSON geometry into a pandas DataFrame.
    
    Args:
        csv_path: Path to the CSV file containing GeoJSON geometry data
        
    Returns:
        pandas.DataFrame: DataFrame with geometry and name columns
    """
    df = pandas.read_csv(csv_path)
    return df


class TestGeospatialExport(unittest.TestCase):
    """Test cases for geospatial export functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bigquery_csv = "wy-co-wkt-bigquery.csv"
        self.snowflake_csv = "wy-co-geojson-snowflake.csv"
        
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temporary files
        for filename in os.listdir(self.temp_dir):
            filepath = os.path.join(self.temp_dir, filename)
            try:
                if os.path.isfile(filepath):
                    os.unlink(filepath)
            except Exception as e:
                print(f"Error deleting {filepath}: {e}")
        
        try:
            os.rmdir(self.temp_dir)
        except Exception as e:
            print(f"Error deleting temp directory {self.temp_dir}: {e}")
    
    def test_1_bigquery_csv_to_geopackage(self):
        """Test loading BigQuery CSV into a dataframe and convert its rows into a GeoPackage export."""
        # Test data loading
        df = load_bigquery_csv(self.bigquery_csv)
        self.assertIsInstance(df, pandas.DataFrame)
        self.assertIn("geom", df.columns)
        self.assertIn("name", df.columns)
        self.assertEqual(len(df), 2)  # Should have 2 rows (Wyoming and Colorado)
        
        # Test GeoDataFrame creation
        gdf = create_geodataframe_from_bigquery(df)
        self.assertIsInstance(gdf, geopandas.GeoDataFrame)
        self.assertEqual(len(gdf), 2)
        self.assertEqual(gdf.crs, "EPSG:4326")
        
        # Test GeoPackage export
        output_path = os.path.join(self.temp_dir, "test_bigquery.gpkg")
        process_bigquery_to_geopackage(df, output_path, "test_table")
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)
    
    def test_2_snowflake_csv_to_geopackage(self):
        """Test loading Snowflake CSV into a dataframe and convert its rows into a GeoPackage export."""
        # Test data loading
        df = load_snowflake_csv(self.snowflake_csv)
        self.assertIsInstance(df, pandas.DataFrame)
        self.assertIn("GEOM", df.columns)
        self.assertIn("NAME", df.columns)
        self.assertEqual(len(df), 2)  # Should have 2 rows (Wyoming and Colorado)
        
        # Test GeoDataFrame creation
        gdf = create_geodataframe_from_snowflake(df)
        self.assertIsInstance(gdf, geopandas.GeoDataFrame)
        self.assertEqual(len(gdf), 2)
        self.assertEqual(gdf.crs, "EPSG:4326")
        
        # Test GeoPackage export
        output_path = os.path.join(self.temp_dir, "test_snowflake.gpkg")
        process_snowflake_to_geopackage(df, output_path, "test_table")
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)
    
    def test_3_bigquery_csv_to_shapefile(self):
        """Test loading BigQuery CSV into a dataframe and convert its rows into a Shapefile export."""
        # Test data loading
        df = load_bigquery_csv(self.bigquery_csv)
        self.assertIsInstance(df, pandas.DataFrame)
        self.assertIn("geom", df.columns)
        self.assertIn("name", df.columns)
        self.assertEqual(len(df), 2)
        
        # Test GeoDataFrame creation
        gdf = create_geodataframe_from_bigquery(df)
        self.assertIsInstance(gdf, geopandas.GeoDataFrame)
        self.assertEqual(len(gdf), 2)
        self.assertEqual(gdf.crs, "EPSG:4326")
        
        # Test Shapefile export
        output_path = os.path.join(self.temp_dir, "test_bigquery")
        process_bigquery_to_shapefile(df, output_path)
        
        # Verify files were created (.shp, .shx, .dbf, .prj)
        shp_file = f"{output_path}.shp"
        shx_file = f"{output_path}.shx"
        dbf_file = f"{output_path}.dbf"
        prj_file = f"{output_path}.prj"
        
        self.assertTrue(os.path.exists(shp_file))
        self.assertTrue(os.path.exists(shx_file))
        self.assertTrue(os.path.exists(dbf_file))
        self.assertTrue(os.path.exists(prj_file))
        
        # Verify files have content
        self.assertGreater(os.path.getsize(shp_file), 0)
        self.assertGreater(os.path.getsize(shx_file), 0)
        self.assertGreater(os.path.getsize(dbf_file), 0)
        self.assertGreater(os.path.getsize(prj_file), 0)
    
    def test_4_snowflake_csv_to_shapefile(self):
        """Test loading Snowflake CSV into a dataframe and convert its rows into a Shapefile export."""
        # Test data loading
        df = load_snowflake_csv(self.snowflake_csv)
        self.assertIsInstance(df, pandas.DataFrame)
        self.assertIn("GEOM", df.columns)
        self.assertIn("NAME", df.columns)
        self.assertEqual(len(df), 2)
        
        # Test GeoDataFrame creation
        gdf = create_geodataframe_from_snowflake(df)
        self.assertIsInstance(gdf, geopandas.GeoDataFrame)
        self.assertEqual(len(gdf), 2)
        self.assertEqual(gdf.crs, "EPSG:4326")
        
        # Test Shapefile export
        output_path = os.path.join(self.temp_dir, "test_snowflake")
        process_snowflake_to_shapefile(df, output_path)
        
        # Verify files were created (.shp, .shx, .dbf, .prj)
        shp_file = f"{output_path}.shp"
        shx_file = f"{output_path}.shx"
        dbf_file = f"{output_path}.dbf"
        prj_file = f"{output_path}.prj"
        
        self.assertTrue(os.path.exists(shp_file))
        self.assertTrue(os.path.exists(shx_file))
        self.assertTrue(os.path.exists(dbf_file))
        self.assertTrue(os.path.exists(prj_file))
        
        # Verify files have content
        self.assertGreater(os.path.getsize(shp_file), 0)
        self.assertGreater(os.path.getsize(shx_file), 0)
        self.assertGreater(os.path.getsize(dbf_file), 0)
        self.assertGreater(os.path.getsize(prj_file), 0)


if __name__ == "__main__":
    unittest.main()
