#!/usr/bin/env python3
"""
Test cases for Pure Python Geospatial Export (PPGE) module.
"""

import os
import tempfile
import unittest
import csv
import geopandas
import shapely

from ppge.geospatial_export import (
    process_bigquery_rows_to_geopackage,
    process_snowflake_rows_to_geopackage,
    process_bigquery_rows_to_shapefile,
    process_snowflake_rows_to_shapefile,
)


def csv_row_iterator(csv_path: str):
    """
    Generic iterator that yields row dictionaries from a CSV file.
    Args:
        csv_path: Path to the CSV file
    Yields:
        dict: Row dictionaries with all columns
    """
    with open(csv_path, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row


class TestGeospatialExport(unittest.TestCase):
    """Test cases for geospatial export functionality using row iterators."""

    def setUp(self):
        self.bigquery_csv = "wy-co-wkt-bigquery.csv"
        self.snowflake_csv = "wy-co-geojson-snowflake.csv"
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
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

    def validate_exported_data(self, gdf, name_column="name"):
        """
        Validate that the exported data contains the expected states and geometry.
        
        Args:
            gdf: GeoDataFrame to validate
            name_column: Name of the column containing state names
        """
        # Check that we have exactly 2 rows
        self.assertEqual(len(gdf), 2)
        
        # Check first row (Wyoming)
        wyoming_row = gdf.iloc[0]
        self.assertEqual(wyoming_row[name_column], "Wyoming")
        
        # Check that Wyoming polygon contains the specified point
        wyoming_point = shapely.Point(-107.5, 43.0)
        self.assertTrue(wyoming_row.geometry.contains(wyoming_point))
        
        # Check second row (Colorado)
        colorado_row = gdf.iloc[1]
        self.assertEqual(colorado_row[name_column], "Colorado")
        
        # Check that Colorado polygon contains the specified point
        colorado_point = shapely.Point(-105.8, 39.1)
        self.assertTrue(colorado_row.geometry.contains(colorado_point))

    def test_bigquery_rows_to_geopackage(self):
        """Test BigQuery CSV row iterator to GeoPackage export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        
        output_path = os.path.join(self.temp_dir, "test_bigquery_rows.gpkg")
        process_bigquery_rows_to_geopackage(rows, output_path, "test_table")
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)
        
        # Validate the exported data using geopandas
        gdf = geopandas.read_file(output_path)
        self.validate_exported_data(gdf, "name")

    def test_snowflake_rows_to_geopackage(self):
        """Test Snowflake CSV row iterator to GeoPackage export."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        
        output_path = os.path.join(self.temp_dir, "test_snowflake_rows.gpkg")
        process_snowflake_rows_to_geopackage(rows, output_path, "test_table")
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)
        
        # Validate the exported data using geopandas
        gdf = geopandas.read_file(output_path)
        self.validate_exported_data(gdf, "name")

    def test_bigquery_rows_to_shapefile(self):
        """Test BigQuery CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        
        output_path = os.path.join(self.temp_dir, "test_bigquery_rows")
        process_bigquery_rows_to_shapefile(rows, output_path)
        
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
        
        # Validate the exported data using geopandas
        gdf = geopandas.read_file(shp_file)
        self.validate_exported_data(gdf, "name")

    def test_snowflake_rows_to_shapefile(self):
        """Test Snowflake CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        
        output_path = os.path.join(self.temp_dir, "test_snowflake_rows")
        process_snowflake_rows_to_shapefile(rows, output_path)
        
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
        
        # Validate the exported data using geopandas
        gdf = geopandas.read_file(shp_file)
        self.validate_exported_data(gdf, "name")


if __name__ == "__main__":
    unittest.main()
