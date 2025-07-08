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
import json
import ppge


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

    def validate_geojson_data(self, geojson_path, name_column="name"):
        """
        Validate that the exported GeoJSON data contains the expected states and geometry.

        Args:
            geojson_path: Path to the GeoJSON file
            name_column: Name of the column containing state names
        """
        # Read GeoJSON file
        with open(geojson_path, "r") as f:
            geojson_data = json.load(f)

        # Check that it's a FeatureCollection
        self.assertEqual(geojson_data["type"], "FeatureCollection")

        # Check that we have exactly 2 features
        self.assertEqual(len(geojson_data["features"]), 2)

        # Check first feature (Wyoming)
        wyoming_feature = geojson_data["features"][0]
        self.assertEqual(wyoming_feature["properties"][name_column], "Wyoming")
        self.assertEqual(wyoming_feature["type"], "Feature")
        self.assertEqual(wyoming_feature["geometry"]["type"], "Polygon")

        # Check second feature (Colorado)
        colorado_feature = geojson_data["features"][1]
        self.assertEqual(colorado_feature["properties"][name_column], "Colorado")
        self.assertEqual(colorado_feature["type"], "Feature")
        self.assertEqual(colorado_feature["geometry"]["type"], "Polygon")

    def validate_csv_data(
        self, csv_path, name_column="name", geometry_column="geometry"
    ):
        """
        Validate that the exported CSV data contains the expected states and WKT geometry.

        Args:
            csv_path: Path to the CSV file
            name_column: Name of the column containing state names
            geometry_column: Name of the column containing WKT geometry
        """
        # Read CSV file
        with open(csv_path, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

        # Check that we have exactly 2 rows
        self.assertEqual(len(rows), 2)

        # Check first row (Wyoming)
        wyoming_row = rows[0]
        self.assertEqual(wyoming_row[name_column], "Wyoming")
        self.assertIn(geometry_column, wyoming_row)
        self.assertTrue(wyoming_row[geometry_column].startswith("POLYGON"))

        # Check that Wyoming polygon contains the specified point
        wyoming_geom = shapely.from_wkt(wyoming_row[geometry_column])
        wyoming_point = shapely.Point(-107.5, 43.0)
        self.assertTrue(wyoming_geom.contains(wyoming_point))

        # Check second row (Colorado)
        colorado_row = rows[1]
        self.assertEqual(colorado_row[name_column], "Colorado")
        self.assertIn(geometry_column, colorado_row)
        self.assertTrue(colorado_row[geometry_column].startswith("POLYGON"))

        # Check that Colorado polygon contains the specified point
        colorado_geom = shapely.from_wkt(colorado_row[geometry_column])
        colorado_point = shapely.Point(-105.8, 39.1)
        self.assertTrue(colorado_geom.contains(colorado_point))

    def test_bigquery_rows_to_geopackage(self):
        """Test BigQuery CSV row iterator to GeoPackage export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])

        output_path = os.path.join(self.temp_dir, "test_bigquery_rows.gpkg")
        ppge.process_bigquery_rows_to_geopackage(rows, output_path, "test_table")

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
        ppge.process_snowflake_rows_to_geopackage(rows, output_path, "test_table")

        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)

        # Validate the exported data using geopandas
        gdf = geopandas.read_file(output_path)
        self.validate_exported_data(gdf, "NAME")

    def test_bigquery_rows_to_shapefile(self):
        """Test BigQuery CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])

        output_path = os.path.join(self.temp_dir, "test_bigquery_rows")
        ppge.process_bigquery_rows_to_shapefile(rows, output_path)

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
        ppge.process_snowflake_rows_to_shapefile(rows, output_path)

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
        self.validate_exported_data(gdf, "NAME")

    def test_bigquery_rows_to_geojson(self):
        """Test BigQuery CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])

        output_path = os.path.join(self.temp_dir, "test_bigquery_rows.geojson")
        ppge.process_bigquery_rows_to_geojson(rows, output_path)

        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)

        # Validate the exported data using GeoJSON validation
        self.validate_geojson_data(output_path, "name")

    def test_snowflake_rows_to_geojson(self):
        """Test Snowflake CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])

        output_path = os.path.join(self.temp_dir, "test_snowflake_rows.geojson")
        ppge.process_snowflake_rows_to_geojson(rows, output_path)

        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)

        # Validate the exported data using GeoJSON validation
        self.validate_geojson_data(output_path, "NAME")

    def test_bigquery_rows_to_csv(self):
        """Test BigQuery CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])

        output_path = os.path.join(self.temp_dir, "test_bigquery_rows.csv")
        ppge.process_bigquery_rows_to_csv(rows, output_path)

        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)

        # Validate the exported data using CSV validation
        self.validate_csv_data(output_path, "name", "geometry")

    def test_snowflake_rows_to_csv(self):
        """Test Snowflake CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])

        output_path = os.path.join(self.temp_dir, "test_snowflake_rows.csv")
        ppge.process_snowflake_rows_to_csv(rows, output_path)

        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        self.assertGreater(os.path.getsize(output_path), 0)

        # Validate the exported data using CSV validation
        self.validate_csv_data(output_path, "NAME", "geometry")


if __name__ == "__main__":
    unittest.main()
