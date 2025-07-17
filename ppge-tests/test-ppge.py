#!/usr/bin/env python3
"""
Test cases for Pure Python Geospatial Export (PPGE) module.
"""

import os
import tempfile
import unittest
import csv
import json
import io

import shapely

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

    def validate_geojson_data(self, buf, name_column="name"):
        """
        Validate that the exported GeoJSON data contains the expected states and geometry.

        Args:
            buf: BytesIO buffer containing GeoJSON data
            name_column: Name of the column containing state names
        """
        buf.seek(0)
        text = buf.read().decode("utf-8")
        geojson_data = json.loads(text)

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

    def validate_csv_data(self, buf, name_column="name", geometry_column="geometry"):
        """
        Validate that the exported CSV data contains the expected states and WKT geometry.

        Args:
            buf: BytesIO buffer containing CSV data
            name_column: Name of the column containing state names
            geometry_column: Name of the column containing WKT geometry
        """
        buf.seek(0)
        text = buf.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
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

    def test_bigquery_rows_to_shapefile(self):
        """Test BigQuery CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        shp = io.BytesIO()
        shx = io.BytesIO()
        dbf = io.BytesIO()
        prj = io.BytesIO()
        ppge.process_bigquery_rows_to_shapefile(schema, rows, shp, shx, dbf, prj)
        # Save buffers to files for geopandas
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_bigquery_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], [shp, shx, dbf, prj]):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "name")

    def test_snowflake_rows_to_shapefile(self):
        """Test Snowflake CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        schema = [
            ppge.Field("GEOM", ppge.FieldType.GEOG, False),
            ppge.Field("NAME", ppge.FieldType.STR, False),
        ]
        shp = io.BytesIO()
        shx = io.BytesIO()
        dbf = io.BytesIO()
        prj = io.BytesIO()
        ppge.process_snowflake_rows_to_shapefile(schema, rows, shp, shx, dbf, prj)
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_snowflake_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], [shp, shx, dbf, prj]):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "NAME")

    def test_bigquery_rows_to_geojson(self):
        """Test BigQuery CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()
        ppge.process_bigquery_rows_to_geojson(schema, rows, buf)
        self.assertGreater(len(buf.getvalue()), 0)
        self.validate_geojson_data(buf, "name")

    def test_snowflake_rows_to_geojson(self):
        """Test Snowflake CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        schema = [
            ppge.Field("GEOM", ppge.FieldType.GEOG, False),
            ppge.Field("NAME", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()
        ppge.process_snowflake_rows_to_geojson(schema, rows, buf)
        self.assertGreater(len(buf.getvalue()), 0)
        self.validate_geojson_data(buf, "NAME")

    def test_bigquery_rows_to_csv(self):
        """Test BigQuery CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(self.bigquery_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()
        ppge.process_bigquery_rows_to_csv(schema, rows, buf)
        self.assertGreater(len(buf.getvalue()), 0)
        self.validate_csv_data(buf, "name", "geometry")

    def test_snowflake_rows_to_csv(self):
        """Test Snowflake CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(self.snowflake_csv))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        schema = [
            ppge.Field("GEOM", ppge.FieldType.GEOG, False),
            ppge.Field("NAME", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()
        ppge.process_snowflake_rows_to_csv(schema, rows, buf)
        self.assertGreater(len(buf.getvalue()), 0)
        self.validate_csv_data(buf, "NAME", "geometry")

    def test_shapefile_valueerror_on_type(self):
        rows = [
            {"geom": "POINT(0 0)", "name": "Wyoming"},
            {"geom": "POINT(1 1)", "name": "Colorado"},
        ]
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.INT, False),
        ]
        shp = io.BytesIO()
        shx = io.BytesIO()
        dbf = io.BytesIO()
        prj = io.BytesIO()
        with self.assertRaises(ValueError):
            ppge.process_bigquery_rows_to_shapefile(schema, rows, shp, shx, dbf, prj)

    def test_geojson_valueerror_on_type(self):
        rows = [
            {"geom": "POINT(0 0)", "name": "Wyoming"},
            {"geom": "POINT(1 1)", "name": "Colorado"},
        ]
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.INT, False),
        ]
        buf = io.BytesIO()
        with self.assertRaises(ValueError):
            ppge.process_bigquery_rows_to_geojson(schema, rows, buf)

    def test_csv_valueerror_on_type(self):
        rows = [
            {"geom": "POINT(0 0)", "name": "Wyoming"},
            {"geom": "POINT(1 1)", "name": "Colorado"},
        ]
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.INT, False),
        ]
        buf = io.BytesIO()
        with self.assertRaises(ValueError):
            ppge.process_bigquery_rows_to_csv(schema, rows, buf)


if __name__ == "__main__":
    unittest.main()
