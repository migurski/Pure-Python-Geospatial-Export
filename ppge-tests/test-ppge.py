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
import zipfile

import shapely

import ppge


BIGQUERY_POLY_CSV = "wy-co-wkt-bigquery.csv"
SNOWFLAKE_POLY_CSV = "wy-co-geojson-snowflake.csv"
BIGQUERY_POINT_CSV = "denver-cheyenne-wkt-bigquery.csv"
SNOWFLAKE_POINT_CSV = "denver-cheyenne-geojson-snowflake.csv"
BIGQUERY_MALFORMED_CSV = "wy-co-wkt-malformed.csv"


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

        # Check that Wyoming geometry intersects Cheyenne
        cheyenne_poly = shapely.Point(-104.8, 41.1).buffer(1)
        self.assertTrue(wyoming_row.geometry.intersects(cheyenne_poly))

        # Check second row (Colorado)
        colorado_row = gdf.iloc[1]
        self.assertEqual(colorado_row[name_column], "Colorado")

        # Check that Colorado geometry intersects Denver
        denver_poly = shapely.Point(-105.0, 39.7).buffer(1)
        self.assertTrue(colorado_row.geometry.intersects(denver_poly))

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
        self.assertIn(wyoming_feature["geometry"]["type"], ("Point", "Polygon"))

        # Check second feature (Colorado)
        colorado_feature = geojson_data["features"][1]
        self.assertEqual(colorado_feature["properties"][name_column], "Colorado")
        self.assertEqual(colorado_feature["type"], "Feature")
        self.assertIn(colorado_feature["geometry"]["type"], ("Point", "Polygon"))

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
        self.assertTrue(wyoming_row[geometry_column].startswith("PO"))

        # Check that Wyoming geometry intersects Cheyenne
        wyoming_geom = shapely.from_wkt(wyoming_row[geometry_column])
        cheyenne_poly = shapely.Point(-104.8, 41.1).buffer(1)
        self.assertTrue(wyoming_geom.intersects(cheyenne_poly))

        # Check second row (Colorado)
        colorado_row = rows[1]
        self.assertEqual(colorado_row[name_column], "Colorado")
        self.assertIn(geometry_column, colorado_row)
        self.assertTrue(colorado_row[geometry_column].startswith("PO"))

        # Check that Colorado geometry intersects Denver
        colorado_geom = shapely.from_wkt(colorado_row[geometry_column])
        denver_poly = shapely.Point(-105.0, 39.7).buffer(1)
        self.assertTrue(colorado_geom.intersects(denver_poly))

    def test_bigquery_polygon_rows_to_shapefile(self):
        """Test BigQuery CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(BIGQUERY_POLY_CSV))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()
        ppge.process_bigquery_rows_to_shapefile(schema, rows, *fs)
        # Save buffers to files for geopandas
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_bigquery_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], fs):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "name")

    def test_bigquery_point_rows_to_shapefile(self):
        """Test BigQuery CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(BIGQUERY_POINT_CSV))
        self.assertEqual(len(rows), 2)
        self.assertIn("geom", rows[0])
        self.assertIn("name", rows[0])
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()
        ppge.process_bigquery_rows_to_shapefile(schema, rows, *fs)
        # Save buffers to files for geopandas
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_bigquery_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], fs):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "name")

    def test_snowflake_polygon_rows_to_shapefile(self):
        """Test Snowflake CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(SNOWFLAKE_POLY_CSV))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        schema = [
            ppge.Field("GEOM", ppge.FieldType.GEOG, False),
            ppge.Field("NAME", ppge.FieldType.STR, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()
        ppge.process_snowflake_rows_to_shapefile(schema, rows, *fs)
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_snowflake_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], fs):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "NAME")

    def test_snowflake_point_rows_to_shapefile(self):
        """Test Snowflake CSV row iterator to Shapefile export."""
        rows = list(csv_row_iterator(SNOWFLAKE_POINT_CSV))
        self.assertEqual(len(rows), 2)
        self.assertIn("GEOM", rows[0])
        self.assertIn("NAME", rows[0])
        schema = [
            ppge.Field("GEOM", ppge.FieldType.GEOG, False),
            ppge.Field("NAME", ppge.FieldType.STR, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()
        ppge.process_snowflake_rows_to_shapefile(schema, rows, *fs)
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_snowflake_rows")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], fs):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")
            self.validate_exported_data(gdf, "NAME")

    def test_bigquery_polygon_rows_to_geojson(self):
        """Test BigQuery CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(BIGQUERY_POLY_CSV))
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

    def test_bigquery_point_rows_to_geojson(self):
        """Test BigQuery CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(BIGQUERY_POINT_CSV))
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

    def test_snowflake_polygon_rows_to_geojson(self):
        """Test Snowflake CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(SNOWFLAKE_POLY_CSV))
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

    def test_snowflake_point_rows_to_geojson(self):
        """Test Snowflake CSV row iterator to GeoJSON export."""
        rows = list(csv_row_iterator(SNOWFLAKE_POINT_CSV))
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

    def test_bigquery_polygon_rows_to_csv(self):
        """Test BigQuery CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(BIGQUERY_POLY_CSV))
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

    def test_bigquery_point_rows_to_csv(self):
        """Test BigQuery CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(BIGQUERY_POINT_CSV))
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

    def test_snowflake_polygon_rows_to_csv(self):
        """Test Snowflake CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(SNOWFLAKE_POLY_CSV))
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

    def test_snowflake_point_rows_to_csv(self):
        """Test Snowflake CSV row iterator to CSV export with WKT geometry."""
        rows = list(csv_row_iterator(SNOWFLAKE_POINT_CSV))
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

    def test_export_to_shapefile_from_rows_with_literals(self):
        """Test export_to_shapefile_from_rows with row literals."""
        rows = [
            {"geom": "POINT(-104.8 41.1)", "name": "Cheyenne"},
            {"geom": "POINT(-105.0 39.7)", "name": "Denver"},
            {
                "geom": "POLYGON((-109 37, -102 37, -102 41, -109 41, -109 37))",
                "name": "Colorado",
            },
            {"geom": None, "name": "Null Geometry"},
        ]
        schema = [
            ppge.Field(
                "geom", ppge.FieldType.GEOG, True
            ),  # Make nullable to allow null geometry
            ppge.Field("name", ppge.FieldType.STR, False),
        ]

        # Create buffers for shapefile parts
        shp_buf = io.BytesIO()
        shx_buf = io.BytesIO()
        dbf_buf = io.BytesIO()
        prj_buf = io.BytesIO()

        # Export to shapefile
        ppge.export_to_shapefile_from_rows(
            schema,
            rows,
            shp_buf,
            shx_buf,
            dbf_buf,
            prj_buf,
            "geom",
            ppge.GeometryFormat.WKT,
        )

        # Save buffers to files for geopandas validation
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_literals")
            for ext, buf in zip(
                ["shp", "shx", "dbf", "prj"], [shp_buf, shx_buf, dbf_buf, prj_buf]
            ):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")

            # Validate the exported data - should have 3 rows (2 points, 1 null, polygon filtered out)
            self.assertEqual(len(gdf), 3)

            # Check first row (Cheyenne)
            cheyenne_row = gdf.iloc[0]
            self.assertEqual(cheyenne_row["name"], "Cheyenne")

            # Check that Cheyenne point is at expected location
            cheyenne_point = shapely.Point(-104.8, 41.1)
            self.assertTrue(cheyenne_row.geometry.equals(cheyenne_point))

            # Check second row (Denver)
            denver_row = gdf.iloc[1]
            self.assertEqual(denver_row["name"], "Denver")

            # Check that Denver point is at expected location
            denver_point = shapely.Point(-105.0, 39.7)
            self.assertTrue(denver_row.geometry.equals(denver_point))

            # Check third row (Null Geometry)
            null_row = gdf.iloc[2]
            self.assertEqual(null_row["name"], "Null Geometry")
            self.assertTrue(null_row.geometry is None or null_row.geometry.is_empty)

    def test_combine_shapefile_parts(self):
        """Test combining shapefile parts into a zip archive."""
        # Create test data for each shapefile part
        shp_data = b"test shp data"
        shx_data = b"test shx data"
        dbf_data = b"test dbf data"
        prj_data = b"test prj data"

        # Create buffers with test data
        shp_buf = io.BytesIO(shp_data)
        shx_buf = io.BytesIO(shx_data)
        dbf_buf = io.BytesIO(dbf_data)
        prj_buf = io.BytesIO(prj_data)
        zip_buf = io.BytesIO()

        # Combine the parts
        ppge.combine_shapefile_parts(
            "test_shapefile", zip_buf, shp_buf, shx_buf, dbf_buf, prj_buf
        )

        # Verify the zip archive contains all parts
        zip_buf.seek(0)
        with zipfile.ZipFile(zip_buf, "r") as zip_archive:
            # Check that all expected files are present
            expected_files = [
                "test_shapefile.shp",
                "test_shapefile.shx",
                "test_shapefile.dbf",
                "test_shapefile.prj",
            ]
            actual_files = zip_archive.namelist()
            self.assertEqual(set(actual_files), set(expected_files))

            # Check that file contents match original data
            self.assertEqual(zip_archive.read("test_shapefile.shp"), shp_data)
            self.assertEqual(zip_archive.read("test_shapefile.shx"), shx_data)
            self.assertEqual(zip_archive.read("test_shapefile.dbf"), dbf_data)
            self.assertEqual(zip_archive.read("test_shapefile.prj"), prj_data)

    def test_shapefile_valueerror_on_type(self):
        rows = [
            {"geom": "POINT(0 0)", "name": "Wyoming"},
            {"geom": "POINT(1 1)", "name": "Colorado"},
        ]
        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.INT, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()
        with self.assertRaises(ValueError):
            ppge.process_bigquery_rows_to_shapefile(schema, rows, *fs)

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

    def test_malformed_wkt_rows_to_shapefile(self):
        """Test that malformed WKT is handled gracefully in shapefile export."""
        rows = list(csv_row_iterator(BIGQUERY_MALFORMED_CSV))
        self.assertEqual(len(rows), 3)  # 2 valid + 1 malformed

        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        fs = io.BytesIO(), io.BytesIO(), io.BytesIO(), io.BytesIO()

        # This should not raise an error despite malformed WKT
        ppge.process_bigquery_rows_to_shapefile(schema, rows, *fs)

        # Verify the output contains only the 2 valid geometries
        with tempfile.TemporaryDirectory() as temp_dir:
            base = os.path.join(temp_dir, "test_malformed_wkt")
            for ext, buf in zip(["shp", "shx", "dbf", "prj"], fs):
                with open(f"{base}.{ext}", "wb") as f:
                    buf.seek(0)
                    f.write(buf.read())

            import geopandas

            gdf = geopandas.read_file(f"{base}.shp")

            # Should have 2 valid rows (malformed becomes null and gets skipped or has null geometry)
            self.assertLessEqual(len(gdf), 3)  # At most 3 rows (2 valid + 1 null)

            # Check that we have Wyoming and Colorado
            valid_names = [
                row.name
                for row in gdf.itertuples()
                if row.geometry is not None and not row.geometry.is_empty
            ]
            self.assertIn("Wyoming", valid_names)
            self.assertIn("Colorado", valid_names)

    def test_malformed_wkt_rows_to_geojson(self):
        """Test that malformed WKT is handled gracefully in GeoJSON export."""
        rows = list(csv_row_iterator(BIGQUERY_MALFORMED_CSV))
        self.assertEqual(len(rows), 3)

        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()

        # This should not raise an error despite malformed WKT
        ppge.process_bigquery_rows_to_geojson(schema, rows, buf)

        # Verify the output
        buf.seek(0)
        text = buf.read().decode("utf-8")
        geojson_data = json.loads(text)

        self.assertEqual(geojson_data["type"], "FeatureCollection")
        features = geojson_data["features"]

        # Should have 3 features (2 valid + 1 with null geometry)
        self.assertEqual(len(features), 3)

        # Check valid features
        valid_features = [f for f in features if f["geometry"] is not None]
        self.assertEqual(len(valid_features), 2)

        feature_names = [f["properties"]["name"] for f in valid_features]
        self.assertIn("Wyoming", feature_names)
        self.assertIn("Colorado", feature_names)

        # Check that malformed feature has null geometry
        malformed_feature = [
            f for f in features if f["properties"]["name"] == "BadState"
        ][0]
        self.assertIsNone(malformed_feature["geometry"])

    def test_malformed_wkt_rows_to_csv(self):
        """Test that malformed WKT is handled gracefully in CSV export."""
        rows = list(csv_row_iterator(BIGQUERY_MALFORMED_CSV))
        self.assertEqual(len(rows), 3)

        schema = [
            ppge.Field("geom", ppge.FieldType.GEOG, False),
            ppge.Field("name", ppge.FieldType.STR, False),
        ]
        buf = io.BytesIO()

        # This should not raise an error despite malformed WKT
        ppge.process_bigquery_rows_to_csv(schema, rows, buf)

        # Verify the output
        buf.seek(0)
        text = buf.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        output_rows = list(reader)

        # Should have 3 rows
        self.assertEqual(len(output_rows), 3)

        # Check that valid rows have WKT geometry
        valid_rows = [
            row for row in output_rows if row["geometry"] and row["geometry"] != "None"
        ]
        self.assertEqual(len(valid_rows), 2)

        valid_names = [row["name"] for row in valid_rows]
        self.assertIn("Wyoming", valid_names)
        self.assertIn("Colorado", valid_names)

        # Check that malformed row has empty/null geometry
        malformed_row = [row for row in output_rows if row["name"] == "BadState"][0]
        self.assertIn(malformed_row["geometry"], [None, "", "None"])


if __name__ == "__main__":
    unittest.main()
