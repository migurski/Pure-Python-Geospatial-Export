#!/usr/bin/env python3
import csv

import geomet
import geopackage
import shapefile

with open("wy-co.csv") as file:
    rows = list(csv.DictReader(file))

print(rows)

with geopackage.GeoPackage('wy-co.gpkg') as gpkg:
    tbl = gpkg.create("wyco", fields={"name": "TEXT"}, wkid=4326, geometry_type="POLYGON", overwrite=True)
    for row in rows:
        tbl.insert({"name": row["name"], "Shape": row["WKT"]}, geom_format="WKT")

with shapefile.Writer('wy-co.shp', shapeType=5) as shp:
    shp.field('name', 'C')

    for row in rows:
        shp.record(name=row["name"])
        shp.poly(geomet.wkt.loads(row["WKT"])['coordinates'])
    
with open("wy-co.prj", "w") as prj:
    prj.write('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]')
