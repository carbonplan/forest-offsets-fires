from carbonplan_forest_offsets_fires.firms import (
    read_viirs_nrt,
    filter_df,
    make_tile_tempdir,
    write_firms_json,
    build_tippecanoe_cmd,
    build_pbf_cmd,
    upload_tiles,
)
import subprocess

# Light subset of CONUS + Alaska
min_lat = 24
max_lat = 72
min_lon = -180
max_lon = -66

day_range = 3

UPLOAD_TO = 'carbonplan-forest-offsets/web/tiles'
STEM = 'current-firms-pixels'

print("Loading data")
df = read_viirs_nrt(
    min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, day_range=day_range
)
gdf = filter_df(df)
print("Creating temporary json")
tempdir = make_tile_tempdir()
json_fp = write_firms_json(data=gdf, tempdir=tempdir)
print("Running tippecanoe")
tippecanoe_cmd = build_tippecanoe_cmd(input_fn=json_fp, tempdir=tempdir, stem=STEM)
subprocess.run(tippecanoe_cmd)
print("Running mb-util")
pbf_cmd = build_pbf_cmd(tempdir=tempdir, stem=STEM)
subprocess.run(pbf_cmd)
print("Uploading to s3")
upload_tiles(tempdir=tempdir, stem=STEM, dst_bucket=UPLOAD_TO)
