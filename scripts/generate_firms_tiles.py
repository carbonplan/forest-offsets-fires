from carbonplan_forest_offsets_fires.firms import (
    read_viirs_nrt,
    filter_df,
    make_tile_tempdir,
    write_firms_json,
    build_tippecanoe_cmd,
    build_pbf_cmd,
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

df = read_viirs_nrt(
    min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, day_range=day_range
)
gdf = filter_df(df)

tempdir = make_tile_tempdir()
json_fp = write_firms_json(data=gdf, tempdir=tempdir)
tippecanoe_cmd = build_tippecanoe_cmd(input_fn=json_fp, tempdir=tempdir, stem=STEM)
print(tippecanoe_cmd)
subprocess.run(tippecanoe_cmd)
pbf_cmd = build_pbf_cmd(tempdir=tempdir, stem=STEM)
subprocess.run(pbf_cmd)
