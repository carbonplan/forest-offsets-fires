import subprocess

import pandas as pd

from carbonplan_forest_offsets_fires.firms import (
    build_pbf_cmd,
    build_tippecanoe_cmd,
    filter_df,
    make_tile_tempdir,
    mask_df,
    read_firms_nrt,
    upload_tiles,
    write_firms_json,
)

day_range = 3

UPLOAD_TO = 'carbonplan-forest-offsets/web/tiles'
STEM = 'current-firms-pixels'

# Light subset of CONUS + Alaska
params = {'min_lat': 24, 'max_lat': 72, 'min_lon': -180, 'max_lon': -66, 'day_range': day_range}
print("Loading data")
df_snpp = read_firms_nrt(
    **params,
    source="VIIRS_SNPP_NRT",
).pipe(filter_df)
df_noaa20 = read_firms_nrt(
    **params,
    source="VIIRS_NOAA20_NRT",
).pipe(filter_df)
df_modis = read_firms_nrt(
    **params,
    source="MODIS_NRT",
).pipe(filter_df)
df = pd.concat([df_snpp, df_noaa20, df_modis])
gdf = mask_df(df)
print("Writing geoparquet")
gdf.to_parquet('s3://carbonplan-forest-offsets/fires/firms/current-firms-pixels.parquet')
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
