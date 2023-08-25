import xarray as xr  # noqa
from datetime import datetime
import rioxarray  # noqa
from carbonplan_forest_offsets_fires.firms import (
    read_viirs_nrt,
    read_viirs_historical,
    munge_df,
    mask_df,
    rasterize_frp,
    create_pyramids,
)

# Light subset of CONUS + Alaska
min_lat = 24
max_lat = 72
min_lon = -180
max_lon = -66

day_range = 3
levels = 9


def create_paths() -> dict:
    """Returns dictionary containing data paths

    :return: data path dict
    :rtype: dict
    """
    s3_raster_nrt = f"s3://carbonplan-forest-offsets/fires/firms_nrt/raster/{datetime.now().strftime('%Y-%m-%d')}/"
    s3_raster_historical = (
        "s3://carbonplan-forest-offsets/fires/firms_nrt/raster/2023-01-01_2023-08-21"
    )
    s3_pyramid_staging = "s3://carbonplan-forest-offsets/fires/firms_nrt/pyramid/staging/"
    s3_pyramid_historical = "s3://carbonplan-forest-offsets/web/tiles/past-firms-hotspots/"
    s3_pyramid_nrt = "s3://carbonplan-forest-offsets/web/tiles/current-firms-hotspots/"

    return {
        's3_raster_nrt': s3_raster_nrt,
        's3_raster_historical': s3_raster_historical,
        's3_pyramid_staging': s3_pyramid_staging,
        's3_pyramid_historical': s3_pyramid_historical,
        's3_pyramid_nrt': s3_pyramid_nrt,
    }


def write_raster_to_zarr(ds: xr.Dataset, path: str):
    """Writes Xarray dataset to Zarr. Does not write empty chunks and overwrites existing data

    :param ds: Finalized Xarray dataset
    :type ds: xr.Dataset
    :param path: Location to write
    :type path: str
    """
    ds.to_zarr(path, mode="w", consolidated=True, write_empty_chunks=False)


path_dict = create_paths()
print("Reading data")
df = read_viirs_nrt(
    min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, day_range=day_range
)
mdf = munge_df(df)
masked_df = mask_df(mdf)
print("Rasterizing data")
rasterized_ds = rasterize_frp(
    masked_df, min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon
)
print(f"Writing rasterized data to {path_dict['s3_raster_nrt']}")
write_raster_to_zarr(rasterized_ds, path_dict['s3_raster_nrt'])
print(f"Creating pyramids from {path_dict['s3_raster_nrt']}")
dt = create_pyramids(path_dict['s3_raster_nrt'], levels=levels)
print(f"Writing pyramids to {path_dict['s3_pyramid_nrt']}")
dt.to_zarr(path_dict['s3_pyramid_nrt'], consolidated=True, mode='w', write_empty_chunks=False)

df = read_viirs_historical()
mdf = munge_df(df)
masked_df = mask_df(mdf)
print("Rasterizing data")
rasterized_ds = rasterize_frp(
    masked_df, min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon
)
print(f"Writing rasterized data to {path_dict['s3_raster_historical']}")
write_raster_to_zarr(rasterized_ds, path_dict['s3_raster_historical'])
print(f"Creating pyramids from {path_dict['s3_raster_historical']}")
dt = create_pyramids(path_dict['s3_raster_historical'], levels=levels)
print(f"Writing pyramids to {path_dict['s3_pyramid_historical']}")
dt.to_zarr(
    path_dict['s3_pyramid_historical'], consolidated=True, mode='w', write_empty_chunks=False
)
