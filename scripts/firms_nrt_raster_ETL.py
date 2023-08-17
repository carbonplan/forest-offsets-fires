import xarray as xr  # noqa
from datetime import datetime
import rioxarray  # noqa
from carbonplan_forest_offsets_fires.firms import (
    read_viirs,
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
    s3_raster = f"s3://carbonplan-forest-offsets/fires/firms_nrt/raster/{datetime.now().strftime('%Y-%m-%d')}/"
    s3_pyramid_staging = "s3://carbonplan-forest-offsets/fires/firms_nrt/pyramid/staging/"
    s3_pyramid = "s3://carbonplan-forest-offsets/web/tiles/current-firms-hotspots/"

    return {
        's3_raster': s3_raster,
        's3_pyramid_staging': s3_pyramid_staging,
        's3_pyramid': s3_pyramid,
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
df = read_viirs(
    min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, day_range=day_range
)
mdf = munge_df(df)
masked_df = mask_df(mdf)
rasterized_ds = rasterize_frp(
    masked_df, min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon
)
write_raster_to_zarr(rasterized_ds, path_dict['s3_raster'])
dt = create_pyramids(path_dict['s3_raster'], levels=levels)
dt.to_zarr(path_dict['s3_pyramid_staging'], consolidated=True, mode='w', write_empty_chunks=False)
