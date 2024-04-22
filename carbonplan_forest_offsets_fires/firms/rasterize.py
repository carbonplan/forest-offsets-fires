import os

import numpy as np
import pandas as pd
import pygmt
import rioxarray  # noqa
import xarray as xr  # noqa
from carbonplan_data.utils import set_zarr_encoding as set_web_zarr_encoding
from ndpyramid import pyramid_reproject

key = os.environ["FIRMS_MAP_KEY"]
pixels_per_tile = 256


def rasterize_frp(
    df: pd.DataFrame,
    *,
    min_lat: float = -90,
    max_lat: float = 90,
    min_lon: float = -180,
    max_lon: float = 180,
) -> xr.Dataset:
    active = pygmt.xyz2grd(
        data=df[['longitude', 'latitude', 'registered']],
        region=[min_lon, max_lon, min_lat, max_lat],
        spacing="400e",
        duplicate="u",
        registration="p",
    )
    active.chunk({'lon': pixels_per_tile, 'lat': pixels_per_tile})
    active = xr.where(active.notnull(), 1, 0)
    active = active.to_dataset(name='active')

    return active


def create_pyramids(raster_path: str, levels: int):
    """Creates pyramids from Zarr store

    :param raster_path: Input Zarr store
    :type raster_path: str
    :param levels: Number of pyramid levels to generate
    :type levels: int, optional
    """
    ds = xr.open_zarr(raster_path).rio.write_crs("EPSG:4326")
    dt = pyramid_reproject(ds, levels=levels, resampling="average")
    for child in dt.children:
        dt[child]['active'] = xr.where(dt[child]['active'] > 0, dt[child]['active'], np.nan)
        dt[child].ds = set_web_zarr_encoding(
            dt[child].ds, codec_config={"id": "zlib", "level": 1}, float_dtype="float32"
        )
    return dt
