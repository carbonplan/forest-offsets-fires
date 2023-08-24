import os
import pandas as pd
import xarray as xr  # noqa
import pygmt
import geopandas as gpd
import rioxarray  # noqa
import numpy as np
from ndpyramid import pyramid_reproject
from carbonplan_data.utils import set_zarr_encoding as set_web_zarr_encoding

key = os.environ["FIRMS_MAP_KEY"]
pixels_per_tile = 256


def read_viirs_historical() -> pd.DataFrame:
    return pd.read_parquet(
        's3://carbonplan-forest-offsets/fires/firms/fire_nrt_SV-C2_28285.parquet'
    )


def read_viirs_nrt(
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    day_range: int,
) -> pd.DataFrame:
    """Read NRT VIIRS fire data from nasa api

    :param min_lat: minimum latitude
    :type min_lat: float
    :param max_lat: maximum latitude
    :type max_lat: float
    :param min_lon: minimum longitude
    :type min_lon: float
    :param max_lon: maximum longitude
    :type max_lon: float
    :param day_range: number of days to query
    :type day_range: int
    :return: Pandas DataFrame of VIIRS data
    :rtype: pd.DataFrame
    """

    base = "https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
    subset_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    return pd.read_csv(f"{base}{key}/VIIRS_SNPP_NRT/{subset_str}/{day_range}/")


def munge_df(df: pd.DataFrame) -> pd.DataFrame:
    """Extract only high-confidence values, format dates and
    subset

    :param df: DataFrame containing VIIRS data
    :type df: pd.DataFrame
    :return: cleaned dataframe
    :return: pd.DataFrame
    """

    # only select high confidence
    df = df[df['confidence'] != 'l']
    df['registered'] = 1
    # subset dataframe
    return df[['latitude', 'longitude', 'registered']]


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


def mask_df(df: pd.DataFrame) -> pd.DataFrame:
    "Mask DataFrame to only include points within the US"
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    us = world[world.name == "United States of America"]

    masked_points = gdf.sjoin(us, how='inner')
    masked_points = masked_points[['latitude', 'longitude', 'registered']]

    return masked_points


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
