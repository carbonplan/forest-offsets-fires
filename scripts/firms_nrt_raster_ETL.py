import os
import pandas as pd
from datetime import datetime
import xarray as xr  # noqa
import pygmt

from ndpyramid import pyramid_reproject
import regionmask
import rioxarray  # noqa
import numpy as np
from carbonplan_data.utils import set_zarr_encoding as set_web_zarr_encoding

from tempfile import TemporaryDirectory

td = TemporaryDirectory()

key = os.environ["FIRMS_MAP_KEY"]

# Light subset of CONUS + Alaska
min_lat = 24
max_lat = 72
min_lon = -180
max_lon = -66

pixels_per_tile = 512 * 2
day_range = 3
levels = 6


def create_paths() -> dict:
    """Returns dictionary containing data paths

    :return: data path dict
    :rtype: dict
    """
    s3_raster = f"s3://carbonplan-forest-offsets/fires/firms_nrt/raster/{datetime.now().strftime('%Y-%m-%d')}/"
    s3_pyramid = f"s3://carbonplan-forest-offsets/fires/firms_nrt/pyramid/{datetime.now().strftime('%Y-%m-%d')}/"
    return {'s3_raster': s3_raster, 's3_pyramid': s3_pyramid}


def read_viirs(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    pixels_per_tile: int,
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
    :param pixels_per_tile: number of pixels per tile
    :type pixels_per_tile: int
    :param day_range: number of days to query
    :type day_range: int
    :return: Pandas DataFrame of VIIRS data
    :rtype: pd.DataFrame
    """

    base = "https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
    subset_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    return pd.read_csv(f"{base}{key}/VIIRS_SNPP_NRT/{subset_str}/{day_range}/")


def munge_df(df: pd.DataFrame, epsg: str = "4326") -> pd.DataFrame:
    """Extract only high-confidence values, format dates and
    subset

    :param df: DataFrame containing VIIRS data
    :type df: pd.DataFrame
    :param epsg: EPSG code
    :type epsg: 4326
    :return: cleaned dataframe
    :return: pd.DataFrame
    """

    # only select high confidence
    df = df[df['confidence'] == 'h']
    df['registered'] = 1
    # format acquisition date
    df['acq_date'] = pd.to_datetime(df['acq_date']).dt.strftime('%Y-%m-%d')

    # subset dataframe
    return df[['latitude', 'longitude', 'acq_date', 'registered']]


def df_to_ds(df: pd.DataFrame) -> xr.Dataset:
    # converts viirs location dataframe to xarray dataset for masking
    df['time'] = pd.to_datetime(df['acq_date'])
    df = df.set_index('time')
    ds = xr.Dataset.from_dataframe(df)
    ds = ds.set_coords(("time", "latitude", "longitude"))
    return ds


def rasterize_frp(df: pd.DataFrame) -> xr.Dataset:
    active = pygmt.xyz2grd(
        data=df[['longitude', 'latitude', 'registered']],
        region=[min_lon, max_lon, min_lat, max_lat],
        spacing="400e",
        duplicate="u",
        registration="p",
    )
    active.chunk({'lon': pixels_per_tile, 'lat': pixels_per_tile})
    active = xr.where(active.notnull(), 1, 0).astype('i1')
    active.attrs['_FillValue'] = 0
    active = active.to_dataset(name='active')

    return active


def mask_ds(ds: xr.Dataset) -> pd.DataFrame:
    # mask conus + alaska
    mask = regionmask.defined_regions.natural_earth_v5_0_0.us_states_50.mask(
        ds, lon_name='longitude', lat_name='latitude'
    )

    mds = mask.to_dataset(name="registered")
    df = mds.to_dataframe()
    return df


def write_raster_to_zarr(ds: xr.Dataset, path: str):
    ds.to_zarr(
        path,
        encoding={'active': {"write_empty_chunks": False, "dtype": 'i1'}},
        mode="w",
        consolidated=True,
    )


def create_pyarmids(raster_path: str, pyramid_path: str, levels: int = levels):
    ds = xr.open_zarr(raster_path)
    dt = pyramid_reproject(ds.rio.write_crs("EPSG:4326"), levels=levels, resampling="sum")
    for child in dt.children:
        dt[child]['active'] = xr.where(dt[child]['active'] > 0, 1, np.nan)
        dt[child].ds = set_web_zarr_encoding(
            dt[child].ds, codec_config={"id": "zlib", "level": 1}, float_dtype="float32"
        )
    dt.to_zarr(pyramid_path, consolidated=True, mode='w')


path_dict = create_paths()
df = read_viirs(min_lat, max_lat, min_lon, max_lon, pixels_per_tile, day_range)
mdf = munge_df(df)
ds = df_to_ds(mdf)
masked_df = mask_ds(ds)
# rasterized_ds = rasterize_frp(masked_df)
# write_raster_to_zarr(rasterized_ds, path_dict['s3_raster'])
# create_pyarmids(path_dict['s3_raster'], path_dict['s3_pyramid'], levels)
