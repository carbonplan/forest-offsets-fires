import os

import fsspec
import geopandas as gpd
import pandas as pd

key = os.environ["FIRMS_MAP_KEY"]
url = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"


def read_viirs_historical() -> pd.DataFrame:
    return pd.read_parquet(
        's3://carbonplan-forest-offsets/fires/firms/fire_nrt_SV-C2_28285.parquet'
    )


def read_firms_nrt(
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    day_range: int,
    source: str,
) -> pd.DataFrame:
    """
    Read NRT fire data from nasa api

    Parameters
    ----------

    min_lat, max_lat, min_lon, max_lon: float
        Minumum and maximum latitude and longitude values for the API query.

    day_range: int
        Number of days to include in the API query.

    source: str
        Data source for the API query. Must be one of:
        "VIIRS_NOAA20_NRT", "MODIS_NRT", "VIIRS_SNPP_NRT"

    Returns
    -------

    """
    sources = ["VIIRS_NOAA20_NRT", "MODIS_NRT", "VIIRS_SNPP_NRT"]
    if source not in sources:
        raise ValueError(f"Invalid souce {source}; must be one of {sources}")
    base = "https://firms2.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
    subset_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    return pd.read_csv(f"{base}{key}/{source}/{subset_str}/{day_range}/")


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame based on confidence"""
    if df.dtypes['confidence'] == 'int64':
        df = df[df['confidence'] > 35]
    else:
        df = df[df['confidence'] != 'l']
    return df


def mask_df(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Filter to only include points in the United States"""
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    world = gpd.read_file(url)
    us = world[world.SOVEREIGNT == "United States of America"]

    masked_points = gdf.sjoin(us, how='inner')
    masked_points = masked_points[['frp', 'geometry']]

    return masked_points


def upload_tiles(
    *,
    tempdir: str,
    stem: str = "current-firms-pixels",
    dst_bucket: str = "carbonplan-scratch/web/tiles",
):
    """Upload pdf tiles to s3"""
    fs = fsspec.filesystem('s3', anon=False)
    lpath = f'{tempdir}/processed/{stem}/'
    rpath = f'{dst_bucket}/{stem}/'
    if rpath == 'carbonplan-forest-offsets/web/tiles/current-firms-pixels/':
        fs.rm(rpath, recursive=True)
        fs.put(lpath, rpath, recursive=True)
    else:
        raise ValueError(f"Unexpected target path {rpath}")
