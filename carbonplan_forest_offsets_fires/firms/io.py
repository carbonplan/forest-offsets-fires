import os
import pandas as pd
import geopandas as gpd


key = os.environ["FIRMS_MAP_KEY"]


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

    base = "https://firms2.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
    subset_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    return pd.read_csv(f"{base}{key}/VIIRS_SNPP_NRT/{subset_str}/{day_range}/")


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame based on confidence and location"""
    # Exclude low confidence
    df = df[df['confidence'] != 'l']
    # Filter to only include points in the United States
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    us = world[world.name == "United States of America"]

    masked_points = gdf.sjoin(us, how='inner')
    masked_points = masked_points[['frp', 'geometry']]

    return masked_points
