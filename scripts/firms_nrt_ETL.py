import geopandas as gpd
import os
import pandas as pd
from datetime import datetime
import subprocess
import fsspec
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


def munge_and_geodataframe(df: pd.DataFrame, epsg: "4326") -> gpd.GeoDataFrame:
    """Extract only high-confidence values, format dates,
    subset and convert DataFrame to GeoDataFrame

    :param df: DataFrame containing VIIRS data
    :type df: pd.DataFrame
    :param epsg: EPSG code
    :type epsg: 4326
    :return: GeoDataFrame
    :rtype: gpd.GeoDataFrame
    """

    # only select high confidence
    df = df[df['confidence'] == 'h']
    # format acquisition date
    df['acq_date'] = pd.to_datetime(df['acq_date']).dt.strftime('%Y-%m-%d')
    # convert DataFrame to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs=f"EPSG:{epsg}"
    )
    # subset geodataframe
    return gdf[['latitude', 'longitude', 'acq_date', 'geometry']]


def create_paths() -> dict:
    """Returns dictionary containing data paths

    :return: data path dict
    :rtype: dict
    """
    json_path = td.name + "tmp.geojson"
    mbtile_path = td.name + "tmp.mbtiles"
    pbf_path = td.name + "tmp.pbf"
    s3_pbf = (
        f"s3://carbonplan-forest-offsets/fires/firms_nrt/{datetime.now().strftime('%Y-%m-%d')}/pbf/"
    )
    return {
        'json_path': json_path,
        'mbtile_path': mbtile_path,
        'pbf_path': pbf_path,
        's3_pbf': s3_pbf,
    }


def write_geodataframe_to_json(gdf: gpd.GeoDataFrame, json_path: str):
    """Writes geodataframe to GeoJSON"""
    gdf.to_file(json_path, driver='GeoJSON')


def tipppecanoe_process(mbtile_path: str, json_path: str):
    """Uses tippecanoe cli to convert geojson to mbtile"""
    subprocess.call(
        f'tippecanoe -o {mbtile_path} --no-feature-limit --no-tile-size-limit --extend-zooms-if-still-dropping --no-tile-compression {json_path}',  # noqa
        shell=True,
        cwd='.',
    )


def mbutil_process(mbtile_path: str, pbf_path: str):
    """Uses mbutil to convert mbtile to pbf"""
    subprocess.call(f'mb-util --image_format=pbf {mbtile_path} {pbf_path}', shell=True, cwd='.')


def transfer_pbf_to_s3(pbf_path: str, s3_pbf: str):
    """Transfers pbf files from local storage to s3"""
    fs = fsspec.filesystem('s3', anon=False)
    fs.put(pbf_path, s3_pbf, recursive=True)


path_dict = create_paths()
df = read_viirs(min_lat, max_lat, min_lon, max_lon, pixels_per_tile, day_range)
gdf = munge_and_geodataframe(df, epsg="4326")
write_geodataframe_to_json(gdf, path_dict['json_path'])
tipppecanoe_process(path_dict['mbtile_path'], path_dict['json_path'])
mbutil_process(path_dict['mbtile_path'], path_dict['pbf_path'])
transfer_pbf_to_s3(path_dict['pbf_path'], path_dict['s3_pbf'])
