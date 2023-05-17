import os
import tempfile
from datetime import datetime
from pathlib import Path

import fsspec
import geopandas
import prefect
from fsspec import get_filesystem_class

NIFC_BUCKET = 'carbonplan-forest-offsets/fires/nifc-data'


def get_nifc_filename(bucket: str, as_of: datetime = None) -> str:
    try:
        fs = get_filesystem_class('s3')
        if as_of:
            fns = fs(account_name='carbonplan').glob(f"{bucket}/{as_of.strftime('%Y-%m-%d')}*")
        else:
            fns = fs(account_name='carbonplan').glob(f'{bucket}/*')
        sorted_fns = sorted(fns)

        return ''.join(['s3://', sorted_fns[-1]])
    except IndexError:
        err_msg = f'No NIFC perimeters in {bucket} for that date'
        raise IndexError(err_msg)


def load_nifc_data(nifc_filename: str) -> geopandas.GeoDataFrame:
    with fsspec.open(nifc_filename) as f:
        gdf = geopandas.read_parquet(f)
    gdf = gdf.rename(
        columns={'attr_FireDiscoveryDateTime': 'start_date', 'poly_IncidentName': 'name'}
    )
    return gdf


@prefect.task
def load_nifc_asof(as_of: datetime = None) -> geopandas.GeoDataFrame:
    fn = get_nifc_filename(NIFC_BUCKET, as_of)
    perims = load_nifc_data(fn)
    perims = perims.to_crs('epsg:5070')
    return perims


@prefect.task
def get_nifc_unary_union(gdf: geopandas.GeoDataFrame):
    """apply unary untion to gdf"""
    return gdf.unary_union.buffer(0)


@prefect.task
def get_fires_json(nifc_data: geopandas.GeoDataFrame) -> str:
    """Create json that we pass to tippecanoe for tiling"""
    return nifc_data[['poly_IRWINID', 'geometry']].to_crs('EPSG:4326').to_json()


@prefect.task
def write_fire_json(data: str, tempdir: str) -> str:
    out_fn = Path(tempdir) / 'fires.json'
    with open(out_fn, 'w') as f:
        f.write(data)
    return out_fn


@prefect.task
def make_tile_tempdir() -> str:
    tempdir = tempfile.mkdtemp(suffix='_data')
    os.makedirs(os.path.join(tempdir, 'tmp'), exist_ok=True)
    os.makedirs(os.path.join(tempdir, 'processed'), exist_ok=True)
    return tempdir


@prefect.task
def build_tippecanoe_cmd(
    input_fn: str, tempdir: str, stem: str, compression_factor: str = 'z9'
) -> str:
    """[summary]

    Arguments:
        input_fn {str} -- [description]
        tempdir {str} -- [description]

    Returns:
        str -- [description]
    """
    return f'tippecanoe -{compression_factor} -o {tempdir}/tmp/{stem}.mbtiles --no-feature-limit --no-tile-size-limit --extend-zooms-if-still-dropping --no-tile-compression {input_fn}'  # noqa


@prefect.task
def build_pbf_cmd(tempdir: str, stem: str) -> str:
    return f'mb-util --image_format=pbf {tempdir}/tmp/{stem}.mbtiles {tempdir}/processed/{stem}'


@prefect.task
def upload_tiles(tempdir: str, stem: str, dst_bucket: str):
    fs = fsspec.get_filesystem_class('s3')()
    lpath = f'{tempdir}/processed/{stem}/'
    rpath = f'{dst_bucket}/{stem}'
    fs.put(lpath, rpath, recursive=True)
