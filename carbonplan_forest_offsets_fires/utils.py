import json
from pathlib import Path

import fsspec
import geopandas
import numpy as np
from bs4 import BeautifulSoup


def list_all_opr_ids(bucket: str = 'carbonplan-forest-offsets/carb-geometries/raw') -> list:
    """Return list of all opr ids
    Inputs:
        bucket {str} -- gcs location of geometries
    Returns:
        list -- all OPR ids as string
    """
    fs = fsspec.filesystem('gs')
    fnames = fs.glob(f'{bucket}/*json')
    opr_ids = [Path(fname).stem for fname in fnames]
    return opr_ids


def extract_northern_corner(polygon):
    """get extreme point for labeling fires"""
    coords = np.array(polygon.coords)
    idx = np.argmax(coords[:, 1])
    max_coords = coords[idx].tolist()
    return max_coords


def load_project_geometry(opr_id: str) -> geopandas.GeoDataFrame:
    with fsspec.open(
        f'https://storage.googleapis.com/carbonplan-forest-offsets/carb-geometries/raw/{opr_id}.json'  # noqa
    ) as f:
        d = json.load(f)

    geo = geopandas.GeoDataFrame.from_features(d)
    geo = geo.set_crs('epsg:4326')
    geo = geo.to_crs('epsg:5070')
    geo.geometry = geo.buffer(0)
    return geo


def get_inciweb_uris() -> dict:
    """Key-value store of fire names and inciweb uris

    Returns:
        dict -- key fire name, value inciweb uri
    """
    uri = 'https://inciweb.nwcg.gov/accessible-view/'

    with fsspec.open(uri) as fp:
        soup = BeautifulSoup(fp, 'html.parser')

    links_with_text = {}
    for a in soup.find_all('a', href=True):
        if a.text and 'incident' in a['href']:
            links_with_text[a.text] = a['href']
    return links_with_text
