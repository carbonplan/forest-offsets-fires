import os
import tempfile
from pathlib import Path

import geopandas as gpd


def get_firms_json(firms_data: gpd.GeoDataFrame) -> str:
    """Create json that we pass to tippecanoe for tiling"""
    return firms_data[['frp', 'geometry']].to_crs('EPSG:4326').to_json()


def write_firms_json(*, data: gpd.GeoDataFrame, tempdir: str) -> str:
    """Write firms data as a GeoJson in a tempdir"""
    data = get_firms_json(data)
    out_fn = Path(tempdir) / 'firms.json'
    with open(out_fn, 'w') as f:
        f.write(data)
    return out_fn


def make_tile_tempdir() -> str:
    """Create a temp dir for intermediate data products"""
    tempdir = tempfile.mkdtemp(suffix='_firms_data')
    os.makedirs(os.path.join(tempdir, 'tmp'), exist_ok=True)
    os.makedirs(os.path.join(tempdir, 'processed'), exist_ok=True)
    return tempdir


def build_tippecanoe_cmd(
    *,
    input_fn: str,
    tempdir: str,
    stem: str = "current-firms-pixels",
    max_zoom_level: int = 9,
) -> str:
    """Create tippecanoe command for generating vector tiles"""
    return [
        "tippecanoe",
        f"-z{max_zoom_level}",
        "-r1",
        "-o",
        f"{tempdir}/tmp/{stem}.mbtiles",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--extend-zooms-if-still-dropping",
        "--no-tile-compression",
        "" f"{input_fn}",
    ]


def build_pbf_cmd(tempdir: str, stem: str = "current-firms-pixels") -> str:
    """Create mb-util command for generating pbf files from vector tiles"""
    return [
        "mb-util",
        "--image_format=pbf",
        f"{tempdir}/tmp/{stem}.mbtiles",
        f"{tempdir}/processed/{stem}",
    ]
