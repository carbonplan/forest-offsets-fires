import json
import subprocess

import fsspec
import geopandas
import prefect

from carbonplan_forest_offsets_fires.utils import list_all_opr_ids


@prefect.task
def get_all_opr_ids():
    """Wrap util in prefect task for use in flow"""
    return list_all_opr_ids()


@prefect.task
def load_simplified_geometry(opr_id: str) -> geopandas.GeoDataFrame:
    """ "Pass raw geometry through mapshaper"""
    fn = f'gs://carbonplan-forest-offsets/carb-geometries/raw/{opr_id}.json'
    with fsspec.open(fn) as f:
        d = json.load(f)

    # mapshaper uses `-` to denote stdin/stdout, so read from - and write to -
    # this shapefile is so broken we have to really goose the simplification
    if opr_id == 'ACR361':
        result = subprocess.run(
            'mapshaper -i - -simplify 10% -o -',
            text=True,
            capture_output=True,
            shell=True,
            input=json.dumps(d),
        )
    else:
        result = subprocess.run(
            'mapshaper -i - -simplify 70% -o -',
            text=True,
            capture_output=True,
            shell=True,
            input=json.dumps(d),
        )
    gdf = geopandas.GeoDataFrame.from_features(json.loads(result.stdout))
    gdf = gdf.set_crs('epsg:4326')
    gdf = gdf.to_crs('epsg:5070')
    return gdf


@prefect.task
def buffer_geometry(gdf: geopandas.GeoDataFrame, buffer_by: int):
    gdf.geometry = gdf.buffer(buffer_by).buffer(-1 * buffer_by)
    return gdf


@prefect.task
def get_project_convex_hulls(project_geoms: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """Load project geometries as convex hulls.
    This dramatically speeds up geospatial joins.

    Arguments:
        project_geoms {geopandas.GeoDataFrame} -- GeoDataFrame where each row is project geom

    Returns:
        geopandas.GeoDataFrame -- GeoDataFrame where geometry is convex hull, not geom
    """
    return geopandas.GeoDataFrame(project_geoms['opr_id'], geometry=project_geoms.convex_hull)


@prefect.task
def load_all_project_geometries() -> geopandas.GeoDataFrame:
    """Load all CARB project geometries

    Returns:
        geopandas.GeoDataFrame -- gepdataframe with all projects in epsg:5070
    """
    fname = 'gs://carbonplan-forest-offsets/carb-geometries/all_carb_geoms.parquet'
    gdf = geopandas.read_parquet(fname)
    gdf = gdf.to_crs('epsg:5070')
    return gdf.reset_index()
