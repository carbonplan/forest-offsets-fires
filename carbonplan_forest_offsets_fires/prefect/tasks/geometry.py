import geopandas
import prefect

from carbonplan_forest_offsets_fires.utils import list_all_opr_ids


@prefect.task
def get_all_opr_ids():
    """Wrap util in prefect task for use in flow"""
    return list_all_opr_ids()


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
    return gdf.to_crs('epsg:5070')
