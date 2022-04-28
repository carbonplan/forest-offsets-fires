import geopandas
import prefect


@prefect.task
def load_project_geometries() -> geopandas.GeoDataFrame:
    """Load all CARB project geometries

    Returns:
        geopandas.GeoDataFrame -- gepdataframe with all projects in epsg:4326
    """
    fname = 'gs://carbonplan-forest-offsets/carb-geometries/all_carb_geoms.parquet'
    return geopandas.read_parquet(fname)
