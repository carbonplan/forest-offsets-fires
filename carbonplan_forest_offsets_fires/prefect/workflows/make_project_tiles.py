import geopandas
import prefect
from prefect.tasks.shell import ShellTask

from carbonplan_forest_offsets_fires.prefect.tasks import geometry, nifc

UPLOAD_TO = 'carbonplan-forest-offsets/web/tiles'

build_tiles_from_json = ShellTask(name='transform json to mbtiles')
build_pbf_from_tiles = ShellTask(name='transform mbtiles to pbf')


@prefect.task
def get_project_json(gdf: geopandas.GeoDataFrame) -> dict:
    """Transform projects to json for tippecanoe"""
    return gdf[['opr_id', 'geometry']].to_crs('epsg:4326').to_json()


with prefect.Flow('make-project-tiles') as flow:
    stem = prefect.Parameter('stem', default='projects')
    tempdir = nifc.make_tile_tempdir()

    projects = geometry.load_all_project_geometries()
    projects_json = get_project_json(projects)

    json_fn = nifc.write_fire_json(projects_json, tempdir)
    tippecanoe_cmd = nifc.build_tippecanoe_cmd(json_fn, tempdir, stem)
    tiles = build_tiles_from_json(command=tippecanoe_cmd)

    # must specify upstream, otherwise race condition
    pbf_cmd = nifc.build_pbf_cmd(tempdir, stem, upstream_tasks=[tiles])

    pbf = build_pbf_from_tiles(command=pbf_cmd)
    nifc.upload_tiles(tempdir, stem, UPLOAD_TO, upstream_tasks=[pbf])
