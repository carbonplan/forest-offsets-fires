from pathlib import Path

import geopandas
import pandas as pd
import prefect
from prefect.executors import LocalDaskExecutor
from prefect.tasks.shell import ShellTask

from carbonplan_forest_offsets_fires.prefect.tasks import geometry, nifc

UPLOAD_TO = 'carbonplan-forest-offsets/web/tiles'

build_tiles_from_json = ShellTask(name='transform json to mbtiles')
build_pbf_from_tiles = ShellTask(name='transform mbtiles to pbf')


@prefect.task
def write_project_json(gdf: geopandas.GeoDataFrame, tempdir: str) -> dict:
    """Transform projects to json for tippecanoe"""
    d = gdf[['opr_id', 'geometry']].to_crs('epsg:4326').to_json()
    out_fn = Path(tempdir) / 'projects.json'
    with open(out_fn, 'w') as f:
        f.write(d)
    return out_fn


@prefect.task
def combine_geometries(mapped_geoms):
    return pd.concat(mapped_geoms)


with prefect.Flow('make-project-tiles') as flow:
    # stem = prefect.Parameter('stem', default='projects')
    tempdir = nifc.make_tile_tempdir()

    opr_ids = geometry.get_all_opr_ids()
    geoms = geometry.load_simplified_geometry.map(opr_ids)
    buffered_geoms = geometry.buffer_geometry.map(geoms, prefect.unmapped(30))
    combo = combine_geometries(buffered_geoms)
    json_fn = write_project_json(combo, tempdir)

    tippecanoe_cmd = nifc.build_tippecanoe_cmd(json_fn, tempdir, 'projects')
    tiles = build_tiles_from_json(command=tippecanoe_cmd)

    # must specify upstream, otherwise race condition
    pbf_cmd = nifc.build_pbf_cmd(tempdir, 'projects', upstream_tasks=[tiles])

    pbf = build_pbf_from_tiles(command=pbf_cmd)
    nifc.upload_tiles(tempdir, 'projects', UPLOAD_TO, upstream_tasks=[pbf])

flow.executor = LocalDaskExecutor(scheduler='processes', num_workers=4)
flow.run_config = prefect.run_configs.KubernetesRun(
    image='carbonplan/fire-monitor-prefect:2022.06.06'
)
