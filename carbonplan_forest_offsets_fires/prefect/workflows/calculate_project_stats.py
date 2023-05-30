import datetime
import json

import fsspec
import geopandas
import prefect
from fuzzywuzzy import process
from prefect import Flow
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.filter import FilterTask

from carbonplan_forest_offsets_fires import utils
from carbonplan_forest_offsets_fires.prefect.tasks import geometry, nifc

NIFC_BUCKET = 'carbonplan-forest-offsets'

serializer = prefect.engine.serializers.JSONSerializer()


def get_fire_metadata(project_fires: geopandas.GeoDataFrame) -> dict:
    centroids = (
        project_fires.centroid.to_crs('epsg:4326')
        .apply(lambda x: [x.centroid.x, x.centroid.y])
        .rename('centroid')
    )
    label_coords = project_fires.convex_hull.exterior.apply(utils.extract_northern_corner).rename(
        'label_coords'
    )
    project_fires = project_fires.join(centroids).join(label_coords)
    return project_fires.set_index('poly_IRWINID')[
        ['name', 'start_date', 'centroid', 'label_coords']
    ].to_dict(orient='index')


@prefect.task
def get_candidate_opr_ids(
    nifc_perimeters: geopandas.GeoDataFrame, proj_hulls: geopandas.GeoDataFrame
) -> list:
    """Intersect fire perimeters and convex hulls of projects and return list of opr_ids

    We do a first pass to speed things up.
    Hulls are super simple geometries which makes the intersection lightning fast.
    Arguments:
        nifc_perimeters {geopandas.GeoDataFrame} -- fire perimeters, to date.
        proj_hulls {geopandas.GeoDataFrame} -- simplified project geometries.

    Returns:
        list -- projects that _might_ have intersection with fire.
    """
    candidate_opr_ids = geopandas.sjoin(nifc_perimeters, proj_hulls).opr_id.unique()
    return candidate_opr_ids


@prefect.task
def summarize_project_fires(
    opr_id: str, nifc_perimeters: geopandas.GeoDataFrame
) -> geopandas.GeoDataFrame:
    """[summary]

    Arguments:
        nifc_perimeters {geopandas.GeoDataFrame} -- [description]
        proj_geom {geopandas.GeoDataFrame} -- [description]

    Returns:
        geopandas.GeoDataFrame -- [description]
    """
    proj_geom = utils.load_project_geometry(opr_id)
    intersecting_fire_idxs = nifc_perimeters.sindex.query(
        proj_geom.geometry[0], predicate='intersects'
    )
    if len(intersecting_fire_idxs) > 0:
        project_fires = nifc_perimeters.iloc[intersecting_fire_idxs]
        fire_geom = project_fires.unary_union.buffer(0)  # prevent double counting burned area
        burned_area = proj_geom.intersection(fire_geom).area.sum()
        burned_frac = burned_area / proj_geom.area.sum()
        fires_summary = get_fire_metadata(project_fires)
        return {
            'opr_id': opr_id,
            'burned_area': burned_area,
            'burned_fraction': round(burned_frac, 3),
            'fires': fires_summary,
        }


@prefect.task
def append_inciweb_urls(project_fires):
    inciweb_uris = utils.get_inciweb_uris()

    annotated_fires = {}
    for k, v in project_fires['fires'].items():
        match = process.extractOne(v['name'], inciweb_uris.keys(), score_cutoff=90)
        inciweb_link = None
        if match is not None:
            inciweb_link = 'https://inciweb.nwcg.gov' + inciweb_uris[match[0]]
        v['url'] = inciweb_link
        annotated_fires[k] = v
    project_fires['fires'] = annotated_fires
    return project_fires


@prefect.task
def write_state_as_of(as_of, annotated_projects: list):
    if not as_of:
        as_of_strs = ['now', datetime.datetime.utcnow().date().strftime('%Y-%m-%d')]
    else:
        as_of_strs = [as_of.strftime('%Y-%m-%d')]

    to_write = {
        'name': 'project-fires',
        'created_at': datetime.datetime.utcnow().date().strftime('%Y-%m-%d %H:%M:%S'),
        'overlapping_fires': annotated_projects,
    }
    # write twice if regular monitoring. once to fixed `now` file and once to dt file
    s3 = fsspec.filesystem('s3', anon=False)
    for as_of_str in as_of_strs:
        with s3.open(f'{NIFC_BUCKET}/fires/project_fires/state_{as_of_str}.json', 'w') as f:
            json.dump(to_write, f)


filter_project_results = FilterTask(filter_func=lambda x: x is not None)

with Flow('project-stats') as flow:
    as_of = DateTimeParameter(name='as_of', required=False)
    nifc_perimeters = nifc.load_nifc_asof(as_of)

    all_proj_geoms = geometry.load_all_project_geometries()
    proj_hulls = geometry.get_project_convex_hulls(all_proj_geoms)

    candidate_opr_ids = get_candidate_opr_ids(nifc_perimeters, proj_hulls)
    project_fires = summarize_project_fires.map(
        candidate_opr_ids, prefect.unmapped(nifc_perimeters)
    )
    filtered_projects = filter_project_results(project_fires)
    appended = append_inciweb_urls.map(filtered_projects)
    write_state_as_of(as_of, appended)

flow.run_config = prefect.run_configs.KubernetesRun(
    image='carbonplan/fire-monitor-prefect:2022.06.06'
)
