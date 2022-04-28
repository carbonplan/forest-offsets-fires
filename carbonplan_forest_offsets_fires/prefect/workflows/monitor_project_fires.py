import csv
from typing import Union

import geopandas
import prefect
import requests
from prefect.tasks.notifications import SlackTask

from carbonplan_forest_offsets_fires.prefect.tasks import geometry


@prefect.task
def get_active_fires() -> geopandas.GeoDataFrame:
    """load NOAA viirs active fire points

    Returns:
        geopandas.GeoDataFrame -- [description]
    """

    url = 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv'  # noqa
    r = requests.get(url)
    records = csv.DictReader(r.text.splitlines())
    lats = []
    lons = []
    for record in records:
        lats.append(float(record['latitude']))
        lons.append(float(record['longitude']))

    geoms = geopandas.points_from_xy(lons, lats, crs='epsg:4326')
    gdf = geopandas.GeoDataFrame(geometry=geoms)
    return gdf


@prefect.task
def get_active_fires_by_project(
    project_geoms: geopandas.GeoDataFrame, active_fires: geopandas.GeoDataFrame
) -> Union[None, dict]:
    """[summary]

    Arguments:
        project_geoms {geopandas.GeoDataFrame} -- [description]
        active_fires {geopandas.GeoDataFrame} -- [description]

    Returns:
        dict -- [description]
    """
    intersection = geopandas.sjoin(active_fires, project_geoms)
    fire_counts = intersection['opr_id'].value_counts().to_dict()
    return fire_counts


@prefect.task
def generate_slack_messages(fire_counts: dict) -> list:
    template = '{opr_id} has {nobs} active fire pixel(s)'
    return [template.format(**{'opr_id': k, 'nobs': v}) for k, v in fire_counts.items()]


@prefect.task
def check_send_messages(fire_counts: Union[None, dict]) -> bool:
    """Logic gate to check if sending slack messages

    https://docs.prefect.io/core/idioms/conditional.html

    Arguments:
        fire_counts {dict} -- [description]

    Returns:
        bool -- [description]
    """
    if fire_counts:
        return True
    else:
        return False


send_slack_alert = SlackTask()

with prefect.Flow('monitor-project-fires') as flow:
    active_fires = get_active_fires()
    project_geoms = geometry.load_project_geometries()
    fire_counts = get_active_fires_by_project(project_geoms, active_fires)
    send_messages = check_send_messages(fire_counts)
    with prefect.case(send_messages, True):
        messages = generate_slack_messages(fire_counts)
        send_slack_alert.map(messages)
