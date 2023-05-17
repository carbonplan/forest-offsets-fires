import datetime
import urllib

import fsspec
import geopandas
import pandas as pd
import prefect
import requests
from prefect.storage import GCS

CRS = '+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs'  # noqa
UPLOAD_TO = 's3://carbonplan-forest-offsets/fires/nifc-data'

schedule = prefect.schedules.IntervalSchedule(interval=datetime.timedelta(hours=3))

NIFC_ENDPOINT = 'https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/' \
            'WFIGS_Interagency_Perimeters_YearToDate/FeatureServer/0/query'

def get_fire_url(url):
    fires = geopandas.read_file(url).to_crs(CRS).reset_index(drop=True)
    return fires


@prefect.task
def get_nifc_perimeter_count():
    """Return count of perimeters in nifc dataset to enable pagination
    Previously, we used a static download provided by NIFC, which could go stale.
    Now, we"re hitting the underlying database (per NIFC suggestion)
    """
    params = {'where': 'OBJECTID >0', 'returnCountOnly': 'true', 'f': 'pjson'}
    r = requests.get(NIFC_ENDPOINT, params=params)
    return r.json()['count']


@prefect.task
def get_paginated_fire_urls(record_count, request_size=1_000):
    """Generate urls for grabbing all NIFC data.
    Geopandas doesnt support requests-style params, so it"s easier to just premake the urls
    """
    record_offsets = range(1, record_count, request_size)

    base_params = {
        'f': 'geojson',
        'where': 'OBJECTID > 0',
        'resultRecordCount': request_size,
        'returnGeometry': 'true',
        'outFields': '*',
    }

    base_url = NIFC_ENDPOINT + '?' # not sure if can go through `params` but this allows to encode url directly
    urls = [
        base_url + urllib.parse.urlencode(base_params) + f'&resultOffset={record_offset}'
        for record_offset in record_offsets
    ]
    return urls


@prefect.task
def get_nifc_perimeters(fire_urls):
    fires = pd.concat([get_fire_url(fire_url) for fire_url in fire_urls]).reset_index(drop=True)
    return fires


@prefect.task
def save_nifc_perimeters(perimeters):
    now = datetime.datetime.utcnow().isoformat()
    with fsspec.open(f'{UPLOAD_TO}/{now}_raw_nifc_perimeters.parquet', mode='wb') as f:
        perimeters.to_parquet(f, compression='gzip')


with prefect.Flow('get-nifc-perimeters') as flow:
    record_count = get_nifc_perimeter_count()
    urls = get_paginated_fire_urls(record_count)
    perimeters = get_nifc_perimeters(urls)
    save_nifc_perimeters(perimeters)


env = {
    'EXTRA_PIP_PACKAGES': 'pyarrow git+https://github.com/carbonplan/forest-offsets-fires@main --no-deps'  # noqa
}


flow.storage = GCS(bucket='carbonplan-prefect')
flow.run_config = prefect.run_configs.KubernetesRun(
    labels=['gcp-us-central1-b'], image='carbonplan/fire-monitor-prefect:2022.06.06', env=env
)
