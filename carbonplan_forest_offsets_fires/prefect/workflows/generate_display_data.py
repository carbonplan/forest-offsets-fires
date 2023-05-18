import json
import pathlib
from datetime import timedelta

import censusgeocode as cg
import fsspec
import geopandas
import prefect
from carbonplan_forest_offsets.load.issuance import load_issuance_table
from carbonplan_forest_offsets.utils import get_centroids

from carbonplan_forest_offsets_fires import utils
from carbonplan_forest_offsets_fires.prefect.tasks.geometry import (
    buffer_geometry,
    load_simplified_geometry,
)

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands': 'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}


@prefect.task
def load_issuance_to_date():
    issuance = load_issuance_table(most_recent=True)
    issuance_to_date = issuance.groupby('arb_id')['allocation'].sum().to_dict()
    return issuance_to_date


@prefect.task
def get_project_area(opr_id):
    geom = utils.load_project_geometry(opr_id)
    return int(round((geom.area / 4046.86).item()))


@prefect.task
def load_arbid_map():
    issuance = load_issuance_table(most_recent=True)
    df = issuance[['opr_id', 'arb_id']].set_index('arb_id').copy()
    return df.opr_id.to_dict()


@prefect.task
def load_display_names():
    """Load json with hand curated, shortened project names"""
    data_path = pathlib.Path(__file__).parents[2]
    with fsspec.open(data_path / 'data' / 'display-names.json') as f:
        display_names = json.load(f)
    return {x['arb_id']: x['name'] for x in display_names}


@prefect.task(max_retries=3, retry_delay=timedelta(seconds=5))
def get_location_name(coords):
    results = cg.coordinates(x=coords[0], y=coords[1])
    name = results['Counties'][0]['NAME'] + ', ' + us_state_abbrev[results['States'][0]['NAME']]
    return name


@prefect.task
def get_display_name(arb_id: str, display_names: dict) -> str:
    return display_names.get(arb_id, 'Null')


@prefect.task
def get_opr_id(arb_id: str, arbid_to_oprid: dict) -> str:
    return arbid_to_oprid.get(arb_id)


@prefect.task()
def get_arb_ids(display_names: dict) -> list:
    """list of all arb ids we need"""
    return list(display_names.keys())


@prefect.task
def get_centroid(gdf: geopandas.GeoDataFrame) -> list:
    centroids = get_centroids(gdf)
    return centroids[0]


@prefect.task
def get_arbocs_to_date(opr_id: str, arbocs_to_date: dict) -> int:
    return int(arbocs_to_date.get(opr_id))


@prefect.task
def construct_record(
    opr_id: str, display_name: str, arbocs: int, area: int, centroid: list, location: str
) -> dict:
    return {
        'id': opr_id,
        'opr_id': opr_id,
        'name': display_name,
        'arbocs': arbocs,
        'area': area,
        'shape_centroid': centroid,
        'location': location,
    }


@prefect.task
def write_results(records: list):
    with fsspec.open('s3://carbonplan-forest-offsets/web/display-data.json', 'w') as f:
        ea_opr_ids = utils.list_all_ea_opr_ids()
        to_write = [record for record in records if record['opr_id'] not in ea_opr_ids]
        json.dump(to_write, f)


with prefect.Flow('generate-display-data') as flow:
    display_names = load_display_names()
    arbid_to_oprid = load_arbid_map()
    arbocs_to_date = load_issuance_to_date()

    arb_ids = get_arb_ids(display_names)
    opr_ids = get_opr_id.map(arb_ids, prefect.unmapped(arbid_to_oprid))

    geometries = load_simplified_geometry.map(opr_ids)
    buffered = buffer_geometry.map(geometries, prefect.unmapped(30))
    centroids = get_centroid.map(buffered)

    arbocs = get_arbocs_to_date.map(arb_ids, prefect.unmapped(arbocs_to_date))
    project_areas = get_project_area.map(opr_ids)
    display_names = get_display_name.map(arb_ids, prefect.unmapped(display_names))
    location_names = get_location_name.map(centroids)
    records = construct_record.map(
        opr_ids, display_names, arbocs, project_areas, centroids, location_names
    )
    write_results(records)

flow.executor = prefect.executors.LocalDaskExecutor(scheduler='processes', num_workers=2)
