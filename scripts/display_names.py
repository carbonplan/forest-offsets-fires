import json
import pathlib

import fsspec
import tqdm
from carbonplan_forest_offsets.load.issuance import load_issuance_table

from carbonplan_forest_offsets_fires import utils


def get_issuance_to_date():
    issuance = load_issuance_table(most_recent=True)
    issuance_to_date = issuance.groupby('arb_id')['allocation'].sum().to_dict()
    return issuance_to_date


def get_arbid_map():
    issuance = load_issuance_table(most_recent=True)
    df = issuance[['opr_id', 'arb_id']].set_index('arb_id').copy()
    return df.opr_id.to_dict()


def get_project_area(opr_id):
    geom = utils.load_project_geometry(opr_id)
    return round((geom.area / 4046.86).item())


def load_display_names():
    data_path = pathlib.Path(__file__).parents[1]
    with fsspec.open(data_path / 'data' / 'display-names.json') as f:
        display_names = json.load(f)
    return {x['arb_id']: x['name'] for x in display_names}


def process_metadata(metadata, arbocs_to_date):
    arbocs = arbocs_to_date.get(metadata['arb_id'], -999)
    return {
        'arb_id': metadata['arb_id'],
        'name': metadata['Project_Name_1'].strip(),
        'arbocs_issued': arbocs,
    }


def main():
    store = []

    display_names = load_display_names()
    arbid_to_oprid = get_arbid_map()
    arbocs_to_date = get_issuance_to_date()

    for arb_id in tqdm.tqdm(display_names.keys()):
        record = {}

        opr_id = arbid_to_oprid[arb_id]

        record['opr_id'] = opr_id
        record['arb_id'] = arb_id
        record['name'] = display_names[arb_id]
        record['arbocs'] = arbocs_to_date[arb_id]
        record['area'] = get_project_area(opr_id)
        store.append(record)

    with fsspec.open('gs://carbonplan-forest-offsets/web/display-data.json', 'w') as f:
        json.dump(store, f)


if __name__ == '__main__':
    main()
