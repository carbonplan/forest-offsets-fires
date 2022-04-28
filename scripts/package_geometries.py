import warnings

import click
import fsspec
import pandas as pd
import tqdm
from carbonplan_forest_offsets.utils import load_project_geometry

from carbonplan_forest_offsets_fires import utils

# geopandas parquet in beta
warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')


@click.command()
@click.option('--outpath', default='gs://carbonplan-forest-offsets/carb-geometries')
def main(outpath):
    """repackage project geometries into single compressed geodataframe

    Explicitly an attempt to save bandwith/egress for monitoring.
    """
    opr_ids = utils.list_all_opr_ids()
    gdf = pd.concat([load_project_geometry(opr_id) for opr_id in tqdm.tqdm(opr_ids)])
    with fsspec.open(f'{outpath}/all_carb_geoms.parquet', 'wb') as f:
        gdf.to_parquet(f, compression='gzip', compression_level=9)


if __name__ == '__main__':
    main()
