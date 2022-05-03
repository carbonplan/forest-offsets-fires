from pathlib import Path

import fsspec


def list_all_opr_ids(bucket: str = 'carbonplan-forest-offsets/carb-geometries/raw') -> list:
    """Return list of all opr ids
    Inputs:
        bucket {str} -- gcs location of geometries
    Returns:
        list -- all OPR ids as string
    """
    fs = fsspec.filesystem('gs')
    fnames = fs.glob(f'{bucket}/*json')
    opr_ids = [Path(fname).stem for fname in fnames]
    return opr_ids
