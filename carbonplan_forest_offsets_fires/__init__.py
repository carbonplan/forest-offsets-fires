import pathlib
from importlib.metadata import PackageNotFoundError as _PackageNotFoundError
from importlib.metadata import version as _version

from intake import open_catalog

try:
    version = _version(__name__)
except _PackageNotFoundError:
    # package is not installed
    version = "unknown"

cat_dir = pathlib.Path(__file__)
cat_file = str(cat_dir.parent / 'data/catalog.yaml')
cat = open_catalog(cat_file)
