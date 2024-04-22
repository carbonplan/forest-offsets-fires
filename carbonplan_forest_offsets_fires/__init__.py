from importlib.metadata import PackageNotFoundError as _PackageNotFoundError
from importlib.metadata import version as _version

try:
    version = _version(__name__)
except _PackageNotFoundError:
    # package is not installed
    version = "unknown"
