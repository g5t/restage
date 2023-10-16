__author__ = "Gregory Tucker"
__affiliation__ = "European Spallation Source ERIC"
import sys
if sys.version_info[0] == 3 and sys.version_info[1] < 8:
    import importlib_metadata
else:
    import importlib.metadata as importlib_metadata

from .tables import (SimulationEntry,
                     SimulationTableEntry,
                     NexusStructureEntry,
                     InstrEntry
                     )
from .database import Database
from .cache import DATABASE


try:
    __version__ = importlib_metadata.version(__name__)
except importlib_metadata.PackageNotFoundError:
    # package is not installed
    pass

__all__ = [
    '__version__',
    'SimulationEntry',
    'SimulationTableEntry',
    'NexusStructureEntry',
    'InstrEntry',
    'Database',
    'DATABASE'
]
