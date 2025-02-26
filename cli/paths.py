import os
from pathlib import Path

BASE_DIR = Path(os.getcwd())

# Data directories
DATA_LANDING_DIR = BASE_DIR / "data" / "landing"
DATA_LANDING_ONTOLOGIES_DIR = DATA_LANDING_DIR / "ontologies"
DATA_LANDING_OPENTARGETS_DIR = DATA_LANDING_DIR / "opentargets"
