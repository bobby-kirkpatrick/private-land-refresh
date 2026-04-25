import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# --- Required environment variables ---
DB_FILE = os.getenv('DB_FILE')
if not DB_FILE:
    raise EnvironmentError(
        "DB_FILE environment variable is not set. Add it to your .env file."
    )

# --- Paths (overridable via env vars) ---
PARCEL_MANIFEST_PATH = os.getenv(
    'PARCEL_MANIFEST_PATH',
    r'D:\GIS Scripts\corelogic-parcel-processing\parcel_layer_file_manifest.json',
)

RAW_DATA_LOCATION = os.getenv('RAW_DATA_LOCATION', r'D:\CoreLogic_Download\Data_test')
DATA_WORKSPACE = os.getenv('DATA_WORKSPACE', r'D:\CoreLogic_Download\processed_parcels_test')

GOVT_NAME_TABLES_DIR = BASE_DIR / 'state_govt_land_name_tables'
XGB_MODELS_DIR = BASE_DIR / 'state_xgb_models'

LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# --- Business-logic thresholds ---
GOVT_OVERLAP_THRESHOLD = 80      # % overlap → classified as government land
GAP_ACRE_THRESHOLD = 160         # gaps larger than this (acres) are deleted
OVERLAP_SLIVER_THRESHOLD = 25    # intersections below this (acres) treated as slivers
QC_LARGE_PARCEL_THRESHOLD = 10   # QC flag-2 vs flag-3 acreage cutoff

# --- Arcpy settings ---
PARALLEL_PROCESSING_FACTOR = "25%"

# --- Sentinel value used in parcel owner fields for "no owner" ---
NULL_OWNER_SENTINEL = '   ,    '
