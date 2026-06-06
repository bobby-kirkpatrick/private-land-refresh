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

# --- Privacy redaction ---
# Path to the enterprise GDB feature class whose points identify properties
# that must have all ownership information removed before delivery.
# Set via .env or environment variable; not required by main.py.
PRIVACY_POINTS_FC: str = os.getenv('PLR_PRIVACY_POINTS_FC', '')

# Ownership / PII fields that are nulled out on redacted parcels.
# gh_govt (classification) and gh_parcel_acres (area) are intentionally
# excluded — they are not personally identifiable information.
REDACT_FIELDS: tuple[str, ...] = (
    'OWN1_LAST', 'OWN1_FRST',
    'OWN2_LAST', 'OWN2_FRST',
    'MAIL_ADDR', 'MAIL_ZIP', 'MAIL_STATE', 'MAIL_CITY',
)

# --- VTPK export settings ---
# Names of the layers to target inside each state's aprx map.
# Override via .env if your project uses different layer names.
VTPK_PRIVATE_LAYER_NAME: str = os.getenv('PLR_VTPK_PRIVATE_LAYER', 'Private Land')
VTPK_GOVT_LAYER_NAME: str = os.getenv('PLR_VTPK_GOVT_LAYER', 'Government Land')

# Default paths used when --aprx / --output are not passed on the CLI.
# Set in .env to avoid typing them on every invocation.
VTPK_DEFAULT_APRX: str = os.getenv('PLR_VTPK_APRX_PATH', '')
VTPK_DEFAULT_OUTPUT: str = os.getenv('PLR_VTPK_OUTPUT_FOLDER', '')

# --- S3 upload settings ---
# Credentials MUST come from .env — never hardcode them in source files.
AWS_ACCESS_KEY_ID: str = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY: str = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_S3_BUCKET: str = os.getenv('AWS_S3_BUCKET', 'gh-gis-source-repo')
# S3 prefix (folder) where VTPKs are stored, e.g. "vectortiles/co/CO_private_land_Q2_2026.vtpk"
AWS_VTPK_S3_PREFIX: str = os.getenv('AWS_VTPK_S3_PREFIX', 'vectortiles')
