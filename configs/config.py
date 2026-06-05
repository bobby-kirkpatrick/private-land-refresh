import json
import os
from configs.settings import DB_FILE, PARCEL_MANIFEST_PATH, RAW_DATA_LOCATION, DATA_WORKSPACE

try:
    with open(PARCEL_MANIFEST_PATH, 'r') as f:
        _manifest = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(
        f"Parcel manifest not found at '{PARCEL_MANIFEST_PATH}'. "
        "Set PARCEL_MANIFEST_PATH in your .env file."
    )
except json.JSONDecodeError as e:
    raise ValueError(f"Parcel manifest is not valid JSON: {e}")


def _govt_land_path(state_folder: str, state_abbr: str) -> str:
    return os.path.join(DB_FILE, rf'entgdb.sde.{state_folder}\entgdb.sde.{state_abbr}_Government_Land')


def _private_land_path(state_folder: str, state_abbr: str) -> str:
    return os.path.join(DB_FILE, rf'entgdb.sde.{state_folder}\entgdb.sde.{state_abbr}_Parcels')


def _state_entry(folder: str, abbr: str) -> dict:
    """Build a full state config entry with all source and publish-target paths."""
    return {
        'govt_land':           _govt_land_path(folder, abbr),
        'parcels':             _manifest.get(abbr),
        'govt_land_target':    _govt_land_path(folder, abbr),
        'private_land_target': _private_land_path(folder, abbr),
    }


dev = {
    'states': {
        # Uncomment states to include them in a run.
        # 'AZ': _state_entry('Arizona',       'AZ'),
        # 'CO': _state_entry('Colorado',      'CO'),
        # 'UT': _state_entry('Utah',          'UT'),
        # 'MT': _state_entry('Montana',       'MT'),
        # 'CA': _state_entry('California',    'CA'),
        # 'WY': _state_entry('Wyoming',       'WY'),
        # 'NV': _state_entry('Nevada',        'NV'),
        # 'ID': _state_entry('Idaho',         'ID'),
        # 'NM': _state_entry('New_Mexico',    'NM'),
        # 'OR': _state_entry('Oregon',        'OR'),
        # 'WA': _state_entry('Washington',    'WA'),
        # 'WI': _state_entry('Wisconsin',     'WI'),
        # 'MI': _state_entry('Michigan',      'MI'),
        # 'TX': _state_entry('Texas',         'TX'),
        # 'MN': _state_entry('Minnesota',     'MN'),
        # 'OK': _state_entry('Oklahoma',      'OK'),
        # 'PA': _state_entry('Pennsylvania',  'PA'),
          'OH': _state_entry('Ohio',          'OH'),
        # 'IL': _state_entry('Illinois',      'IL'),
        # 'KS': _state_entry('Kansas',        'KS'),
        # 'IN': _state_entry('Indiana',       'IN'),
        # 'GA': _state_entry('Georgia',       'GA'),
        # 'SD': _state_entry('South_Dakota',  'SD'),
        # 'IA': _state_entry('Iowa',          'IA'),
        # 'TN': _state_entry('Tennessee',     'TN'),
        # 'NE': _state_entry('Nebraska',      'NE'),
        # 'ND': _state_entry('North_Dakota',  'ND'),
        # 'NC': _state_entry('North_Carolina','NC'),
        # 'NY': _state_entry('New_York',      'NY'),
        # 'MO': _state_entry('Missouri',      'MO'),
        # 'AL': _state_entry('Alabama',       'AL'),
        # 'LA': _state_entry('Louisiana',     'LA'),
        # 'AR': _state_entry('Arkansas',      'AR'),
        # 'KY': _state_entry('Kentucky',      'KY'),
        # 'MS': _state_entry('Mississippi',   'MS'),
        # 'VA': _state_entry('Virginia',      'VA'),
        # 'SC': _state_entry('South_Carolina','SC'),
        # 'WV': _state_entry('West_Virginia', 'WV'),
        # 'ME': _state_entry('Maine',         'ME'),
        # 'FL': _state_entry('Florida',       'FL'),
        # 'MD': _state_entry('Maryland',      'MD'),
        # 'NJ': _state_entry('New_Jersey',    'NJ'),
        # 'VT': _state_entry('Vermont',       'VT'),
        # 'NH': _state_entry('New_Hampshire', 'NH'),
        # 'MA': _state_entry('Massachusetts', 'MA'),
        # 'CT': _state_entry('Connecticut',   'CT'),
        # 'DE': _state_entry('Delaware',      'DE'),
        # 'RI': _state_entry('Rhode_Island',  'RI'),
        # 'AK': _state_entry('Alaska',        'AK'),
        # 'HI': _state_entry('Hawaii',        'HI'),
    },
    'acquisition_processing_parameters': {
        'raw data location': RAW_DATA_LOCATION,
        'data_workspace': DATA_WORKSPACE,
    },
    'state_codes': {
        'WA': '53', 'OR': '41', 'WV': '54', 'NH': '33', 'MA': '25', 'CT': '09',
        'CA': '06', 'DE': '10', 'RI': '44', 'AK': '02', 'HI': '15', 'SC': '45',
        'ME': '23', 'FL': '12', 'MD': '24', 'NJ': '34', 'VT': '50', 'SD': '46',
        'IA': '19', 'TN': '47', 'NE': '31', 'ND': '38', 'NC': '37', 'NY': '36',
        'MO': '29', 'AL': '01', 'LA': '22', 'AR': '05', 'KY': '21', 'TX': '48',
        'MN': '27', 'OK': '40', 'OH': '39', 'IL': '17', 'KS': '20', 'IN': '18',
        'GA': '13', 'WI': '55', 'MI': '26', 'PA': '42', 'CO': '08', 'AZ': '04',
        'UT': '49', 'MT': '30', 'NM': '35', 'NV': '32', 'WY': '56', 'ID': '16',
    },
}

state_full = {
    'AL': 'alabama',       'AK': 'alaska',        'AZ': 'arizona',
    'AR': 'arkansas',      'CA': 'california',    'CO': 'colorado',
    'CT': 'connecticut',   'DE': 'delaware',      'FL': 'florida',
    'GA': 'georgia',       'HI': 'hawaii',        'ID': 'idaho',
    'IL': 'illinois',      'IN': 'indiana',       'IA': 'iowa',
    'KS': 'kansas',        'KY': 'kentucky',      'LA': 'louisiana',
    'ME': 'maine',         'MD': 'maryland',      'MA': 'massachusetts',
    'MI': 'michigan',      'MN': 'minnesota',     'MS': 'mississippi',
    'MO': 'missouri',      'MT': 'montana',       'NE': 'nebraska',
    'NV': 'nevada',        'NH': 'new_hampshire', 'NJ': 'new_jersey',
    'NM': 'new_mexico',    'NY': 'new_york',      'NC': 'north_carolina',
    'ND': 'north_dakota',  'OH': 'ohio',          'OK': 'oklahoma',
    'OR': 'oregon',        'PA': 'pennsylvania',  'RI': 'rhode_island',
    'SC': 'south_carolina','SD': 'south_dakota',  'TN': 'tennessee',
    'TX': 'texas',         'UT': 'utah',          'VT': 'vermont',
    'VA': 'virginia',      'WA': 'washington',    'WV': 'west_virginia',
    'WI': 'wisconsin',     'WY': 'wyoming',
}
