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


dev = {
    'states': {
        # Uncomment states to include them in a run.
        # 'AZ': {'govt_land': _govt_land_path('Arizona',      'AZ'), 'parcels': _manifest.get('AZ')},
        # 'CO': {'govt_land': _govt_land_path('Colorado',     'CO'), 'parcels': _manifest.get('CO')},
        # 'UT': {'govt_land': _govt_land_path('Utah',         'UT'), 'parcels': _manifest.get('UT')},
        # 'MT': {'govt_land': _govt_land_path('Montana',      'MT'), 'parcels': _manifest.get('MT')},
        # 'CA': {'govt_land': _govt_land_path('California',   'CA'), 'parcels': _manifest.get('CA')},
        # 'WY': {'govt_land': _govt_land_path('Wyoming',      'WY'), 'parcels': _manifest.get('WY')},
        # 'NV': {'govt_land': _govt_land_path('Nevada',       'NV'), 'parcels': _manifest.get('NV')},
        # 'ID': {'govt_land': _govt_land_path('Idaho',        'ID'), 'parcels': _manifest.get('ID')},
        # 'NM': {'govt_land': _govt_land_path('New_Mexico',   'NM'), 'parcels': _manifest.get('NM')},
        # 'OR': {'govt_land': _govt_land_path('Oregon',       'OR'), 'parcels': _manifest.get('OR')},
        # 'WA': {'govt_land': _govt_land_path('Washington',   'WA'), 'parcels': _manifest.get('WA')},
        # 'WI': {'govt_land': _govt_land_path('Wisconsin',    'WI'), 'parcels': _manifest.get('WI')},
        # 'MI': {'govt_land': _govt_land_path('Michigan',     'MI'), 'parcels': _manifest.get('MI')},
        # 'TX': {'govt_land': _govt_land_path('Texas',        'TX'), 'parcels': _manifest.get('TX')},
        # 'MN': {'govt_land': _govt_land_path('Minnesota',    'MN'), 'parcels': _manifest.get('MN')},
        # 'OK': {'govt_land': _govt_land_path('Oklahoma',     'OK'), 'parcels': _manifest.get('OK')},
        # 'PA': {'govt_land': _govt_land_path('Pennsylvania', 'PA'), 'parcels': _manifest.get('PA')},
        'OH': {'govt_land': _govt_land_path('Ohio',         'OH'), 'parcels': _manifest.get('OH')},
        # 'IL': {'govt_land': _govt_land_path('Illinois',    'IL'), 'parcels': _manifest.get('IL')},
        # 'KS': {'govt_land': _govt_land_path('Kansas',      'KS'), 'parcels': _manifest.get('KS')},
        # 'IN': {'govt_land': _govt_land_path('Indiana',     'IN'), 'parcels': _manifest.get('IN')},
        # 'GA': {'govt_land': _govt_land_path('Georgia',     'GA'), 'parcels': _manifest.get('GA')},
        # 'SD': {'govt_land': _govt_land_path('South_Dakota','SD'), 'parcels': _manifest.get('SD')},
        # 'IA': {'govt_land': _govt_land_path('Iowa',        'IA'), 'parcels': _manifest.get('IA')},
        # 'TN': {'govt_land': _govt_land_path('Tennessee',   'TN'), 'parcels': _manifest.get('TN')},
        # 'NE': {'govt_land': _govt_land_path('Nebraska',    'NE'), 'parcels': _manifest.get('NE')},
        # 'ND': {'govt_land': _govt_land_path('North_Dakota','ND'), 'parcels': _manifest.get('ND')},
        # 'NC': {'govt_land': _govt_land_path('North_Carolina','NC'), 'parcels': _manifest.get('NC')},
        # 'NY': {'govt_land': _govt_land_path('New_York',    'NY'), 'parcels': _manifest.get('NY')},
        # 'MO': {'govt_land': _govt_land_path('Missouri',    'MO'), 'parcels': _manifest.get('MO')},
        # 'AL': {'govt_land': _govt_land_path('Alabama',     'AL'), 'parcels': _manifest.get('AL')},
        # 'LA': {'govt_land': _govt_land_path('Louisiana',   'LA'), 'parcels': _manifest.get('LA')},
        # 'AR': {'govt_land': _govt_land_path('Arkansas',    'AR'), 'parcels': _manifest.get('AR')},
        # 'KY': {'govt_land': _govt_land_path('Kentucky',    'KY'), 'parcels': _manifest.get('KY')},
        # 'MS': {'govt_land': _govt_land_path('Mississippi', 'MS'), 'parcels': _manifest.get('MS')},
        # 'VA': {'govt_land': _govt_land_path('Virginia',    'VA'), 'parcels': _manifest.get('VA')},
        # 'SC': {'govt_land': _govt_land_path('South_Carolina','SC'), 'parcels': _manifest.get('SC')},
        # 'WV': {'govt_land': _govt_land_path('West_Virginia','WV'), 'parcels': _manifest.get('WV')},
        # 'ME': {'govt_land': _govt_land_path('Maine',       'ME'), 'parcels': _manifest.get('ME')},
        # 'FL': {'govt_land': _govt_land_path('Florida',     'FL'), 'parcels': _manifest.get('FL')},
        # 'MD': {'govt_land': _govt_land_path('Maryland',    'MD'), 'parcels': _manifest.get('MD')},
        # 'NJ': {'govt_land': _govt_land_path('New_Jersey',  'NJ'), 'parcels': _manifest.get('NJ')},
        # 'VT': {'govt_land': _govt_land_path('Vermont',     'VT'), 'parcels': _manifest.get('VT')},
        # 'NH': {'govt_land': _govt_land_path('New_Hampshire','NH'), 'parcels': _manifest.get('NH')},
        # 'MA': {'govt_land': _govt_land_path('Massachusetts','MA'), 'parcels': _manifest.get('MA')},
        # 'CT': {'govt_land': _govt_land_path('Connecticut', 'CT'), 'parcels': _manifest.get('CT')},
        # 'DE': {'govt_land': _govt_land_path('Delaware',    'DE'), 'parcels': _manifest.get('DE')},
        # 'RI': {'govt_land': _govt_land_path('Rhode_Island','RI'), 'parcels': _manifest.get('RI')},
        # 'AK': {'govt_land': _govt_land_path('Alaska',      'AK'), 'parcels': _manifest.get('AK')},
        # 'HI': {'govt_land': _govt_land_path('Hawaii',      'HI'), 'parcels': _manifest.get('HI')},
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
