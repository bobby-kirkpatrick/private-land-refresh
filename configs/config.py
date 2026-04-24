import json
import os
from dotenv import load_dotenv

# remote file config with most recent layers
with open(r'D:\GIS Scripts\corelogic-parcel-processing\parcel_layer_file_manifest.json', 'r') as file:
    # Load the JSON data into a dictionary
    data = json.load(file)

load_dotenv()
database_file_connection = os.getenv('DB_FILE')


dev = {
    'states':  {
        # 'AZ':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Arizona\entgdb.sde.AZ_Government_Land'),
        #         'parcels': data.get('AZ')
        #     },
        # 'CO':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Colorado\entgdb.sde.CO_Government_Land'),
        #         'parcels': data.get('CO')
        #     },
        # 'UT':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Utah\entgdb.sde.UT_Government_Land'),
        #         'parcels': data.get('UT')
        #     },
        # 'MT':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Montana\entgdb.sde.MT_Government_Land'),
        #         'parcels': data.get('MT')
        #     },
        # 'CA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.California\entgdb.sde.CA_Government_Land'),
        #         'parcels': data.get('CA')
        #     },
        # 'WY':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Wyoming\entgdb.sde.WY_Government_Land'),
        #         'parcels': data.get('WY')
        #     },
        # 'NV':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Nevada\entgdb.sde.NV_Government_Land'),
        #         'parcels': data.get('NV')
        #     },
        # 'ID':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Idaho\entgdb.sde.ID_Government_Land'),
        #         'parcels': data.get('ID')
        #     },
        # 'NM':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.New_Mexico\entgdb.sde.NM_Government_Land'),
        #         'parcels': data.get('NM')
        #     },
        # 'OR':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Oregon\entgdb.sde.OR_Government_Land'),
        #         'parcels': data.get('OR')
        #     },
        # 'WA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Washington\entgdb.sde.WA_Government_Land'),
        #         'parcels': data.get('WA')
        #     },
        # 'WI':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Wisconsin\entgdb.sde.WI_Government_Land'),
        #         'parcels': data.get('WI')
        #     },
        # 'MI':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Michigan\entgdb.sde.MI_Government_Land'),
        #         'parcels': data.get('MI')
        #     },
        #  'TX':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Texas\entgdb.sde.TX_Government_Land'),
        #         'parcels': data.get('TX')
        #      },
        # 'MN':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Minnesota\entgdb.sde.MN_Government_Land'),
        #         'parcels': data.get('MN')
        #     },
        # 'OK':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Oklahoma\entgdb.sde.OK_Government_Land'),
        #         'parcels': data.get('OK')
        #     },
        # 'PA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Pennsylvania\entgdb.sde.PA_Government_Land'),
        #         'parcels': data.get('PA')
        #     },
        'OH':
            {
                'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Ohio\entgdb.sde.OH_Government_Land'),
                'parcels': data.get('OH')
            },
        # 'IL':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Illinois\entgdb.sde.IL_Government_Land'),
        #         'parcels': data.get('IL')
        #     },
        # 'KS':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Kansas\entgdb.sde.KS_Government_Land'),
        #         'parcels': data.get('KS')
        #     },
        # 'IN':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Indiana\entgdb.sde.IN_Government_Land'),
        #         'parcels': data.get('IN')
        #     },
        # 'GA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Georgia\entgdb.sde.GA_Government_Land'),
        #         'parcels': data.get('GA')
        #     },
        # 'SD':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.South_Dakota\entgdb.sde.SD_Government_Land'),
        #         'parcels': data.get('SD')
        #     },
        # 'IA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Iowa\entgdb.sde.IA_Government_Land'),
        #         'parcels': data.get('IA')
        #     },
        # 'TN':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Tennessee\entgdb.sde.TN_Government_Land'),
        #         'parcels': data.get('TN')
        #     },
        # 'NE':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Nebraska\entgdb.sde.NE_Government_Land'),
        #         'parcels': data.get('NE')
        #     },
        # 'ND':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.North_Dakota\entgdb.sde.ND_Government_Land'),
        #         'parcels': data.get('ND')
        #     },
        # 'NC':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.North_Carolina\entgdb.sde.NC_Government_Land'),
        #         'parcels': data.get('NC')
        #     },
        # 'NY':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.New_York\entgdb.sde.NY_Government_Land'),
        #         'parcels': data.get('NY')
        #     },
        # 'MO':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Missouri\entgdb.sde.MO_Government_Land'),
        #         'parcels': data.get('MO')
        #     },
        # 'AL':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Alabama\entgdb.sde.AL_Government_Land'),
        #         'parcels': data.get('AL')
        #     },
        # 'LA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Louisiana\entgdb.sde.LA_Government_Land'),
        #         'parcels': data.get('LA')
        #     },
        # 'AR':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Arkansas\entgdb.sde.AR_Government_Land'),
        #         'parcels': data.get('AR')
        #     },
        # 'KY':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Kentucky\entgdb.sde.KY_Government_Land'),
        #         'parcels': data.get('KY')
        #     },
        # 'MS':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Mississippi\entgdb.sde.MS_Government_Land'),
        #         'parcels': data.get('MS')
        #     },
        # 'VA':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Virginia\entgdb.sde.VA_Government_Land'),
        #         'parcels': data.get('VA')
        #     },
        # 'SC':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.South_Carolina\entgdb.sde.SC_Government_Land'),
        #         'parcels': data.get('SC')
        #     },
        # 'WV':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.West_Virginia\entgdb.sde.WV_Government_Land'),
        #         'parcels': data.get('WV')
        #     },
        # 'ME':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Maine\entgdb.sde.ME_Government_Land'),
        #         'parcels': data.get('ME')
        #     },
        # 'FL':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Florida\entgdb.sde.FL_Government_Land'),
        #         'parcels': data.get('FL')
        #     },
        # 'MD':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Maryland\entgdb.sde.MD_Government_Land'),
        #         'parcels': data.get('MD')
        #     },
        # 'NJ':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.New_Jersey\entgdb.sde.NJ_Government_Land'),
        #         'parcels': data.get('NJ')
        #     },
        # 'VT':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Vermont\entgdb.sde.VT_Government_Land'),
        #         'parcels': data.get('VT')
        #     },
        # 'NH':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.New_Hampshire\entgdb.sde.NH_Government_Land'),
        #         'parcels': data.get('NH')
        #     },
        # 'MA':
        #     {
        #         'govt_land':os.path.join(database_file_connection, r'entgdb.sde.Massachusetts\entgdb.sde.MA_Government_Land'),
        #         'parcels': data.get('MA')
        #     },
        # 'CT':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Connecticut\entgdb.sde.CT_Government_Land'),
        #         'parcels': data.get('CT')
        #     },
        # 'DE':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Delaware\entgdb.sde.DE_Government_Land'),
        #         'parcels': data.get('DE')
        #     },
        # 'RI':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Rhode_Island\entgdb.sde.RI_Government_Land'),
        #         'parcels': data.get('RI')
        #     },
        # 'AK':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Alaska\entgdb.sde.AK_Government_Land'),
        #         'parcels': data.get('AK')
        #     },
        # 'HI':
        #     {
        #         'govt_land': os.path.join(database_file_connection, r'entgdb.sde.Hawaii\entgdb.sde.HI_Government_Land'),
        #         'parcels': data.get('HI')
        #     }
                },
    'acquisition_processing_parameters': 
            {
            'raw data location': r'D:\CoreLogic_Download\Data_test',
            'data_workspace': r'D:\CoreLogic_Download\processed_parcels_test'
            },
    'state_codes': 
            {
            'WA': '53','OR': '41', 'WV': '54', 'NH': '33', 'MA': '25', 'CT': '09', 'CA': '06', 'DE': '10', 'RI': '44', 'AK': '02', 'HI': '15', 'SC': '45', 'ME': '23', 'FL': '12', 'MD': '24', 'NJ': '34', 'VT': '50','SD': '46', 'IA': '19', 'TN': '47', 'NE': '31', 'ND': '38', 'NC': '37', 'NY': '36', 'MO': '29', 'AL': '01', 'LA': '22', 'AR': '05', 'KY': '21', 'TX': '48', 'MN': '27', 'OK': '40', 'OH': '39', 'IL': '17', 'KS': '20', 'IN': '18', 'GA': '13', 'WI': '55', 'MI': '26', 'PA': '42', 'CO': '08', 'AZ': '04', 'UT': '49', 'MT': '30', 'NM': '35', 'NV': '32', 'WY': '56', 'ID': '16'
            }       
}



state_full = {
    'AL': 'alabama',
    'AK': 'alaska',
    'AZ': 'arizona',
    'AR': 'arkansas',
    'CA': 'california',
    'CO': 'colorado',
    'CT': 'connecticut',
    'DE': 'delaware',
    'FL': 'florida',
    'GA': 'georgia',
    'HI': 'hawaii',
    'ID': 'idaho',
    'IL': 'illinois',
    'IN': 'indiana',
    'IA': 'iowa',
    'KS': 'kansas',
    'KY': 'kentucky',
    'LA': 'louisiana',
    'ME': 'maine',
    'MD': 'maryland',
    'MA': 'massachusetts',
    'MI': 'michigan',
    'MN': 'minnesota',
    'MS': 'mississippi',
    'MO': 'missouri',
    'MT': 'montana',
    'NE': 'nebraska',
    'NV': 'nevada',
    'NH': 'new_hampshire',
    'NJ': 'new_jersey',
    'NM': 'new_mexico',
    'NY': 'new_york',
    'NC': 'north_carolina',
    'ND': 'north_dakota',
    'OH': 'ohio',
    'OK': 'oklahoma',
    'OR': 'oregon',
    'PA': 'pennsylvania',
    'RI': 'rhode_island',
    'SC': 'south_carolina',
    'SD': 'south_dakota',
    'TN': 'tennessee',
    'TX': 'texas',
    'UT': 'utah',
    'VT': 'vermont',
    'VA': 'virginia',
    'WA': 'washington',
    'WV': 'west_virginia',
    'WI': 'wisconsin',
    'WY': 'wyoming'
}