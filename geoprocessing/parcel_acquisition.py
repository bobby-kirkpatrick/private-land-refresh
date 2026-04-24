import arcpy
import os
import time
import shutil
import zipfile
from datetime import datetime, timedelta

today = time.strftime("%m_%d_%Y")
# uncomment lines 10 and 11
yesterday = datetime.now() - timedelta(1)
today = datetime.strftime(yesterday, '%m_%d_%Y')

start = time.time()

# check and download data from FTP

state_codes = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '02', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '08',
    'CA': '06', 'AL': '01', 'AR': '05', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '04', 'ID': '16', 'CT': '09',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46'
}


class ParcelProcessing:
    def __init__(self, config):
        self.data = config['acquisition_processing_parameters']['raw data location']
        self.env = config['acquisition_processing_parameters']['data_workspace']
        self.state_data = config['states']
        # switch to state_code
        self.state_codes_test = config['state_codes']

    def set_workspaces(self):
        # project folder
        location = self.env
        self.parcel_folder = os.path.join(location, 'corelogic_parcels_{}'.format(today))
        if os.path.exists(self.parcel_folder):
            print('Parcel folder already exist at {}'.format(os.path.join(location, self.parcel_folder)))
        else:
            os.mkdir(os.path.join(location, self.parcel_folder))
            print('New parcel folder created at {}'.format(os.path.join(location, self.parcel_folder)))

        location = self.env
        self.temp_folder = os.path.join(location, 'temp')
        if os.path.exists(self.temp_folder):
            print('Temp folder already exist at {}'.format(os.path.join(location, self.temp_folder)))
        else:
            os.mkdir(os.path.join(location, self.temp_folder))
            print('New temp folder created at {}'.format(os.path.join(location, self.temp_folder)))

        # state gdb in prooject folder
        #TODO swithc to full list
        for k in self.state_codes_test.keys():
            gdb_location = self.parcel_folder
            state_gdb = '{}_parcels_{}.gdb'.format(k, today)
            if arcpy.Exists(os.path.join(gdb_location, state_gdb)):
                print('State gdb already exist at {}'.format(os.path.join(gdb_location, state_gdb)))
            else:
                arcpy.CreateFileGDB_management(gdb_location, state_gdb)
                print('State gdb created at {}'.format(os.path.join(gdb_location, state_gdb)))



    def extract_counties(self):
        #TODO swithc to full list
        for k, v in self.state_codes_test.items():
            state = k
            fips = v

            # create temp folder to extract state shapefiles to
            self.temp_state_folder = os.path.join(self.env, 'temp', '{}_temp'.format(state))
            if os.path.exists(self.temp_state_folder):
                print('temp state folder already exist at {}'.format(self.temp_state_folder))
            else:
                os.mkdir(self.temp_state_folder)
                print('New temp state folder created at {}'.format(self.temp_state_folder))

                # extract each county to the temp folder
                for f in os.listdir(self.data):
                    folder_fips = f[3:5]

                    if folder_fips == fips:
                        shutil.unpack_archive(os.path.join(self.data, f), self.temp_state_folder)
                    print('{} extracted'.format(state))

    def merge_counties(self):
        merged_state = {}
        #TODO switch to full list
        for k, v in self.state_codes_test.items():
            state = k
            fips = v

            self.state_gdb = os.path.join(self.parcel_folder, '{}_parcels_{}.gdb'.format(state, today))
            print(self.state_gdb)

            temp_state_folder = os.path.join(self.env, 'temp', '{}_temp'.format(state))
            county_shapefile_list = []
            for f in os.listdir(temp_state_folder):
                if f.endswith('.shp'):
                    county_shapefile_list.append(os.path.join(temp_state_folder, f))


            self.output = os.path.join(self.state_gdb, '{}_parcels'.format(state))

            if arcpy.Exists(self.output):
                print("Merged output already exists for {}".format(state))
            else:
                arcpy.Merge_management(county_shapefile_list, self.output)
                print("Merged output created for {}".format(state))

            merged_state[state] = self.output

            print('{} merge complete'.format(state))

        print(merged_state)
        return merged_state


    def field_processing(self, states):
        # fields to add gh_govt, gh_parcel_acres, gh_parcelID, mail_addr
        # shorten mail zip
        add_field_dict = {
            'gh_govt': ['gh_govt', 'TEXT', '', 10, None, ''],
            'gh_parcel_acres': ['gh_parcel_acres', 'DOUBLE', '', None, None, ''],
            'unit_nm': ['unit_nm', 'TEXT', '', 150, None, ''],
            'gh_govtype': ['gh_govtype', 'TEXT', '', 150, None, ''],
            'mail_addr': ['mail_addr', 'TEXT', '', 50, None, ''],
            'overlap_perc': ['overlap_perc', 'DOUBLE', '', None, None, '']
        }

        for state, path in states.items():

            # list fields to check for existence
            fields = [f.name for f in arcpy.ListFields(path)]

            add_field_metadata = []

            for k, v in add_field_dict.items():
                if k not in fields:
                    add_field_metadata.append(v)

            # add fields
            if len(add_field_metadata) > 0:
                arcpy.management.AddFields(path, add_field_metadata)
                print('fields added')
            else:
                print('fields already added')


            # calculate fields
            arcpy.CalculateFields_management(
                in_table=path, expression_type="PYTHON3",
                fields=[["gh_parcel_acres", "!shape.area@acres!"],
                ["mail_addr", "!MAIL_NBR! + ' ' + !MAIL_DIR! + ' ' + !MAIL_STR! + ' ' + !MAIL_MODE!"],
                ["MAIL_ZIP", "!MAIL_ZIP![:5]"]]
            )

            arcpy.management.CalculateField(path, "mail_addr", "!mail_addr!.strip().replace('  ', ' ').replace('   ', ' ')", "PYTHON3")
            arcpy.management.CalculateField(path, "mail_addr", "!mail_addr!.strip().replace('  ', ' ')", "PYTHON3")
            

            print('gh_parcel_acres, mail_addr, MAIL_ZIP calculated')

    
    def calc_govt_overlap(self, states):


        for state, path in states.items():

            govt_land = self.state_data[state]['govt_land']
        

            # dissolve govt land layers into single feature to get govt land footprint
            self.dissolved_govt_features = os.path.join(os.path.join(self.parcel_folder, '{}_parcels_{}.gdb'.format(state, today)), '{}_dissolved_govt_features'.format(state))
            if arcpy.Exists(self.dissolved_govt_features):
                print('{}_dissolved_govt_features already exists'.format(state))
            else:
                arcpy.Dissolve_management(govt_land, self.dissolved_govt_features)
                print('{} govt land layer dissolved'.format(state))

            govt_dissolved_time = time.time()
            print('time elapsed {}'.format(govt_dissolved_time - start))

            # intersect dissovled govt lands and parcels
            intx_output = os.path.join(os.path.join(self.parcel_folder, '{}_parcels_{}.gdb'.format(state, today)), '{}_govt_private_intx'.format(state))
            if arcpy.Exists(intx_output):
                print('{}_govt_private_intx already exists'.format(state))
            else:
                arcpy.Intersect_analysis([self.dissolved_govt_features, path], intx_output)
                print('{}_govt_private_intx created'.format(state))

            # dissolve intersect to get all govt land polygons for a parcel
            intx_field_names = [f.name for f in arcpy.ListFields(intx_output)]
            rm_fields = ['OBJECTID', 'Shape_Length', 'Shape_Area']
            dissolve_fields = [e for e in intx_field_names if e not in rm_fields]

            temp_dissolved_intx_govt = os.path.join(os.path.join(self.parcel_folder, '{}_parcels_{}.gdb'.format(state, today)),
                                                    '{}_dissolved_intx_private_govt_features'.format(state))
            if arcpy.Exists(temp_dissolved_intx_govt):
                print('{}_dissolved_intx_private_govt_features already exists'.format(state))
            else:
                arcpy.Dissolve_management(intx_output, temp_dissolved_intx_govt, dissolve_fields)
                print('{}_dissolved_intx_private_govt_features created'.format(state))

            # add overlap acres field and calculate
            field_metadata = [['overlap_acres', 'DOUBLE', '', None, None, ''],
                            ['overlap_perc', 'DOUBLE', '', None, None, '']]
            try:
                arcpy.management.AddFields(temp_dissolved_intx_govt, field_metadata)
            except:
                print('overlap fields already added')

            arcpy.management.CalculateField(temp_dissolved_intx_govt, "overlap_acres", "!shape.area@acres!", "PYTHON3")
            arcpy.management.CalculateField(temp_dissolved_intx_govt, "overlap_perc",
                                            "(!overlap_acres!/!gh_parcel_acres!) * 100", "PYTHON3")
            print('overalap fields calcualted')

            # join  overlap percentage back to parcels
            # search cursor and update dict with parcel ID and overlap perc values
            overlap_dict = {}
            with arcpy.da.SearchCursor(temp_dissolved_intx_govt, ["parcel_id", "overlap_perc"]) as cursor:
                for row in cursor:
                    overlap_dict[row[0]] = row[1]

            # update parcel layer with overlap percentage
            with arcpy.da.UpdateCursor(path, ["parcel_id", "overlap_perc"]) as cursor:
                for row in cursor:
                    if row[0] in overlap_dict.keys():
                        overlap_perc_1 = overlap_dict.get(row[0])
                        row[1] = overlap_perc_1
                    else:
                        row[1] = 0

                    cursor.updateRow(row)

            arcpy.Delete_management([self.dissolved_govt_features, intx_output, temp_dissolved_intx_govt])

            print("Overlap percentage added to parcels")
