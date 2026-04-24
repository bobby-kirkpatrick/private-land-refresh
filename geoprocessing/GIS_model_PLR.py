import arcpy
import os
import time
import shutil
from arcpy import gapro

print('imports done')

month = time.strftime("%m")
year = time.strftime("%Y")

start = time.time()

def get_quarter(month, year):
    if month in ['01', '02', '03']:
        return f'Q1_{year}'
    elif month in ['04', '05', '06']:
        return f'Q2_{year}'
    elif month in ['07', '08', '09']:
        return f'Q3_{year}'
    else:
        return f'Q4_{year}'


quarter = get_quarter(month, year)


class PLR_GIS_model:
    def __init__(self, data, state, env='LOCAL'):
        self.data = data
        self.state = state
        self.env = env

        # set environment
        if env == 'LOCAL':
            self.workspace = os.getcwd()
        else:
            self.workspace = env

        # set config
        self.govt_land = data['govt_land']
        self.parcels = data['parcels']

        print(self.parcels)

    def set_workspaces(self):
        location = self.workspace
        temp_folder = '{}_temp_{}.gdb'.format(self.state, quarter)
        final_folder = '{}_private_land_{}.gdb'.format(self.state, quarter)
        # temp
        if os.path.exists(os.path.join(location, temp_folder)):
            print('Temp workspace already exist at {}'.format(os.path.join(location, temp_folder)))
        else:
            arcpy.CreateFileGDB_management(location, temp_folder)
            print('New temp workspace created at {}'.format(os.path.join(location, temp_folder)))

        # final
        if os.path.exists(os.path.join(location, final_folder)):
            print('Final workspace already exist at {}'.format(os.path.join(location, final_folder)))
        else:
            arcpy.CreateFileGDB_management(location, final_folder)
            print('Final workspace created at {}'.format(os.path.join(location, final_folder)))

        self.temp_dir = os.path.join(location, temp_folder)
        self.final_dir = os.path.join(location, final_folder)
        temp_workspace_time = time.time()
        print('time elapsed {}'.format(temp_workspace_time - start))

    def label_private_public(self):
        # dissolve govt land layers into single feature to get govt land footprint
        dissolved_govt_features = os.path.join(self.temp_dir, '{}_dissolved_govt_features'.format(self.state))
        if arcpy.Exists(dissolved_govt_features):
            print('{}_dissolved_govt_features already exists'.format(self.state))
        else:
            arcpy.Dissolve_management(self.govt_land, dissolved_govt_features)
            print('{} govt land layer dissolved'.format(self.state))

        govt_dissolved_time = time.time()
        print('time elapsed {}'.format(govt_dissolved_time - start))

        # create points at the centroid of each corelogic polygon (inside)
        corelogic_centroids = os.path.join(self.temp_dir, '{}_corelogic_centroids'.format(self.state))
        if arcpy.Exists(corelogic_centroids):
            print('{}_corelogic_centroids already exists'.format(self.state))
        else:
            arcpy.FeatureToPoint_management(self.parcels, corelogic_centroids, "INSIDE")
            print('{} corelogic centroids created'.format(self.state))

        centroid_time = time.time()
        print('time elapsed {}'.format(centroid_time - start))

        # intersect CL centroids with dissolved govt land layer
        centroid_govt_intersect = os.path.join(self.temp_dir, '{}_centroid_govt_intx'.format(self.state))
        if arcpy.Exists(centroid_govt_intersect):
            print('{}_centroid_govt_intx already exists'.format(self.state))
        else:
            arcpy.Intersect_analysis([corelogic_centroids, dissolved_govt_features], centroid_govt_intersect)
            print('{}_centroid_govt_intx created'.format(self.state))

        centroid_intx_time = time.time()
        print('time elapsed {}'.format(centroid_intx_time - start))

        # create dictionary like {FID: [gh_govtype, Unit_Nm]}
        govt_intx_dict = {}
        intx_fields = ['FID_{}_corelogic_centroids'.format(self.state), 'gh_govtype', 'unit_nm']

        with arcpy.da.SearchCursor(centroid_govt_intersect, intx_fields) as cursor:
            for row in cursor:
                govt_intx_dict[row[0]] = [row[1], row[2]]

        # update cursor on CL data - if FID is in list gh_govt = true else = false
        corelogic_update_fields = ['OBJECTID', 'gh_govt', 'gh_govtype', 'unit_nm', 'overlap_perc']

        with arcpy.da.UpdateCursor(self.parcels, corelogic_update_fields) as cursor:
            for row in cursor:
                if row[0] in govt_intx_dict.keys():
                    govt_type = govt_intx_dict.get(row[0])[0]
                    unit_nm = govt_intx_dict.get(row[0])[1]
                    row[1] = 'TRUE'
                    row[2] = govt_type
                    row[3] = unit_nm
                if row[4] >= 80:
                    row[1] = 'TRUE'
                else:
                    row[1] = 'FALSE'

                cursor.updateRow(row)
        print('{} private land labled'.format(self.state))

        label_time = time.time()
        print('time elapsed {}'.format(label_time - start))
