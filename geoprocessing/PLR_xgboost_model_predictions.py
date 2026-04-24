import pandas as pd
import arcpy
import time
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import os
import csv
from xgboost import XGBClassifier


start = time.time()

month = time.strftime("%m")
year = time.strftime("%Y")


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

# read in feature class
class PLR_xgboost_model:
    def __init__(self, data, state, env='LOCAL'):
        self.data = data
        print(self.data)
        self.state = state
        print(self.state)
        self.env = env

        # set environment
        if env == 'LOCAL':
            self.workspace = os.getcwd()
        else:
            self.workspace = env

        # set config
        self.govt_land = data['govt_land']
        print(self.govt_land)
        self.parcels = data['parcels']
        print(self.parcels)

    def set_workspaces(self):
        location = self.workspace
        temp_folder = '{}_temp_{}.gdb'.format(self.state, quarter)
        # temp
        if os.path.exists(os.path.join(location, temp_folder)):
            print('Temp workspace already exist at {}'.format(os.path.join(location, temp_folder)))
        else:
            arcpy.CreateFileGDB_management(location, temp_folder)
            print('New temp workspace created at {}'.format(os.path.join(location, temp_folder)))

        self.temp_dir = os.path.join(location, temp_folder)
        temp_workspace_time = time.time()
        print('time elapsed {}'.format(temp_workspace_time - start))

    # centroid calculation and column update
    def add_centroid_attr(self):
        # dissolve govt land layers into single feature to get govt land footprint
        self.dissolved_govt_features = os.path.join(self.temp_dir, '{}_dissolved_govt_features'.format(self.state))
        if arcpy.Exists(self.dissolved_govt_features):
            print('{}_dissolved_govt_features already exists'.format(self.state))
        else:
            arcpy.Dissolve_management(self.govt_land, self.dissolved_govt_features)
            print('{} govt land layer dissolved'.format(self.state))

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
            arcpy.Intersect_analysis([corelogic_centroids, self.dissolved_govt_features], centroid_govt_intersect)
            print('{}_centroid_govt_intx created'.format(self.state))

        # add IDs of centroids that intx govt footprint to list
        centroid_ID_govt_intx = {}
        with arcpy.da.SearchCursor(centroid_govt_intersect,
                                   ["FID_{}_corelogic_centroids".format(self.state), "PARCEL_ID"]) as cursor:
            for row in cursor:
                centroid_ID_govt_intx[row[0]] = row[1]
        print("centroid intersect list created")

        # add column and update where centroid intx govt land
        field_metadata = [["govt_centroid", 'LONG', '', None, 0, ''],
                          ["private_centroid", 'LONG', '', None, 0, '']]
        try:
            print("adding centroid fields")
            arcpy.management.AddFields(self.parcels, field_metadata)
            print("centroid fields added")
        except:
            print('centroid fields already added')

        # update the field indicating if the parcel centroid intersects the govt footprint
        corelogic_update_fields = ["OBJECTID", "govt_centroid", "private_centroid"]

        print("begining centroid field update")
        with arcpy.da.UpdateCursor(self.parcels, corelogic_update_fields) as cursor:
            for row in cursor:
                if row[0] in centroid_ID_govt_intx.keys():
                    row[1] = 1
                else:
                    row[2] = 1

                cursor.updateRow(row)

        print("cenroid column values updated")

        centroid_intx_time = time.time()
        print('time elapsed {}'.format(centroid_intx_time - start))

    def add_xgb_field(self):
        arcpy.management.AddField(self.parcels, "xgb_gh_govt", "TEXT", '', 10, '', '')

    # search cursor to put all govt names in a list (search where gh_govt = true), update new column with govt/private label, need to figure out what to do with no owner info
    def label_owner_type(self):
        # add 3 columns, type long, initial value of 0, 1.govt_owner 2.private_owner 3.no_owner
        # also add column for concatenated owner names
        owner_field_metadata = [["govt_owner", 'LONG', '', None, 0, ''],
                                ["private_owner", 'LONG', '', None, 0, ''],
                                ["no_owner", 'LONG', '', None, 0, ''],
                                ["full_name", "TEXT", '', 255, '', '']]

        try:
            print("adding owner type fields")
            arcpy.management.AddFields(self.parcels, owner_field_metadata)
            print("owner type fields added")
        except:
            print('owner type fields already added')

        # concat all owner names
        owner_concat = "!OWN1_FRST!" + "' '" + "!OWN1_LAST!" + "', '" + "!OWN2_FRST!" + "' '" + "!OWN2_LAST!"
        arcpy.CalculateField_management(self.parcels, "full_name", owner_concat, "PYTHON3")
        print("owner names concatenated")

        # add names where gh_govt=TRUE to a dictionary
        govt_name_dict = {}
        with open(
                os.path.join(self.workspace, r'state_govt_land_name_tables\{}_govt_names.csv'.format(self.state)),
                'r') as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                full_name = row['full_name']
                if full_name in govt_name_dict:
                    govt_name_dict[full_name] += 1
                else:
                    govt_name_dict[full_name] = 1

        govt_name_dict.pop('   ,    ')

        # update cursor to update each owner type value
        owner_update_fields = ["full_name", "govt_owner", "private_owner", "no_owner"]

        print("begining owner field update")
        with arcpy.da.UpdateCursor(self.parcels, owner_update_fields) as cursor:
            for row in cursor:
                if row[0] in govt_name_dict.keys():
                    row[1] = 1
                if row[0] == '   ,    ':
                    row[3] = 1
                if row[0] not in govt_name_dict.keys() and row[0] != '   ,    ':
                    row[2] = 1

                cursor.updateRow(row)

        print("finished owner field update")
        owner_field_update_time = time.time()
        print('time elapsed {}'.format(owner_field_update_time - start))

    # export to new feature class and drop all columns except: private/govt name label, govt overlap percentage, centroid intersect label, and gh_govt value
    def export_state(self):
        # export parcels to temp layer via insert cursor
        self.df_parcels = os.path.join(self.temp_dir, "parcels_dataframe_data")
        if arcpy.Exists(self.df_parcels):
            print("df parcels already created")
        else:
            arcpy.CreateFeatureclass_management(self.temp_dir, "parcels_dataframe_data", spatial_reference=self.parcels)
            print("df parcels created")

        owner_field_metadata = [["gh_govt", "TEXT", '', 10, '', ''],
                                ['overlap_perc', 'DOUBLE', '', None, None, ''],
                                ["govt_centroid", 'LONG', '', None, 0, ''],
                                ["private_centroid", 'LONG', '', None, 0, ''],
                                ["govt_owner", 'LONG', '', None, 0, ''],
                                ["private_owner", 'LONG', '', None, 0, ''],
                                ["no_owner", 'LONG', '', None, 0, '']]

        try:
            print("adding feature class dataframe fields")
            arcpy.management.AddFields(self.df_parcels, owner_field_metadata)
            print("feature class dataframe fields added")
        except:
            print('feature class dataframe fields already added')

        insert_fields = ["gh_govt", "overlap_perc", "govt_centroid", "private_centroid", "govt_owner",
                         "private_owner", "no_owner", "SHAPE@"]

        print("updating fields")
        # insert each row from the orginal parcel layer into the df parcel feature class, with only the fields specified above
        with arcpy.da.SearchCursor(self.parcels, insert_fields) as sCur:
            with arcpy.da.InsertCursor(self.df_parcels, insert_fields) as iCur:
                for row in sCur:
                    iCur.insertRow(row)

        print("finished creating parcel dataframe feature class")
        df_features_update_time = time.time()
        print('time elapsed {}'.format(df_features_update_time - start))

    def make_new_predictions(self):
        state_df = pd.DataFrame.spatial.from_featureclass(self.df_parcels)
        state_df.drop(['SHAPE'], axis=1, inplace=True)

        

        # drop prediction column value
        cols = state_df.columns.tolist()
        cols.remove('gh_govt')

        x_df = state_df[cols]

        print("initiating xgboost model")
        xgb_model_predict = XGBClassifier()
        print("loading xgboost model")
        xgb_model_predict.load_model(os.path.join(self.workspace, r'state_xgb_models', '{}_xgb_model.json'.format(self.state)))

        # make predictions for test data
        y_preds = xgb_model_predict.predict(x_df)

        y_pred_df = pd.DataFrame(y_preds, columns=['gh_govt_codes'])

        final_df = x_df.join(y_pred_df)

        final_df['gh_govt_xgboost'] = final_df['gh_govt_codes'].map({0: 'FALSE', 1: 'TRUE'})

        final_df.drop(
            columns=["overlap_perc", "govt_centroid", "private_centroid", "govt_owner", "private_owner", "no_owner"],
            inplace=True)

        final_df_dict = final_df.set_index('OBJECTID')['gh_govt_xgboost'].to_dict()

        print("finished model predictions")
        model_prediction_time = time.time()
        print('time elapsed {}'.format(model_prediction_time - start))

        return final_df_dict

    def label_predctions(self, prediction_dict):
        print('updating xgb gh_govt values')
        with arcpy.da.UpdateCursor(self.parcels, ['OBJECTID', 'xgb_gh_govt']) as cursor:
            for row in cursor:
                if row[0] in prediction_dict.keys():
                    row[1] = prediction_dict.get(row[0])
                cursor.updateRow(row)

        print("finished updating xgb gh_govt values")
        update_model_predictions = time.time()
        print('time elapsed {}'.format(update_model_predictions - start))