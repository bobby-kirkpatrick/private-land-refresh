import pandas as pd
import arcpy
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import time
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
from collections import Counter

print('imports done')

# need a list of govt names for the state
# need the state parcel with govt overlap percentage calculated
# need a column that indicates if parcel centroid intersects with govt footprint (needs to be category encoded)
# need a column that indicates if the owner name is in the govt names list (needs to be category encoded)
# need gh_govt value

today = time.strftime("%m_%d_%Y")
start = time.time()

test_data = {
    # 'wisconsin':
    #     {
    #         'govt_land': r'D:\PLR_T2_States\T2_govt_land.gdb\WI_Government_Land_QC3',
    #         'parcels': r'D:\PLR_T2_States\T2_QC3_parcels.gdb\WI_Parcels_QC3'
    #     },
    # 'texas':
    #     {
    #         'govt_land': r'D:\PLR_T2_States\T2_govt_land.gdb\TX_Government_Land_QC3',
    #         'parcels': r'D:\PLR_T2_States\T2_QC3_parcels.gdb\TX_Parcels_QC3'
    #     },
    # 'oklahoma':
    #     {
    #         'govt_land': r'D:\PLR_T2_States\T2_govt_land.gdb\OK_Government_Land_QC3',
    #         'parcels': r'D:\PLR_T2_States\T2_QC3_parcels.gdb\OK_Parcels_QC3'
    #     },
    'pennsylvania':
        {
            'govt_land': r'D:\PLR_T2_States\T2_govt_land.gdb\PA_Government_Land_QC3',
            'parcels': r'D:\PLR_T2_States\T2_QC3_parcels.gdb\PA_Parcels_QC3'
        }
}


# read in feature class
class PLR_xgboost_model_training:
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
        temp_folder = '{}_temp_{}.gdb'.format(self.state, today)
        # temp
        if os.path.exists(os.path.join(location, temp_folder)):
            print('Temp workspace already exist at {}'.format(os.path.join(location, temp_folder)))
        else:
            arcpy.CreateFileGDB_management(location, temp_folder)
            print('New temp workspace created at {}'.format(os.path.join(location, temp_folder)))

        self.temp_dir = os.path.join(location, temp_folder)
        temp_workspace_time = time.time()
        print('time elapsed {}'.format(temp_workspace_time - start))

    # calculate govt overlap
    def calc_govt_overlap(self):
        # dissolve govt land layers into single feature to get govt land footprint
        self.dissolved_govt_features = os.path.join(self.temp_dir, '{}_dissolved_govt_features'.format(self.state))
        if arcpy.Exists(self.dissolved_govt_features):
            print('{}_dissolved_govt_features already exists'.format(self.state))
        else:
            arcpy.Dissolve_management(self.govt_land, self.dissolved_govt_features)
            print('{} govt land layer dissolved'.format(self.state))

        govt_dissolved_time = time.time()
        print('time elapsed {}'.format(govt_dissolved_time - start))

        # overlap calculation - this probably should be moved to the parcel acquisition and processing phase
        # intersect dissovled govt lands and parcels
        intx_output = os.path.join(self.temp_dir, '{}_govt_private_intx'.format(self.state))
        if arcpy.Exists(intx_output):
            print('{}_govt_private_intx already exists'.format(self.state))
        else:
            arcpy.Intersect_analysis([self.dissolved_govt_features, self.parcels], intx_output)
            print('{}_govt_private_intx created'.format(self.state))

        # dissolve intersect to get all govt land polygons for a parcel
        intx_field_names = [f.name for f in arcpy.ListFields(intx_output)]
        rm_fields = ['OBJECTID', 'Shape_Length', 'Shape_Area']
        dissolve_fields = [e for e in intx_field_names if e not in rm_fields]

        temp_dissolved_intx_govt = os.path.join(self.temp_dir,
                                                '{}_dissolved_intx_private_govt_features'.format(self.state))
        if arcpy.Exists(temp_dissolved_intx_govt):
            print('{}_dissolved_intx_private_govt_features already exists'.format(self.state))
        else:
            arcpy.Dissolve_management(intx_output, temp_dissolved_intx_govt, dissolve_fields)
            print('{}_dissolved_intx_private_govt_features created'.format(self.state))

        # add overlap acres field and calculate
        field_metadata = [['overlap_acres', 'DOUBLE', '', None, None, ''],
                          ['overlap_perc', 'DOUBLE', '', None, None, '']]
        try:
            arcpy.management.AddFields(temp_dissolved_intx_govt, field_metadata)
        except:
            print('overlap fields already added')

        # add overlap fields to parcels
        try:
            arcpy.management.AddFields(self.parcels, field_metadata)
        except:
            print('overlap fields already added')

        arcpy.management.CalculateField(temp_dissolved_intx_govt, "overlap_acres", "!shape.area@acres!", "PYTHON3")
        try:
            arcpy.management.CalculateField(temp_dissolved_intx_govt, "overlap_perc", "(!overlap_acres!/!gh_parcel_acres!) * 100", "PYTHON3")
        except:
            arcpy.management.CalculateField(temp_dissolved_intx_govt, "overlap_perc", "(!overlap_acres!/!acres!) * 100", "PYTHON3")
        print('overalap fields calcualted')

        # join  overlap percentage back to parcels
        # search cursor and update dict with parcel ID and overlap perc values
        overlap_dict = {}
        with arcpy.da.SearchCursor(temp_dissolved_intx_govt, ["parcel_id", "overlap_perc"]) as cursor:
            for row in cursor:
                overlap_dict[row[0]] = row[1]

        # update parcel layer with overlap percentage
        with arcpy.da.UpdateCursor(self.parcels, ["parcel_id", "overlap_perc"]) as cursor:
            for row in cursor:
                if row[0] in overlap_dict.keys():
                    overlap_perc_1 = overlap_dict.get(row[0])
                    row[1] = overlap_perc_1
                else:
                    row[1] = 0

                cursor.updateRow(row)

        print("Overlap percentage added to parcels")

    # centroid calculation and column update
    def add_centroid_attr(self):
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

    # search cursor to put all govt names in a list (search where gh_govt = true), update new column with govt/private label, need to figure out what to do with no owner info
    def label_owner_type(self):
        # add 4 columns, type long, initial value of 0, 1.govt_owner 2.private_owner 3.no_owner
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
        with arcpy.da.SearchCursor(self.parcels, ["full_name"], where_clause="gh_govt = 'TRUE'") as cursor:
            for row in cursor:
                govt_name_dict[row[0]] = 1

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

    def train_model(self):
        state_df = pd.DataFrame.spatial.from_featureclass(self.df_parcels)
        state_df.drop('SHAPE', axis=1, inplace=True)

        print(state_df.head())
        print(state_df.dtypes)

        # category encode gh_govt name
        state_df['gh_govt'] = state_df['gh_govt'].map({'FALSE': 1, 'TRUE': 2, 'UNKNOWN': 3})
        cols = state_df.columns.tolist()
        cols.remove('gh_govt')

        state_df = state_df[state_df['gh_govt'] != 'UNKNOWN']

        x_df = state_df[cols]
        y_df = state_df[['gh_govt']]

        X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=15)

        print("initiating xgboost model")
        xgb_model = XGBClassifier()
        print("training xgboost model")
        xgb_model.fit(X_train, y_train.values.ravel())
        print(xgb_model)
        model_train_time = time.time()
        print('time elapsed {}'.format(model_train_time - start))

        # make predictions for test data
        y_pred = xgb_model.predict(X_test)
        print(y_pred)
        vc = Counter(y_pred)
        print(vc)
        predictions = [round(value) for value in y_pred]

        # evaluate predictions
        accuracy = accuracy_score(y_test.values.ravel(), predictions)
        print("model accuracy: {}%".format(accuracy * 100))

        # save xgb model
        xgb_model.save_model(os.path.join(r'D:\private-land-refresh\private-land-refresh', 'state_xgb_models', '{}_xgb_model.json'.format(self.state)))

        return state_df



def private_land_refresh(config):
    for k, v in config.items():
        state = k
        data = v

        plr_xgb = PLR_xgboost_model_training(data, state)
        temp_workspace = plr_xgb.set_workspaces()
        plr_xgb.calc_govt_overlap()
        plr_xgb.add_centroid_attr()
        plr_xgb.label_owner_type()
        plr_xgb.export_state()
        df = plr_xgb.train_model()


private_land_refresh(test_data)
