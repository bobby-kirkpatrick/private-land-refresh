import os
import time
import arcpy
import shutil
from arcpy import gapro



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

class PLR_QC_model:
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

    def qc_counts(self):
        parcel_count = int(arcpy.GetCount_management(self.parcels)[0])
        print(f'{self.state} has {parcel_count} parcels')

        model_agreement_rows = arcpy.SelectLayerByAttribute_management(self.parcels, "NEW_SELECTION", '"gh_govt" = "xgb_gh_govt"')
        model_agreement_count = int(arcpy.GetCount_management(model_agreement_rows)[0])
        print(f'{model_agreement_count} parcels with PLR model agreement')
        print(f'{(model_agreement_count/parcel_count) * 100}% PLR model agreement rate in {self.state}')
        print(f'Begining QC process for {parcel_count - model_agreement_count} parcels')

    def label_qc(self):
        arcpy.management.AddField(self.parcels, "qc", "SHORT")

        # add qc flag field for rows where model labels differ
        flag_qc_fields = ['qc', 'gh_govt', 'xgb_gh_govt']
        with arcpy.da.UpdateCursor(self.parcels, "qc", where_clause='gh_govt <> xgb_gh_govt') as cursor:
            for row in cursor:
                row[0] = 1
                cursor.updateRow(row)

        # update gh_govt label for qc rows using business logic
        print(f"QC'ing gh_govt label for {self.state}")
        qc_fields = ['gh_govt', 'xgb_gh_govt', 'private_owner', 'gh_parcel_acres', 'qc', 'full_name', 'overlap_perc', 'govt_centroid', 'govt_owner']
        with arcpy.da.UpdateCursor(self.parcels, qc_fields) as cursor:
            for row in cursor:
                if (row[1] == 'FALSE' and row[0] == 'TRUE' and row[2] == 1 and row[3] >= 10 and row[4] == 1):
                    row[0] = 'TRUE'
                    row[4] = 2
                elif (row[1] == 'FALSE' and row[0] == 'TRUE' and row[2] == 1 and row[3] < 10 and row[4] == 1):
                    row[0] = 'FALSE'
                    row[4] = 3
                elif (row[4] == 1 and row[5] == '   ,    ' and row[6] >= 80):
                    row[0] = 'TRUE'
                    row[4] = 4
                elif (row[4] == 1 and row[5] == '   ,    ' and row[6] < 80):
                    row[0] = 'UNKNOWN'
                    row[4] = 5
                elif (row[4] == 1 and row[7] == 1 and row[8] == 1):
                    row[0] = 'TRUE'
                    row[4] = 6
                elif row[4] == 1:
                    row[4] = 7

                cursor.updateRow(row)
        print(f"Label QC complete for {self.state}")

    def gap_qc(self):
        # Make a layer from the feature class
        govt_true = f"{self.state}_GovtTrue"
        arcpy.MakeFeatureLayer_management(self.parcels, govt_true, where_clause="gh_govt IN ('TRUE')")

        # symmetrical difference between govt layer and govt true parcel
        sym_diff_fc = os.path.join(self.temp_dir, f"{self.state}_symDiff")
        if arcpy.Exists(sym_diff_fc):
            print(f"{self.state}_symDiff already exists")
        else:
            arcpy.analysis.SymDiff(govt_true, self.govt_land, sym_diff_fc, join_attributes="ONLY_FID")
            print(f"{self.state}_symDiff created")

        # feature layer where symdiff is a gap
        symDiff_gap = f"{self.state}_symDiff_gap"
        parcel_layer_name = name = self.parcels.split('.gdb\\')[1]
        field_name = 'FID_' + parcel_layer_name
        where = f"{field_name} <> -1"
        print(where)
        arcpy.MakeFeatureLayer_management(sym_diff_fc, symDiff_gap, where_clause=where)

        # multipart to single part
        symDiff_singlePart_fc = os.path.join(self.temp_dir, f"{self.state}_SD_SP_fc")
        if arcpy.Exists(symDiff_singlePart_fc):
            print(f"{self.state}_symDiff_singlePart already exists")
        else:
            arcpy.MultipartToSinglepart_management(symDiff_gap, symDiff_singlePart_fc)
            print(f"{self.state}_symDiff_singlePart created")

            # add and calcualte acres to single part gaps
            field_metadata = [['gap_acres', 'LONG', '', None, None, ''],
                              ['Unit_Nm', 'TEXT', '', None, None, ''],
                              ['gh_govtype', 'TEXT', '', None, None, '']]

            arcpy.management.AddFields(symDiff_singlePart_fc, field_metadata)
            arcpy.management.CalculateField(symDiff_singlePart_fc, "gap_acres", "!shape.area@acres!", "PYTHON3")
            print(f"{self.state} single part gap acres calculated")

        # delete large gaps
        with arcpy.da.UpdateCursor(symDiff_singlePart_fc, "gap_acres") as cursor:
            for row in cursor:
                if row[0] >= 160:
                    cursor.deleteRow ()


        # spatial join govt features to gap features
        gap_spatial_join = os.path.join(self.temp_dir, f"{self.state}_gap_SJ")
        if arcpy.Exists(gap_spatial_join):
            print(f'{self.state} gap spatial join already exists')
        else:
            arcpy.analysis.SpatialJoin(symDiff_singlePart_fc, self.govt_land, gap_spatial_join, match_option='CLOSEST')
            print(f'{self.state} gap spatial join complete')

        gap_data = ["Unit_Nm_1", "gh_govtype_1", "SHAPE@"]
        govt_fields = ["Unit_Nm", "gh_govtype", "SHAPE@"]

        print("updating fields")
        # insert each row from the orginal parcel layer into the df parcel feature class, with only the fields specified above
        with arcpy.da.SearchCursor(gap_spatial_join, gap_data) as sCur:
            with arcpy.da.InsertCursor(self.govt_land, govt_fields) as iCur:
                for row in sCur:
                    iCur.insertRow(row)

    def overlap_qc(self):
        # select private land
        govt_false = f"{self.state}_GovtFalse"
        arcpy.MakeFeatureLayer_management(self.parcels, govt_false, where_clause="gh_govt IN ('FALSE')")

        # intersect to find where govt land overlaps private land
        overlap_intx = os.path.join(self.temp_dir, f'{self.state}_govt_overlap_intx')
        if arcpy.Exists(overlap_intx):
            print(f'{self.state} govt overlap intx already exists')
        else:
            arcpy.analysis.Intersect([govt_false, self.govt_land], overlap_intx)
            print(f'{self.state} govt overlap intx created')

        arcpy.management.AddField(overlap_intx, 'intx_ac', 'LONG')
        arcpy.management.CalculateField(overlap_intx, "intx_ac", "!shape.area@acres!", "PYTHON3")


        # erase overlaps over private
        private_intx = f"{self.state}_private_overlaps"
        arcpy.MakeFeatureLayer_management(self.parcels, private_intx, where_clause="private_owner = 1 And private_centroid = 1")

        govt_land_private_erase = os.path.join(self.temp_dir, f'{self.state}_govt_land_private_erased')
        if arcpy.Exists(govt_land_private_erase):
            print(f'{self.state} govt private erase already exists')
        else:
            arcpy.RepairGeometry_management(self.govt_land)
            arcpy.analysis.Erase(self.govt_land, private_intx, govt_land_private_erase)
            print(f'{self.state} govt private erase intx created')

        # erase overlap, gives ownership to govt where parcel has no owner and govt centroid
        no_name_govt_intx = f"{self.state}_no_name_govt_intx"
        arcpy.MakeFeatureLayer_management(overlap_intx, no_name_govt_intx, where_clause="full_name IN ('   ,    ') AND govt_centroid = 1")

        parcel_no_name_govt_erase = os.path.join(self.temp_dir, f'{self.state}_parcels_erase_1')
        if arcpy.Exists(parcel_no_name_govt_erase):
            print(f'{self.state}_parcels_erase_1 already exists')
        else:
            arcpy.analysis.Erase(self.parcels, no_name_govt_intx, parcel_no_name_govt_erase)
            print(f'{self.state}_parcels_erase_1 created')

        # erase slivers in no name private polygons
        private_overlap_sliver_intx = f"{self.state}_private_overlap_sliver_intx"
        arcpy.MakeFeatureLayer_management(overlap_intx, private_overlap_sliver_intx, where_clause="full_name IN ('   ,    ') AND intx_ac < 25")


        govt_overlap_sliver_erase = os.path.join(self.temp_dir, f'{self.state}_govt_land_private_erased_2')
        if arcpy.Exists(govt_overlap_sliver_erase):
            print(f'{self.state} govt overlap sliver erase already exists')
        else:
            arcpy.analysis.Erase(govt_land_private_erase, private_overlap_sliver_intx, govt_overlap_sliver_erase)
            print(f'{self.state} govt overlap sliver erase created')

        # erase govt overlap where there is legit govt
        private_overlap_govt_intx = f"{self.state}_private_overlap_govt_intx"
        arcpy.MakeFeatureLayer_management(overlap_intx, private_overlap_govt_intx, where_clause="full_name IN ('   ,    ') AND intx_ac >= 25")

        private_overlap_govt_intx_erase = os.path.join(self.temp_dir, f'{self.state}_parcels_erase_2')
        if arcpy.Exists(private_overlap_govt_intx_erase):
            print(f'{self.state} private overlap govt intx erase erase already exists')
        else:
            arcpy.analysis.Erase(parcel_no_name_govt_erase, private_overlap_govt_intx, private_overlap_govt_intx_erase)
            print(f'{self.state} private overlap govt intx erase created')


        # erase real govt from private name land
        govt_land_private_name_intx = f"{self.state}_govt_land_private_name_intx"
        arcpy.MakeFeatureLayer_management(overlap_intx, govt_land_private_name_intx, where_clause="govt_centroid = 1 And private_owner = 1 And intx_ac >= 25")

        private_land_govt_intx = os.path.join(self.temp_dir, f'{self.state}_parcels_erase_3')
        if arcpy.Exists(private_land_govt_intx):
            print(f'{self.state} govt land private name intx already exists')
        else:
            arcpy.analysis.Erase(private_overlap_govt_intx_erase, govt_land_private_name_intx, private_land_govt_intx)
            print(f'{self.state} private overlap govt intx erase created')

        # erase from private where intx is govt centroid and govt name is 1
        govt_name_centroid_intx = f"{self.state}_govt_name_centroid_intx"
        arcpy.MakeFeatureLayer_management(overlap_intx, govt_name_centroid_intx, where_clause="govt_centroid = 1 And govt_owner = 1")

        private_land_govt_overlap_erased = os.path.join(self.temp_dir, f'{self.state}_parcels_erase_4')
        if arcpy.Exists(private_land_govt_overlap_erased):
            print(f'{self.state} private land govt overlap erased already exists')
        else:
            arcpy.analysis.Erase(private_land_govt_intx, govt_name_centroid_intx, private_land_govt_overlap_erased)
            print(f'{self.state} private land govt overlap erased created')

    def qc_post_process(self):
        arcpy.env.workspace = self.temp_dir

        featureclasses = arcpy.ListFeatureClasses()

        #TODO update state_parcels_erase_4 to only the columns needed for the dissolve

        state_private = f'{self.state}_parcels_erase_4'
        state_govt = f'{self.state}_govt_land_private_erased_2'

        featureclasses.remove(state_private)
        featureclasses.remove(state_govt)

        delete_list = []
        for fc in featureclasses:
            full_path = os.path.join(self.temp_dir, fc)
            delete_list.append(full_path)

        arcpy.Delete_management(delete_list)
