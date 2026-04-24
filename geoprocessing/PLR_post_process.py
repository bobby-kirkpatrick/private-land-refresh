import os
import time
import arcpy

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

class PLR_post_process:
    def __init__(self, state, env='LOCAL'):
        self.state = state
        self.env = env

        # set environment
        if env == 'LOCAL':
            self.workspace = os.getcwd()
        else:
            self.workspace = env

        # directories
        self.temp = os.path.join(self.workspace, f'{self.state}_temp_{quarter}.gdb')
        self.final = os.path.join(self.workspace, f'{self.state}_private_land_{quarter}.gdb')

        if arcpy.Exists(self.final):
            print(f'{self.final} already exists')
        else:
            arcpy.management.CreateFileGDB(self.workspace, f'{self.state}_private_land_{quarter}.gdb')

        # layers
        self.govt_land = os.path.join(self.temp, f'{self.state}_govt_land_private_erased_2')
        self.parcels = os.path.join(self.temp, f'{self.state}_parcels_erase_4')

    def create_dissolve_fc(self):
        self.parcels_dissolve_prep = os.path.join(self.temp, "dissolve_prep_fc")
        sr = arcpy.SpatialReference(4326)
        if arcpy.Exists(self.parcels_dissolve_prep):
            print("dissolve prep parcels already created")
        else:
            arcpy.CreateFeatureclass_management(self.temp, "dissolve_prep_fc", spatial_reference=sr)
            print("dissolve prep parcels")


        owner_field_metadata = [["gh_govt", "TEXT", '', 10, '', ''],
                                ["OWN1_LAST", "TEXT", '', 60, '', ''],
                                ["OWN1_FRST", "TEXT", '', 45, '', ''],
                                ["OWN2_LAST", "TEXT", '', 60, '', ''],
                                ["OWN2_FRST", "TEXT", '', 45, '', ''],
                                ["MAIL_CITY", "TEXT", '', 40, '', ''],
                                ["MAIL_STATE", "TEXT", '', 2, '', ''],
                                ["MAIL_ZIP", "TEXT", '', 9, '', ''],
                                ["mail_addr", "TEXT", '', 50, '', ''],
                           ]

        try:
            print("adding feature class dissolve fields")
            arcpy.management.AddFields(self.parcels_dissolve_prep, owner_field_metadata)
            print("feature class dissolve fields added")
        except:
            print('feature class dissolve fields already added')

        insert_fields = ['gh_govt', 'OWN1_LAST', 'OWN1_FRST', 'OWN2_LAST', 'OWN2_FRST', 'MAIL_ADDR', 'MAIL_ZIP', 'MAIL_STATE', 'MAIL_CITY', "SHAPE@"]


        print("updating fields")
        # insert each row from the orginal parcel layer into the df parcel feature class, with only the fields specified above
        with arcpy.da.SearchCursor(self.parcels, insert_fields) as sCur:
            with arcpy.da.InsertCursor(self.parcels_dissolve_prep, insert_fields) as iCur:
                for row in sCur:
                    iCur.insertRow(row)


    def post_process_govt_land(self):
        # dissolve with single part and then calcualte acres
        dissolve_govt_land_output = os.path.join(self.final, f'{self.state}_Govt_Land_{quarter}')

        if arcpy.Exists(dissolve_govt_land_output):
            print(f'{self.state} govt land final layer already completed')
        else:
            arcpy.management.Dissolve(self.govt_land, dissolve_govt_land_output, ['Unit_Nm', 'gh_govtype'], multi_part='SINGLE_PART')
            arcpy.management.AddField(dissolve_govt_land_output, "Acres", "LONG")
            arcpy.management.CalculateField(dissolve_govt_land_output, "Acres", "!shape.area@acres!", "PYTHON3")
            print(f'{self.state} govt land final layer completed')


    def private_land_dissolve(self, processing_factor="25%"):
        # for private land WITH owner information
        # make a feature layer with all the gh_govt=FALSE corelogic parcels (i.e. private) and at least one piece of owner info (name or address)
        arcpy.MakeFeatureLayer_management(self.parcels_dissolve_prep, "private_parcels",
                                          where_clause="gh_govt = 'FALSE' And (OWN1_LAST <> ' ' Or OWN1_FRST <> ' ' Or OWN2_LAST <> ' ' Or OWN2_FRST <> ' ' Or mail_addr <> '')")

        private_parcels = os.path.join(self.temp, 'private_parcels')

        if arcpy.Exists(private_parcels):
            print('private parcels temp already exists')
        else:
            arcpy.CopyFeatures_management("private_parcels", private_parcels)

        if processing_factor:
            arcpy.env.parallelProcessingFactor = "25%"
        else:
            arcpy.env.parallelProcessingFactor = "{}%".format(processing_factor)

        self.dissolve_output = os.path.join(self.temp, '{}_private_dissolved_named'.format(self.state))
        if arcpy.Exists(self.dissolve_output):
            print('{}_private_dissolved already exists'.format(self.state))
        else:
            arcpy.gapro.DissolveBoundaries(input_layer=private_parcels, out_feature_class=self.dissolve_output,
                                           multipart='MULTI_PART', dissolve_fields='DISSOLVE_FIELDS',
                                           fields=['OWN1_LAST', 'OWN1_FRST', 'OWN2_LAST', 'OWN2_FRST', 'MAIL_ADDR',
                                                   'MAIL_ZIP', 'MAIL_STATE', 'MAIL_CITY'])
            print('{}_private_dissolved_private created'.format(self.state))

        arcpy.Delete_management("private_parcels")

    def append_private_no_owner_parcels(self):
        # for private land with NO owner info
        arcpy.MakeFeatureLayer_management(self.parcels_dissolve_prep, "no_owner_private_parcels",
                                          where_clause="gh_govt = 'FALSE' And (OWN1_LAST = ' ' AND OWN1_FRST = ' ' AND OWN2_LAST = ' ' AND OWN2_FRST = ' ' AND mail_addr = '')")

        arcpy.Append_management("no_owner_private_parcels", self.dissolve_output, "NO_TEST")

        arcpy.Delete_management("no_owner_private_parcels")
        print("No owner parcel appending complete")

    def multipart_to_singlepart(self):
        final_private = os.path.join(self.final, f'{self.state}_Private_Land_{quarter}')
        arcpy.MultipartToSinglepart_management(self.dissolve_output, final_private)
        arcpy.management.AddField(final_private, "gh_parcel_acres", "DOUBLE")
        arcpy.management.CalculateField(final_private, "gh_parcel_acres", "!shape.area@acres!", "PYTHON3")