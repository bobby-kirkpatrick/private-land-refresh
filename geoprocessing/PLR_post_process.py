from pathlib import Path

import arcpy

from configs.settings import PARALLEL_PROCESSING_FACTOR
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger

logger = get_logger(__name__)


class PLR_post_process:
    """
    Final output stage: dissolves private and government land into
    quarter-stamped feature classes with acreage fields.
    """

    def __init__(self, state: str, env: str = 'LOCAL') -> None:
        self.state = state
        self.workspace: Path = Path.cwd() if env == 'LOCAL' else Path(env)
        self.quarter: str = get_quarter()

        self.temp: Path = self.workspace / f'{self.state}_temp_{self.quarter}.gdb'
        self.final: Path = self.workspace / f'{self.state}_private_land_{self.quarter}.gdb'

        if arcpy.Exists(str(self.final)):
            logger.info("Final GDB already exists: %s", self.final)
        else:
            arcpy.management.CreateFileGDB(str(self.workspace), self.final.name)
            logger.info("Final GDB created: %s", self.final)

        self.govt_land: str = str(self.temp / f'{self.state}_govt_land_private_erased_2')
        self.parcels: str = str(self.temp / f'{self.state}_parcels_erase_4')

    def create_dissolve_fc(self) -> None:
        """Copy key fields from QC output into a clean dissolve-prep feature class."""
        self.parcels_dissolve_prep: Path = self.temp / 'dissolve_prep_fc'

        if arcpy.Exists(str(self.parcels_dissolve_prep)):
            logger.info("dissolve_prep_fc already exists")
        else:
            arcpy.CreateFeatureclass_management(
                str(self.temp), 'dissolve_prep_fc',
                spatial_reference=arcpy.SpatialReference(4326),
            )
            logger.info("dissolve_prep_fc created")

        try:
            arcpy.management.AddFields(str(self.parcels_dissolve_prep), [
                ['gh_govt',    'TEXT', '', 10, '', ''],
                ['OWN1_LAST',  'TEXT', '', 60, '', ''],
                ['OWN1_FRST',  'TEXT', '', 45, '', ''],
                ['OWN2_LAST',  'TEXT', '', 60, '', ''],
                ['OWN2_FRST',  'TEXT', '', 45, '', ''],
                ['MAIL_CITY',  'TEXT', '', 40, '', ''],
                ['MAIL_STATE', 'TEXT', '', 2,  '', ''],
                ['MAIL_ZIP',   'TEXT', '', 9,  '', ''],
                ['mail_addr',  'TEXT', '', 50, '', ''],
            ])
            logger.info("Dissolve prep fields added")
        except arcpy.ExecuteError:
            logger.debug("Dissolve prep fields already exist, skipping")

        insert_fields = [
            'gh_govt', 'OWN1_LAST', 'OWN1_FRST', 'OWN2_LAST', 'OWN2_FRST',
            'MAIL_ADDR', 'MAIL_ZIP', 'MAIL_STATE', 'MAIL_CITY', 'SHAPE@',
        ]
        with arcpy.da.SearchCursor(self.parcels, insert_fields) as s_cur:
            with arcpy.da.InsertCursor(str(self.parcels_dissolve_prep), insert_fields) as i_cur:
                for row in s_cur:
                    i_cur.insertRow(row)

        logger.info("Dissolve prep feature class populated")

    def post_process_govt_land(self) -> None:
        """Dissolve QC'd government land and calculate acreage."""
        output: Path = self.final / f'{self.state}_Govt_Land_{self.quarter}'

        if arcpy.Exists(str(output)):
            logger.info("%s govt land final layer already exists", self.state)
            return

        arcpy.management.Dissolve(
            self.govt_land, str(output),
            ['Unit_Nm', 'gh_govtype'],
            multi_part='SINGLE_PART',
        )
        arcpy.management.AddField(str(output), 'Acres', 'LONG')
        arcpy.management.CalculateField(str(output), 'Acres', '!shape.area@acres!', 'PYTHON3')
        logger.info("%s govt land final layer complete", self.state)

    def private_land_dissolve(self) -> None:
        """Dissolve named private parcels by owner and address fields."""
        arcpy.env.parallelProcessingFactor = PARALLEL_PROCESSING_FACTOR

        arcpy.MakeFeatureLayer_management(
            str(self.parcels_dissolve_prep),
            'private_parcels',
            where_clause=(
                "gh_govt = 'FALSE' And "
                "(OWN1_LAST <> ' ' Or OWN1_FRST <> ' ' Or "
                "OWN2_LAST <> ' ' Or OWN2_FRST <> ' ' Or mail_addr <> '')"
            ),
        )

        private_parcels_fc: Path = self.temp / 'private_parcels'
        if not arcpy.Exists(str(private_parcels_fc)):
            arcpy.CopyFeatures_management('private_parcels', str(private_parcels_fc))

        self.dissolve_output: Path = self.temp / f'{self.state}_private_dissolved_named'
        if arcpy.Exists(str(self.dissolve_output)):
            logger.info("%s_private_dissolved_named already exists", self.state)
        else:
            arcpy.gapro.DissolveBoundaries(
                input_layer=str(private_parcels_fc),
                out_feature_class=str(self.dissolve_output),
                multipart='MULTI_PART',
                dissolve_fields='DISSOLVE_FIELDS',
                fields=['OWN1_LAST', 'OWN1_FRST', 'OWN2_LAST', 'OWN2_FRST',
                        'MAIL_ADDR', 'MAIL_ZIP', 'MAIL_STATE', 'MAIL_CITY'],
            )
            logger.info("%s_private_dissolved_named created", self.state)

        arcpy.Delete_management('private_parcels')

    def append_private_no_owner_parcels(self) -> None:
        """Append no-owner private parcels to the dissolved output."""
        arcpy.MakeFeatureLayer_management(
            str(self.parcels_dissolve_prep),
            'no_owner_private_parcels',
            where_clause=(
                "gh_govt = 'FALSE' And "
                "(OWN1_LAST = ' ' AND OWN1_FRST = ' ' AND "
                "OWN2_LAST = ' ' AND OWN2_FRST = ' ' AND mail_addr = '')"
            ),
        )
        arcpy.Append_management('no_owner_private_parcels', str(self.dissolve_output), 'NO_TEST')
        arcpy.Delete_management('no_owner_private_parcels')
        logger.info("No-owner parcels appended for %s", self.state)

    def multipart_to_singlepart(self) -> None:
        """Convert dissolved multipart private land to single-part and calculate acreage."""
        final_private: Path = self.final / f'{self.state}_Private_Land_{self.quarter}'
        arcpy.MultipartToSinglepart_management(str(self.dissolve_output), str(final_private))
        arcpy.management.AddField(str(final_private), 'gh_parcel_acres', 'DOUBLE')
        arcpy.management.CalculateField(str(final_private), 'gh_parcel_acres', '!shape.area@acres!', 'PYTHON3')
        logger.info("%s_Private_Land_%s created", self.state, self.quarter)
