import shutil
from datetime import datetime, timedelta
from pathlib import Path

import arcpy

from utils.logging_config import get_logger

logger = get_logger(__name__)

# Use yesterday's date to match CoreLogic delivery convention
_yesterday = datetime.now() - timedelta(days=1)
TODAY: str = _yesterday.strftime('%m_%d_%Y')

STATE_CODES: dict = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '02', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '08',
    'CA': '06', 'AL': '01', 'AR': '05', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '04', 'ID': '16', 'CT': '09',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46',
}


class ParcelProcessing:
    """Acquires raw CoreLogic parcel downloads and prepares them for the PLR pipeline."""

    def __init__(self, config: dict) -> None:
        self.raw_data: Path = Path(config['acquisition_processing_parameters']['raw data location'])
        self.env: Path = Path(config['acquisition_processing_parameters']['data_workspace'])
        self.state_data: dict = config['states']
        self.state_codes: dict = config['state_codes']

    def set_workspaces(self) -> None:
        """Create dated parcel folder, temp folder, and per-state GDBs."""
        self.parcel_folder: Path = self.env / f'corelogic_parcels_{TODAY}'
        self.temp_folder: Path = self.env / 'temp'

        for path, label in [
            (self.parcel_folder, 'Parcel folder'),
            (self.temp_folder,   'Temp folder'),
        ]:
            if path.exists():
                logger.info("%s already exists: %s", label, path)
            else:
                path.mkdir(parents=True)
                logger.info("%s created: %s", label, path)

        for state in self.state_codes:
            gdb_name = f'{state}_parcels_{TODAY}.gdb'
            gdb_path = self.parcel_folder / gdb_name
            if arcpy.Exists(str(gdb_path)):
                logger.debug("State GDB already exists: %s", gdb_path)
            else:
                arcpy.CreateFileGDB_management(str(self.parcel_folder), gdb_name)
                logger.info("State GDB created: %s", gdb_path)

    def extract_counties(self) -> None:
        """Unzip county shapefiles into per-state temp folders."""
        for state, fips in self.state_codes.items():
            temp_state: Path = self.temp_folder / f'{state}_temp'

            if temp_state.exists():
                logger.info("Temp state folder already exists: %s", temp_state)
                continue

            temp_state.mkdir()
            for filename in self.raw_data.iterdir():
                if filename.name[3:5] == fips:
                    shutil.unpack_archive(str(filename), str(temp_state))
            logger.info("%s counties extracted", state)

    def merge_counties(self) -> dict:
        """Merge county shapefiles into a single state-level feature class per state."""
        merged: dict = {}

        for state in self.state_codes:
            temp_state: Path = self.temp_folder / f'{state}_temp'
            state_gdb: Path = self.parcel_folder / f'{state}_parcels_{TODAY}.gdb'
            output: Path = state_gdb / f'{state}_parcels'

            county_shapefiles = [str(f) for f in temp_state.iterdir() if f.suffix == '.shp']

            if arcpy.Exists(str(output)):
                logger.info("Merged output already exists for %s", state)
            else:
                arcpy.Merge_management(county_shapefiles, str(output))
                logger.info("Merged output created for %s", state)

            merged[state] = str(output)

        logger.info("County merge complete for %d states", len(merged))
        return merged

    def field_processing(self, states: dict) -> None:
        """Add and calculate standard PLR fields on merged parcel feature classes."""
        add_field_schema: dict = {
            'gh_govt':         ['gh_govt',         'TEXT',   '', 10,   None, ''],
            'gh_parcel_acres': ['gh_parcel_acres',  'DOUBLE', '', None, None, ''],
            'unit_nm':         ['unit_nm',          'TEXT',   '', 150,  None, ''],
            'gh_govtype':      ['gh_govtype',        'TEXT',   '', 150,  None, ''],
            'mail_addr':       ['mail_addr',         'TEXT',   '', 50,   None, ''],
            'overlap_perc':    ['overlap_perc',      'DOUBLE', '', None, None, ''],
        }

        for state, path in states.items():
            existing = {f.name for f in arcpy.ListFields(path)}
            to_add = [v for k, v in add_field_schema.items() if k not in existing]

            if to_add:
                arcpy.management.AddFields(path, to_add)
                logger.info("%s: %d fields added", state, len(to_add))
            else:
                logger.debug("%s: all fields already present", state)

            arcpy.CalculateFields_management(
                in_table=path,
                expression_type='PYTHON3',
                fields=[
                    ['gh_parcel_acres', '!shape.area@acres!'],
                    ['mail_addr', "!MAIL_NBR! + ' ' + !MAIL_DIR! + ' ' + !MAIL_STR! + ' ' + !MAIL_MODE!"],
                    ['MAIL_ZIP', '!MAIL_ZIP![:5]'],
                ],
            )
            arcpy.management.CalculateField(
                path, 'mail_addr',
                "!mail_addr!.strip().replace('   ', ' ').replace('  ', ' ')",
                'PYTHON3',
            )
            logger.info("%s: gh_parcel_acres, mail_addr, MAIL_ZIP calculated", state)

    def calc_govt_overlap(self, states: dict) -> None:
        """Calculate the percentage of each parcel overlapping government land."""
        for state, path in states.items():
            govt_land: str = self.state_data[state]['govt_land']
            state_gdb: Path = self.parcel_folder / f'{state}_parcels_{TODAY}.gdb'

            dissolved_govt: Path = state_gdb / f'{state}_dissolved_govt_features'
            if arcpy.Exists(str(dissolved_govt)):
                logger.info("%s_dissolved_govt_features already exists", state)
            else:
                arcpy.Dissolve_management(govt_land, str(dissolved_govt))
                logger.info("%s govt land dissolved", state)

            intx_output: Path = state_gdb / f'{state}_govt_private_intx'
            if arcpy.Exists(str(intx_output)):
                logger.info("%s_govt_private_intx already exists", state)
            else:
                arcpy.Intersect_analysis([str(dissolved_govt), path], str(intx_output))
                logger.info("%s_govt_private_intx created", state)

            keep_out = {'OBJECTID', 'Shape_Length', 'Shape_Area'}
            dissolve_fields = [
                f.name for f in arcpy.ListFields(str(intx_output))
                if f.name not in keep_out
            ]

            dissolved_intx: Path = state_gdb / f'{state}_dissolved_intx_private_govt_features'
            if arcpy.Exists(str(dissolved_intx)):
                logger.info("%s_dissolved_intx already exists", state)
            else:
                arcpy.Dissolve_management(str(intx_output), str(dissolved_intx), dissolve_fields)
                logger.info("%s_dissolved_intx created", state)

            try:
                arcpy.management.AddFields(str(dissolved_intx), [
                    ['overlap_acres', 'DOUBLE', '', None, None, ''],
                    ['overlap_perc',  'DOUBLE', '', None, None, ''],
                ])
            except arcpy.ExecuteError:
                logger.debug("%s overlap fields already exist", state)

            arcpy.management.CalculateField(str(dissolved_intx), 'overlap_acres', '!shape.area@acres!', 'PYTHON3')
            arcpy.management.CalculateField(
                str(dissolved_intx), 'overlap_perc',
                '(!overlap_acres!/!gh_parcel_acres!) * 100', 'PYTHON3',
            )
            logger.info("%s overlap percentage calculated", state)

            overlap_dict: dict = {}
            with arcpy.da.SearchCursor(str(dissolved_intx), ['parcel_id', 'overlap_perc']) as cursor:
                for row in cursor:
                    overlap_dict[row[0]] = row[1]

            with arcpy.da.UpdateCursor(path, ['parcel_id', 'overlap_perc']) as cursor:
                for row in cursor:
                    row[1] = overlap_dict.get(row[0], 0)
                    cursor.updateRow(row)

            arcpy.Delete_management([str(dissolved_govt), str(intx_output), str(dissolved_intx)])
            logger.info("%s overlap percentage joined to parcels", state)
