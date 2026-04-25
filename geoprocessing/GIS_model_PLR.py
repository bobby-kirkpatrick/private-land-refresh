from pathlib import Path

import arcpy

from configs.settings import GOVT_OVERLAP_THRESHOLD
from geoprocessing.base_model import BaseModel
from utils.geo_utils import build_centroid_govt_intersect


class PLR_GIS_model(BaseModel):
    """GIS centroid-and-overlap classification stage."""

    def label_private_public(self) -> None:
        """Label each parcel TRUE/FALSE based on centroid location and overlap percentage."""
        dissolved_govt, centroid_intersect = build_centroid_govt_intersect(
            self.govt_land, self.parcels, self.temp_dir, self.state, self.logger
        )

        govt_intx_dict: dict = {}
        intx_fields = [f'FID_{self.state}_corelogic_centroids', 'gh_govtype', 'unit_nm']
        with arcpy.da.SearchCursor(str(centroid_intersect), intx_fields) as cursor:
            for row in cursor:
                govt_intx_dict[row[0]] = [row[1], row[2]]

        with arcpy.da.UpdateCursor(
            self.parcels,
            ['OBJECTID', 'gh_govt', 'gh_govtype', 'unit_nm', 'overlap_perc'],
        ) as cursor:
            for row in cursor:
                if row[0] in govt_intx_dict:
                    row[2] = govt_intx_dict[row[0]][0]
                    row[3] = govt_intx_dict[row[0]][1]
                    row[1] = 'TRUE'
                if row[4] >= GOVT_OVERLAP_THRESHOLD:
                    row[1] = 'TRUE'
                else:
                    row[1] = 'FALSE'
                cursor.updateRow(row)

        self.logger.info("%s private land labelled", self.state)
        self.logger.debug("label_private_public elapsed: %.1fs", self._elapsed())
