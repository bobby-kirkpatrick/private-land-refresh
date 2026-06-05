"""
Privacy redaction for PLR final outputs.

Identifies parcels in the completed Private_Land feature class that
contain one or more privacy-sensitive points (sourced from the enterprise
GDB) and nulls out all ownership / PII fields on those parcels.

Intended to run after main.py has completed for a state, via redact.py.
"""
from __future__ import annotations

from pathlib import Path

import arcpy

from configs.settings import PRIVACY_POINTS_FC, REDACT_FIELDS
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger

# OBJECTIDs are batched into chunks this size to stay within arcpy's
# SQL IN-clause limit (varies by geodatabase type but 1 000 is safe).
_OID_BATCH_SIZE = 1_000


class PLR_privacy_redact:
    """
    Redacts ownership attributes from parcels that intersect privacy-
    sensitive point locations.

    Parameters
    ----------
    state:
        Full lowercase state name (e.g. 'colorado').
    quarter:
        Quarter string (e.g. 'Q2_2026').  Auto-detected if omitted.
    env:
        'LOCAL' uses the current working directory as the workspace root;
        any other value is treated as an explicit directory path.
    """

    def __init__(
        self,
        state: str,
        quarter: str | None = None,
        env: str = 'LOCAL',
    ) -> None:
        self.state = state
        self.workspace: Path = Path.cwd() if env == 'LOCAL' else Path(env)
        self.quarter: str = quarter or get_quarter()
        self.logger = get_logger(f'{type(self).__module__}.{type(self).__name__}')

        self.final_gdb: Path = (
            self.workspace / f'{self.state}_private_land_{self.quarter}.gdb'
        )
        self.final_fc: str = str(
            self.final_gdb / f'{self.state}_Private_Land_{self.quarter}'
        )

        # Validate inputs up front so failures are caught early.
        if not arcpy.Exists(self.final_fc):
            raise FileNotFoundError(
                f"Final output FC not found: {self.final_fc}\n"
                f"Ensure the pipeline has completed successfully for "
                f"'{self.state}' before running redaction."
            )

        if not PRIVACY_POINTS_FC:
            raise EnvironmentError(
                "PLR_PRIVACY_POINTS_FC is not configured.\n"
                "Set the PLR_PRIVACY_POINTS_FC environment variable (or add it "
                "to your .env file) to the full path of the enterprise GDB "
                "privacy points feature class."
            )

        if not arcpy.Exists(PRIVACY_POINTS_FC):
            raise FileNotFoundError(
                f"Privacy points FC not found: {PRIVACY_POINTS_FC}\n"
                "Check that the enterprise GDB connection is active and the "
                "PLR_PRIVACY_POINTS_FC path is correct."
            )

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def parcel_count(self) -> int:
        """Return the total number of parcels in the final output FC."""
        return int(arcpy.GetCount_management(self.final_fc)[0])

    def find_redact_oids(self) -> list[int]:
        """
        Spatially query the final output FC to find OBJECTIDs of every
        parcel that contains at least one privacy-sensitive point.

        Uses SelectLayerByLocation so the query runs against arcpy's
        spatial index rather than doing a full row-by-row scan.

        Returns
        -------
        list[int]
            OBJECTIDs to be redacted.  Empty list if no parcels match.
        """
        lyr = f'{self.state}_privacy_check'
        if arcpy.Exists(lyr):
            arcpy.Delete_management(lyr)

        arcpy.MakeFeatureLayer_management(self.final_fc, lyr)

        arcpy.SelectLayerByLocation_management(
            in_layer=lyr,
            overlap_type='INTERSECT',
            select_features=PRIVACY_POINTS_FC,
            selection_type='NEW_SELECTION',
        )

        hit_count = int(arcpy.GetCount_management(lyr)[0])
        self.logger.info(
            "%s: %d parcel(s) intersect privacy points", self.state, hit_count
        )

        oids: list[int] = []
        if hit_count > 0:
            with arcpy.da.SearchCursor(lyr, ['OBJECTID']) as cursor:
                for row in cursor:
                    oids.append(row[0])

        arcpy.Delete_management(lyr)
        return oids

    def redact_ownership(self, oids: list[int]) -> int:
        """
        Null out all ownership / PII fields for the given OBJECTIDs.

        Processes OBJECTIDs in batches to stay within SQL IN-clause limits.

        Parameters
        ----------
        oids:
            OBJECTIDs returned by :meth:`find_redact_oids`.

        Returns
        -------
        int
            Number of rows actually updated (should equal ``len(oids)``).
        """
        if not oids:
            self.logger.info("%s: no parcels require redaction", self.state)
            return 0

        null_row = tuple(None for _ in REDACT_FIELDS)
        redacted = 0

        for i in range(0, len(oids), _OID_BATCH_SIZE):
            chunk = oids[i : i + _OID_BATCH_SIZE]
            where = f"OBJECTID IN ({','.join(str(o) for o in chunk)})"
            with arcpy.da.UpdateCursor(
                self.final_fc, list(REDACT_FIELDS), where_clause=where
            ) as cursor:
                for _ in cursor:
                    cursor.updateRow(null_row)
                    redacted += 1

        self.logger.info(
            "%s: %d parcel(s) redacted (%d field(s) nulled each)",
            self.state, redacted, len(REDACT_FIELDS),
        )
        return redacted
