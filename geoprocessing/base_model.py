"""Base class shared by all PLR pipeline model stages."""
from __future__ import annotations

import time
from pathlib import Path

import arcpy

from utils.geo_utils import get_quarter
from utils.logging_config import get_logger

_pipeline_start = time.time()


class BaseModel:
    """
    Common foundation for XGBoost, GIS, and QC pipeline stages.

    Provides:
      - Workspace resolution (LOCAL vs explicit path)
      - Quarter string computed once at construction
      - Idempotent GDB creation via _ensure_gdb()
      - Standard set_workspaces() that creates temp + final GDBs
      - Per-class logger under the subclass module name
    """

    def __init__(self, data: dict, state: str, env: str = 'LOCAL') -> None:
        self.state = state
        self.env = env
        self.workspace: Path = Path.cwd() if env == 'LOCAL' else Path(env)
        self.govt_land: str = data['govt_land']
        self.parcels: str = data['parcels']
        self.quarter: str = get_quarter()
        self.logger = get_logger(
            f'{type(self).__module__}.{type(self).__name__}'
        )

        self.logger.debug(
            "%s init | state=%s | parcels=%s | govt_land=%s",
            type(self).__name__, state, self.parcels, self.govt_land,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_gdb(self, path: Path, label: str = 'Workspace') -> Path:
        """Create a file geodatabase at *path* if it does not already exist."""
        if path.exists():
            self.logger.info("%s already exists: %s", label, path)
        else:
            arcpy.CreateFileGDB_management(str(path.parent), path.name)
            self.logger.info("%s created: %s", label, path)
        return path

    def _elapsed(self) -> float:
        return time.time() - _pipeline_start

    # ------------------------------------------------------------------
    # Workspace setup
    # ------------------------------------------------------------------

    def set_workspaces(self) -> None:
        """Create temp and final file geodatabases for this state and quarter."""
        self.temp_dir = self._ensure_gdb(
            self.workspace / f'{self.state}_temp_{self.quarter}.gdb',
            'Temp workspace',
        )
        self.final_dir = self._ensure_gdb(
            self.workspace / f'{self.state}_private_land_{self.quarter}.gdb',
            'Final workspace',
        )
        self.logger.debug("set_workspaces elapsed: %.1fs", self._elapsed())

    # ------------------------------------------------------------------
    # Geometry repair
    # ------------------------------------------------------------------

    def repair_geometry(self) -> None:
        """
        Run RepairGeometry on both the parcel and government land feature
        classes before any processing begins.

        Centralising this here ensures geometry is always clean at pipeline
        entry, rather than only inside overlap_qc where problems surface late.
        """
        for fc, label in [(self.parcels, 'parcels'), (self.govt_land, 'govt land')]:
            if arcpy.Exists(fc):
                self.logger.info("Repairing geometry: %s %s", self.state, label)
                arcpy.RepairGeometry_management(fc)
                self.logger.info("Geometry repair complete: %s %s", self.state, label)
            else:
                self.logger.warning(
                    "Skipping geometry repair — %s %s not found: %s",
                    self.state, label, fc,
                )
