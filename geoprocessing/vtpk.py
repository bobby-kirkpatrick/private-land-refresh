"""
VTPK creation stage for the PLR pipeline.

Generates a single vector tile package per state from the ArcGIS Pro project
that references the published enterprise GDB data.  Each VTPK contains both
the Private Land and Government Land layers, visible simultaneously.

Each call to ``create_vtpk()`` opens a fresh ``ArcGISProject`` instance so
that concurrent callers (ThreadPoolExecutor) do not share mutable aprx state.
"""
from __future__ import annotations

import os
from pathlib import Path

import arcpy

from configs.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_S3_BUCKET,
    AWS_VTPK_S3_PREFIX,
    VTPK_GOVT_LAYER_NAME,
    VTPK_PRIVATE_LAYER_NAME,
)
from utils.logging_config import get_logger
from utils.vtpk_report import LayerVtpkResult

# Both layer names that must be made visible before CreateVectorTilePackage.
_TARGET_LAYER_NAMES: tuple[str, str] = (VTPK_PRIVATE_LAYER_NAME, VTPK_GOVT_LAYER_NAME)


class PLR_vtpk:
    """
    Creates a combined VTPK (Private Land + Government Land) for one state
    from the aprx that references enterprise SDE data.

    Parameters
    ----------
    abbr:
        State abbreviation (e.g. ``'CO'``).
    state:
        Full lowercase state name (e.g. ``'colorado'``).
    map_name:
        Name of the map inside the aprx that corresponds to this state
        (e.g. ``'Colorado'``).
    aprx_path:
        Absolute path to the ``.aprx`` project file.
    output_path:
        Directory where the ``.vtpk`` file will be written.
    quarter:
        Quarter string (e.g. ``'Q2_2026'``).
    """

    def __init__(
        self,
        abbr: str,
        state: str,
        map_name: str,
        aprx_path: str,
        output_path: str,
        quarter: str,
    ) -> None:
        self.abbr = abbr
        self.state = state
        self.map_name = map_name
        self.aprx_path = aprx_path
        self.output_path = Path(output_path)
        self.quarter = quarter
        self.logger = get_logger(f'{type(self).__module__}.{type(self).__name__}')

        self.output_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_map(self, aprx: arcpy.mp.ArcGISProject) -> arcpy.mp.Map | None:
        """
        Locate the state map inside the aprx.

        Tries the configured ``map_name`` first, then falls back to swapping
        spaces for underscores (and vice-versa) to tolerate minor naming
        inconsistencies in the project file.
        """
        for candidate in (
            self.map_name,
            self.map_name.replace(' ', '_'),
            self.map_name.replace('_', ' '),
        ):
            found = aprx.listMaps(candidate)
            if found:
                return found[0]
        return None

    # ------------------------------------------------------------------ #
    # VTPK creation                                                        #
    # ------------------------------------------------------------------ #

    def create_vtpk(self) -> LayerVtpkResult:
        """
        Generate a single VTPK containing both Private Land and Government
        Land layers for this state.

        Opens a fresh ``ArcGISProject`` instance (required for thread safety),
        hides all non-target layers, makes both target layers visible, then
        calls ``CreateVectorTilePackage``.

        Output filename pattern: ``{abbr}_{quarter}.vtpk``
        e.g. ``CO_Q2_2026.vtpk``

        Returns
        -------
        LayerVtpkResult
            Populated with the outcome of this operation.
        """
        vtpk_filename = f"{self.abbr}_{self.quarter}.vtpk"
        vtpk_path = str(self.output_path / vtpk_filename)
        layer_label = ' + '.join(_TARGET_LAYER_NAMES)

        result = LayerVtpkResult(
            layer_type='combined',
            layer_name=layer_label,
            map_name=self.map_name,
            vtpk_path=vtpk_path,
        )

        # Open a fresh project instance per call (required for thread safety)
        aprx = arcpy.mp.ArcGISProject(self.aprx_path)
        m = self._resolve_map(aprx)

        if m is None:
            available = [mp.name for mp in aprx.listMaps()]
            result.status = 'failed'
            result.error = (
                f"Map '{self.map_name}' not found in aprx. "
                f"Available maps: {available}"
            )
            self.logger.error("[%s] %s", self.abbr, result.error)
            return result

        # Locate the two target layers (non-group, case-insensitive name match)
        target_names_lower = {n.lower() for n in _TARGET_LAYER_NAMES}
        target_layers = [
            lyr for lyr in m.listLayers()
            if not lyr.isGroupLayer
            and lyr.name.lower() in target_names_lower
        ]

        if not target_layers:
            available_layers = [
                lyr.name for lyr in m.listLayers() if not lyr.isGroupLayer
            ]
            result.status = 'skipped'
            result.error = (
                f"Neither '{VTPK_PRIVATE_LAYER_NAME}' nor '{VTPK_GOVT_LAYER_NAME}' "
                f"found in map '{m.name}'. Available layers: {available_layers}"
            )
            self.logger.warning("[%s] %s", self.abbr, result.error)
            return result

        # Warn if only one of the two expected layers was found
        found_names_lower = {lyr.name.lower() for lyr in target_layers}
        missing = [n for n in _TARGET_LAYER_NAMES if n.lower() not in found_names_lower]
        if missing:
            self.logger.warning(
                "[%s] Layer(s) not found in map '%s' — VTPK will be partial: %s",
                self.abbr, m.name, missing,
            )

        # Visibility: keep group layers on (preserves layer hierarchy rendering),
        # hide every non-group layer, then show the two target layers only.
        for lyr in m.listLayers():
            lyr.visible = lyr.isGroupLayer
        for lyr in target_layers:
            lyr.visible = True

        # Remove any pre-existing VTPK to prevent GP tool errors
        if os.path.exists(vtpk_path):
            os.remove(vtpk_path)
            self.logger.debug("[%s] Removed existing VTPK: %s", self.abbr, vtpk_path)

        self.logger.info(
            "[%s] Creating VTPK — layers=[%s] → %s",
            self.abbr, layer_label, vtpk_path,
        )
        arcpy.management.CreateVectorTilePackage(
            in_map=m,
            output_file=vtpk_path,
            service_type='ONLINE',
        )

        result.status = 'success'
        self.logger.info("[%s] VTPK created: %s", self.abbr, vtpk_path)
        return result

    # ------------------------------------------------------------------ #
    # S3 upload                                                            #
    # ------------------------------------------------------------------ #

    def upload_to_s3(self, vtpk_path: str) -> bool:
        """
        Upload the completed VTPK to S3.

        S3 key pattern:
            ``{AWS_VTPK_S3_PREFIX}/{abbr.lower()}/{filename}``
            e.g. ``vectortiles/co/CO_Q2_2026.vtpk``

        Returns
        -------
        bool
            ``True`` on success, ``False`` if credentials are missing or
            the upload fails.
        """
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            self.logger.warning(
                "[%s] AWS credentials not configured — skipping S3 upload. "
                "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env.",
                self.abbr,
            )
            return False

        try:
            import boto3  # imported lazily so the module works without boto3 installed

            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
            s3_client = session.client('s3')

            filename = os.path.basename(vtpk_path)
            s3_key = f"{AWS_VTPK_S3_PREFIX}/{self.abbr.lower()}/{filename}"

            s3_client.upload_file(vtpk_path, AWS_S3_BUCKET, s3_key)
            self.logger.info(
                "[%s] Uploaded → s3://%s/%s", self.abbr, AWS_S3_BUCKET, s3_key,
            )
            return True

        except Exception as exc:
            self.logger.error("[%s] S3 upload failed: %s", self.abbr, exc)
            return False
