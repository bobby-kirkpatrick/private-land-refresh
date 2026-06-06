"""
VTPK creation stage for the PLR pipeline.

Generates vector tile packages from the ArcGIS Pro project that references
the published enterprise GDB data.  One VTPK is produced per layer type
(Private Land, Government Land) per state.

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

# Ordered tuple used by vtpk_creator.py to iterate layers consistently.
LAYER_TYPES: tuple[str, ...] = ('private_land', 'govt_land')

# Maps layer_type keys to the aprx layer names configured in settings.
_LAYER_NAME_MAP: dict[str, str] = {
    'private_land': VTPK_PRIVATE_LAYER_NAME,
    'govt_land':    VTPK_GOVT_LAYER_NAME,
}


class PLR_vtpk:
    """
    Creates VTPKs for one state from the aprx that references enterprise SDE data.

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
        Directory where ``.vtpk`` files will be written.
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
        inconsistencies.
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

    def create_vtpk(self, layer_type: str) -> LayerVtpkResult:
        """
        Generate a VTPK for one layer type (``'private_land'`` or
        ``'govt_land'``).

        Opens a fresh ``ArcGISProject`` instance to avoid thread-safety
        issues when called concurrently.  Hides all non-target layers in
        the map before calling ``CreateVectorTilePackage``.

        Returns
        -------
        LayerVtpkResult
            Populated with the outcome of this operation.
        """
        target_layer_name = _LAYER_NAME_MAP[layer_type]
        vtpk_filename = f"{self.abbr}_{layer_type}_{self.quarter}.vtpk"
        vtpk_path = str(self.output_path / vtpk_filename)

        result = LayerVtpkResult(
            layer_type=layer_type,
            layer_name=target_layer_name,
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

        # Find target layer — non-group, case-insensitive name match
        target_layers = [
            lyr for lyr in m.listLayers()
            if not lyr.isGroupLayer
            and lyr.name.lower() == target_layer_name.lower()
        ]

        if not target_layers:
            available_layers = [
                lyr.name for lyr in m.listLayers() if not lyr.isGroupLayer
            ]
            result.status = 'skipped'
            result.error = (
                f"Layer '{target_layer_name}' not found in map '{m.name}'. "
                f"Available layers: {available_layers}"
            )
            self.logger.warning("[%s] %s", self.abbr, result.error)
            return result

        if len(target_layers) > 1:
            self.logger.warning(
                "[%s] Multiple '%s' layers found in map '%s'. Using the first.",
                self.abbr, target_layer_name, m.name,
            )

        target_layer = target_layers[0]

        # Visibility: keep group layers on (so target inherits visibility
        # through its ancestry), hide all non-group layers, then show target.
        for lyr in m.listLayers():
            lyr.visible = lyr.isGroupLayer
        target_layer.visible = True

        # Remove any pre-existing VTPK to prevent GP errors
        if os.path.exists(vtpk_path):
            os.remove(vtpk_path)
            self.logger.debug("[%s] Removed existing VTPK: %s", self.abbr, vtpk_path)

        self.logger.info(
            "[%s] Creating VTPK — layer='%s' → %s",
            self.abbr, target_layer_name, vtpk_path,
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

    def upload_to_s3(self, vtpk_path: str, layer_type: str) -> bool:
        """
        Upload a completed VTPK to S3.

        S3 key pattern:
            ``{AWS_VTPK_S3_PREFIX}/{abbr.lower()}/{filename}``

        Returns
        -------
        bool
            ``True`` on success, ``False`` if credentials are missing or
            the upload fails.
        """
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            self.logger.warning(
                "[%s] AWS credentials not configured — skipping S3 upload for %s. "
                "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env.",
                self.abbr, layer_type,
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
                "[%s] Uploaded %s → s3://%s/%s",
                self.abbr, layer_type, AWS_S3_BUCKET, s3_key,
            )
            return True

        except Exception as exc:
            self.logger.error(
                "[%s] S3 upload failed for %s: %s", self.abbr, layer_type, exc,
            )
            return False
