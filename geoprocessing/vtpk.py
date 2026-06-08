"""
VTPK creation stage for the PLR pipeline.

Generates two vector tile packages per state (one for Private Land, one for
Government Land) from the ArcGIS Pro project that references published
enterprise GDB data.  After each VTPK is created, release CSVs are written
and uploaded to S3:

    Per-layer VTPK  → vectortiles/{abbr_lower}/{state}_{ts}_{LayerSlug}.vtpk
    Per-layer CSV   → vectortilerelease/{state}_{ts}_{LayerSlug}.csv
    State-level CSV → vectortilerelease/{state}_{ts}.csv  (one row per layer,
                      appended after each layer so the final upload contains
                      both rows)

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
    AWS_VTPK_RELEASE_S3_PREFIX,
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
    'private_land': VTPK_PRIVATE_LAYER_NAME,   # e.g. 'Private Land'
    'govt_land':    VTPK_GOVT_LAYER_NAME,       # e.g. 'Government Land'
}


# ---------------------------------------------------------------------------
# Filename / slug helpers (ported from original vtpk_creator logic)
# ---------------------------------------------------------------------------

def _correct_layer_name(layername: str) -> str:
    """
    Sanitise a layer name for safe use in a file path.

    Matches the original ``correct_layer_name`` behaviour:
        'Government Land' → 'Government-Land'
        'Private Land'    → 'Private-Land'
    """
    import re
    mlayer = (
        layername
        .replace(' ', '-').replace('<', '-le-').replace('>', '-gt-')
        .replace('&', '-amp-').replace('+', '-pls-').replace('!', '-ex-')
        .replace('\\', '').replace('/', '')
    )
    mlayer = mlayer.replace('--', '-')
    mlayer = re.sub(r'[!@#$%^&*()+><?,]', '', mlayer)
    mlayer = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', mlayer)
    return mlayer


def _get_vtpk_name(layer_slug: str, map_name: str, timestamp: str) -> str:
    """
    Build the VTPK / CSV base name following the original convention:

        {map_name_lower_hyphenated}_{timestamp_ms}_{layer_slug}

    Examples
    --------
    >>> _get_vtpk_name('Government-Land', 'Hawaii', '1780935493960')
    'hawaii_1780935493960_Government-Land'
    """
    safe_map = map_name.replace(' ', '-').lower()
    safe_layer = _correct_layer_name(layer_slug)
    return f"{safe_map}_{timestamp}_{safe_layer}"


class PLR_vtpk:
    """
    Creates VTPKs and release CSVs for one state from the aprx that
    references enterprise SDE data.

    Produces per run:
        • {state}_{ts}_Private-Land.vtpk
        • {state}_{ts}_Government-Land.vtpk
        • {state}_{ts}_Private-Land.csv   (single row)
        • {state}_{ts}_Government-Land.csv (single row)
        • {state}_{ts}.csv                 (two rows — one per layer)

    Parameters
    ----------
    abbr:
        State abbreviation (e.g. ``'HI'``).
    state:
        Full lowercase state name (e.g. ``'hawaii'``).
    map_name:
        Name of the map inside the aprx (e.g. ``'Hawaii'``).
    aprx_path:
        Absolute path to the ``.aprx`` project file.
    output_path:
        Directory where ``.vtpk`` and ``.csv`` files will be written.
    quarter:
        Quarter string (e.g. ``'Q2_2026'``).
    timestamp:
        Millisecond epoch string shared across all states in one run
        (e.g. ``'1780935493960'``).
    """

    def __init__(
        self,
        abbr: str,
        state: str,
        map_name: str,
        aprx_path: str,
        output_path: str,
        quarter: str,
        timestamp: str,
    ) -> None:
        self.abbr = abbr
        self.state = state
        self.map_name = map_name
        self.aprx_path = aprx_path
        self.output_path = Path(output_path)
        self.quarter = quarter
        self.timestamp = timestamp
        self.logger = get_logger(f'{type(self).__module__}.{type(self).__name__}')

        self.output_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Path helpers                                                         #
    # ------------------------------------------------------------------ #

    @property
    def _state_slug(self) -> str:
        """Lowercase, hyphenated map name — used as the state segment in filenames."""
        return self.map_name.replace(' ', '-').lower()

    def state_csv_path(self) -> str:
        """
        Absolute path for the combined state-level release CSV.
        e.g. ``/output/hawaii_1780935493960.csv``
        """
        return str(self.output_path / f"{self._state_slug}_{self.timestamp}.csv")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_map(self, aprx: arcpy.mp.ArcGISProject) -> arcpy.mp.Map | None:
        """
        Locate the state map inside the aprx, trying space/underscore variants
        to tolerate minor naming inconsistencies.
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

    def _write_csv_row(self, csv_path: str, layer_slug: str, mode: str = 'a') -> None:
        """
        Write one CSV row in the original format:
            {state_lower},{layer_slug},N

        Parameters
        ----------
        mode:
            ``'w'`` for a fresh per-layer CSV; ``'a'`` to append to the
            accumulated state-level CSV.
        """
        with open(csv_path, mode) as fh:
            fh.write(f"{self.map_name.lower()},{layer_slug},N\n")

    # ------------------------------------------------------------------ #
    # VTPK creation                                                        #
    # ------------------------------------------------------------------ #

    def create_vtpk(self, layer_type: str) -> LayerVtpkResult:
        """
        Generate a VTPK for one layer type (``'private_land'`` or
        ``'govt_land'``), then write release CSVs.

        VTPK filename:  ``{state}_{timestamp}_{LayerSlug}.vtpk``
        Per-layer CSV:  ``{state}_{timestamp}_{LayerSlug}.csv``
        State CSV:      ``{state}_{timestamp}.csv`` (row appended)

        Opens a fresh ``ArcGISProject`` instance per call for thread safety.

        Returns
        -------
        LayerVtpkResult
            Populated with the outcome — including ``layer_csv_path`` and
            ``layer_csv_uploaded`` on success.
        """
        target_layer_name = _LAYER_NAME_MAP[layer_type]
        layer_slug = _correct_layer_name(target_layer_name)   # e.g. 'Government-Land'

        vtpk_base = _get_vtpk_name(layer_slug, self.map_name, self.timestamp)
        vtpk_path = str(self.output_path / f"{vtpk_base}.vtpk")

        result = LayerVtpkResult(
            layer_type=layer_type,
            layer_name=target_layer_name,
            map_name=self.map_name,
            vtpk_path=vtpk_path,
        )

        # --- Open fresh project instance (thread safety) ---
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

        # --- Find target layer (non-group, case-insensitive name match) ---
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

        # --- Visibility: show only this layer ---
        for lyr in m.listLayers():
            lyr.visible = lyr.isGroupLayer   # keep group layers on for hierarchy
        target_layer.visible = True

        # --- Remove pre-existing VTPK ---
        if os.path.exists(vtpk_path):
            os.remove(vtpk_path)
            self.logger.debug("[%s] Removed existing VTPK: %s", self.abbr, vtpk_path)

        # --- Create VTPK ---
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

        # --- Append row to state-level release CSV ---
        state_csv = self.state_csv_path()
        self._write_csv_row(state_csv, layer_slug, mode='a')
        self.logger.debug(
            "[%s] Appended '%s' row to state CSV: %s",
            self.abbr, layer_slug, state_csv,
        )

        return result

    # ------------------------------------------------------------------ #
    # S3 uploads                                                           #
    # ------------------------------------------------------------------ #

    def _s3_client(self):
        """Return an authenticated boto3 S3 client, or None if credentials missing."""
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            self.logger.warning(
                "[%s] AWS credentials not configured — skipping S3 upload. "
                "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env.",
                self.abbr,
            )
            return None
        import boto3
        return boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        ).client('s3')

    def upload_to_s3(self, vtpk_path: str) -> bool:
        """
        Upload a VTPK to the ``vectortiles`` S3 prefix.

        S3 key: ``{AWS_VTPK_S3_PREFIX}/{abbr_lower}/{filename}``
        e.g. ``vectortiles/hi/hawaii_1780935493960_Government-Land.vtpk``
        """
        client = self._s3_client()
        if client is None:
            return False
        try:
            filename = os.path.basename(vtpk_path)
            s3_key = f"{AWS_VTPK_S3_PREFIX}/{self.abbr.lower()}/{filename}"
            client.upload_file(vtpk_path, AWS_S3_BUCKET, s3_key)
            self.logger.info(
                "[%s] VTPK uploaded → s3://%s/%s", self.abbr, AWS_S3_BUCKET, s3_key,
            )
            return True
        except Exception as exc:
            self.logger.error("[%s] VTPK S3 upload failed: %s", self.abbr, exc)
            return False

    def upload_csv_to_s3(self, csv_path: str) -> bool:
        """
        Upload a release CSV to the ``vectortilerelease`` S3 prefix.

        S3 key: ``{AWS_VTPK_RELEASE_S3_PREFIX}/{filename}``
        e.g. ``vectortilerelease/hawaii_1780935493960_Government-Land.csv``
        """
        client = self._s3_client()
        if client is None:
            return False
        try:
            filename = os.path.basename(csv_path)
            s3_key = f"{AWS_VTPK_RELEASE_S3_PREFIX}/{filename}"
            client.upload_file(csv_path, AWS_S3_BUCKET, s3_key)
            self.logger.info(
                "[%s] CSV uploaded → s3://%s/%s", self.abbr, AWS_S3_BUCKET, s3_key,
            )
            return True
        except Exception as exc:
            self.logger.error("[%s] CSV S3 upload failed: %s", self.abbr, exc)
            return False
