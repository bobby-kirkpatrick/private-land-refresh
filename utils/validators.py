"""
Pre-flight validation helpers for the PLR pipeline.

Call validate_state_inputs() for every state before any processing begins.
Collect all errors across all states, then raise PipelineValidationError
once so the operator sees every problem in a single run rather than one
at a time.
"""
from __future__ import annotations

import logging
from pathlib import Path

import arcpy

from configs.settings import XGB_MODELS_DIR, GOVT_NAME_TABLES_DIR

# Fields that must exist on a parcel feature class before the pipeline starts.
# These are added by parcel_acquisition.field_processing(); if they are missing
# the acquisition stage was skipped or failed.
REQUIRED_PARCEL_FIELDS: list[str] = [
    'OBJECTID',
    'PARCEL_ID',
    'OWN1_FRST',
    'OWN1_LAST',
    'OWN2_FRST',
    'OWN2_LAST',
    'gh_parcel_acres',
    'overlap_perc',
    'gh_govt',
    'gh_govtype',
    'unit_nm',
    'mail_addr',
]

# Fields that must exist on the government land feature class.
REQUIRED_GOVT_FIELDS: list[str] = [
    'OBJECTID',
    'gh_govtype',
    'unit_nm',
]


class PipelineValidationError(Exception):
    """
    Raised when one or more pre-flight validation checks fail.

    The message contains a newline-separated list of every problem found,
    so the operator can fix everything before re-running.
    """


# ---------------------------------------------------------------------------
# Low-level checks — each returns a (possibly empty) list of error strings
# ---------------------------------------------------------------------------

def _check_fc_exists(path: str, label: str, logger: logging.Logger) -> list[str]:
    """Return an error string if *path* does not exist as an arcpy dataset."""
    if not arcpy.Exists(path):
        msg = f"[MISSING FC] {label}: {path}"
        logger.error(msg)
        return [msg]
    return []


def _check_fields(
    path: str,
    required: list[str],
    label: str,
    logger: logging.Logger,
) -> list[str]:
    """Return error strings for any required fields absent from *path*."""
    if not arcpy.Exists(path):
        return []  # already caught by _check_fc_exists
    existing = {f.name for f in arcpy.ListFields(path)}
    missing = sorted(f for f in required if f not in existing)
    if missing:
        msg = f"[MISSING FIELDS] {label}: {missing}"
        logger.error(msg)
        return [msg]
    return []


def _check_file(path: Path, label: str, logger: logging.Logger) -> list[str]:
    """Return an error string if *path* does not exist on disk."""
    if not path.exists():
        msg = f"[MISSING FILE] {label}: {path}"
        logger.error(msg)
        return [msg]
    return []


def _check_spatial_ref(
    parcels: str,
    govt_land: str,
    state: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Warn if parcel and government land layers are in different spatial references.
    Returns a warning string (not an error — arcpy reprojects on the fly for most
    operations, but a mismatch is worth surfacing to the operator).
    """
    warnings: list[str] = []
    if not (arcpy.Exists(parcels) and arcpy.Exists(govt_land)):
        return warnings

    parcel_sr = arcpy.Describe(parcels).spatialReference
    govt_sr = arcpy.Describe(govt_land).spatialReference

    if parcel_sr.factoryCode != govt_sr.factoryCode:
        msg = (
            f"[SR MISMATCH] {state}: parcels={parcel_sr.name} "
            f"(WKID {parcel_sr.factoryCode}) vs "
            f"govt_land={govt_sr.name} (WKID {govt_sr.factoryCode}). "
            "arcpy will reproject on the fly, but verify outputs."
        )
        logger.warning(msg)
        warnings.append(msg)

    return warnings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_state_inputs(
    state: str,
    govt_land: str,
    parcels: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Run all pre-flight checks for a single state.

    Parameters
    ----------
    state:
        State name used for file lookups (e.g. 'ohio').
    govt_land:
        Full path to the government land feature class.
    parcels:
        Full path to the parcel feature class.
    logger:
        Logger to write individual check results to.

    Returns
    -------
    list[str]
        All error messages found.  Empty list means the state passed validation.
        Spatial reference mismatches are logged as warnings but are not included
        in the returned error list.
    """
    errors: list[str] = []

    # 1 — Feature class existence
    errors += _check_fc_exists(parcels,   f"{state} parcels",    logger)
    errors += _check_fc_exists(govt_land, f"{state} govt land",  logger)

    # 2 — Required field presence
    errors += _check_fields(parcels,   REQUIRED_PARCEL_FIELDS, f"{state} parcels",    logger)
    errors += _check_fields(govt_land, REQUIRED_GOVT_FIELDS,   f"{state} govt land",  logger)

    # 3 — Model and lookup files
    errors += _check_file(
        XGB_MODELS_DIR / f'{state}_xgb_model.json',
        f"{state} XGBoost model",
        logger,
    )
    errors += _check_file(
        GOVT_NAME_TABLES_DIR / f'{state}_govt_names.csv',
        f"{state} govt name table",
        logger,
    )

    # 4 — Spatial reference (warn only, not an error)
    _check_spatial_ref(parcels, govt_land, state, logger)

    return errors


def validate_all_states(
    states_config: dict,
    state_full: dict,
    logger: logging.Logger,
    raise_on_error: bool = True,
) -> dict[str, list[str]]:
    """
    Run validate_state_inputs() for every enabled state in *states_config*.

    Parameters
    ----------
    states_config:
        The ``config['states']`` dict mapping abbreviation → {govt_land, parcels}.
    state_full:
        Mapping of abbreviation → full lowercase name (e.g. 'OH' → 'ohio').
    logger:
        Pipeline logger.
    raise_on_error:
        If True (default), raise PipelineValidationError when any state fails.

    Returns
    -------
    dict[str, list[str]]
        Mapping of state abbreviation → list of error strings.
        States that passed have an empty list.
    """
    results: dict[str, list[str]] = {}

    for abbr, data in states_config.items():
        state = state_full[abbr]
        logger.info("Validating inputs for %s (%s)…", state, abbr)
        results[abbr] = validate_state_inputs(
            state, data['govt_land'], data['parcels'], logger
        )

    failed = {abbr: errs for abbr, errs in results.items() if errs}

    if failed and raise_on_error:
        all_errors = [
            f"  [{abbr}] {msg}"
            for abbr, errs in failed.items()
            for msg in errs
        ]
        raise PipelineValidationError(
            f"{len(all_errors)} validation error(s) found before pipeline start:\n"
            + "\n".join(all_errors)
        )

    return results
