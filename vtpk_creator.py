"""
PLR Vector Tile Package Creator — standalone post-publish step.

Generates VTPKs for the Private Land and Government Land layers from the
ArcGIS Pro project file that references the published enterprise GDB data,
then uploads each package to S3.

This script is intentionally separate from main.py and publish.py so that:
  - VTPKs can be regenerated independently without re-running the pipeline.
  - Individual states can be exported as they complete, or all at once.
  - S3 upload can be disabled during testing (--no-upload).

Prerequisites
-------------
  1. Run main.py to generate the local pipeline outputs.
  2. Run publish.py to push outputs to the enterprise GDB.
  3. Confirm the ArcGIS Pro project (.aprx) is pointed at the published data.
  4. Set required values in .env (see below).

Required .env values
--------------------
    PLR_VTPK_APRX_PATH=D:\\aprx\\PLR_AllStates.aprx
    PLR_VTPK_OUTPUT_FOLDER=D:\\bobby-workspace\\2026_plr_vtpks
    AWS_ACCESS_KEY_ID=<your key>
    AWS_SECRET_ACCESS_KEY=<your secret>

Optional .env overrides
-----------------------
    AWS_S3_BUCKET=gh-gis-source-repo        (default)
    AWS_VTPK_S3_PREFIX=vectortiles          (default)
    PLR_VTPK_PRIVATE_LAYER=Private Land     (default)
    PLR_VTPK_GOVT_LAYER=Government Land     (default)

Usage examples
--------------
# Export specific states (most common):
    python vtpk_creator.py --states CO SD UT

# Override the aprx and output folder on the fly:
    python vtpk_creator.py --states CO --aprx "D:\\aprx\\PLR.aprx" --output "D:\\vtpks"

# Skip S3 upload (local VTPK only):
    python vtpk_creator.py --states CO --no-upload

# Skip the confirmation gateway (for automation / scheduled runs):
    python vtpk_creator.py --states CO --force

# Process up to 3 states concurrently:
    python vtpk_creator.py --states CO AZ UT --max-workers 3
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import arcpy

from configs import state_full
from configs.settings import VTPK_DEFAULT_APRX, VTPK_DEFAULT_OUTPUT
from geoprocessing.vtpk import PLR_vtpk
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger
from utils.vtpk_report import LayerVtpkResult, StateVtpkResult, VtpkReport

logger = get_logger(__name__)

arcpy.env.overwriteOutput = True


# ---------------------------------------------------------------------------
# Map name resolution
# ---------------------------------------------------------------------------

def _map_name_for_state(abbr: str) -> str:
    """
    Derive the aprx map name for a given state abbreviation.

    Convention: full title-cased state name with underscores replaced by spaces.
        'CO'  → 'Colorado'
        'NM'  → 'New Mexico'
        'WV'  → 'West Virginia'
    """
    return state_full[abbr].replace('_', ' ').title()


# ---------------------------------------------------------------------------
# Confirmation gateway
# ---------------------------------------------------------------------------

def _confirm_settings(
    aprx_path: str,
    output_folder: str,
    quarter: str,
    state_labels: list[str],
    upload: bool,
    force: bool,
) -> bool:
    """
    Display a pre-run summary and prompt the operator to confirm before
    any files are created or data is uploaded.

    Parameters
    ----------
    state_labels:
        Human-readable strings such as ``['CO (Colorado)', 'AZ (Arizona)']``.
    force:
        When ``True``, skip the prompt and return ``True`` immediately.

    Returns
    -------
    bool
        ``True`` to proceed, ``False`` to abort.
    """
    if force:
        logger.warning("--force flag set: skipping confirmation gateway.")
        return True

    print()
    print("=" * 65)
    print("  PLR VTPK CREATOR — CONFIRM SETTINGS")
    print("=" * 65)
    print()
    print(f"  ArcGIS Project : {aprx_path}")
    print(f"  Output Folder  : {output_folder}")
    print(f"  Quarter        : {quarter}")
    print(f"  States         : {', '.join(state_labels)}")
    print(f"  S3 Upload      : {'enabled' if upload else 'disabled (--no-upload)'}")
    print()
    print("  Layers exported per state:")
    print("    • Private Land")
    print("    • Government Land")
    print()
    print("  VTPKs are generated from PUBLISHED enterprise GDB data.")
    print("  Ensure publish.py has been run successfully before proceeding.")
    print()

    if not os.path.exists(aprx_path):
        print(f"  ⚠  WARNING: ArcGIS Project not found at the path above.")
        print(f"     Update PLR_VTPK_APRX_PATH in .env or pass --aprx.")
        print()

    if not os.path.isdir(output_folder):
        print(f"  ℹ  Output folder does not exist yet — it will be created.")
        print()

    while True:
        response = input("  Enter 'yes' to continue or 'no' to abort: ").strip().lower()
        if response == 'yes':
            print()
            logger.info("Settings confirmed — proceeding with VTPK export.")
            return True
        if response == 'no':
            print()
            print("  Export aborted.  Update your settings and re-run when ready.")
            print()
            return False
        print("  Please enter 'yes' or 'no'.")


# ---------------------------------------------------------------------------
# Per-state worker
# ---------------------------------------------------------------------------

def _export_state(
    abbr: str,
    map_name: str,
    aprx_path: str,
    output_folder: str,
    quarter: str,
    upload: bool,
    state_result: StateVtpkResult,
) -> None:
    """
    Generate VTPKs for both layers of one state, then optionally upload
    each to S3.

    Designed to be called from a ``ThreadPoolExecutor`` worker.  All state
    is isolated to this call — no shared mutable references are used.
    """
    stage_start = time.time()
    try:
        creator = PLR_vtpk(
            abbr=abbr,
            state=state_full[abbr],
            map_name=map_name,
            aprx_path=aprx_path,
            output_path=output_folder,
            quarter=quarter,
        )

        # One VTPK per state containing both Private Land and Government Land
        logger.info("[%s] Exporting combined VTPK…", abbr)
        try:
            layer_result: LayerVtpkResult = creator.create_vtpk()
        except arcpy.ExecuteError:
            msg = arcpy.GetMessages(2)
            logger.error("[%s] ArcPy error: %s", abbr, msg)
            layer_result = LayerVtpkResult(
                layer_type='combined',
                layer_name='',
                map_name=map_name,
                vtpk_path='',
                status='failed',
                error=msg,
            )
        except Exception as exc:
            logger.exception("[%s] Unexpected error during VTPK creation", abbr)
            layer_result = LayerVtpkResult(
                layer_type='combined',
                layer_name='',
                map_name=map_name,
                vtpk_path='',
                status='failed',
                error=str(exc),
            )

        # Upload immediately after successful creation
        if upload and layer_result.status == 'success' and layer_result.vtpk_path:
            layer_result.uploaded = creator.upload_to_s3(layer_result.vtpk_path)

        state_result.layers.append(layer_result)

        state_result.mark_complete()
        logger.info(
            "[%s] Export complete — status=%s | %.1fs",
            abbr, state_result.status, time.time() - stage_start,
        )

    except Exception:
        logger.exception("[%s] Unhandled error during VTPK setup", abbr)
        state_result.status = 'failed'
    finally:
        state_result.elapsed_seconds = round(time.time() - stage_start, 2)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='plr_vtpk',
        description='PLR VTPK Creator — generate and upload vector tile packages.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--states', nargs='+', metavar='ABBR', required=True,
        help='State abbreviations to export (e.g. CO AZ UT). Required.',
    )
    parser.add_argument(
        '--aprx', metavar='PATH',
        help=(
            'Path to the .aprx project file. '
            'Defaults to PLR_VTPK_APRX_PATH in .env.'
        ),
    )
    parser.add_argument(
        '--output', metavar='PATH',
        help=(
            'Directory where .vtpk files will be written. '
            'Defaults to PLR_VTPK_OUTPUT_FOLDER in .env.'
        ),
    )
    parser.add_argument(
        '--quarter', metavar='Qn_YYYY',
        help='Override the auto-detected quarter (e.g. Q2_2026).',
    )
    parser.add_argument(
        '--max-workers', type=int, default=2, metavar='N',
        help=(
            'Number of states to process concurrently (default: 2). '
            'Reduce to 1 if ArcPy errors occur during concurrent export.'
        ),
    )
    parser.add_argument(
        '--no-upload', action='store_true',
        help='Write VTPKs to the output folder only; skip S3 upload.',
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Skip the interactive confirmation gateway (useful in automation).',
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(args: argparse.Namespace) -> None:
    run_start = time.time()

    # --- Resolve paths ---
    aprx_path: str = args.aprx or VTPK_DEFAULT_APRX
    output_folder: str = args.output or VTPK_DEFAULT_OUTPUT

    if not aprx_path:
        logger.error(
            "No .aprx path provided. "
            "Use --aprx or set PLR_VTPK_APRX_PATH in your .env file."
        )
        sys.exit(1)

    if not output_folder:
        logger.error(
            "No output folder provided. "
            "Use --output or set PLR_VTPK_OUTPUT_FOLDER in your .env file."
        )
        sys.exit(1)

    quarter = args.quarter or get_quarter()
    upload = not args.no_upload

    # --- Resolve and validate states ---
    requested = [s.upper() for s in args.states]
    available = set(state_full.keys())

    unknown = set(requested) - available
    if unknown:
        logger.warning(
            "Unknown state abbreviation(s) (skipping): %s", sorted(unknown)
        )

    valid = [a for a in requested if a in available]
    if not valid:
        logger.error("No valid states to process. Exiting.")
        sys.exit(1)

    map_names: dict[str, str] = {abbr: _map_name_for_state(abbr) for abbr in valid}

    # --- Confirmation gateway ---
    state_labels = [f"{a} ({map_names[a]})" for a in valid]
    if not _confirm_settings(
        aprx_path=aprx_path,
        output_folder=output_folder,
        quarter=quarter,
        state_labels=state_labels,
        upload=upload,
        force=args.force,
    ):
        sys.exit(0)

    # Validate aprx path after confirmation so the gateway message is visible
    if not os.path.exists(aprx_path):
        logger.error("ArcGIS Project not found: %s", aprx_path)
        sys.exit(1)

    # --- Initialise report ---
    report = VtpkReport(
        quarter=quarter,
        started_at=time.strftime('%Y-%m-%dT%H:%M:%S'),
        aprx_path=aprx_path,
        output_folder=output_folder,
        states_requested=valid,
    )
    results: dict[str, StateVtpkResult] = {
        abbr: report.add_state(abbr, state_full[abbr], map_names[abbr])
        for abbr in valid
    }

    logger.info("====== PLR VTPK export started ======")
    logger.info(
        "States: %s | Quarter: %s | Workers: %d | S3: %s",
        valid, quarter, args.max_workers, 'enabled' if upload else 'disabled',
    )

    # --- Export: parallel by state when max_workers > 1 ---
    if args.max_workers > 1 and len(valid) > 1:
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(
                    _export_state,
                    abbr,
                    map_names[abbr],
                    aprx_path,
                    output_folder,
                    quarter,
                    upload,
                    results[abbr],
                ): abbr
                for abbr in valid
            }
            for future in as_completed(futures):
                abbr = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception(
                        "[%s] Worker raised an unhandled exception", abbr
                    )
    else:
        for abbr in valid:
            logger.info("--- Exporting: %s (%s) ---", abbr, map_names[abbr])
            _export_state(
                abbr=abbr,
                map_name=map_names[abbr],
                aprx_path=aprx_path,
                output_folder=output_folder,
                quarter=quarter,
                upload=upload,
                state_result=results[abbr],
            )

    # --- Finalise and write report ---
    total_elapsed = time.time() - run_start
    report.finalize(total_elapsed)
    report_path = report.write()

    for line in report.summary_lines():
        logger.info(line)

    logger.info("VTPK report written to: %s", report_path)
    logger.info(
        "====== PLR VTPK export complete — %.1f seconds ======", total_elapsed
    )


if __name__ == '__main__':
    _parser = _build_parser()
    _args = _parser.parse_args()
    main(_args)
