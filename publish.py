"""
PLR Database Publish — standalone post-processing step.

Publishes completed Private_Land and Govt_Land feature classes from the
local final GDBs into the enterprise geodatabase.

For each layer the script:
  1. Validates that source and target field schemas are compatible.
  2. Truncates the enterprise table (ONLY if validation passed).
  3. Appends the new data using an explicit name-matched field mapping.

This script is intentionally separate from main.py so that:
  - Pipeline outputs can be inspected and redacted before publishing.
  - Individual states can be published as they complete without waiting
    for the full batch.

Usage examples
--------------
# Publish a single completed state:
    python publish.py --states CO

# Publish multiple states:
    python publish.py --states CO MT OK WY

# Override the auto-detected quarter:
    python publish.py --states CO --quarter Q2_2026

# Override the workspace (where final GDBs live):
    python publish.py --states CO --workspace "D:\\output\\Q2_2026"

# Skip the backup confirmation (for automation / CI):
    python publish.py --states CO --force
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import arcpy

from configs import dev, state_full
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger
from utils.publish_report import LayerPublishResult, PublishReport, StatePublishResult
from geoprocessing.publish import PLR_publish

logger = get_logger(__name__)

_LAYER_TYPES = ('private_land', 'govt_land')


# ---------------------------------------------------------------------------
# Backup confirmation gateway
# ---------------------------------------------------------------------------

def _confirm_backup(force: bool) -> bool:
    """
    Prompt the operator to confirm that database backups have been made.

    Returns True if the operator confirms (or --force was passed).
    Returns False and prints a message if the operator declines.
    """
    if force:
        logger.warning(
            "--force flag set: skipping backup confirmation. "
            "Ensure backups exist before continuing."
        )
        return True

    print()
    print("=" * 65)
    print("  PLR DATABASE PUBLISH — BACKUP CONFIRMATION")
    print("=" * 65)
    print()
    print("  This script will TRUNCATE and REPLACE data in the enterprise")
    print("  geodatabase.  A truncated table CANNOT be restored without a")
    print("  prior backup.")
    print()
    print("  Have you created backups of all affected database tables?")
    print()

    while True:
        response = input("  Enter 'yes' to continue or 'no' to abort: ").strip().lower()
        if response == 'yes':
            print()
            logger.info("Backup confirmed by operator — proceeding with publish.")
            return True
        if response == 'no':
            print()
            print("  Publish aborted.  Create backups and re-run when ready.")
            print()
            return False
        print("  Please enter 'yes' or 'no'.")


# ---------------------------------------------------------------------------
# Per-state publish runner
# ---------------------------------------------------------------------------

def _publish_state(
    abbr: str,
    state: str,
    state_config: dict,
    quarter: str,
    state_result: StatePublishResult,
) -> None:
    """Run validate → truncate → append for both layers of one state."""
    stage_start = time.time()
    try:
        publisher = PLR_publish(state, state_config, quarter=quarter)

        for layer_type in _LAYER_TYPES:
            logger.info("[%s] Publishing %s…", abbr, layer_type)
            try:
                layer_result: LayerPublishResult = publisher.publish_layer(layer_type)
            except arcpy.ExecuteError:
                msg = arcpy.GetMessages(2)
                logger.error("[%s] ArcPy error on %s: %s", abbr, layer_type, msg)
                layer_result = LayerPublishResult(
                    layer_type=layer_type,
                    source_fc=publisher.sources[layer_type],
                    target_fc=publisher.targets[layer_type],
                    status='failed',
                    error=msg,
                )
            except Exception as exc:
                logger.exception("[%s] Unexpected error on %s", abbr, layer_type)
                layer_result = LayerPublishResult(
                    layer_type=layer_type,
                    source_fc=publisher.sources[layer_type],
                    target_fc=publisher.targets[layer_type],
                    status='failed',
                    error=str(exc),
                )
            state_result.layers.append(layer_result)

        state_result.mark_complete()
        logger.info(
            "[%s] Publish complete — status=%s", abbr, state_result.status
        )

    except Exception as exc:
        logger.exception("[%s] Unhandled error during publish setup", abbr)
        state_result.status = 'failed'

    finally:
        state_result.elapsed_seconds = round(time.time() - stage_start, 2)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='plr_publish',
        description='PLR Database Publish — truncate and replace enterprise GDB layers.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--states', nargs='+', metavar='ABBR', required=True,
        help='State abbreviations to publish (e.g. CO MT OK). Required.',
    )
    parser.add_argument(
        '--quarter', metavar='Qn_YYYY',
        help='Override the auto-detected quarter (e.g. Q2_2026).',
    )
    parser.add_argument(
        '--workspace', metavar='PATH',
        help='Directory containing the final Private_Land GDBs. '
             'Defaults to the current working directory.',
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Skip the interactive backup confirmation (use in automation).',
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(args: argparse.Namespace) -> None:
    run_start = time.time()

    # --- Backup confirmation ---
    if not _confirm_backup(args.force):
        sys.exit(0)

    # --- Apply overrides ---
    if args.workspace:
        os.chdir(args.workspace)
        logger.info("Workspace: %s", args.workspace)

    quarter = args.quarter or get_quarter()
    if args.quarter:
        logger.info("Quarter override: %s", quarter)

    # --- Resolve states ---
    requested = [s.upper() for s in args.states]
    available_in_config = set(dev['states'].keys())
    available_full = set(state_full.keys())

    unknown = set(requested) - available_full
    if unknown:
        logger.warning(
            "Unknown state abbreviation(s) (skipping): %s", sorted(unknown)
        )

    not_in_config = (set(requested) - unknown) - available_in_config
    if not_in_config:
        logger.warning(
            "State(s) not enabled in configs/config.py (skipping): %s — "
            "uncomment them in dev['states'] to publish.",
            sorted(not_in_config),
        )

    valid = [a for a in requested if a in available_in_config]
    if not valid:
        logger.error("No valid states to publish.  Exiting.")
        sys.exit(1)

    # --- Initialise report ---
    report = PublishReport(
        quarter=quarter,
        started_at=time.strftime('%Y-%m-%dT%H:%M:%S'),
        states_requested=valid,
    )
    results: dict[str, StatePublishResult] = {
        abbr: report.add_state(abbr, state_full[abbr])
        for abbr in valid
    }

    logger.info("====== PLR publish started ======")
    logger.info("States: %s | Quarter: %s", valid, quarter)

    # --- Publish each state ---
    for abbr in valid:
        logger.info("--- Publishing: %s (%s) ---", abbr, state_full[abbr])
        _publish_state(
            abbr=abbr,
            state=state_full[abbr],
            state_config=dev['states'][abbr],
            quarter=quarter,
            state_result=results[abbr],
        )

    # --- Finalise and write report ---
    total_elapsed = time.time() - run_start
    report.finalize(total_elapsed)
    report_path = report.write()

    for line in report.summary_lines():
        logger.info(line)

    logger.info("Publish report written to: %s", report_path)
    logger.info(
        "====== PLR publish complete — %.1f seconds ======", total_elapsed
    )


if __name__ == '__main__':
    _parser = _build_parser()
    _args = _parser.parse_args()
    main(_args)
