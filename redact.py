"""
PLR Privacy Redaction — standalone post-processing step.

Nulls out all ownership / PII attributes on final Private_Land parcels
that intersect privacy-sensitive locations stored in the enterprise GDB
privacy points feature class (PLR_PRIVACY_POINTS_FC in configs/settings.py
or .env).

This script is intentionally decoupled from main.py so that:

  1. Pipeline outputs can be inspected before redaction is applied.
  2. Redaction can be run on completed states while other states are still
     being processed by the main pipeline.

Before running
--------------
Ensure PLR_PRIVACY_POINTS_FC is set in your .env file:

    PLR_PRIVACY_POINTS_FC=D:\\db_connections\\enterprise.sde\\schema.privacy_points

Usage examples
--------------
# Redact a single completed state:
    python redact.py --states CO

# Redact multiple states at once:
    python redact.py --states CO MT OK WY

# Override the auto-detected quarter:
    python redact.py --states CO --quarter Q2_2026

# Override the workspace (where the final GDBs live):
    python redact.py --states CO --workspace D:\\output\\Q2_2026
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import arcpy

from configs import state_full
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger
from utils.redaction_report import RedactionReport, RedactionResult
from geoprocessing.privacy_redact import PLR_privacy_redact

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Per-state runner
# ---------------------------------------------------------------------------

def _redact_state(
    abbr: str,
    state: str,
    result: RedactionResult,
    quarter: str,
) -> None:
    """Run the full redaction sequence for one state."""
    stage_start = time.time()
    try:
        redactor = PLR_privacy_redact(state, quarter=quarter)

        result.parcels_inspected = redactor.parcel_count()
        logger.info("%s: %d total parcels in final output", abbr, result.parcels_inspected)

        oids = redactor.find_redact_oids()
        redacted = redactor.redact_ownership(oids)

        result.parcels_redacted = redacted
        result.mark_success()
        logger.info(
            "--- Redaction complete: %s | %d/%d parcels redacted ---",
            abbr, redacted, result.parcels_inspected,
        )

    except FileNotFoundError as exc:
        logger.error("Redaction %s: missing resource — %s", abbr, exc)
        result.mark_failed(str(exc))
    except EnvironmentError as exc:
        logger.error("Redaction %s: configuration error — %s", abbr, exc)
        result.mark_failed(str(exc))
    except arcpy.ExecuteError:
        msg = arcpy.GetMessages(2)
        logger.error("Redaction %s: ArcPy error — %s", abbr, msg)
        result.mark_failed(msg)
    except Exception as exc:
        logger.exception("Redaction %s: unexpected error", abbr)
        result.mark_failed(str(exc))
    finally:
        result.elapsed_seconds = round(time.time() - stage_start, 2)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='plr_redact',
        description='PLR Privacy Redaction — null ownership info on sensitive parcels.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--states', nargs='+', metavar='ABBR', required=True,
        help='State abbreviations to redact (e.g. CO MT OK). Required.',
    )
    parser.add_argument(
        '--quarter', metavar='Qn_YYYY',
        help='Override the auto-detected quarter (e.g. Q2_2026).',
    )
    parser.add_argument(
        '--workspace', metavar='PATH',
        help='Directory where the final Private_Land GDBs are located. '
             'Defaults to the current working directory.',
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(args: argparse.Namespace) -> None:
    run_start = time.time()

    if args.workspace:
        os.chdir(args.workspace)
        logger.info("Workspace: %s", args.workspace)

    quarter = args.quarter or get_quarter()
    if args.quarter:
        logger.info("Quarter override: %s", quarter)

    # Resolve requested states against the known state mapping
    requested = [s.upper() for s in args.states]
    available = set(state_full.keys())
    unknown = set(requested) - available
    if unknown:
        logger.warning(
            "Unknown state abbreviation(s) (skipping): %s. Available: %s",
            sorted(unknown), sorted(available),
        )
    valid = [a for a in requested if a in available]
    if not valid:
        logger.error("No valid states to process. Exiting.")
        sys.exit(1)

    # Initialise report
    report = RedactionReport(
        quarter=quarter,
        started_at=time.strftime('%Y-%m-%dT%H:%M:%S'),
        states_requested=valid,
    )
    results: dict[str, RedactionResult] = {
        abbr: report.add_state(abbr, state_full[abbr])
        for abbr in valid
    }

    logger.info("====== PLR redaction started ======")
    logger.info("States: %s | Quarter: %s", valid, quarter)

    # Process each state sequentially so outputs can be verified between runs
    for abbr in valid:
        logger.info("--- Redacting: %s (%s) ---", abbr, state_full[abbr])
        _redact_state(abbr, state_full[abbr], results[abbr], quarter)

    # Finalise and write report
    total_elapsed = time.time() - run_start
    report.finalize(total_elapsed)
    report_path = report.write()

    for line in report.summary_lines():
        logger.info(line)

    logger.info("Redaction report written to: %s", report_path)
    logger.info(
        "====== PLR redaction complete — %.1f seconds ======", total_elapsed
    )


if __name__ == '__main__':
    _parser = _build_parser()
    _args = _parser.parse_args()
    main(_args)
