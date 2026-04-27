"""
PLR (Private Land Refresh) pipeline entry point.

Usage examples
--------------
# Run all enabled states (as configured in configs/config.py):
    python main.py

# Run a specific subset of states:
    python main.py --states OH PA IN

# Validate all inputs without running any processing:
    python main.py --dry-run

# Reprocess a previous quarter in a custom workspace:
    python main.py --states OH --quarter Q1_2026 --workspace D:\\output\\q1

# Combine flags:
    python main.py --states OH CA --dry-run --quarter Q2_2026
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import arcpy

from configs import dev, state_full
from configs.settings import PARALLEL_PROCESSING_FACTOR
from geoprocessing.PLR_xgboost_model_predictions import PLR_xgboost_model
from geoprocessing.GIS_model_PLR import PLR_GIS_model
from geoprocessing.PLR_QC_steps import PLR_QC_model
from geoprocessing.PLR_post_process import PLR_post_process
from utils.geo_utils import get_quarter
from utils.logging_config import get_logger
from utils.run_report import RunReport, StateResult
from utils.validators import PipelineValidationError, validate_all_states

logger = get_logger(__name__)

arcpy.env.parallelProcessingFactor = PARALLEL_PROCESSING_FACTOR


# ---------------------------------------------------------------------------
# Pipeline stage functions
# ---------------------------------------------------------------------------

def _run_xgboost(config: dict, results: dict) -> None:
    for abbr, data in config['states'].items():
        state = state_full[abbr]
        result: StateResult = results[abbr]
        stage_start = time.time()
        logger.info("--- XGBoost: starting %s ---", state)
        try:
            model = PLR_xgboost_model(data, state)
            model.set_workspaces()
            model.repair_geometry()
            model.add_centroid_attr()
            model.add_xgb_field()
            model.label_owner_type()
            model.export_state()
            predictions = model.make_new_predictions()
            model.label_predctions(predictions)
            logger.info("--- XGBoost: %s complete ---", state)
        except FileNotFoundError as exc:
            logger.error("XGBoost %s: missing file — %s", state, exc)
            result.mark_stage_failed('xgboost', str(exc))
        except arcpy.ExecuteError:
            msg = arcpy.GetMessages(2)
            logger.error("XGBoost %s: ArcPy error — %s", state, msg)
            result.mark_stage_failed('xgboost', msg)
        except Exception as exc:
            logger.exception("XGBoost %s: unexpected error", state)
            result.mark_stage_failed('xgboost', str(exc))
        finally:
            result.elapsed_seconds += round(time.time() - stage_start, 2)


def _run_gis_model(config: dict, results: dict) -> None:
    for abbr, data in config['states'].items():
        state = state_full[abbr]
        result: StateResult = results[abbr]
        stage_start = time.time()
        logger.info("--- GIS model: starting %s ---", state)
        try:
            model = PLR_GIS_model(data, state)
            model.set_workspaces()
            model.label_private_public()
            logger.info("--- GIS model: %s complete ---", state)
        except arcpy.ExecuteError:
            msg = arcpy.GetMessages(2)
            logger.error("GIS model %s: ArcPy error — %s", state, msg)
            result.mark_stage_failed('gis_model', msg)
        except Exception as exc:
            logger.exception("GIS model %s: unexpected error", state)
            result.mark_stage_failed('gis_model', str(exc))
        finally:
            result.elapsed_seconds += round(time.time() - stage_start, 2)


def _run_qc(config: dict, results: dict) -> None:
    for abbr, data in config['states'].items():
        state = state_full[abbr]
        result: StateResult = results[abbr]
        stage_start = time.time()
        logger.info("--- QC: starting %s ---", state)
        try:
            model = PLR_QC_model(data, state)
            model.set_workspaces()
            model.qc_counts()
            model.label_qc()
            model.gap_qc()
            model.overlap_qc()
            model.qc_post_process()

            # Harvest QC stats into the run report
            result.parcel_count = model.parcel_count
            result.agreement_count = model.agreement_count
            result.agreement_pct = round(model.agreement_pct, 2)

            logger.info("--- QC: %s complete ---", state)
        except arcpy.ExecuteError:
            msg = arcpy.GetMessages(2)
            logger.error("QC %s: ArcPy error — %s", state, msg)
            result.mark_stage_failed('qc', msg)
        except Exception as exc:
            logger.exception("QC %s: unexpected error", state)
            result.mark_stage_failed('qc', str(exc))
        finally:
            result.elapsed_seconds += round(time.time() - stage_start, 2)


def _run_post_process(config: dict, results: dict) -> None:
    for abbr in config['states']:
        state = state_full[abbr]
        result: StateResult = results[abbr]
        stage_start = time.time()
        logger.info("--- Post-process: starting %s ---", state)
        try:
            pp = PLR_post_process(state)
            pp.create_dissolve_fc()
            pp.post_process_govt_land()
            pp.private_land_dissolve()
            pp.append_private_no_owner_parcels()
            pp.multipart_to_singlepart()
            result.mark_success()
            logger.info("--- Post-process: %s complete ---", state)
        except arcpy.ExecuteError:
            msg = arcpy.GetMessages(2)
            logger.error("Post-process %s: ArcPy error — %s", state, msg)
            result.mark_stage_failed('post_process', msg)
        except Exception as exc:
            logger.exception("Post-process %s: unexpected error", state)
            result.mark_stage_failed('post_process', str(exc))
        finally:
            result.elapsed_seconds += round(time.time() - stage_start, 2)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _filter_states(config: dict, requested: list[str]) -> dict:
    """Return a copy of config with only the requested state abbreviations."""
    available = set(config['states'].keys())
    unknown = set(requested) - available
    if unknown:
        logger.warning(
            "Requested states not found in config (skipping): %s. Available: %s",
            sorted(unknown), sorted(available),
        )
    filtered = {k: v for k, v in config['states'].items() if k in requested}
    if not filtered:
        raise SystemExit(
            f"No valid states to process. Requested: {sorted(requested)}, "
            f"available in config: {sorted(available)}"
        )
    return {**config, 'states': filtered}


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='plr_pipeline',
        description='Private Land Refresh — geospatial parcel classification pipeline.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--states', nargs='+', metavar='ABBR',
        help='State abbreviations to process (e.g. OH PA IN).',
    )
    parser.add_argument(
        '--quarter', metavar='Qn_YYYY',
        help='Override the auto-detected quarter (e.g. Q1_2026).',
    )
    parser.add_argument(
        '--workspace', metavar='PATH',
        help='Override the working directory where GDBs are written.',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Validate inputs and exit without processing.',
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(config: dict, args: argparse.Namespace | None = None) -> None:
    pipeline_start = time.time()

    # --- Apply CLI overrides before anything else ---
    if args is not None and args.quarter:
        os.environ['PLR_QUARTER'] = args.quarter
        logger.info("Quarter override: %s", args.quarter)

    if args is not None and args.workspace:
        os.chdir(args.workspace)
        logger.info("Workspace override: %s", args.workspace)

    if args is not None and args.states:
        config = _filter_states(config, [s.upper() for s in args.states])

    dry_run = args is not None and args.dry_run
    quarter = get_quarter()

    # --- Initialise run report ---
    report = RunReport(
        quarter=quarter,
        started_at=time.strftime('%Y-%m-%dT%H:%M:%S'),
        states_requested=list(config['states'].keys()),
    )
    results: dict[str, StateResult] = {
        abbr: report.add_state(abbr, state_full[abbr])
        for abbr in config['states']
    }

    logger.info("====== PLR pipeline started ======")
    logger.info(
        "States: %s | Quarter: %s | Dry-run: %s",
        list(config['states'].keys()), quarter, dry_run,
    )

    # --- Pre-flight validation ---
    logger.info("Running pre-flight validation…")
    try:
        validate_all_states(config['states'], state_full, logger, raise_on_error=True)
        logger.info("Pre-flight validation passed.")
    except PipelineValidationError as exc:
        logger.error("Pre-flight validation FAILED:\n%s", exc)
        sys.exit(1)

    if dry_run:
        logger.info("Dry-run complete — no data was processed.")
        return

    # --- Pipeline stages ---
    logger.info("Stage 1/4: XGBoost model predictions")
    _run_xgboost(config, results)

    logger.info("Stage 2/4: GIS model predictions")
    _run_gis_model(config, results)

    logger.info("Stage 3/4: QC process")
    _run_qc(config, results)

    logger.info("Stage 4/4: Post-processing")
    _run_post_process(config, results)

    # --- Finalise and write run report ---
    total_elapsed = time.time() - pipeline_start
    report.finalize(total_elapsed)
    report_path = report.write()

    for line in report.summary_lines():
        logger.info(line)

    logger.info("Run report written to: %s", report_path)
    logger.info("====== PLR pipeline complete — %.1f seconds ======", total_elapsed)


if __name__ == '__main__':
    _parser = _build_parser()
    _args = _parser.parse_args()
    main(dev, _args)
