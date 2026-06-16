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

# Run only the QC and post-process stages (XGBoost + GIS already done):
    python main.py --states CO --stages qc post_process

# Re-run just post-process after fixing an output issue:
    python main.py --states CO --stages post_process

# Process 3 states in parallel (requires ArcGIS Pro Advanced license):
    python main.py --states CO MT OK SD --max-workers 3

# Combine flags:
    python main.py --states OH CA --dry-run --quarter Q2_2026
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import fields as dataclass_fields

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
            model.repair_geometry()
            model.qc_counts()
            model.label_qc()
            model.classification_counts()
            model.gap_qc()
            model.overlap_qc()
            model.qc_post_process()

            # Harvest QC stats into the run report
            result.parcel_count = model.parcel_count
            result.agreement_count = model.agreement_count
            result.agreement_pct = round(model.agreement_pct, 2)
            result.true_count = model.true_count
            result.false_count = model.false_count
            result.unknown_count = model.unknown_count
            result.qc_flag_counts = model.qc_flag_counts

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
# Parallel worker
# ---------------------------------------------------------------------------

def _process_state(args: tuple) -> StateResult:
    """
    Worker process entry point — runs all active pipeline stages for one state.

    Must be a module-level function (not a lambda or nested function) so that
    Python's multiprocessing 'spawn' start method (used on Windows) can
    pickle and transmit it to the child process.

    Each worker process re-imports this module, which re-executes
    ``import arcpy`` and ``arcpy.env.parallelProcessingFactor = ...`` in
    isolation, giving every state its own fully independent arcpy session.
    """
    abbr, data, active_stages_list = args
    active_stages = frozenset(active_stages_list)

    # ------------------------------------------------------------------ #
    # Worker logging — replace handlers inherited from module-level setup #
    # to avoid RotatingFileHandler write-lock conflicts on Windows.       #
    # Workers log to a per-state file; the main process log captures the  #
    # high-level start/complete/error events.                             #
    # ------------------------------------------------------------------ #
    from configs.settings import LOG_DIR
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    _fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    _log_file = LOG_DIR / f'plr_{abbr.lower()}_{time.strftime("%Y%m%d_%H%M%S")}.log'
    _fh = logging.FileHandler(str(_log_file), encoding='utf-8')
    _fh.setFormatter(_fmt)
    _sh = logging.StreamHandler()
    _sh.setFormatter(logging.Formatter(
        f'[{abbr}] %(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
    ))

    # Clear every handler that was set up during module import so we don't
    # also write to the main process's rotating log file.
    for _lgr in list(logging.Logger.manager.loggerDict.values()):
        if isinstance(_lgr, logging.Logger):
            _lgr.handlers.clear()
            _lgr.propagate = True
    logging.root.handlers = [_fh, _sh]
    logging.root.setLevel(logging.INFO)

    # ------------------------------------------------------------------ #
    # Run stages                                                          #
    # ------------------------------------------------------------------ #
    result = StateResult(abbr=abbr, state=state_full[abbr])
    single_config = {'states': {abbr: data}}
    results_dict: dict[str, StateResult] = {abbr: result}

    if 'xgboost' in active_stages:
        _run_xgboost(single_config, results_dict)
    if 'gis' in active_stages:
        _run_gis_model(single_config, results_dict)
    if 'qc' in active_stages:
        _run_qc(single_config, results_dict)
    if 'post_process' in active_stages:
        _run_post_process(single_config, results_dict)

    return result


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
    parser.add_argument(
        '--stages', nargs='+',
        choices=['xgboost', 'gis', 'qc', 'post_process'],
        metavar='STAGE',
        help=(
            'Run only the specified stage(s). '
            'Choices: xgboost gis qc post_process. '
            'Useful for re-running later stages when earlier GDBs already exist. '
            'Example: --stages qc post_process'
        ),
    )
    parser.add_argument(
        '--max-workers', type=int, default=1, metavar='N',
        help=(
            'Number of states to process in parallel (default: 1 = sequential). '
            '2-4 is recommended. Each worker is a separate process with its own '
            'arcpy session. Requires ArcGIS Pro Advanced license and sufficient '
            'RAM (~8-12 GB per concurrent state). Each state writes its own log '
            'file under the logs directory when running in parallel.'
        ),
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
    max_workers: int = args.max_workers if args is not None and hasattr(args, 'max_workers') else 1

    _ALL_STAGES = ('xgboost', 'gis', 'qc', 'post_process')
    active_stages: frozenset[str] = (
        frozenset(args.stages) if args is not None and args.stages
        else frozenset(_ALL_STAGES)
    )

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
        "States: %s | Quarter: %s | Dry-run: %s | Stages: %s | Workers: %d",
        list(config['states'].keys()), quarter, dry_run,
        sorted(active_stages), max_workers,
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
    if max_workers > 1:
        # ------------------------------------------------------------------ #
        # Parallel mode: each state runs all its stages in a worker process. #
        # Results are merged back into the run report as workers complete.   #
        # ------------------------------------------------------------------ #
        logger.info(
            "Parallel mode: dispatching %d state(s) across %d worker(s)",
            len(config['states']), max_workers,
        )
        state_args = [
            (abbr, data, list(active_stages))
            for abbr, data in config['states'].items()
        ]
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_state, a): a[0]
                for a in state_args
            }
            for future in as_completed(futures):
                abbr = futures[future]
                try:
                    returned: StateResult = future.result()
                    # Merge the worker's StateResult back into the report object
                    # (report holds a reference to results[abbr], so we update
                    # it in place to keep the report consistent).
                    original = results[abbr]
                    for f in dataclass_fields(StateResult):
                        if f.name not in ('abbr', 'state'):
                            setattr(original, f.name, getattr(returned, f.name))
                    logger.info(
                        "Worker finished: %s — status=%s elapsed=%.1fs",
                        abbr, returned.status, returned.elapsed_seconds,
                    )
                except Exception as exc:
                    logger.exception("Worker for %s raised an unhandled exception", abbr)
                    results[abbr].mark_stage_failed('pipeline', str(exc))
    else:
        # ------------------------------------------------------------------ #
        # Sequential mode (default): stages run one at a time across states. #
        # ------------------------------------------------------------------ #
        if 'xgboost' in active_stages:
            logger.info("Stage xgboost: XGBoost model predictions")
            _run_xgboost(config, results)
        else:
            logger.info("Stage xgboost: skipped (not in --stages)")

        if 'gis' in active_stages:
            logger.info("Stage gis: GIS model predictions")
            _run_gis_model(config, results)
        else:
            logger.info("Stage gis: skipped (not in --stages)")

        if 'qc' in active_stages:
            logger.info("Stage qc: QC process")
            _run_qc(config, results)
        else:
            logger.info("Stage qc: skipped (not in --stages)")

        if 'post_process' in active_stages:
            logger.info("Stage post_process: Post-processing")
            _run_post_process(config, results)
        else:
            logger.info("Stage post_process: skipped (not in --stages)")

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
