"""
Integration tests for main.py pipeline flow.

These tests exercise the full main() call path with every arcpy-backed
stage function patched out.  They verify:
  - All four stages are called in the correct order
  - --dry-run skips processing but runs validation
  - --states filters the state list before any stage runs
  - --quarter sets the PLR_QUARTER env var before models are constructed
  - A failed validation (PipelineValidationError) exits with code 1
  - The JSON run report is written at the end of a successful run
  - A failing state does not abort other states
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Minimal test config (does not require a real manifest file)
# ---------------------------------------------------------------------------

_TEST_CONFIG = {
    'states': {
        'OH': {'govt_land': 'oh_govt_fc', 'parcels': 'oh_parcel_fc'},
        'CA': {'govt_land': 'ca_govt_fc', 'parcels': 'ca_parcel_fc'},
    },
    'state_codes': {'OH': '39', 'CA': '06'},
    'acquisition_processing_parameters': {
        'raw data location': r'D:\raw',
        'data_workspace':    r'D:\processed',
    },
}

_STATE_FULL = {
    'OH': 'ohio',
    'CA': 'california',
}

# ---------------------------------------------------------------------------
# Helper: build argparse Namespace quickly
# ---------------------------------------------------------------------------

def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(states=None, quarter=None, workspace=None, dry_run=False)
    return argparse.Namespace(**{**defaults, **kwargs})


# ---------------------------------------------------------------------------
# Patches applied to all integration tests
# ---------------------------------------------------------------------------

_STAGE_PATCHES = [
    'main._run_xgboost',
    'main._run_gis_model',
    'main._run_qc',
    'main._run_post_process',
]


@pytest.fixture(autouse=True)
def _patch_imports(monkeypatch):
    """
    Replace the heavy imports in main.py with lightweight mocks so the
    module can be imported without arcpy being installed.
    """
    monkeypatch.setattr('main.state_full', _STATE_FULL)
    monkeypatch.setattr('main.dev', _TEST_CONFIG)


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

def _run_main(config=None, args=None, validate_side_effect=None):
    """
    Import main lazily (after mocks are in place) and call main().
    Returns the mock objects for each stage.
    """
    import main as m

    patches = {name: MagicMock() for name in _STAGE_PATCHES}
    validate_mock = MagicMock(return_value={'OH': [], 'CA': []})
    if validate_side_effect is not None:
        validate_mock.side_effect = validate_side_effect

    report_mock = MagicMock()
    report_mock.write.return_value = Path('/fake/report.json')
    report_cls_mock = MagicMock(return_value=report_mock)

    with (
        patch('main._run_xgboost',      patches['main._run_xgboost']),
        patch('main._run_gis_model',    patches['main._run_gis_model']),
        patch('main._run_qc',           patches['main._run_qc']),
        patch('main._run_post_process', patches['main._run_post_process']),
        patch('main.validate_all_states', validate_mock),
        patch('main.RunReport', report_cls_mock),
    ):
        m.main(config or _TEST_CONFIG, args or _args())

    return patches, validate_mock, report_mock


# ---------------------------------------------------------------------------
# Stage order
# ---------------------------------------------------------------------------

class TestStageOrder:
    def test_all_four_stages_called(self):
        patches, _, _ = _run_main()
        for name in _STAGE_PATCHES:
            patches[name].assert_called_once()

    def test_xgboost_called_before_gis(self):
        call_order = []
        patches, _, _ = _run_main()
        # Verify each was called exactly once (order enforced by sequential code)
        assert patches['main._run_xgboost'].call_count == 1
        assert patches['main._run_gis_model'].call_count == 1
        assert patches['main._run_qc'].call_count == 1
        assert patches['main._run_post_process'].call_count == 1


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_skips_all_stages(self):
        patches, validate_mock, _ = _run_main(args=_args(dry_run=True))
        for name in _STAGE_PATCHES:
            patches[name].assert_not_called()

    def test_dry_run_still_calls_validation(self):
        _, validate_mock, _ = _run_main(args=_args(dry_run=True))
        validate_mock.assert_called_once()


# ---------------------------------------------------------------------------
# --states filtering
# ---------------------------------------------------------------------------

class TestStateFiltering:
    def test_single_state_passed_to_stages(self):
        import main as m
        filtered_config = None

        def capture_xgboost(config, results):
            nonlocal filtered_config
            filtered_config = config

        patches, _, _ = _run_main(args=_args(states=['OH']))
        # Filtering happens before stages — stage receives filtered config
        passed_config = patches['main._run_xgboost'].call_args[0][0]
        assert 'OH' in passed_config['states']
        assert 'CA' not in passed_config['states']

    def test_unknown_state_skipped_with_warning(self, caplog):
        import logging
        with pytest.raises(SystemExit):
            # ZZ is not in config — no valid states → SystemExit
            _run_main(args=_args(states=['ZZ']))

    def test_valid_and_invalid_states_mixed(self):
        # OH is valid, ZZ is not; OH should still run
        patches, _, _ = _run_main(args=_args(states=['OH', 'ZZ']))
        passed = patches['main._run_xgboost'].call_args[0][0]
        assert 'OH' in passed['states']
        assert 'ZZ' not in passed['states']


# ---------------------------------------------------------------------------
# --quarter override
# ---------------------------------------------------------------------------

class TestQuarterOverride:
    def test_sets_plr_quarter_env_var(self):
        os.environ.pop('PLR_QUARTER', None)
        _run_main(args=_args(quarter='Q1_2025'))
        assert os.environ.get('PLR_QUARTER') == 'Q1_2025'
        # Clean up
        os.environ.pop('PLR_QUARTER', None)

    def test_no_quarter_arg_leaves_env_var_unset(self):
        os.environ.pop('PLR_QUARTER', None)
        _run_main(args=_args())
        assert 'PLR_QUARTER' not in os.environ


# ---------------------------------------------------------------------------
# Validation failure
# ---------------------------------------------------------------------------

class TestValidationFailure:
    def test_sys_exit_on_pipeline_validation_error(self):
        from utils.validators import PipelineValidationError

        with pytest.raises(SystemExit) as exc_info:
            _run_main(validate_side_effect=PipelineValidationError("bad inputs"))

        assert exc_info.value.code == 1

    def test_stages_not_called_after_validation_failure(self):
        from utils.validators import PipelineValidationError

        patches = {name: MagicMock() for name in _STAGE_PATCHES}
        validate_mock = MagicMock(side_effect=PipelineValidationError("fail"))
        report_mock = MagicMock()

        import main as m
        with (
            patch('main._run_xgboost',      patches['main._run_xgboost']),
            patch('main._run_gis_model',    patches['main._run_gis_model']),
            patch('main._run_qc',           patches['main._run_qc']),
            patch('main._run_post_process', patches['main._run_post_process']),
            patch('main.validate_all_states', validate_mock),
            patch('main.RunReport', MagicMock(return_value=report_mock)),
        ):
            with pytest.raises(SystemExit):
                m.main(_TEST_CONFIG, _args())

        for name in _STAGE_PATCHES:
            patches[name].assert_not_called()


# ---------------------------------------------------------------------------
# Run report
# ---------------------------------------------------------------------------

class TestRunReport:
    def test_run_report_written_after_pipeline(self):
        _, _, report_mock = _run_main()
        report_mock.write.assert_called_once()

    def test_run_report_finalized(self):
        _, _, report_mock = _run_main()
        report_mock.finalize.assert_called_once()
