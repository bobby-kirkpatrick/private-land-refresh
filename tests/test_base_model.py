"""
Unit tests for geoprocessing/base_model.py

Exercises BaseModel in isolation with arcpy fully mocked.
Subclass WorkspaceModel is used since BaseModel can't be instantiated
alone (it's designed to be subclassed).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from geoprocessing.base_model import BaseModel


# ---------------------------------------------------------------------------
# Minimal concrete subclass for testing
# ---------------------------------------------------------------------------

class _TestModel(BaseModel):
    """Concrete subclass with no additional behaviour."""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def model(sample_state_config, tmp_path) -> _TestModel:
    """A freshly constructed model with workspace pointing at tmp_path."""
    with patch('geoprocessing.base_model.Path.cwd', return_value=tmp_path):
        return _TestModel(sample_state_config, 'ohio')


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_state_set(self, model):
        assert model.state == 'ohio'

    def test_parcels_set(self, model, sample_state_config):
        assert model.parcels == sample_state_config['parcels']

    def test_govt_land_set(self, model, sample_state_config):
        assert model.govt_land == sample_state_config['govt_land']

    def test_quarter_is_string(self, model):
        assert isinstance(model.quarter, str)
        assert model.quarter.startswith('Q')

    def test_workspace_is_path(self, model, tmp_path):
        assert isinstance(model.workspace, Path)

    def test_logger_named_for_class(self, model):
        assert 'TestModel' in model.logger.name or '_TestModel' in model.logger.name


# ---------------------------------------------------------------------------
# _ensure_gdb
# ---------------------------------------------------------------------------

class TestEnsureGdb:
    def test_creates_gdb_when_not_exists(self, model, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        target = tmp_path / 'new.gdb'
        result = model._ensure_gdb(target, 'Test GDB')
        arcpy_mock.CreateFileGDB_management.assert_called_once_with(
            str(target.parent), target.name
        )
        assert result == target

    def test_skips_creation_when_exists(self, model, arcpy_mock, tmp_path):
        target = tmp_path / 'existing.gdb'
        target.mkdir()  # make it actually exist on disk
        result = model._ensure_gdb(target, 'Test GDB')
        arcpy_mock.CreateFileGDB_management.assert_not_called()
        assert result == target

    def test_returns_path_in_both_cases(self, model, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        target = tmp_path / 'any.gdb'
        assert model._ensure_gdb(target) == target


# ---------------------------------------------------------------------------
# set_workspaces
# ---------------------------------------------------------------------------

class TestSetWorkspaces:
    def test_temp_dir_assigned(self, model, tmp_path):
        # Both GDBs don't exist on disk, so _ensure_gdb will call CreateFileGDB
        model.set_workspaces()
        expected_temp = tmp_path / f'ohio_temp_{model.quarter}.gdb'
        assert model.temp_dir == expected_temp

    def test_final_dir_assigned(self, model, tmp_path):
        model.set_workspaces()
        expected_final = tmp_path / f'ohio_private_land_{model.quarter}.gdb'
        assert model.final_dir == expected_final

    def test_creates_two_gdbs(self, model, arcpy_mock, tmp_path):
        model.set_workspaces()
        assert arcpy_mock.CreateFileGDB_management.call_count == 2

    def test_skips_creation_for_existing_gdb(self, model, arcpy_mock, tmp_path):
        # Create the temp GDB on disk so _ensure_gdb sees it
        (tmp_path / f'ohio_temp_{model.quarter}.gdb').mkdir()
        model.set_workspaces()
        # Only the final GDB should be created
        assert arcpy_mock.CreateFileGDB_management.call_count == 1


# ---------------------------------------------------------------------------
# repair_geometry
# ---------------------------------------------------------------------------

class TestRepairGeometry:
    def test_calls_repair_on_both_fcs(self, model, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        model.repair_geometry()
        calls = [call(model.parcels), call(model.govt_land)]
        arcpy_mock.RepairGeometry_management.assert_has_calls(calls, any_order=True)
        assert arcpy_mock.RepairGeometry_management.call_count == 2

    def test_skips_repair_when_fc_missing(self, model, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        model.repair_geometry()
        arcpy_mock.RepairGeometry_management.assert_not_called()

    def test_repairs_existing_fc_only(self, model, arcpy_mock):
        # parcels exists, govt_land does not
        def _exists(path):
            return path == model.parcels
        arcpy_mock.Exists.side_effect = _exists

        model.repair_geometry()
        arcpy_mock.RepairGeometry_management.assert_called_once_with(model.parcels)


# ---------------------------------------------------------------------------
# run_report integration helpers
# ---------------------------------------------------------------------------

class TestRunReport:
    def test_state_result_tracks_failure(self):
        from utils.run_report import StateResult
        r = StateResult(abbr='OH', state='ohio')
        r.mark_stage_failed('xgboost', 'file not found')
        assert r.status == 'failed'
        assert 'xgboost' in r.failed_stages
        assert len(r.errors) == 1

    def test_state_result_success(self):
        from utils.run_report import StateResult
        r = StateResult(abbr='OH', state='ohio')
        r.mark_success()
        assert r.status == 'success'

    def test_run_report_summary(self):
        from utils.run_report import RunReport, StateResult
        report = RunReport(quarter='Q2_2026', started_at='2026-04-27T10:00:00')
        r = report.add_state('OH', 'ohio')
        r.parcel_count = 500_000
        r.agreement_count = 490_000
        r.agreement_pct = 98.0
        r.mark_success()
        report.finalize(3600.0)

        lines = report.summary_lines()
        assert any('OH' in l for l in lines)
        assert any('98.0' in l for l in lines)
        assert report.success_count == 1
        assert report.failed_count == 0

    def test_run_report_writes_json(self, tmp_path):
        from utils.run_report import RunReport
        import json
        report = RunReport(quarter='Q2_2026', started_at='2026-04-27T10:00:00')
        report.add_state('OH', 'ohio').mark_success()
        report.finalize(10.0)

        path = report.write(tmp_path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert data['quarter'] == 'Q2_2026'
        assert data['state_results'][0]['abbr'] == 'OH'
