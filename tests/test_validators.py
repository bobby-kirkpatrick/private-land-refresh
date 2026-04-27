"""
Unit tests for utils/validators.py

arcpy is fully mocked via conftest.py.  File-system checks use tmp_path
to exercise real Path operations without needing actual geodatabases.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from utils.validators import (
    PipelineValidationError,
    _check_fc_exists,
    _check_fields,
    _check_file,
    _check_spatial_ref,
    validate_state_inputs,
    validate_all_states,
    REQUIRED_PARCEL_FIELDS,
    REQUIRED_GOVT_FIELDS,
)


# ---------------------------------------------------------------------------
# _check_fc_exists
# ---------------------------------------------------------------------------

class TestCheckFcExists:
    def test_no_error_when_exists(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        errors = _check_fc_exists('some/path', 'test FC', MagicMock())
        assert errors == []

    def test_error_when_missing(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        errors = _check_fc_exists('bad/path', 'test FC', MagicMock())
        assert len(errors) == 1
        assert 'MISSING FC' in errors[0]
        assert 'test FC' in errors[0]

    def test_error_message_includes_path(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        errors = _check_fc_exists('specific/path/fc', 'label', MagicMock())
        assert 'specific/path/fc' in errors[0]


# ---------------------------------------------------------------------------
# _check_fields
# ---------------------------------------------------------------------------

class TestCheckFields:
    def _make_field(self, name: str) -> MagicMock:
        f = MagicMock()
        f.name = name
        return f

    def test_no_error_when_all_fields_present(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        arcpy_mock.ListFields.return_value = [
            self._make_field('OBJECTID'),
            self._make_field('PARCEL_ID'),
            self._make_field('gh_parcel_acres'),
        ]
        errors = _check_fields('fc', ['OBJECTID', 'PARCEL_ID'], 'test', MagicMock())
        assert errors == []

    def test_error_when_field_missing(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        arcpy_mock.ListFields.return_value = [self._make_field('OBJECTID')]
        errors = _check_fields('fc', ['OBJECTID', 'MISSING_FIELD'], 'label', MagicMock())
        assert len(errors) == 1
        assert 'MISSING_FIELD' in errors[0]

    def test_skips_check_when_fc_missing(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        errors = _check_fields('bad/fc', ['OBJECTID'], 'label', MagicMock())
        assert errors == []  # existence check handles this case

    def test_multiple_missing_fields_in_one_error(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        arcpy_mock.ListFields.return_value = []  # no fields at all
        errors = _check_fields('fc', ['A', 'B', 'C'], 'label', MagicMock())
        assert len(errors) == 1
        assert 'A' in errors[0]
        assert 'B' in errors[0]
        assert 'C' in errors[0]


# ---------------------------------------------------------------------------
# _check_file
# ---------------------------------------------------------------------------

class TestCheckFile:
    def test_no_error_for_existing_file(self, tmp_path):
        real_file = tmp_path / 'model.json'
        real_file.write_text('{}')
        errors = _check_file(real_file, 'model', MagicMock())
        assert errors == []

    def test_error_for_missing_file(self, tmp_path):
        missing = tmp_path / 'does_not_exist.json'
        errors = _check_file(missing, 'XGB model', MagicMock())
        assert len(errors) == 1
        assert 'MISSING FILE' in errors[0]
        assert 'XGB model' in errors[0]


# ---------------------------------------------------------------------------
# _check_spatial_ref (warns only, not an error)
# ---------------------------------------------------------------------------

class TestCheckSpatialRef:
    def test_no_warning_when_same_wkid(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True
        sr = MagicMock()
        sr.factoryCode = 4326
        sr.name = 'GCS_WGS_1984'
        arcpy_mock.Describe.return_value.spatialReference = sr

        warnings = _check_spatial_ref('parcels', 'govt', 'ohio', MagicMock())
        assert warnings == []

    def test_warning_when_different_wkid(self, arcpy_mock):
        arcpy_mock.Exists.return_value = True

        sr_parcel = MagicMock()
        sr_parcel.factoryCode = 4326
        sr_parcel.name = 'GCS_WGS_1984'

        sr_govt = MagicMock()
        sr_govt.factoryCode = 102100
        sr_govt.name = 'WGS_1984_Web_Mercator'

        arcpy_mock.Describe.side_effect = [
            MagicMock(spatialReference=sr_parcel),
            MagicMock(spatialReference=sr_govt),
        ]

        logger = MagicMock()
        warnings = _check_spatial_ref('parcels', 'govt', 'ohio', logger)
        assert len(warnings) == 1
        assert 'SR MISMATCH' in warnings[0]
        logger.warning.assert_called_once()

    def test_skips_when_fc_missing(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        warnings = _check_spatial_ref('bad', 'bad', 'ohio', MagicMock())
        assert warnings == []


# ---------------------------------------------------------------------------
# validate_state_inputs
# ---------------------------------------------------------------------------

class TestValidateStateInputs:
    def _setup_happy_path(self, arcpy_mock, tmp_path):
        """Configure mocks so every check passes."""
        arcpy_mock.Exists.return_value = True

        # All required parcel fields present
        def _make_field(name):
            f = MagicMock()
            f.name = name
            return f

        all_fields = [_make_field(n) for n in REQUIRED_PARCEL_FIELDS + REQUIRED_GOVT_FIELDS]
        arcpy_mock.ListFields.return_value = all_fields

        # Matching spatial references
        sr = MagicMock()
        sr.factoryCode = 4326
        sr.name = 'GCS_WGS_1984'
        arcpy_mock.Describe.return_value.spatialReference = sr

        # Create real model + CSV files on disk
        xgb_dir = tmp_path / 'state_xgb_models'
        csv_dir = tmp_path / 'state_govt_land_name_tables'
        xgb_dir.mkdir()
        csv_dir.mkdir()
        (xgb_dir / 'ohio_xgb_model.json').write_text('{}')
        (csv_dir / 'ohio_govt_names.csv').write_text('full_name\ntest\n')

        return xgb_dir, csv_dir

    def test_passes_with_all_valid_inputs(self, arcpy_mock, tmp_path):
        xgb_dir, csv_dir = self._setup_happy_path(arcpy_mock, tmp_path)

        with (
            patch('utils.validators.XGB_MODELS_DIR', xgb_dir),
            patch('utils.validators.GOVT_NAME_TABLES_DIR', csv_dir),
        ):
            errors = validate_state_inputs('ohio', 'govt_fc', 'parcel_fc', MagicMock())

        assert errors == []

    def test_returns_errors_for_missing_fcs(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        xgb_dir = tmp_path / 'state_xgb_models'
        csv_dir = tmp_path / 'state_govt_land_name_tables'

        with (
            patch('utils.validators.XGB_MODELS_DIR', xgb_dir),
            patch('utils.validators.GOVT_NAME_TABLES_DIR', csv_dir),
        ):
            errors = validate_state_inputs('ohio', 'bad_govt', 'bad_parcel', MagicMock())

        fc_errors = [e for e in errors if 'MISSING FC' in e]
        assert len(fc_errors) == 2  # parcels + govt land


# ---------------------------------------------------------------------------
# validate_all_states
# ---------------------------------------------------------------------------

class TestValidateAllStates:
    def test_raises_pipeline_validation_error_on_failure(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False  # everything is missing
        states_config = {'OH': {'govt_land': 'bad', 'parcels': 'bad'}}
        state_full = {'OH': 'ohio'}

        with pytest.raises(PipelineValidationError) as exc_info:
            validate_all_states(states_config, state_full, MagicMock(), raise_on_error=True)

        assert 'ohio' in str(exc_info.value).lower() or 'OH' in str(exc_info.value)

    def test_returns_results_without_raising_when_flag_off(self, arcpy_mock):
        arcpy_mock.Exists.return_value = False
        states_config = {'OH': {'govt_land': 'bad', 'parcels': 'bad'}}
        state_full = {'OH': 'ohio'}

        results = validate_all_states(
            states_config, state_full, MagicMock(), raise_on_error=False
        )
        assert 'OH' in results
        assert len(results['OH']) > 0

    def test_empty_list_for_passing_state(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True

        def _field(name):
            f = MagicMock()
            f.name = name
            return f

        all_fields = [_field(n) for n in REQUIRED_PARCEL_FIELDS + REQUIRED_GOVT_FIELDS]
        arcpy_mock.ListFields.return_value = all_fields
        sr = MagicMock()
        sr.factoryCode = 4326
        sr.name = 'test'
        arcpy_mock.Describe.return_value.spatialReference = sr

        xgb_dir = tmp_path / 'state_xgb_models'
        csv_dir = tmp_path / 'state_govt_land_name_tables'
        xgb_dir.mkdir()
        csv_dir.mkdir()
        (xgb_dir / 'ohio_xgb_model.json').write_text('{}')
        (csv_dir / 'ohio_govt_names.csv').write_text('full_name\n')

        states_config = {'OH': {'govt_land': 'govt_fc', 'parcels': 'parcel_fc'}}
        state_full = {'OH': 'ohio'}

        with (
            patch('utils.validators.XGB_MODELS_DIR', xgb_dir),
            patch('utils.validators.GOVT_NAME_TABLES_DIR', csv_dir),
        ):
            results = validate_all_states(
                states_config, state_full, MagicMock(), raise_on_error=False
            )

        assert results['OH'] == []

    def test_error_message_lists_all_problems(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        states_config = {
            'OH': {'govt_land': 'bad', 'parcels': 'bad'},
            'CA': {'govt_land': 'bad', 'parcels': 'bad'},
        }
        state_full = {'OH': 'ohio', 'CA': 'california'}

        with pytest.raises(PipelineValidationError) as exc_info:
            validate_all_states(
                states_config, state_full, MagicMock(), raise_on_error=True
            )

        msg = str(exc_info.value)
        assert 'OH' in msg
        assert 'CA' in msg
