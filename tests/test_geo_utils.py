"""
Unit tests for utils/geo_utils.py

Tests cover:
  - get_quarter() for every month of the year
  - get_quarter() with PLR_QUARTER env-var override
  - Idempotency helpers (dissolve, centroids, intersect) skip arcpy when FC exists
  - Idempotency helpers call arcpy when FC does not exist
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

# conftest.py has already mocked arcpy into sys.modules
from utils.geo_utils import (
    get_quarter,
    dissolve_govt_land,
    create_centroids,
    intersect_features,
    build_centroid_govt_intersect,
)


# ---------------------------------------------------------------------------
# get_quarter
# ---------------------------------------------------------------------------

class TestGetQuarter:
    """Quarter string is derived from current month or from PLR_QUARTER override."""

    @pytest.mark.parametrize("month,expected", [
        ('01', 'Q1_2026'), ('02', 'Q1_2026'), ('03', 'Q1_2026'),
        ('04', 'Q2_2026'), ('05', 'Q2_2026'), ('06', 'Q2_2026'),
        ('07', 'Q3_2026'), ('08', 'Q3_2026'), ('09', 'Q3_2026'),
        ('10', 'Q4_2026'), ('11', 'Q4_2026'), ('12', 'Q4_2026'),
    ])
    def test_quarter_from_month(self, month: str, expected: str):
        with patch('utils.geo_utils.time') as mock_time:
            mock_time.strftime.side_effect = lambda fmt: month if '%m' in fmt else '2026'
            os.environ.pop('PLR_QUARTER', None)
            assert get_quarter() == expected

    def test_env_var_override(self):
        os.environ['PLR_QUARTER'] = 'Q1_2025'
        try:
            assert get_quarter() == 'Q1_2025'
        finally:
            del os.environ['PLR_QUARTER']

    def test_env_var_override_whitespace_stripped(self):
        os.environ['PLR_QUARTER'] = '  Q3_2024  '
        try:
            assert get_quarter() == 'Q3_2024'
        finally:
            del os.environ['PLR_QUARTER']

    def test_empty_env_var_falls_through_to_clock(self):
        os.environ['PLR_QUARTER'] = ''
        try:
            with patch('utils.geo_utils.time') as mock_time:
                mock_time.strftime.side_effect = lambda fmt: '06' if '%m' in fmt else '2026'
                os.environ.pop('PLR_QUARTER', None)
                result = get_quarter()
            assert result.startswith('Q')
        finally:
            os.environ.pop('PLR_QUARTER', None)


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------

class TestDissolveGovtLand:
    def test_skips_dissolve_when_fc_exists(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True
        output = tmp_path / 'dissolved.gdb' / 'layer'
        logger = MagicMock()

        dissolve_govt_land('source_fc', output, logger)

        arcpy_mock.Dissolve_management.assert_not_called()

    def test_calls_dissolve_when_fc_missing(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        output = tmp_path / 'layer'
        logger = MagicMock()

        dissolve_govt_land('source_fc', output, logger)

        arcpy_mock.Dissolve_management.assert_called_once_with('source_fc', str(output))

    def test_returns_output_path(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True
        output = tmp_path / 'layer'
        result = dissolve_govt_land('source_fc', output, MagicMock())
        assert result == output


class TestCreateCentroids:
    def test_skips_when_exists(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True
        output = tmp_path / 'centroids'
        create_centroids('parcels_fc', output, MagicMock())
        arcpy_mock.FeatureToPoint_management.assert_not_called()

    def test_calls_feature_to_point_when_missing(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        output = tmp_path / 'centroids'
        create_centroids('parcels_fc', output, MagicMock())
        arcpy_mock.FeatureToPoint_management.assert_called_once_with(
            'parcels_fc', str(output), 'INSIDE'
        )


class TestIntersectFeatures:
    def test_skips_when_exists(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True
        output = tmp_path / 'intx'
        intersect_features(['a', 'b'], output, MagicMock())
        arcpy_mock.Intersect_analysis.assert_not_called()

    def test_calls_intersect_when_missing(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = False
        output = tmp_path / 'intx'
        intersect_features(['a', 'b'], output, MagicMock())
        arcpy_mock.Intersect_analysis.assert_called_once_with(['a', 'b'], str(output))


class TestBuildCentroidGovtIntersect:
    def test_returns_two_paths(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True  # all already exist
        dissolved, intersect = build_centroid_govt_intersect(
            'govt_fc', 'parcels_fc', tmp_path, 'ohio', MagicMock()
        )
        assert dissolved == tmp_path / 'ohio_dissolved_govt_features'
        assert intersect == tmp_path / 'ohio_centroid_govt_intx'

    def test_correct_fc_names(self, arcpy_mock, tmp_path):
        arcpy_mock.Exists.return_value = True
        dissolved, intersect = build_centroid_govt_intersect(
            'govt', 'parcels', tmp_path, 'california', MagicMock()
        )
        assert 'california_dissolved_govt_features' in str(dissolved)
        assert 'california_centroid_govt_intx' in str(intersect)
