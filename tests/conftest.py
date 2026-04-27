"""
Shared pytest configuration and fixtures for the PLR test suite.

The most important job of this file is installing an arcpy mock into
``sys.modules`` BEFORE any project module is imported.  Because conftest.py
is executed during pytest's collection phase (before test modules are
imported), patching here guarantees that every ``import arcpy`` in the
project resolves to our mock — no ArcGIS Pro installation required.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# 1.  Environment variables required by configs/settings.py
# ---------------------------------------------------------------------------
_FIXTURES = Path(__file__).parent / 'fixtures'

os.environ.setdefault('DB_FILE', 'test_sde_connection.sde')
os.environ.setdefault('PARCEL_MANIFEST_PATH', str(_FIXTURES / 'manifest.json'))
# Clear any leftover quarter override from a previous test run
os.environ.pop('PLR_QUARTER', None)

# ---------------------------------------------------------------------------
# 2.  arcpy stub
#     arcpy.ExecuteError must be a *real* exception class so that
#     ``except arcpy.ExecuteError:`` blocks work correctly.
# ---------------------------------------------------------------------------
class _ExecuteError(Exception):
    """Stand-in for arcpy.ExecuteError."""


def _make_arcpy_mock() -> MagicMock:
    mock = MagicMock(name='arcpy')
    mock.ExecuteError = _ExecuteError

    # Exists() returns False by default; tests override as needed
    mock.Exists.return_value = False

    # GetCount_management returns a result object whose [0] is a count string
    mock.GetCount_management.return_value = ['0']

    # GetMessages returns empty string
    mock.GetMessages.return_value = ''

    # da sub-module: cursors behave as context managers returning empty iterables
    da = MagicMock(name='arcpy.da')
    da.SearchCursor.return_value.__enter__ = lambda s: iter([])
    da.SearchCursor.return_value.__exit__ = MagicMock(return_value=False)
    da.UpdateCursor.return_value.__enter__ = lambda s: iter([])
    da.UpdateCursor.return_value.__exit__ = MagicMock(return_value=False)
    da.InsertCursor.return_value.__enter__ = lambda s: MagicMock()
    da.InsertCursor.return_value.__exit__ = MagicMock(return_value=False)
    mock.da = da

    mock.env = MagicMock()
    return mock


_arcpy = _make_arcpy_mock()

sys.modules.setdefault('arcpy',            _arcpy)
sys.modules.setdefault('arcpy.da',         _arcpy.da)
sys.modules.setdefault('arcpy.management', _arcpy.management)
sys.modules.setdefault('arcpy.analysis',   _arcpy.analysis)
sys.modules.setdefault('arcpy.gapro',      _arcpy.gapro)

# Other heavy dependencies that won't be available in CI
_xgb = MagicMock(name='xgboost')
sys.modules.setdefault('xgboost', _xgb)

_arcgis = MagicMock(name='arcgis')
sys.modules.setdefault('arcgis',          _arcgis)
sys.modules.setdefault('arcgis.features', _arcgis.features)

# ---------------------------------------------------------------------------
# 3.  Shared pytest fixtures
# ---------------------------------------------------------------------------
import pytest   # noqa: E402  (import after sys.modules patching)


@pytest.fixture()
def arcpy_mock() -> MagicMock:
    """Expose the module-level arcpy mock so tests can configure return values."""
    return _arcpy


@pytest.fixture()
def sample_state_config() -> dict:
    """Minimal state config dict matching the shape BaseModel expects."""
    return {
        'govt_land': r'D:\test\ohio_temp.gdb\ohio_govt_land',
        'parcels':   r'D:\test\ohio_parcels.gdb\ohio_parcels',
    }


@pytest.fixture()
def fixtures_dir() -> Path:
    return _FIXTURES
