import csv
import inspect
import json
from pathlib import Path
from typing import List, Optional, Tuple

import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from xgboost import XGBClassifier

from configs.settings import GOVT_NAME_TABLES_DIR, XGB_MODELS_DIR, NULL_OWNER_SENTINEL
from geoprocessing.base_model import BaseModel
from utils.geo_utils import build_centroid_govt_intersect

# Fallback decode map used when no .meta.json file exists alongside the model.
# Matches the encoding used in PLR_xgboost_model_training.train_model():
#   state_df['gh_govt'].map({'FALSE': 1, 'TRUE': 2, 'UNKNOWN': 3})
_DEFAULT_INVERSE_LABEL_MAP: dict[int, str] = {1: 'FALSE', 2: 'TRUE', 3: 'UNKNOWN'}

# Boolean fields that existed in XGBoost 1.x but are absent from the current
# XGBClassifier sklearn signature (removed or renamed in 2.x).  Combined with
# the inspect-based set, this covers both wrapper-level and booster-level fields.
_XGB_REMOVED_BOOL_FIELDS: frozenset[str] = frozenset({
    # sklearn wrapper (deprecated/removed)
    'use_label_encoder',
    # Learner / booster training params stored as 0/1 in XGBoost 1.x JSON
    'disable_default_eval_metric',
    'nthread_is_default',
    'seed_per_iteration',
    'boost_from_average',
    # GBTree / hist booster params
    'single_precision_histogram',
    'dense_eval_ordering',
    'has_categorical',
    'allow_non_zero_for_missing',
    'predict_raw_margin',
    'training',
    'use_draft_approx',
    'special_missing',
    'updater_seq',
})

# Key-name patterns that reliably indicate a boolean field in XGBoost model JSON.
# These catch any additional booster-level fields not in the explicit set above.
# NOTE: integers inside *arrays* are never converted (the key-name check only
# applies to scalar dict values), so tree-node arrays like split_type are safe.
_XGB_BOOL_PREFIXES: tuple[str, ...] = (
    'use_', 'disable_', 'allow_', 'enable_', 'has_', 'is_', 'no_', 'with_', 'single_',
)
_XGB_BOOL_SUFFIXES: tuple[str, ...] = (
    '_default', '_encoder', '_is_default', '_categorical',
)

# Fields that are always structural integers in XGBoost model JSON — values of
# 0 or 1 are legitimate integers, not booleans.  The nuclear patch must never
# convert these, otherwise XGBoost raises "Invalid cast, from Boolean to Integer"
# when it tries to read back a tree index or node count.
_XGB_INT_ONLY_FIELDS: frozenset[str] = frozenset({
    'id',                # tree index inside the trees array
    'num_trees',         # total number of boosting rounds
    'num_nodes',         # node count per tree
    'num_roots',         # always 1 for standard trees
    'num_feature',       # number of input features
    'num_deleted',       # deleted-node count (pruning bookkeeping)
    'num_parallel_tree', # random-forest mode multiplier
    'size_leaf_vector',  # 0 for single-output models
    'num_output_group',  # output-group count
    'num_class',         # class count for multiclass models
    'best_iteration',    # early-stopping tracker
    'best_ntree_limit',  # early-stopping limit
    'n_estimators',      # sklearn-wrapper alias for num_trees
})


def _xgb_bool_param_names() -> frozenset[str]:
    """
    Return the union of:
      • XGBClassifier parameter names whose current default is a Python bool
        (covers sklearn wrapper params present in the installed XGBoost version)
      • _XGB_REMOVED_BOOL_FIELDS (booster-level / legacy params not in the
        current sklearn signature but still present in older model JSON files)

    Evaluated once at import time.
    """
    try:
        dynamic = {
            name
            for name, param in inspect.signature(XGBClassifier.__init__).parameters.items()
            if param.default is not inspect.Parameter.empty
            and isinstance(param.default, bool)
        }
    except Exception:
        dynamic = set()
    return frozenset(dynamic | _XGB_REMOVED_BOOL_FIELDS)


_XGB_BOOL_PARAMS: frozenset[str] = _xgb_bool_param_names()


def _is_xgb_bool_field(key: str) -> bool:
    """Return True if *key* names a field that should be boolean in XGBoost 2.x."""
    if key in _XGB_BOOL_PARAMS:
        return True
    k = key.lower()
    return (
        any(k.startswith(p) for p in _XGB_BOOL_PREFIXES)
        or any(k.endswith(s) for s in _XGB_BOOL_SUFFIXES)
    )


def _patch_bool_ints(obj: object, bool_keys: frozenset[str]) -> None:
    """
    Recursively walk a parsed JSON object and convert integer 0/1 values to
    Python booleans for any key that names a boolean XGBoost parameter.

    XGBoost 1.x serialised boolean parameters as JSON integers; XGBoost 2.x
    requires proper JSON booleans when loading the same model file.
    """
    if isinstance(obj, dict):
        for key, val in obj.items():
            if (
                isinstance(val, int) and not isinstance(val, bool)
                and val in (0, 1)
                and _is_xgb_bool_field(key)
            ):
                obj[key] = bool(val)
            else:
                _patch_bool_ints(val, bool_keys)
    elif isinstance(obj, list):
        for item in obj:
            _patch_bool_ints(item, bool_keys)


def _find_int01_keys(obj: object, found: Optional[set] = None) -> set:
    """
    Return the set of key names that still have integer 0/1 scalar values
    anywhere in the JSON tree after the targeted patch has run.

    Groups by key name (not path) and traverses all lists without a size
    limit, so fields buried inside the trees array are not missed.  Used for
    diagnostic logging when the targeted patch is insufficient.
    """
    if found is None:
        found = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, int) and not isinstance(v, bool) and v in (0, 1):
                found.add(k)
            else:
                _find_int01_keys(v, found)
    elif isinstance(obj, list):
        for item in obj:
            _find_int01_keys(item, found)
    return found


def _patch_all_bool_ints(obj: object) -> None:
    """
    Nuclear fallback: convert integer 0/1 scalar dict values to Python bool,
    skipping keys listed in _XGB_INT_ONLY_FIELDS (tree indices, node counts,
    etc.) that are legitimate integers whose values happen to be 0 or 1.

    Integers inside *lists* (e.g. tree-node arrays for split_type, cleft,
    cright, default_left) are intentionally left unchanged so tree structure
    is not corrupted.
    """
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key in _XGB_INT_ONLY_FIELDS:
                continue  # structural integer — never convert
            if isinstance(val, int) and not isinstance(val, bool) and val in (0, 1):
                obj[key] = bool(val)
            else:
                _patch_all_bool_ints(val)
    elif isinstance(obj, list):
        for item in obj:
            _patch_all_bool_ints(item)


def _load_xgb_model(model_path: Path, logger) -> 'XGBClassifier':
    """
    Load an XGBoost model, patching integer-to-boolean type incompatibilities
    that arise when a model trained with XGBoost 1.x is loaded under 2.x.

    If the standard load succeeds the patching is skipped entirely.
    A patched copy is written alongside the original as
    ``<name>_compat.json`` so subsequent loads are fast.  If a stale compat
    file exists but still fails to load (e.g. created by an older version of
    this code with an incomplete field list), it is deleted and regenerated.
    """
    xgb_model = XGBClassifier()
    try:
        xgb_model.load_model(str(model_path))
        return xgb_model
    except Exception as exc:
        if 'Invalid cast' not in str(exc) and 'Boolean' not in str(exc):
            raise  # unrelated error — propagate as-is

    logger.warning(
        "Model %s appears to have been saved with an older XGBoost version "
        "(Integer→Boolean cast error).  Attempting compatibility patch…",
        model_path.name,
    )

    compat_path = model_path.with_name(model_path.stem + '_compat.json')

    # Try reusing an existing compat file; delete it if it still fails to load.
    if compat_path.exists():
        try:
            xgb_model = XGBClassifier()
            xgb_model.load_model(str(compat_path))
            logger.info("Model loaded via existing compatibility patch")
            return xgb_model
        except Exception:
            logger.warning(
                "Existing compat file %s still fails — regenerating with updated patch…",
                compat_path.name,
            )
            compat_path.unlink()

    # Build a fresh compat file using the full set of known boolean params.
    with open(model_path, 'r', encoding='utf-8') as fh:
        data = json.load(fh)

    _patch_bool_ints(data, _XGB_BOOL_PARAMS)

    with open(compat_path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh)
    logger.info(
        "Patched model written to %s (bool fields checked: %s)",
        compat_path.name, sorted(_XGB_BOOL_PARAMS),
    )

    try:
        xgb_model = XGBClassifier()
        xgb_model.load_model(str(compat_path))
        logger.info("Model loaded successfully via compatibility patch")
        return xgb_model
    except Exception as exc2:
        if 'Invalid cast' not in str(exc2) and 'Boolean' not in str(exc2):
            raise
        logger.warning(
            "Targeted patch load error (identifies which field still needs conversion): %s",
            exc2,
        )

    # Targeted patch still insufficient — diagnose and escalate to nuclear compat.
    remaining_keys = _find_int01_keys(data)
    logger.warning(
        "Targeted patch insufficient. Keys with remaining int 0/1 values "
        "(add to _XGB_REMOVED_BOOL_FIELDS if they are boolean): %s",
        sorted(remaining_keys) or "(none found — field may already be bool after patch)",
    )

    nuclear_path = model_path.with_name(model_path.stem + '_compat_nuclear.json')
    with open(model_path, 'r', encoding='utf-8') as fh:
        nuclear_data = json.load(fh)
    _patch_all_bool_ints(nuclear_data)
    with open(nuclear_path, 'w', encoding='utf-8') as fh:
        json.dump(nuclear_data, fh)
    logger.warning("Nuclear compat written to %s — attempting load…", nuclear_path.name)

    xgb_model = XGBClassifier()
    xgb_model.load_model(str(nuclear_path))
    logger.info("Model loaded via nuclear compatibility patch")
    return xgb_model


class PLR_xgboost_model(BaseModel):
    """XGBoost-based parcel ownership classification stage."""

    def set_workspaces(self) -> None:
        """Create temp GDB only (final GDB is owned by the GIS model stage)."""
        self.temp_dir = self._ensure_gdb(
            self.workspace / f'{self.state}_temp_{self.quarter}.gdb',
            'Temp workspace',
        )
        self.logger.debug("set_workspaces elapsed: %.1fs", self._elapsed())

    def add_centroid_attr(self) -> None:
        """Flag each parcel with whether its centroid falls inside government land."""
        dissolved_govt, centroid_intersect = build_centroid_govt_intersect(
            self.govt_land, self.parcels, self.temp_dir, self.state, self.logger
        )
        self.dissolved_govt_features = str(dissolved_govt)

        govt_parcel_ids: set = set()
        with arcpy.da.SearchCursor(str(centroid_intersect), ['PARCEL_ID']) as cursor:
            for row in cursor:
                govt_parcel_ids.add(row[0])
        self.logger.info("Centroid intersect dict built: %d records", len(govt_parcel_ids))

        try:
            arcpy.management.AddFields(self.parcels, [
                ['govt_centroid',    'LONG', '', None, 0, ''],
                ['private_centroid', 'LONG', '', None, 0, ''],
            ])
            self.logger.info("Centroid fields added")
        except arcpy.ExecuteError:
            self.logger.debug("Centroid fields already exist, skipping")

        with arcpy.da.UpdateCursor(
            self.parcels, ['PARCEL_ID', 'govt_centroid', 'private_centroid']
        ) as cursor:
            for row in cursor:
                if row[0] in govt_parcel_ids:
                    row[1] = 1
                    row[2] = 0
                else:
                    row[1] = 0
                    row[2] = 1
                cursor.updateRow(row)

        self.logger.info("Centroid field values updated")
        self.logger.debug("add_centroid_attr elapsed: %.1fs", self._elapsed())

    def add_xgb_field(self) -> None:
        """Add the xgb_gh_govt prediction output field."""
        try:
            arcpy.management.AddField(self.parcels, 'xgb_gh_govt', 'TEXT', field_length=10)
            self.logger.info("xgb_gh_govt field added")
        except arcpy.ExecuteError:
            self.logger.debug("xgb_gh_govt field already exists, skipping")

    def label_owner_type(self) -> None:
        """Classify each parcel as govt_owner, private_owner, or no_owner."""
        try:
            arcpy.management.AddFields(self.parcels, [
                ['govt_owner',    'LONG', '', None, 0, ''],
                ['private_owner', 'LONG', '', None, 0, ''],
                ['no_owner',      'LONG', '', None, 0, ''],
                ['full_name',     'TEXT', '', 255,  '', ''],
            ])
            self.logger.info("Owner type fields added")
        except arcpy.ExecuteError:
            self.logger.debug("Owner type fields already exist, skipping")

        owner_concat = (
            "!OWN1_FRST!" + "' '" + "!OWN1_LAST!" +
            "', '" + "!OWN2_FRST!" + "' '" + "!OWN2_LAST!"
        )
        arcpy.CalculateField_management(self.parcels, 'full_name', owner_concat, 'PYTHON3')
        self.logger.info("Owner names concatenated")

        govt_name_table: Path = GOVT_NAME_TABLES_DIR / f'{self.state}_govt_names.csv'
        if not govt_name_table.exists():
            raise FileNotFoundError(f"Govt name table not found: {govt_name_table}")

        govt_name_dict: dict = {}
        with open(govt_name_table, 'r') as f:
            for row in csv.DictReader(f):
                name = row['full_name']
                govt_name_dict[name] = govt_name_dict.get(name, 0) + 1
        govt_name_dict.pop(NULL_OWNER_SENTINEL, None)
        self.logger.info("Govt name dict loaded: %d entries", len(govt_name_dict))

        with arcpy.da.UpdateCursor(
            self.parcels, ['full_name', 'govt_owner', 'private_owner', 'no_owner']
        ) as cursor:
            for row in cursor:
                if row[0] in govt_name_dict:
                    row[1] = 1
                if row[0] == NULL_OWNER_SENTINEL:
                    row[3] = 1
                if row[0] not in govt_name_dict and row[0] != NULL_OWNER_SENTINEL:
                    row[2] = 1
                cursor.updateRow(row)

        self.logger.info("Owner field update complete")
        self.logger.debug("label_owner_type elapsed: %.1fs", self._elapsed())

    def export_state(self) -> None:
        """Export a slim feature class with only the fields needed for prediction."""
        df_parcels_path: Path = self.temp_dir / 'parcels_dataframe_data'
        self.df_parcels: str = str(df_parcels_path)

        if arcpy.Exists(self.df_parcels):
            self.logger.info("df parcels feature class already exists")
        else:
            arcpy.CreateFeatureclass_management(
                str(self.temp_dir), 'parcels_dataframe_data', spatial_reference=self.parcels
            )
            self.logger.info("df parcels feature class created")

        try:
            arcpy.management.AddFields(self.df_parcels, [
                ['gh_govt',          'TEXT',   '', 10,   '',   ''],
                ['overlap_perc',     'DOUBLE', '', None, None, ''],
                ['govt_centroid',    'LONG',   '', None, 0,    ''],
                ['private_centroid', 'LONG',   '', None, 0,    ''],
                ['govt_owner',       'LONG',   '', None, 0,    ''],
                ['private_owner',    'LONG',   '', None, 0,    ''],
                ['no_owner',         'LONG',   '', None, 0,    ''],
            ])
            self.logger.info("Dataframe feature class fields added")
        except arcpy.ExecuteError:
            self.logger.debug("Dataframe feature class fields already exist, skipping")

        insert_fields = [
            'gh_govt', 'overlap_perc', 'govt_centroid', 'private_centroid',
            'govt_owner', 'private_owner', 'no_owner', 'SHAPE@',
        ]
        with arcpy.da.SearchCursor(self.parcels, insert_fields) as s_cur:
            with arcpy.da.InsertCursor(self.df_parcels, insert_fields) as i_cur:
                for row in s_cur:
                    i_cur.insertRow(row)

        self.logger.info("Parcel dataframe feature class populated")
        self.logger.debug("export_state elapsed: %.1fs", self._elapsed())

    def make_new_predictions(self) -> dict:
        """Load the trained XGBoost model and return an OBJECTID → label dict."""
        state_df = pd.DataFrame.spatial.from_featureclass(self.df_parcels)
        state_df.drop(['SHAPE'], axis=1, inplace=True)

        x_df = state_df[[c for c in state_df.columns if c != 'gh_govt']]

        model_path: Path = XGB_MODELS_DIR / f'{self.state}_xgb_model.json'
        if not model_path.exists():
            raise FileNotFoundError(f"XGBoost model not found: {model_path}")

        # Load label decode map from companion metadata file if it exists;
        # fall back to the hard-coded default for models trained before metadata
        # persistence was added.
        meta_path: Path = model_path.with_suffix('.meta.json')
        if meta_path.exists():
            with open(meta_path, encoding='utf-8') as fh:
                meta = json.load(fh)
            inverse_label_map: dict[int, str] = {
                int(k): v for k, v in meta['inverse_label_map'].items()
            }
            self.logger.info("Loaded model metadata from %s", meta_path.name)
        else:
            inverse_label_map = _DEFAULT_INVERSE_LABEL_MAP
            self.logger.warning(
                "No metadata file found for %s model (%s); "
                "using default label encoding %s",
                self.state, meta_path.name, inverse_label_map,
            )

        self.logger.info("Loading XGBoost model from %s", model_path)
        xgb_model = _load_xgb_model(model_path, self.logger)

        y_preds = xgb_model.predict(x_df)
        pred_df = pd.DataFrame(y_preds, columns=['gh_govt_codes'])
        final_df = x_df.join(pred_df)
        final_df['gh_govt_xgboost'] = final_df['gh_govt_codes'].map(inverse_label_map)
        final_df.drop(
            columns=[
                'overlap_perc', 'govt_centroid', 'private_centroid',
                'govt_owner', 'private_owner', 'no_owner',
            ],
            inplace=True,
        )
        result: dict = final_df.set_index('OBJECTID')['gh_govt_xgboost'].to_dict()

        self.logger.info("XGBoost predictions complete: %d records", len(result))
        self.logger.debug("make_new_predictions elapsed: %.1fs", self._elapsed())
        return result

    def label_predctions(self, prediction_dict: dict) -> None:
        """Write XGBoost prediction labels back to the parcel feature class."""
        with arcpy.da.UpdateCursor(self.parcels, ['OBJECTID', 'xgb_gh_govt']) as cursor:
            for row in cursor:
                if row[0] in prediction_dict:
                    row[1] = prediction_dict[row[0]]
                cursor.updateRow(row)

        self.logger.info("XGB gh_govt field updated")
        self.logger.debug("label_predctions elapsed: %.1fs", self._elapsed())
