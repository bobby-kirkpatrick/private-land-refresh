import csv
import json
from pathlib import Path

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

# XGBoost 1.x stored certain boolean parameters as integers (0/1) in the model
# JSON.  XGBoost 2.x strictly requires boolean types and raises
# "Invalid cast, from Integer to Boolean" when loading those older files.
# This list covers the known offending fields.
_XGB_LEGACY_BOOL_FIELDS = ['use_label_encoder']


def _load_xgb_model(model_path: Path, logger) -> 'XGBClassifier':
    """
    Load an XGBoost model, patching integer-to-boolean type incompatibilities
    that arise when a model trained with XGBoost 1.x is loaded under 2.x.

    If the standard load succeeds the patching is skipped entirely.
    A patched copy is written alongside the original as
    ``<name>_compat.json`` so subsequent loads are fast.
    """
    import re

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

    if not compat_path.exists():
        with open(model_path, 'r', encoding='utf-8') as fh:
            raw = fh.read()
        for field in _XGB_LEGACY_BOOL_FIELDS:
            raw = re.sub(rf'("{re.escape(field)}"\s*:\s*)0\b', r'\g<1>false', raw)
            raw = re.sub(rf'("{re.escape(field)}"\s*:\s*)1\b', r'\g<1>true', raw)
        with open(compat_path, 'w', encoding='utf-8') as fh:
            fh.write(raw)
        logger.info("Patched model written to %s", compat_path.name)
    else:
        logger.info("Using existing patched model %s", compat_path.name)

    xgb_model.load_model(str(compat_path))
    logger.info("Model loaded successfully via compatibility patch")
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

        centroid_id_govt: dict = {}
        with arcpy.da.SearchCursor(
            str(centroid_intersect),
            [f'FID_{self.state}_corelogic_centroids', 'PARCEL_ID'],
        ) as cursor:
            for row in cursor:
                centroid_id_govt[row[0]] = row[1]
        self.logger.info("Centroid intersect dict built: %d records", len(centroid_id_govt))

        try:
            arcpy.management.AddFields(self.parcels, [
                ['govt_centroid',    'LONG', '', None, 0, ''],
                ['private_centroid', 'LONG', '', None, 0, ''],
            ])
            self.logger.info("Centroid fields added")
        except arcpy.ExecuteError:
            self.logger.debug("Centroid fields already exist, skipping")

        with arcpy.da.UpdateCursor(
            self.parcels, ['OBJECTID', 'govt_centroid', 'private_centroid']
        ) as cursor:
            for row in cursor:
                if row[0] in centroid_id_govt:
                    row[1] = 1
                else:
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
