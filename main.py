import time

import arcpy

from configs import dev, state_full
from configs.settings import PARALLEL_PROCESSING_FACTOR
from geoprocessing.PLR_xgboost_model_predictions import PLR_xgboost_model
from geoprocessing.GIS_model_PLR import PLR_GIS_model
from geoprocessing.PLR_QC_steps import PLR_QC_model
from geoprocessing.PLR_post_process import PLR_post_process
from utils.logging_config import get_logger

logger = get_logger(__name__)

arcpy.env.parallelProcessingFactor = PARALLEL_PROCESSING_FACTOR


def xgboost_model_predictions(config: dict):
    for state_abbr, data in config['states'].items():
        state = state_full[state_abbr]
        logger.info("--- XGBoost: starting %s ---", state)
        try:
            plr_xgb = PLR_xgboost_model(data, state)
            plr_xgb.set_workspaces()
            plr_xgb.add_centroid_attr()
            plr_xgb.add_xgb_field()
            plr_xgb.label_owner_type()
            plr_xgb.export_state()
            predictions = plr_xgb.make_new_predictions()
            plr_xgb.label_predctions(predictions)
            logger.info("--- XGBoost: %s complete ---", state)
        except FileNotFoundError as e:
            logger.error("XGBoost %s: missing file — %s", state, e)
        except arcpy.ExecuteError as e:
            logger.error("XGBoost %s: ArcPy error — %s", state, arcpy.GetMessages(2))
        except Exception:
            logger.exception("XGBoost %s: unexpected error", state)


def gis_model_predictions(config: dict):
    for state_abbr, data in config['states'].items():
        state = state_full[state_abbr]
        logger.info("--- GIS model: starting %s ---", state)
        try:
            plr_gis = PLR_GIS_model(data, state)
            plr_gis.set_workspaces()
            plr_gis.label_private_public()
            logger.info("--- GIS model: %s complete ---", state)
        except arcpy.ExecuteError as e:
            logger.error("GIS model %s: ArcPy error — %s", state, arcpy.GetMessages(2))
        except Exception:
            logger.exception("GIS model %s: unexpected error", state)


def plr_qc(config: dict):
    for state_abbr, data in config['states'].items():
        state = state_full[state_abbr]
        logger.info("--- QC: starting %s ---", state)
        try:
            qc = PLR_QC_model(data, state)
            qc.set_workspaces()
            qc.qc_counts()
            qc.label_qc()
            qc.gap_qc()
            qc.overlap_qc()
            qc.qc_post_process()
            logger.info("--- QC: %s complete ---", state)
        except arcpy.ExecuteError:
            logger.error("QC %s: ArcPy error — %s", state, arcpy.GetMessages(2))
        except Exception:
            logger.exception("QC %s: unexpected error", state)


def plr_post_process(config: dict):
    for state_abbr in config['states']:
        state = state_full[state_abbr]
        logger.info("--- Post-process: starting %s ---", state)
        try:
            pp = PLR_post_process(state)
            pp.create_dissolve_fc()
            pp.post_process_govt_land()
            pp.private_land_dissolve()
            pp.append_private_no_owner_parcels()
            pp.multipart_to_singlepart()
            logger.info("--- Post-process: %s complete ---", state)
        except arcpy.ExecuteError:
            logger.error("Post-process %s: ArcPy error — %s", state, arcpy.GetMessages(2))
        except Exception:
            logger.exception("Post-process %s: unexpected error", state)


def main(config: dict):
    start = time.time()
    logger.info("====== PLR pipeline started ======")

    logger.info("Stage 1/4: XGBoost model predictions")
    xgboost_model_predictions(config)

    logger.info("Stage 2/4: GIS model predictions")
    gis_model_predictions(config)

    logger.info("Stage 3/4: QC process")
    plr_qc(config)

    logger.info("Stage 4/4: Post-processing")
    plr_post_process(config)

    elapsed = time.time() - start
    logger.info("====== PLR pipeline complete — %.1f seconds ======", elapsed)


if __name__ == '__main__':
    main(dev)
