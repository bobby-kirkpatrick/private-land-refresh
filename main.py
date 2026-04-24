from geoprocessing.PLR_xgboost_model_predictions import PLR_xgboost_model
from geoprocessing.GIS_model_PLR import PLR_GIS_model
from geoprocessing.PLR_QC_steps import PLR_QC_model
from geoprocessing.PLR_post_process import PLR_post_process
from configs import dev, state_full
import arcpy
import time
arcpy.env.parallelProcessingFactor = "25%"



def xgboost_model_predictions(config):
    for k, v in config['states'].items():
        try:
            state = state_full.get(k)
            data = v

            plr_xgb = PLR_xgboost_model(data, state)
            temp_workspace = plr_xgb.set_workspaces()
            plr_xgb.add_centroid_attr()
            plr_xgb.add_xgb_field()
            plr_xgb.label_owner_type()
            plr_xgb.export_state()
            prediction_dictionary = plr_xgb.make_new_predictions()
            plr_xgb.label_predctions(prediction_dictionary)

        except Exception as e:
            print(f'unable to make xgboost predections for {state} due to {e}')


def gis_model_predictions(config):
    for k, v in config['states'].items():
        try:
            state = state_full.get(k)
            data = v

            plr_gis = PLR_GIS_model(data, state)
            workspaces = plr_gis.set_workspaces()
            plr_gis.label_private_public()

        except Exception as e:
            print(f'unable to make GIS predections for {state} due to {e}')

def PLR_QC(config):
    for k, v in config['states'].items():
        try:
            state = state_full.get(k)
            data = v

            qc = PLR_QC_model(data, state)
            workspaces = qc.set_workspaces()
            qc.qc_counts()
            qc.label_qc()
            qc.gap_qc()
            qc.overlap_qc()
            qc.qc_post_process()

        except Exception as e:
            print(f'unable to complete QC process for {state} due to {e}')

def PLR_postProcess(config):
    for k, v in config['states'].items():
        state = state_full.get(k)

        post_process = PLR_post_process(state)
        post_process.create_dissolve_fc()
        post_process.post_process_govt_land()
        post_process.private_land_dissolve()
        post_process.append_private_no_owner_parcels()
        post_process.multipart_to_singlepart()

def main(config):
    start = time.time()
    print("making XGBoost model predictions")
    xgboost_model_predictions(config)
    print("XGBoost model predictions complete")
    print("making GIS model predictions")
    gis_model_predictions(config)
    print("GIS model predictions complete")
    print("Beginning PLR QC process")
    PLR_QC(config)
    print("PLR QC process complete")
    finish = time.time()
    total = finish - start
    print(f'Process took {total} seconds')
    PLR_postProcess(config)


if __name__ == '__main__':
    main(dev)
