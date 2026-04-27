"""Shared geospatial utility functions used across PLR pipeline modules."""
import os
import time
import logging
from pathlib import Path

import arcpy


def get_quarter() -> str:
    """
    Return the active quarter string, e.g. 'Q2_2026'.

    If the environment variable ``PLR_QUARTER`` is set (e.g. via
    ``--quarter`` on the CLI) that value is returned as-is, allowing
    operators to reprocess a specific quarter without touching the clock.
    """
    override = os.environ.get('PLR_QUARTER', '').strip()
    if override:
        return override

    month = time.strftime("%m")
    year = time.strftime("%Y")
    if month in ('01', '02', '03'):
        return f'Q1_{year}'
    elif month in ('04', '05', '06'):
        return f'Q2_{year}'
    elif month in ('07', '08', '09'):
        return f'Q3_{year}'
    return f'Q4_{year}'


def dissolve_govt_land(
    govt_land: str,
    output: Path,
    logger: logging.Logger,
) -> Path:
    """Dissolve government land layer into a single footprint. Idempotent."""
    if arcpy.Exists(str(output)):
        logger.info("%s already exists", output.name)
    else:
        arcpy.Dissolve_management(govt_land, str(output))
        logger.info("%s dissolved", output.name)
    return output


def create_centroids(
    parcels: str,
    output: Path,
    logger: logging.Logger,
) -> Path:
    """Create inside-centroid points for parcel polygons. Idempotent."""
    if arcpy.Exists(str(output)):
        logger.info("%s already exists", output.name)
    else:
        arcpy.FeatureToPoint_management(parcels, str(output), "INSIDE")
        logger.info("%s created", output.name)
    return output


def intersect_features(
    inputs: list,
    output: Path,
    logger: logging.Logger,
) -> Path:
    """Intersect a list of feature classes. Idempotent."""
    if arcpy.Exists(str(output)):
        logger.info("%s already exists", output.name)
    else:
        arcpy.Intersect_analysis(inputs, str(output))
        logger.info("%s created", output.name)
    return output


def build_centroid_govt_intersect(
    govt_land: str,
    parcels: str,
    temp_dir: Path,
    state: str,
    logger: logging.Logger,
) -> tuple:
    """
    Run the full dissolve → centroid → intersect sequence shared by
    the XGBoost and GIS model stages.

    Returns (dissolved_govt_path, centroid_intersect_path) as Path objects.
    """
    dissolved = dissolve_govt_land(
        govt_land,
        temp_dir / f'{state}_dissolved_govt_features',
        logger,
    )
    centroids = create_centroids(
        parcels,
        temp_dir / f'{state}_corelogic_centroids',
        logger,
    )
    intersect = intersect_features(
        [str(centroids), str(dissolved)],
        temp_dir / f'{state}_centroid_govt_intx',
        logger,
    )
    return dissolved, intersect
