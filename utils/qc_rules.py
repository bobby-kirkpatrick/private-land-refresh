"""
Pure QC business-logic rules for parcel label reconciliation.

``apply_qc_rule`` contains no arcpy imports and has no side effects, making
it directly unit-testable without ArcGIS Pro.  The arcpy cursor loop in
``PLR_QC_steps.PLR_QC_model.label_qc`` delegates every row decision here.
"""
from __future__ import annotations

from configs.settings import (
    GOVT_OVERLAP_THRESHOLD,
    QC_LARGE_PARCEL_THRESHOLD,
    NULL_OWNER_SENTINEL,
)

# Sentinel value returned when no rule changes the row
_UNCHANGED = object()


def apply_qc_rule(
    gh: str,
    xgb: str,
    priv_own: int,
    acres: float,
    qc: int,
    name: str,
    overlap: float,
    govt_cen: int,
    govt_own: int,
) -> tuple[str, int]:
    """
    Apply QC business-logic rules to a single parcel row.

    This function is intentionally free of arcpy dependencies so it can be
    called directly in unit tests.

    Parameters
    ----------
    gh:        Current ``gh_govt`` label ('TRUE' / 'FALSE' / 'UNKNOWN').
    xgb:       XGBoost prediction ('TRUE' / 'FALSE').
    priv_own:  1 if parcel has a private owner name, else 0.
    acres:     Parcel area in acres (``gh_parcel_acres``).
    qc:        QC flag value (1 = model disagreement, 0 = agreement).
    name:      Concatenated owner full name (``full_name`` field).
    overlap:   Percentage of parcel overlapping government land.
    govt_cen:  1 if parcel centroid falls inside government land, else 0.
    govt_own:  1 if owner name matches a government entity, else 0.

    Returns
    -------
    (new_gh_govt, new_qc_flag)
        The updated label and flag.  If no rule fires the original values
        are returned unchanged.

    QC Flag Reference
    -----------------
    0   Model agreement — row not evaluated here.
    1   Disagreement detected (set by the calling cursor, not this function).
    2   XGB=FALSE, GIS=TRUE, private name, large parcel → keep TRUE.
    3   XGB=FALSE, GIS=TRUE, private name, small parcel  → change to FALSE.
    4   No owner name, high govt overlap → TRUE.
    5   No owner name, low govt overlap  → UNKNOWN.
    6   Govt centroid + govt owner name  → TRUE.
    7   Unresolved disagreement          → label unchanged, flag set to 7.
    """
    if qc != 1:
        # Row is in model agreement — nothing to resolve
        return gh, qc

    # Rule 2: XGB says private, GIS says govt, large private-named parcel → keep govt
    if xgb == 'FALSE' and gh == 'TRUE' and priv_own == 1 and acres >= QC_LARGE_PARCEL_THRESHOLD:
        return 'TRUE', 2

    # Rule 3: Same as rule 2 but small parcel → flip to private
    if xgb == 'FALSE' and gh == 'TRUE' and priv_own == 1 and acres < QC_LARGE_PARCEL_THRESHOLD:
        return 'FALSE', 3

    # Rule 4: No owner info, high overlap → government
    if name == NULL_OWNER_SENTINEL and overlap >= GOVT_OVERLAP_THRESHOLD:
        return 'TRUE', 4

    # Rule 5: No owner info, low overlap → unresolved
    if name == NULL_OWNER_SENTINEL and overlap < GOVT_OVERLAP_THRESHOLD:
        return 'UNKNOWN', 5

    # Rule 6: Centroid inside govt land AND owner matches govt name table → government
    if govt_cen == 1 and govt_own == 1:
        return 'TRUE', 6

    # Rule 7: Disagreement remains unresolved → preserve label, mark for review
    return gh, 7
