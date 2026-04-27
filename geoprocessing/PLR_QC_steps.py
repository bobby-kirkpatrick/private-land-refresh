from pathlib import Path

import arcpy

from configs.settings import (
    GAP_ACRE_THRESHOLD,
    OVERLAP_SLIVER_THRESHOLD,
    NULL_OWNER_SENTINEL,
)
from geoprocessing.base_model import BaseModel
from utils.qc_rules import apply_qc_rule


class PLR_QC_model(BaseModel):
    """Quality-control stage that reconciles XGBoost vs GIS model disagreements."""

    def qc_counts(self) -> None:
        """
        Log parcel counts and model agreement rates.

        Stores results as instance attributes so ``main.py`` can pick them
        up for the run report without re-querying the feature class:
          - ``self.parcel_count``
          - ``self.agreement_count``
          - ``self.agreement_pct``
        """
        self.parcel_count: int = int(arcpy.GetCount_management(self.parcels)[0])
        self.logger.info("%s: %d total parcels", self.state, self.parcel_count)

        agreement_layer = arcpy.SelectLayerByAttribute_management(
            self.parcels, "NEW_SELECTION", '"gh_govt" = "xgb_gh_govt"'
        )
        self.agreement_count: int = int(arcpy.GetCount_management(agreement_layer)[0])
        self.agreement_pct: float = (
            self.agreement_count / self.parcel_count * 100 if self.parcel_count else 0.0
        )

        self.logger.info(
            "%s model agreement: %d / %d parcels (%.1f%%)",
            self.state, self.agreement_count, self.parcel_count, self.agreement_pct,
        )
        self.logger.info(
            "%s: %d parcels require QC",
            self.state, self.parcel_count - self.agreement_count,
        )

    def label_qc(self) -> None:
        """Apply business-logic QC flags and correct gh_govt labels on disagreeing parcels."""
        try:
            arcpy.management.AddField(self.parcels, 'qc', 'SHORT')
            self.logger.info("qc field added")
        except arcpy.ExecuteError:
            self.logger.debug("qc field already exists, skipping")

        with arcpy.da.UpdateCursor(
            self.parcels, 'qc', where_clause='gh_govt <> xgb_gh_govt'
        ) as cursor:
            for row in cursor:
                row[0] = 1
                cursor.updateRow(row)

        self.logger.info("QC flag 1 applied to disagreeing parcels for %s", self.state)

        qc_fields = [
            'gh_govt', 'xgb_gh_govt', 'private_owner', 'gh_parcel_acres',
            'qc', 'full_name', 'overlap_perc', 'govt_centroid', 'govt_owner',
        ]
        with arcpy.da.UpdateCursor(self.parcels, qc_fields) as cursor:
            for row in cursor:
                gh, xgb, priv_own, acres, qc, name, overlap, govt_cen, govt_own = row
                new_gh, new_qc = apply_qc_rule(
                    gh, xgb, priv_own, acres, qc, name, overlap, govt_cen, govt_own
                )
                row[0], row[4] = new_gh, new_qc
                cursor.updateRow(row)

        self.logger.info("Label QC complete for %s", self.state)

    def gap_qc(self) -> None:
        """Identify and insert gap features between govt-labelled parcels and the govt layer."""
        govt_true = f"{self.state}_GovtTrue"
        arcpy.MakeFeatureLayer_management(
            self.parcels, govt_true, where_clause="gh_govt IN ('TRUE')"
        )

        sym_diff_fc: Path = self.temp_dir / f'{self.state}_symDiff'
        if arcpy.Exists(str(sym_diff_fc)):
            self.logger.info("%s_symDiff already exists", self.state)
        else:
            arcpy.analysis.SymDiff(govt_true, self.govt_land, str(sym_diff_fc), join_attributes="ONLY_FID")
            self.logger.info("%s_symDiff created", self.state)

        parcel_layer_name = self.parcels.split('.gdb\\')[1]
        sym_diff_gap = f"{self.state}_symDiff_gap"
        arcpy.MakeFeatureLayer_management(
            str(sym_diff_fc), sym_diff_gap,
            where_clause=f"FID_{parcel_layer_name} <> -1",
        )

        sym_diff_sp: Path = self.temp_dir / f'{self.state}_SD_SP_fc'
        if arcpy.Exists(str(sym_diff_sp)):
            self.logger.info("%s_symDiff_singlePart already exists", self.state)
        else:
            arcpy.MultipartToSinglepart_management(sym_diff_gap, str(sym_diff_sp))
            arcpy.management.AddFields(str(sym_diff_sp), [
                ['gap_acres', 'LONG', '', None, None, ''],
                ['Unit_Nm',   'TEXT', '', None, None, ''],
                ['gh_govtype','TEXT', '', None, None, ''],
            ])
            arcpy.management.CalculateField(str(sym_diff_sp), 'gap_acres', '!shape.area@acres!', 'PYTHON3')
            self.logger.info("%s single-part gap acres calculated", self.state)

        with arcpy.da.UpdateCursor(str(sym_diff_sp), 'gap_acres') as cursor:
            for row in cursor:
                if row[0] >= GAP_ACRE_THRESHOLD:
                    cursor.deleteRow()
        self.logger.info("Gaps >= %d acres removed", GAP_ACRE_THRESHOLD)

        gap_sj: Path = self.temp_dir / f'{self.state}_gap_SJ'
        if arcpy.Exists(str(gap_sj)):
            self.logger.info("%s gap spatial join already exists", self.state)
        else:
            arcpy.analysis.SpatialJoin(str(sym_diff_sp), self.govt_land, str(gap_sj), match_option='CLOSEST')
            self.logger.info("%s gap spatial join complete", self.state)

        with arcpy.da.SearchCursor(str(gap_sj), ['Unit_Nm_1', 'gh_govtype_1', 'SHAPE@']) as s_cur:
            with arcpy.da.InsertCursor(self.govt_land, ['Unit_Nm', 'gh_govtype', 'SHAPE@']) as i_cur:
                for row in s_cur:
                    i_cur.insertRow(row)

        self.logger.info("Gap QC features inserted into govt land layer")

    def overlap_qc(self) -> None:
        """Erase government land from private parcels in overlapping areas."""
        govt_false = f"{self.state}_GovtFalse"
        arcpy.MakeFeatureLayer_management(
            self.parcels, govt_false, where_clause="gh_govt IN ('FALSE')"
        )

        overlap_intx: Path = self.temp_dir / f'{self.state}_govt_overlap_intx'
        if arcpy.Exists(str(overlap_intx)):
            self.logger.info("%s govt overlap intx already exists", self.state)
        else:
            arcpy.analysis.Intersect([govt_false, self.govt_land], str(overlap_intx))
            self.logger.info("%s govt overlap intx created", self.state)

        try:
            arcpy.management.AddField(str(overlap_intx), 'intx_ac', 'LONG')
        except arcpy.ExecuteError:
            self.logger.debug("intx_ac field already exists")
        arcpy.management.CalculateField(str(overlap_intx), 'intx_ac', '!shape.area@acres!', 'PYTHON3')

        private_intx = f"{self.state}_private_overlaps"
        arcpy.MakeFeatureLayer_management(
            self.parcels, private_intx,
            where_clause="private_owner = 1 And private_centroid = 1",
        )

        govt_private_erase: Path = self.temp_dir / f'{self.state}_govt_land_private_erased'
        if arcpy.Exists(str(govt_private_erase)):
            self.logger.info("%s govt private erase already exists", self.state)
        else:
            arcpy.RepairGeometry_management(self.govt_land)
            arcpy.analysis.Erase(self.govt_land, private_intx, str(govt_private_erase))
            self.logger.info("%s govt private erase created", self.state)

        def _make_overlap_layer(name: str, where: str) -> str:
            arcpy.MakeFeatureLayer_management(str(overlap_intx), name, where_clause=where)
            return name

        no_name_govt = _make_overlap_layer(
            f"{self.state}_no_name_govt_intx",
            f"full_name IN ('{NULL_OWNER_SENTINEL}') AND govt_centroid = 1",
        )
        erase_1: Path = self.temp_dir / f'{self.state}_parcels_erase_1'
        if not arcpy.Exists(str(erase_1)):
            arcpy.analysis.Erase(self.parcels, no_name_govt, str(erase_1))
            self.logger.info("%s_parcels_erase_1 created", self.state)

        sliver = _make_overlap_layer(
            f"{self.state}_sliver_intx",
            f"full_name IN ('{NULL_OWNER_SENTINEL}') AND intx_ac < {OVERLAP_SLIVER_THRESHOLD}",
        )
        govt_sliver_erase: Path = self.temp_dir / f'{self.state}_govt_land_private_erased_2'
        if not arcpy.Exists(str(govt_sliver_erase)):
            arcpy.analysis.Erase(str(govt_private_erase), sliver, str(govt_sliver_erase))
            self.logger.info("%s govt overlap sliver erase created", self.state)

        large_overlap = _make_overlap_layer(
            f"{self.state}_large_overlap_intx",
            f"full_name IN ('{NULL_OWNER_SENTINEL}') AND intx_ac >= {OVERLAP_SLIVER_THRESHOLD}",
        )
        erase_2: Path = self.temp_dir / f'{self.state}_parcels_erase_2'
        if not arcpy.Exists(str(erase_2)):
            arcpy.analysis.Erase(str(erase_1), large_overlap, str(erase_2))
            self.logger.info("%s_parcels_erase_2 created", self.state)

        govt_name_intx = _make_overlap_layer(
            f"{self.state}_govt_name_intx",
            f"govt_centroid = 1 And private_owner = 1 And intx_ac >= {OVERLAP_SLIVER_THRESHOLD}",
        )
        erase_3: Path = self.temp_dir / f'{self.state}_parcels_erase_3'
        if not arcpy.Exists(str(erase_3)):
            arcpy.analysis.Erase(str(erase_2), govt_name_intx, str(erase_3))
            self.logger.info("%s_parcels_erase_3 created", self.state)

        govt_centroid_intx = _make_overlap_layer(
            f"{self.state}_govt_centroid_intx",
            "govt_centroid = 1 And govt_owner = 1",
        )
        erase_4: Path = self.temp_dir / f'{self.state}_parcels_erase_4'
        if not arcpy.Exists(str(erase_4)):
            arcpy.analysis.Erase(str(erase_3), govt_centroid_intx, str(erase_4))
            self.logger.info("%s_parcels_erase_4 created", self.state)

        self.logger.info("Overlap QC complete for %s", self.state)

    def qc_post_process(self) -> None:
        """Delete all temp feature classes except the two final outputs."""
        arcpy.env.workspace = str(self.temp_dir)
        feature_classes = arcpy.ListFeatureClasses()

        keep = {
            f'{self.state}_parcels_erase_4',
            f'{self.state}_govt_land_private_erased_2',
        }
        delete_list = [
            str(self.temp_dir / fc)
            for fc in feature_classes
            if fc not in keep
        ]

        if delete_list:
            arcpy.Delete_management(delete_list)
            self.logger.info("Cleaned up %d temp feature classes", len(delete_list))
        else:
            self.logger.info("No temp feature classes to clean up")
