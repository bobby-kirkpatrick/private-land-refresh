"""
Database publish stage for the PLR pipeline.

For each state, publishes the completed Private_Land and Govt_Land feature
classes from the local final GDB into the enterprise geodatabase via a
truncate-then-append pattern.

Field validation is performed BEFORE any truncate so that a schema mismatch
cannot leave the database in an empty state.
"""
from __future__ import annotations

from pathlib import Path

import arcpy

from utils.geo_utils import get_quarter
from utils.logging_config import get_logger
from utils.publish_report import LayerPublishResult

# System field types excluded from schema comparison and field mapping.
_SYSTEM_TYPES = frozenset({'OID', 'Geometry'})

# Auto-computed geometry fields that appear in some feature classes but
# are not user-managed and should not be included in field mapping.
_SYSTEM_NAMES = frozenset({
    'SHAPE_LENGTH', 'SHAPE_AREA', 'SHAPE.LEN', 'SHAPE.AREA',
    'ST_LENGTH(SHAPE)', 'ST_AREA(SHAPE)',
})


class PLR_publish:
    """
    Publishes Private_Land and Govt_Land outputs to the enterprise GDB.

    Parameters
    ----------
    state:
        Full lowercase state name (e.g. 'colorado').
    state_config:
        The per-state dict from configs/config.py containing
        ``govt_land_target`` and ``private_land_target`` paths.
    quarter:
        Quarter string (e.g. 'Q2_2026').  Auto-detected if omitted.
    env:
        'LOCAL' uses the current working directory as the workspace root.
    """

    def __init__(
        self,
        state: str,
        state_config: dict,
        quarter: str | None = None,
        env: str = 'LOCAL',
    ) -> None:
        self.state = state
        self.workspace: Path = Path.cwd() if env == 'LOCAL' else Path(env)
        self.quarter: str = quarter or get_quarter()
        self.logger = get_logger(f'{type(self).__module__}.{type(self).__name__}')

        self.final_gdb: Path = (
            self.workspace / f'{self.state}_private_land_{self.quarter}.gdb'
        )

        # Resolve publish targets from config
        self.targets: dict[str, str] = {
            'private_land': state_config['private_land_target'],
            'govt_land':    state_config['govt_land_target'],
        }

        # Resolve local source FCs
        self.sources: dict[str, str] = {
            'private_land': str(
                self.final_gdb / f'{self.state}_Private_Land_{self.quarter}'
            ),
            'govt_land': str(
                self.final_gdb / f'{self.state}_Govt_Land_{self.quarter}'
            ),
        }

    # ------------------------------------------------------------------ #
    # Field validation                                                     #
    # ------------------------------------------------------------------ #

    def validate_fields(
        self, source_fc: str, target_fc: str
    ) -> tuple[list[str], list[str]]:
        """
        Compare source and target field schemas.

        Returns
        -------
        (errors, warnings)
            *errors* — mismatches that will block the publish (required target
            fields absent from source, meaning they'd be left NULL).
            *warnings* — informational differences that don't block the publish
            (extra source fields not present in target, which are simply
            dropped during append).
        """
        def _user_fields(fc: str) -> dict[str, arcpy.Field]:
            return {
                f.name.upper(): f
                for f in arcpy.ListFields(fc)
                if f.type not in _SYSTEM_TYPES
                and f.name.upper() not in _SYSTEM_NAMES
            }

        source_fields = _user_fields(source_fc)
        target_fields = _user_fields(target_fc)

        errors: list[str] = []
        warnings: list[str] = []

        # Required target fields not present in source — BLOCKS publish
        missing_required = {
            name for name, fld in target_fields.items()
            if name not in source_fields and not fld.isNullable
        }
        if missing_required:
            errors.append(
                f"Required target field(s) missing from source "
                f"(append would violate NOT NULL constraint): "
                f"{sorted(missing_required)}"
            )

        # Optional target fields not present in source — WARNS only
        missing_optional = {
            name for name, fld in target_fields.items()
            if name not in source_fields and fld.isNullable
        }
        if missing_optional:
            warnings.append(
                f"Optional target field(s) not in source "
                f"(will remain NULL after append): {sorted(missing_optional)}"
            )

        # Extra source fields not in target — dropped silently, worth noting
        extra_source = set(source_fields) - set(target_fields)
        if extra_source:
            warnings.append(
                f"Source field(s) not in target (will be ignored): "
                f"{sorted(extra_source)}"
            )

        return errors, warnings

    # ------------------------------------------------------------------ #
    # Field mapping                                                        #
    # ------------------------------------------------------------------ #

    def _build_field_mapping(
        self, source_fc: str, target_fc: str
    ) -> arcpy.FieldMappings:
        """
        Build a FieldMappings object that maps source fields to target fields
        by name (case-insensitive).  Fields in the target that have no source
        match are omitted from the mapping (they remain at their default /
        NULL value after append).
        """
        # Index source fields by uppercase name
        source_index: dict[str, str] = {
            f.name.upper(): f.name
            for f in arcpy.ListFields(source_fc)
            if f.type not in _SYSTEM_TYPES
            and f.name.upper() not in _SYSTEM_NAMES
        }

        fm_obj = arcpy.FieldMappings()

        for target_field in arcpy.ListFields(target_fc):
            if (target_field.type in _SYSTEM_TYPES
                    or target_field.name.upper() in _SYSTEM_NAMES):
                continue

            source_name = source_index.get(target_field.name.upper())
            if source_name is None:
                continue  # No source field for this target field — skip

            fmap = arcpy.FieldMap()
            fmap.addInputField(source_fc, source_name)

            # Set output field name to match target exactly
            out_field = fmap.outputField
            out_field.name = target_field.name
            out_field.aliasName = target_field.aliasName
            fmap.outputField = out_field

            fm_obj.addFieldMap(fmap)

        return fm_obj

    # ------------------------------------------------------------------ #
    # Per-layer publish                                                    #
    # ------------------------------------------------------------------ #

    def publish_layer(self, layer_type: str) -> LayerPublishResult:
        """
        Validate, truncate, and append one layer (``'private_land'`` or
        ``'govt_land'``).

        The truncate only happens AFTER field validation passes so a schema
        mismatch can never leave the enterprise table empty.

        Returns
        -------
        LayerPublishResult
            Populated with the outcome of this operation.
        """
        source_fc = self.sources[layer_type]
        target_fc = self.targets[layer_type]

        result = LayerPublishResult(
            layer_type=layer_type,
            source_fc=source_fc,
            target_fc=target_fc,
        )

        # --- Pre-flight checks ---
        if not arcpy.Exists(source_fc):
            result.status = 'failed'
            result.error = f"Source FC not found: {source_fc}"
            self.logger.error("[%s] %s source not found: %s",
                              self.state, layer_type, source_fc)
            return result

        if not arcpy.Exists(target_fc):
            result.status = 'failed'
            result.error = f"Target FC not found: {target_fc}"
            self.logger.error("[%s] %s target not found: %s",
                              self.state, layer_type, target_fc)
            return result

        # --- Field validation ---
        self.logger.info("[%s] Validating %s field schema…", self.state, layer_type)
        errors, warnings = self.validate_fields(source_fc, target_fc)

        result.field_errors = errors
        result.field_warnings = warnings

        for w in warnings:
            self.logger.warning("[%s] %s — %s", self.state, layer_type, w)

        if errors:
            result.status = 'failed'
            for e in errors:
                self.logger.error(
                    "[%s] %s — field validation FAILED: %s",
                    self.state, layer_type, e,
                )
            self.logger.error(
                "[%s] Skipping truncate/append for %s due to field errors.",
                self.state, layer_type,
            )
            return result

        self.logger.info(
            "[%s] %s field validation passed — proceeding with publish.",
            self.state, layer_type,
        )

        # --- Truncate ---
        self.logger.info("[%s] Truncating %s…", self.state, layer_type)
        arcpy.management.TruncateTable(target_fc)
        self.logger.info("[%s] %s truncated.", self.state, layer_type)

        # --- Append ---
        self.logger.info("[%s] Appending %s…", self.state, layer_type)
        field_mapping = self._build_field_mapping(source_fc, target_fc)
        arcpy.management.Append(
            inputs=[source_fc],
            target=target_fc,
            schema_type='NO_TEST',
            field_mapping=field_mapping,
        )

        result.rows_appended = int(arcpy.GetCount_management(target_fc)[0])
        result.status = 'success'
        self.logger.info(
            "[%s] %s published — %d rows appended.",
            self.state, layer_type, result.rows_appended,
        )
        return result
