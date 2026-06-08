"""
Audit-trail report for the PLR VTPK creation step.

Written to the logs directory as a timestamped JSON file after every
vtpk_creator.py run.  Records exactly which states were processed, which
layers were exported, whether S3 upload succeeded, and any errors that
prevented a package from being created.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from configs.settings import LOG_DIR


@dataclass
class LayerVtpkResult:
    """Outcome of a single VTPK creation (one layer, one state)."""
    layer_type: str       # 'private_land' | 'govt_land'
    layer_name: str       # aprx layer name targeted
    map_name: str         # aprx map name used
    vtpk_path: str        # absolute local path to the .vtpk file
    status: str = 'pending'  # pending | success | skipped | failed
    uploaded: bool = False   # True if VTPK S3 upload succeeded
    error: str = ''          # populated on failure or skip


@dataclass
class StateVtpkResult:
    """Aggregates VTPK results for both layers of one state."""
    abbr: str
    state: str
    map_name: str
    status: str = 'pending'   # pending | success | partial | failed
    layers: list[LayerVtpkResult] = field(default_factory=list)
    state_csv_path: str = ''         # combined release CSV (e.g. hawaii_….csv)
    state_csv_uploaded: bool = False
    elapsed_seconds: float = 0.0

    def mark_complete(self) -> None:
        """Derive overall status from individual layer statuses."""
        statuses = {lr.status for lr in self.layers}
        if statuses == {'success'}:
            self.status = 'success'
        elif 'success' in statuses:
            self.status = 'partial'
        else:
            self.status = 'failed'


@dataclass
class VtpkReport:
    """Aggregates VTPK creation results across all states for one run."""
    quarter: str
    started_at: str
    aprx_path: str
    output_folder: str
    states_requested: list[str] = field(default_factory=list)
    results: list[StateVtpkResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    finished_at: str = ''

    # ------------------------------------------------------------------ #
    # Mutators                                                             #
    # ------------------------------------------------------------------ #

    def add_state(self, abbr: str, state: str, map_name: str) -> StateVtpkResult:
        result = StateVtpkResult(abbr=abbr, state=state, map_name=map_name)
        self.results.append(result)
        return result

    # ------------------------------------------------------------------ #
    # Finalise and write                                                   #
    # ------------------------------------------------------------------ #

    def finalize(self, total_elapsed: float) -> None:
        self.total_elapsed_seconds = round(total_elapsed, 2)
        self.finished_at = time.strftime('%Y-%m-%dT%H:%M:%S')

    def write(self, output_dir: Optional[Path] = None) -> Path:
        out = output_dir or LOG_DIR
        out.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        path = out / f'vtpk_report_{self.quarter}_{timestamp}.json'
        with open(path, 'w', encoding='utf-8') as fp:
            json.dump(asdict(self), fp, indent=2)
        return path

    # ------------------------------------------------------------------ #
    # Summary helpers                                                      #
    # ------------------------------------------------------------------ #

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.status == 'success')

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status in ('failed', 'partial'))

    def summary_lines(self) -> list[str]:
        lines = [
            f"Quarter    : {self.quarter}",
            f"Started    : {self.started_at}",
            f"Finished   : {self.finished_at}",
            f"Elapsed    : {self.total_elapsed_seconds:.1f}s",
            f"ArcGIS Pro : {self.aprx_path}",
            f"Output     : {self.output_folder}",
            f"States     : {self.success_count} fully exported, "
            f"{self.failed_count} failed/partial",
        ]
        for sr in self.results:
            icon = {'success': '✓', 'partial': '~', 'failed': '✗', 'pending': '?'}.get(
                sr.status, '?'
            )
            state_csv_tag = ' | state_csv=✓' if sr.state_csv_uploaded else ''
            lines.append(
                f"  {icon} {sr.abbr} ({sr.state}) "
                f"| map={sr.map_name} | {sr.elapsed_seconds:.1f}s{state_csv_tag}"
            )
            if sr.state_csv_path:
                lines.append(f"      state CSV : {sr.state_csv_path}")
            for lr in sr.layers:
                status_icon = (
                    '✓' if lr.status == 'success'
                    else ('~' if lr.status == 'skipped' else '✗')
                )
                s3_tag = ' | S3=✓' if lr.uploaded else ''
                lines.append(
                    f"      {status_icon} {lr.layer_type:<15} "
                    f"| status={lr.status}{s3_tag}"
                )
                if lr.vtpk_path and lr.status == 'success':
                    lines.append(f"            VTPK : {lr.vtpk_path}")
                if lr.error:
                    lines.append(f"            NOTE : {lr.error}")
        return lines
