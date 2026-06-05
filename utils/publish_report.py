"""
Audit-trail report for the PLR publish step.

Written to the logs directory as a timestamped JSON file after every
publish run.  Records exactly which layers were published, whether field
validation passed, how many rows were appended, and any errors that
prevented a layer from being published.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from configs.settings import LOG_DIR


@dataclass
class LayerPublishResult:
    """Outcome of a single truncate-and-append operation for one layer."""
    layer_type: str          # 'private_land' | 'govt_land'
    source_fc: str           # local file GDB path
    target_fc: str           # enterprise SDE path
    status: str = 'pending'  # pending | success | failed | skipped
    field_errors: list[str] = field(default_factory=list)   # schema mismatches
    field_warnings: list[str] = field(default_factory=list) # non-fatal differences
    rows_appended: int = 0
    error: str = ''


@dataclass
class StatePublishResult:
    """Aggregates results for both layers of one state."""
    abbr: str
    state: str
    status: str = 'pending'  # pending | success | partial | failed
    layers: list[LayerPublishResult] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    def mark_complete(self) -> None:
        """Set overall status from individual layer statuses."""
        statuses = {lr.status for lr in self.layers}
        if statuses == {'success'}:
            self.status = 'success'
        elif 'success' in statuses:
            self.status = 'partial'   # at least one layer published
        else:
            self.status = 'failed'


@dataclass
class PublishReport:
    """Aggregates publish results across all states for one run."""
    quarter: str
    started_at: str
    states_requested: list[str] = field(default_factory=list)
    results: list[StatePublishResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    finished_at: str = ''

    # ------------------------------------------------------------------ #
    # Mutators                                                             #
    # ------------------------------------------------------------------ #

    def add_state(self, abbr: str, state: str) -> StatePublishResult:
        result = StatePublishResult(abbr=abbr, state=state)
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
        path = out / f'publish_report_{self.quarter}_{timestamp}.json'
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
            f"Quarter  : {self.quarter}",
            f"Started  : {self.started_at}",
            f"Finished : {self.finished_at}",
            f"Elapsed  : {self.total_elapsed_seconds:.1f}s",
            f"States   : {self.success_count} fully published, "
            f"{self.failed_count} failed/partial",
        ]
        for sr in self.results:
            icon = {'success': '✓', 'partial': '~', 'failed': '✗', 'pending': '?'}.get(
                sr.status, '?'
            )
            lines.append(f"  {icon} {sr.abbr} ({sr.state}) | {sr.elapsed_seconds:.1f}s")
            for lr in sr.layers:
                layer_icon = '✓' if lr.status == 'success' else '✗'
                lines.append(
                    f"      {layer_icon} {lr.layer_type:<15} | "
                    f"rows={lr.rows_appended:,} | status={lr.status}"
                )
                for fe in lr.field_errors:
                    lines.append(f"            FIELD ERROR  : {fe}")
                for fw in lr.field_warnings:
                    lines.append(f"            FIELD WARNING: {fw}")
                if lr.error:
                    lines.append(f"            ERROR        : {lr.error}")
        return lines
