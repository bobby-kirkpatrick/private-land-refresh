"""
Run summary report for the PLR pipeline.

A ``RunReport`` is created at the start of ``main.main()``, updated
incrementally as each state completes each stage, and written to a
timestamped JSON file in the logs directory when the pipeline finishes.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from configs.settings import LOG_DIR


@dataclass
class StateResult:
    """Tracks the outcome of the full pipeline for one state."""
    abbr: str
    state: str
    status: str = 'pending'           # pending | success | failed
    parcel_count: int = 0
    agreement_count: int = 0
    agreement_pct: float = 0.0
    elapsed_seconds: float = 0.0
    failed_stages: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def mark_stage_failed(self, stage: str, error: str) -> None:
        self.status = 'failed'
        self.failed_stages.append(stage)
        self.errors.append(f"[{stage}] {error}")

    def mark_success(self) -> None:
        if self.status != 'failed':
            self.status = 'success'


@dataclass
class RunReport:
    """Aggregates results across all states for one pipeline run."""
    quarter: str
    started_at: str
    states_requested: list[str] = field(default_factory=list)
    state_results: list[StateResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    finished_at: str = ''

    # ------------------------------------------------------------------ #
    # Mutators                                                             #
    # ------------------------------------------------------------------ #

    def add_state(self, abbr: str, state: str) -> StateResult:
        """Register a state and return its mutable StateResult."""
        result = StateResult(abbr=abbr, state=state)
        self.state_results.append(result)
        return result

    def get_state(self, abbr: str) -> Optional[StateResult]:
        for r in self.state_results:
            if r.abbr == abbr:
                return r
        return None

    # ------------------------------------------------------------------ #
    # Finalise and write                                                   #
    # ------------------------------------------------------------------ #

    def finalize(self, total_elapsed: float) -> None:
        self.total_elapsed_seconds = round(total_elapsed, 2)
        self.finished_at = time.strftime('%Y-%m-%dT%H:%M:%S')

    def write(self, output_dir: Optional[Path] = None) -> Path:
        """
        Serialise the report to a timestamped JSON file.

        Parameters
        ----------
        output_dir:
            Directory to write the file into.  Defaults to ``LOG_DIR``
            from ``configs.settings``.

        Returns
        -------
        Path
            The path of the written file.
        """
        out = output_dir or LOG_DIR
        out.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        report_path = out / f'run_report_{self.quarter}_{timestamp}.json'
        with open(report_path, 'w', encoding='utf-8') as fp:
            json.dump(asdict(self), fp, indent=2)
        return report_path

    # ------------------------------------------------------------------ #
    # Summary helpers                                                      #
    # ------------------------------------------------------------------ #

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.state_results if r.status == 'success')

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.state_results if r.status == 'failed')

    def summary_lines(self) -> list[str]:
        lines = [
            f"Quarter : {self.quarter}",
            f"Started : {self.started_at}",
            f"Finished: {self.finished_at}",
            f"Elapsed : {self.total_elapsed_seconds:.1f}s",
            f"States  : {self.success_count} succeeded, {self.failed_count} failed",
        ]
        for r in self.state_results:
            icon = '✓' if r.status == 'success' else '✗'
            lines.append(
                f"  {icon} {r.abbr} ({r.state}) | "
                f"{r.parcel_count:,} parcels | "
                f"{r.agreement_pct:.1f}% agreement | "
                f"{r.elapsed_seconds:.1f}s"
            )
            for err in r.errors:
                lines.append(f"      ERROR: {err}")
        return lines
