"""
Audit-trail report for the PLR privacy redaction step.

A RedactionReport is created at the start of redact.py, updated as each
state completes, and written to a timestamped JSON file in the logs
directory.  The JSON file serves as the compliance record showing exactly
which states were processed, how many parcels were redacted, and when.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from configs.settings import LOG_DIR


@dataclass
class RedactionResult:
    """Tracks the outcome of a privacy redaction run for one state."""
    abbr: str
    state: str
    status: str = 'pending'           # pending | success | failed
    parcels_inspected: int = 0        # total parcels in the final output FC
    parcels_redacted: int = 0         # parcels that had ownership info nulled
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def mark_success(self) -> None:
        if self.status != 'failed':
            self.status = 'success'

    def mark_failed(self, error: str) -> None:
        self.status = 'failed'
        self.errors.append(error)


@dataclass
class RedactionReport:
    """Aggregates redaction results across all processed states."""
    quarter: str
    started_at: str
    states_requested: list[str] = field(default_factory=list)
    results: list[RedactionResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    finished_at: str = ''

    # ------------------------------------------------------------------ #
    # Mutators                                                             #
    # ------------------------------------------------------------------ #

    def add_state(self, abbr: str, state: str) -> RedactionResult:
        result = RedactionResult(abbr=abbr, state=state)
        self.results.append(result)
        return result

    # ------------------------------------------------------------------ #
    # Finalise and write                                                   #
    # ------------------------------------------------------------------ #

    def finalize(self, total_elapsed: float) -> None:
        self.total_elapsed_seconds = round(total_elapsed, 2)
        self.finished_at = time.strftime('%Y-%m-%dT%H:%M:%S')

    def write(self, output_dir: Optional[Path] = None) -> Path:
        """Write the report to a timestamped JSON file and return its path."""
        out = output_dir or LOG_DIR
        out.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        path = out / f'redaction_report_{self.quarter}_{timestamp}.json'
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
        return sum(1 for r in self.results if r.status == 'failed')

    @property
    def total_redacted(self) -> int:
        return sum(r.parcels_redacted for r in self.results)

    def summary_lines(self) -> list[str]:
        lines = [
            f"Quarter  : {self.quarter}",
            f"Started  : {self.started_at}",
            f"Finished : {self.finished_at}",
            f"Elapsed  : {self.total_elapsed_seconds:.1f}s",
            f"States   : {self.success_count} succeeded, {self.failed_count} failed",
            f"Redacted : {self.total_redacted:,} parcel(s) total",
        ]
        for r in self.results:
            icon = '✓' if r.status == 'success' else '✗'
            lines.append(
                f"  {icon} {r.abbr} ({r.state}) | "
                f"inspected={r.parcels_inspected:,} | "
                f"redacted={r.parcels_redacted:,} | "
                f"{r.elapsed_seconds:.1f}s"
            )
            for err in r.errors:
                lines.append(f"      ERROR: {err}")
        return lines
