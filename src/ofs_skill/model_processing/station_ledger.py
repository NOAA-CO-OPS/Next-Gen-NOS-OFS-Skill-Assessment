"""
Station-drop ledger.

The 1-D skill workflow reduces the set of observation stations at several
independent points before a station finally lands (or fails to land) in the
per-variable skill CSV. Historically these reductions were scattered across
modules and only surfaced as ``INFO``/``ERROR`` log lines, which made the
final station count look non-deterministic to users (see issue #200, active
issue #1: "45 raw obs stations -> 32 in the CSV, and changing the search
radius swaps which IDs survive without changing the count").

This module provides a single, in-memory ledger that records, per station,
the stage at which it was dropped and why. It is deliberately dependency-free
(no pandas requirement to record; pandas only used for the optional CSV dump)
and side-effect-light so it can be threaded through the existing pipeline
without changing any matching/pairing behaviour.

Typical use::

    ledger = StationLedger(ofs='necofs', variable='water_level',
                           whichcast='hindcast', filetype='stations')
    ledger.note_stage('obs_inventory', count_in=120)
    ledger.note_stage('obs_data_available', count_in=59, count_out=45)
    ...
    ledger.drop('8531680', stage='node_match',
                reason='nearest model station 6.2 km away (> 4.0 km cutoff)')
    ...
    ledger.log_summary(logger)
    ledger.to_csv(path)   # optional

The ledger never raises on a bad record; recording problems must never take
down a skill run. All public methods are safe to call from worker threads
because appends to a Python list and dict writes keyed by unique station ID
are atomic under CPython's GIL for these simple operations, and we further
guard mutation with a lock.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class StageCount:
    """Count of stations entering and leaving a named pipeline stage."""

    stage: str
    count_in: int | None = None
    count_out: int | None = None
    note: str = ''


@dataclass
class DropRecord:
    """A single station dropped at a specific stage, with a reason."""

    station_id: str
    stage: str
    reason: str


@dataclass
class StationLedger:
    """Accumulates per-stage counts and per-station drop reasons.

    Parameters
    ----------
    ofs, variable, whichcast, filetype
        Identifying context for the run this ledger describes. Used only
        for labelling the summary output; may be left blank.

    Notes
    -----
    The instance holds a ``threading.Lock`` to guard mutation from worker
    threads. That lock is excluded from ``__init__`` (``init=False``) and is
    recreated on unpickling via ``__getstate__``/``__setstate__`` so the
    ledger can be safely deep-copied or sent across a process boundary; a
    fresh lock is created for the copy rather than attempting (and failing)
    to pickle the original. In the current workflow the ledger is shared
    between threads via a shallow ``copy.copy`` of ``prop`` (same lock,
    intentional); the pickling support only guards future use under
    process-based parallelism.
    """

    ofs: str = ''
    variable: str = ''
    whichcast: str = ''
    filetype: str = ''
    stages: list[StageCount] = field(default_factory=list)
    drops: list[DropRecord] = field(default_factory=list)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False, init=False
    )

    def __getstate__(self) -> dict:
        """Exclude the unpicklable lock from the serialised state."""
        state = self.__dict__.copy()
        state.pop('_lock', None)
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state and give the copy its own fresh lock."""
        self.__dict__.update(state)
        self._lock = threading.Lock()

    # -- recording ---------------------------------------------------------

    def note_stage(
        self,
        stage: str,
        count_in: int | None = None,
        count_out: int | None = None,
        note: str = '',
    ) -> None:
        """Record how many stations entered/left a named pipeline stage.

        Either count may be omitted when it is not cheaply known at the
        call site; the summary tolerates ``None`` gaps.
        """
        with self._lock:
            self.stages.append(
                StageCount(
                    stage=str(stage),
                    count_in=count_in,
                    count_out=count_out,
                    note=str(note),
                )
            )

    def drop(self, station_id: Any, stage: str, reason: str) -> None:
        """Record that a single station was dropped at ``stage``.

        Recording is best-effort: a failure to stringify an exotic
        ``station_id`` is swallowed so that a bookkeeping bug can never abort
        a skill run. The exception scope is narrowed to the conversion/append
        errors that can plausibly occur here rather than a blanket catch.
        """
        try:
            with self._lock:
                self.drops.append(
                    DropRecord(
                        station_id=str(station_id),
                        stage=str(stage),
                        reason=str(reason),
                    )
                )
        except (TypeError, ValueError):  # pragma: no cover - defensive only
            pass

    # -- reporting ---------------------------------------------------------

    def has_stage(self, stage: str) -> bool:
        """Return True when ``stage`` was recorded via ``note_stage``.

        Lets callers distinguish a ledger that witnessed a pipeline stage
        (e.g. fresh node matching) from one attached to a pass that reused
        cached artifacts and never ran that stage.
        """
        with self._lock:
            return any(s.stage == stage for s in self.stages)

    @property
    def has_drops(self) -> bool:
        """Return True when at least one station drop was recorded."""
        with self._lock:
            return bool(self.drops)

    def drops_by_stage(self) -> dict[str, list[DropRecord]]:
        """Group drop records by the stage that dropped them."""
        grouped: dict[str, list[DropRecord]] = {}
        with self._lock:
            for rec in self.drops:
                grouped.setdefault(rec.stage, []).append(rec)
        return grouped

    def _label(self) -> str:
        parts = [
            p for p in (self.ofs, self.variable, self.whichcast, self.filetype) if p
        ]
        return ' / '.join(parts) if parts else '(unlabelled)'

    def log_summary(self, logger: logging.Logger) -> None:
        """Emit a human-readable accounting of every drop stage.

        Uses ``WARNING`` level for the drop tallies so they are visible in
        default-level logs (previously many drops were only ``INFO``/``ERROR``
        on individual stations and easy to miss in aggregate).
        """
        try:
            logger.info('===== Station accounting ledger: %s =====', self._label())
            with self._lock:
                stages = list(self.stages)
                drops = list(self.drops)

            for sc in stages:
                if sc.count_in is not None and sc.count_out is not None:
                    logger.info(
                        '  stage %-22s %s in -> %s out%s',
                        sc.stage,
                        sc.count_in,
                        sc.count_out,
                        f'  ({sc.note})' if sc.note else '',
                    )
                else:
                    known = sc.count_out if sc.count_out is not None else sc.count_in
                    logger.info(
                        '  stage %-22s %s station(s)%s',
                        sc.stage,
                        known,
                        f'  ({sc.note})' if sc.note else '',
                    )

            if not drops:
                logger.info('  no stations were dropped after obs retrieval')
                return

            grouped: dict[str, list[DropRecord]] = {}
            for rec in drops:
                grouped.setdefault(rec.stage, []).append(rec)

            logger.warning(
                '  %d station(s) dropped after obs retrieval, by stage:',
                len(drops),
            )
            for stage, recs in grouped.items():
                ids = ', '.join(sorted({r.station_id for r in recs}))
                logger.warning('    %-22s %d dropped: %s', stage, len(recs), ids)
                # One representative reason per stage keeps the log compact
                # while still explaining the mechanism to the user.
                logger.info('      e.g. %s: %s', recs[0].station_id, recs[0].reason)
        # Reporting-only code: any failure is swallowed (logged at debug)
        # because a summary formatting bug must never abort a skill run.
        except Exception:  # pylint: disable=broad-exception-caught  # pragma: no cover
            logger.debug('StationLedger.log_summary failed', exc_info=True)

    def to_csv(self, path: str) -> str | None:
        """Write the per-station drop records to ``path`` as CSV.

        Returns the path on success, ``None`` on failure (best-effort;
        never raises, so a failed write cannot abort a skill run). Requires
        pandas, imported lazily so importing this module stays cheap.

        Cell values that begin with a spreadsheet formula trigger
        (``= + - @`` or a leading tab/CR) are prefixed with a single quote so
        the file is safe to open directly in Excel/LibreOffice. Station IDs
        come from external providers, so this guards against CSV formula
        injection in the human-facing artifact.
        """
        try:
            # Imported lazily so importing this module stays cheap.
            import pandas as pd  # pylint: disable=import-outside-toplevel

            def _neutralize(value: str) -> str:
                text = str(value)
                if text and text[0] in ('=', '+', '-', '@', '\t', '\r'):
                    return "'" + text
                return text

            with self._lock:
                rows = [
                    {
                        'ofs': self.ofs,
                        'variable': self.variable,
                        'whichcast': self.whichcast,
                        'filetype': self.filetype,
                        'station_id': _neutralize(r.station_id),
                        'dropped_at_stage': r.stage,
                        'reason': _neutralize(r.reason),
                    }
                    for r in self.drops
                ]
            pd.DataFrame(
                rows,
                columns=[
                    'ofs',
                    'variable',
                    'whichcast',
                    'filetype',
                    'station_id',
                    'dropped_at_stage',
                    'reason',
                ],
            ).to_csv(path, index=False)
            return path
        except (OSError, ImportError, ValueError):  # pragma: no cover
            logging.getLogger(__name__).debug(
                'StationLedger.to_csv failed for %s', path, exc_info=True,
            )
            return None
