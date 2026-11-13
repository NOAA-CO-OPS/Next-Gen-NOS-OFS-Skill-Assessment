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
(standard library only, including the CSV dump) and side-effect-light so it
can be threaded through the existing pipeline without changing any
matching/pairing behaviour.

One ledger per run, one CSV per OFS
-----------------------------------
A run assesses several variables and, often, several whichcasts. Rather than
one ledger object -- and one CSV -- per (variable, whichcast) combination
(which produced up to sixteen near-identical files per run, issue #224), a
single run-scoped :class:`StationLedger` is created per OFS and each
variable/whichcast pass records through a lightweight
:class:`StationLedgerView` obtained from :meth:`StationLedger.for_context`.
Every record carries its own ``variable``/``whichcast``/``filetype`` stamp, so
the combined ``station_ledger_{ofs}.csv`` can be filtered on those columns.

Per-record stamping also fixes a mislabelling problem: ``write_ofs_ctlfile``
builds the model control files for *every* variable in one call, reached from
whichever variable the skill loop happens to process first. Drops belonging to
currents or temperature were therefore filed under the first variable's label.
Call sites that know the variable they are working on now pass it explicitly.

Cast-independent stages
-----------------------
Some stages operate on artifacts that are shared by every whichcast -- the
observation inventory, the observation control file, and the model control
file are all built once and reused by nowcast, forecast_b, and so on.
Stamping such a record with the whichcast that happened to trigger the work
would be misleading, so those stages (see :data:`CAST_INDEPENDENT_STAGES`)
are recorded with ``whichcast='all'``. A view for any whichcast sees them.

Typical use::

    ledger = StationLedger(ofs='necofs')
    view = ledger.for_context(variable='water_level',
                              whichcast='hindcast', filetype='stations')
    view.note_stage('obs_ctl', count_in=59, count_out=45)
    view.drop('8531680', stage='node_match',
              reason='nearest model station 6.2 km away (> 4.0 km cutoff)')
    view.log_summary(logger)
    ledger.to_csv(path)   # combined, merged with anything already there

The inventory reduction (stations that never reached the observation
control file) is reconciled by
:mod:`ofs_skill.model_processing.station_ledger_inventory`, which records
into a ledger or view built here.

Merging across invocations
--------------------------
The combined CSV is merged rather than overwritten, so a second whichcast
(or a pass that reused cached control files and never re-ran station
matching) adds to the file instead of replacing it. Rows on disk survive
unless the writing pass *executed* their stage. Several stages only ever
emit drop rows (:data:`DROP_ONLY_STAGES`), so a clean pass produces nothing
to supersede them with; those call sites declare themselves through
:meth:`StationLedger.mark_stage_run` instead. Without that declaration a
station dropped at ``pairing`` in January would still be reported as dropped
by a June run that assessed it successfully.

The ledger never raises on a bad record; recording problems must never take
down a skill run. All public methods are safe to call from worker threads
because appends to a Python list and dict writes keyed by unique station ID
are atomic under CPython's GIL for these simple operations, and we further
guard mutation with a lock.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Any

# Whichcast stamp used for stages whose artifacts are shared by every cast.
CAST_ALL = 'all'

# Stages that build or read an artifact shared across whichcasts. Records for
# these are stamped ``whichcast='all'`` rather than with the cast that
# happened to trigger the work.
CAST_INDEPENDENT_STAGES = frozenset({
    'inventory',
    'inventory_variable_flag',
    'obs_ctl',
    'obs_ctl_lines',
    'model_ctl',
    'node_match',
    'node_match_collision',
    'depth_match',
})

# Pipeline order of the stages the ledger knows about, earliest first. Used to
# sort the CSV and to pick the *first* stage that explains a station's absence
# when several records exist for the same station.
STAGE_ORDER: tuple[str, ...] = (
    'inventory',
    'inventory_variable_flag',
    'obs_ctl',
    'obs_ctl_lines',
    'model_ctl',
    'node_match',
    'node_match_collision',
    'depth_match',
    'id_mismatch',
    'temporal_overlap',
    'pairing',
)

# Stages whose drops are an expected, benign reduction rather than a loss
# worth a WARNING (a station that simply does not measure the variable).
EXPECTED_DROP_STAGES = frozenset({'inventory_variable_flag'})

# Stages that only ever produce drop records. They have no accompanying
# ``note_stage`` call, so a pass that executed them and dropped nothing emits
# no rows at all. Such a pass must still declare that it ran them (see
# :meth:`StationLedger.mark_stage_run`), otherwise the merging CSV write would
# find nothing to supersede and would retain an earlier run's drop rows
# forever -- reporting stations as dropped that the current run assessed
# successfully (issue #224).
DROP_ONLY_STAGES = frozenset({
    'inventory_variable_flag',
    'depth_match',
    'id_mismatch',
    'temporal_overlap',
    'pairing',
})

# The drop-only stages executed together by the observation/model pairing
# loop in ``get_skill.skill``; declared as a group whenever that loop runs.
PAIRING_STAGES: tuple[str, ...] = (
    'id_mismatch',
    'temporal_overlap',
    'pairing',
)

# Short ctl/name_var key -> the long variable name used in ``prop.var_list``.
LONG_VARIABLE_NAMES = {
    'wl': 'water_level',
    'temp': 'water_temperature',
    'salt': 'salinity',
    'cu': 'currents',
}

# Inverse of the above, for building the inventory ``has_<var>`` column name.
SHORT_VARIABLE_NAMES = {
    long_name: short for short, long_name in LONG_VARIABLE_NAMES.items()
}

# Column order of the combined ``station_ledger_{ofs}.csv``.
LEDGER_COLUMNS: tuple[str, ...] = (
    'ofs',
    'variable',
    'whichcast',
    'filetype',
    'record_type',
    'stage',
    'station_id',
    'reason',
    'count_in',
    'count_out',
    'note',
    'run_start',
    'run_end',
)

# Number of station IDs printed in full per stage in ``log_summary`` before
# the list is truncated. Inventory-level stages can hold hundreds of IDs.
_LOG_ID_LIMIT = 15

# Serialises the read-modify-write of the combined ledger CSV. Several
# variables, whichcasts, and forecast-cycle worker threads all refresh the
# one ``station_ledger_{ofs}.csv``; without this a concurrent emit could read
# the file while another had already truncated it and silently drop rows.
_WRITE_LOCK = threading.Lock()


def stage_rank(stage: str) -> int:
    """Return the pipeline position of ``stage`` (unknown stages sort last)."""
    try:
        return STAGE_ORDER.index(str(stage))
    except ValueError:
        return len(STAGE_ORDER)


def stamp_whichcast(stage: str, whichcast: str) -> str:
    """Return the whichcast a record for ``stage`` should carry.

    Cast-independent stages are stamped :data:`CAST_ALL`; everything else
    keeps the caller's whichcast.
    """
    return CAST_ALL if str(stage) in CAST_INDEPENDENT_STAGES else str(whichcast)


def _neutralize(value: Any) -> str:
    """Prefix spreadsheet formula triggers with a quote.

    Station IDs and reasons reach the CSV from external providers, so this
    guards the human-facing artifact against CSV formula injection.
    """
    text = '' if value is None else str(value)
    if text and text[0] in ('=', '+', '-', '@', '\t', '\r'):
        return "'" + text
    return text


@dataclass
class StageCount:
    """Count of stations entering and leaving a named pipeline stage."""

    stage: str
    count_in: int | None = None
    count_out: int | None = None
    note: str = ''
    variable: str = ''
    whichcast: str = ''
    filetype: str = ''


@dataclass
class DropRecord:
    """A single station dropped at a specific stage, with a reason."""

    station_id: str
    stage: str
    reason: str
    variable: str = ''
    whichcast: str = ''
    filetype: str = ''


def _matches_context(
    record: StageCount | DropRecord,
    variable: str,
    whichcast: str,
    filetype: str,
) -> bool:
    """Return True when ``record`` belongs to the given view context.

    A record stamped ``whichcast='all'`` (a cast-independent stage) belongs
    to every cast's view.
    """
    if record.variable != variable or record.filetype != filetype:
        return False
    return record.whichcast in (whichcast, CAST_ALL)


def _build_rows(
    ofs: str,
    run_start: str,
    run_end: str,
    stages: list[StageCount],
    drops: list[DropRecord],
) -> list[dict[str, str]]:
    """Flatten stage counts and drop records into CSV row dicts."""
    rows: list[dict[str, str]] = []
    for sc in stages:
        rows.append({
            'ofs': ofs,
            'variable': sc.variable,
            'whichcast': sc.whichcast,
            'filetype': sc.filetype,
            'record_type': 'stage',
            'stage': sc.stage,
            'station_id': '',
            'reason': '',
            'count_in': '' if sc.count_in is None else str(sc.count_in),
            'count_out': '' if sc.count_out is None else str(sc.count_out),
            'note': _neutralize(sc.note),
            'run_start': run_start,
            'run_end': run_end,
        })
    for rec in drops:
        rows.append({
            'ofs': ofs,
            'variable': rec.variable,
            'whichcast': rec.whichcast,
            'filetype': rec.filetype,
            'record_type': 'drop',
            'stage': rec.stage,
            'station_id': _neutralize(rec.station_id),
            'reason': _neutralize(rec.reason),
            'count_in': '',
            'count_out': '',
            'note': '',
            'run_start': run_start,
            'run_end': run_end,
        })
    return rows


def _row_key(row: dict[str, str]) -> tuple[str, str, str, str, str]:
    """Replacement key: rows sharing it are superseded by a fresh pass."""
    return (
        str(row.get('ofs', '')),
        str(row.get('variable', '')),
        str(row.get('whichcast', '')),
        str(row.get('filetype', '')),
        str(row.get('stage', '')),
    )


def _read_existing_rows(path: str) -> list[dict[str, str]]:
    """Read a previously written combined ledger CSV.

    Returns ``[]`` for a missing, unreadable, or foreign-format file so the
    write degrades to a plain overwrite rather than raising.
    """
    try:
        if not os.path.isfile(path):
            return []
        with open(path, newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return []
            # A file written by an older/other tool must not be merged: its
            # rows cannot be keyed reliably.
            if not {'ofs', 'variable', 'whichcast', 'stage'}.issubset(
                set(reader.fieldnames)
            ):
                return []
            return [
                {col: str(row.get(col) or '') for col in LEDGER_COLUMNS}
                for row in reader
            ]
    except (OSError, csv.Error, UnicodeDecodeError, ValueError):
        return []


def _atomic_write(path: str, rows: list[dict[str, str]]) -> None:
    """Write ``rows`` to ``path`` via a same-directory temp file + rename.

    The combined ledger is a read-modify-write shared by every variable,
    whichcast, and (under ``parallel_forecast_cycles``) several worker
    threads. Renaming a fully written temp file into place means a
    concurrent reader -- or a run interrupted mid-write -- never sees a
    truncated ledger.

    The CSV writer is pinned to ``\n`` so the artifact matches the LF line
    endings the rest of the pipeline's CSV output uses rather than the
    ``csv`` module's default CRLF.
    """
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    handle = tempfile.NamedTemporaryFile(  # pylint: disable=consider-using-with
        mode='w', newline='', encoding='utf-8', delete=False,
        dir=directory, prefix='.station_ledger_', suffix='.tmp',
    )
    replaced = False
    try:
        with handle:
            writer = csv.DictWriter(
                handle, fieldnames=list(LEDGER_COLUMNS), lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(handle.name, path)
        replaced = True
    finally:
        if not replaced:
            try:
                os.unlink(handle.name)
            except OSError:  # pragma: no cover - best effort cleanup
                pass


def _write_rows(
    path: str,
    rows: list[dict[str, str]],
    merge_existing: bool,
    superseded: set[tuple[str, str, str, str, str]] | None = None,
) -> str | None:
    """Write ``rows`` to ``path``, optionally merging retained older rows.

    Rows already on disk are kept unless this pass *executed* the stage they
    belong to, keyed by ``(ofs, variable, whichcast, filetype, stage)``.
    ``superseded`` carries those executed-stage keys; it is what lets a pass
    that ran a drop-only stage cleanly (no drops at all, and therefore no
    rows of its own) still clear a previous run's drop rows. Keys derived
    from ``rows`` are added to it, so a caller that passes nothing keeps the
    older rows-only behaviour.

    Returns the path on success and ``None`` on failure -- never raises, so
    a bookkeeping problem cannot abort a skill run.
    """
    try:
        with _WRITE_LOCK:
            combined = list(rows)
            if merge_existing:
                keys = {_row_key(row) for row in rows}
                keys |= set(superseded or ())
                combined = [
                    row for row in _read_existing_rows(path)
                    if _row_key(row) not in keys
                ] + combined
            combined.sort(key=lambda r: (
                str(r.get('variable', '')),
                str(r.get('whichcast', '')),
                stage_rank(r.get('stage', '')),
                str(r.get('stage', '')),
                str(r.get('record_type', '')),
                str(r.get('station_id', '')),
            ))
            _atomic_write(path, combined)
        return path
    except (OSError, csv.Error, ValueError, TypeError):  # pragma: no cover
        logging.getLogger(__name__).debug(
            'StationLedger.to_csv failed for %s', path, exc_info=True,
        )
        return None


def _log_summary(
    logger: logging.Logger,
    label: str,
    stages: list[StageCount],
    drops: list[DropRecord],
) -> None:
    """Emit a human-readable accounting of every drop stage."""
    try:
        logger.info('===== Station accounting ledger: %s =====', label)
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
            logger.info(
                '  no stations were dropped between the inventory and the skill table'
            )
            return

        grouped: dict[str, list[DropRecord]] = {}
        for rec in drops:
            grouped.setdefault(rec.stage, []).append(rec)

        unexpected = [r for r in drops if r.stage not in EXPECTED_DROP_STAGES]
        # The header must account for every line listed underneath it. With
        # inventory reconciliation feeding hundreds of expected drops into a
        # normal run, a header counting only the unexpected ones contradicts
        # its own listing (issue #224).
        if unexpected:
            logger.warning(
                '  %d station drop record(s) before the skill table '
                '(%d unexpected, %d expected), by stage:',
                len(drops), len(unexpected), len(drops) - len(unexpected),
            )
        else:
            logger.info(
                '  %d station drop record(s) before the skill table, all at '
                'expected stages, by stage:', len(drops),
            )
        for stage in sorted(grouped, key=stage_rank):
            recs = grouped[stage]
            ids = _format_ids(sorted({r.station_id for r in recs}))
            # An inventory flag drop means the station simply does not
            # measure this variable -- expected, so it stays at INFO.
            emit = logger.info if stage in EXPECTED_DROP_STAGES else logger.warning
            emit('    %-22s %d dropped: %s', stage, len(recs), ids)
            # One representative reason per stage keeps the log compact
            # while still explaining the mechanism to the user.
            logger.info('      e.g. %s: %s', recs[0].station_id, recs[0].reason)
    # Reporting-only code: any failure is swallowed (logged at debug)
    # because a summary formatting bug must never abort a skill run.
    except Exception:  # pylint: disable=broad-exception-caught  # pragma: no cover
        logger.debug('StationLedger.log_summary failed', exc_info=True)


def _format_ids(ids: list[str], limit: int = _LOG_ID_LIMIT) -> str:
    """Join station IDs for logging, truncating very long lists."""
    if len(ids) <= limit:
        return ', '.join(ids)
    shown = ', '.join(ids[:limit])
    return f'{shown}, and {len(ids) - limit} more'


@dataclass
class StationLedger:
    """Accumulates per-stage counts and per-station drop reasons.

    Parameters
    ----------
    ofs
        OFS identifier this ledger describes; one ledger covers a whole run.
    variable, whichcast, filetype
        Default context stamped onto records made directly on the ledger.
        The skill workflow leaves these blank and records through
        :meth:`for_context` views instead.
    run_start, run_end
        Run window, written to the CSV so a reader can tell which run a
        retained row came from.

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
    run_start: str = ''
    run_end: str = ''
    stages: list[StageCount] = field(default_factory=list)
    drops: list[DropRecord] = field(default_factory=list)
    stages_run: set[tuple[str, str, str, str]] = field(default_factory=set)
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
        self.stages_run = set(state.get('stages_run') or ())
        self._lock = threading.Lock()

    def __deepcopy__(self, memo: dict) -> StationLedger:
        """Return this very instance rather than a forked copy.

        ``prop`` is deep-copied per station in the plotting loop, per
        forecast cycle, and per variable in ``get_node_ofs``. Copying the
        ledger along with it forks the records: everything a worker writes
        lands on a copy that is discarded when the worker returns, so
        station-distance and ADCP bin-pruning drops never reached the CSV
        under ``parallel_variables=True`` (issue #224). It also copied the
        whole run's record list on every station, which is measurable on a
        several-hundred-station inventory.

        The ledger is an append-only, lock-guarded run-scoped sink, so
        sharing the single instance across threads is both correct and
        cheap. Pickling still produces an independent copy, which is the
        right behaviour across a process boundary.
        """
        memo[id(self)] = self
        return self

    # -- context -----------------------------------------------------------

    def for_context(
        self,
        variable: str = '',
        whichcast: str = '',
        filetype: str = '',
    ) -> StationLedgerView:
        """Return a view that records into this ledger under one context.

        The view exposes the same recording/reporting API as the ledger but
        stamps every record with ``variable``/``whichcast``/``filetype`` and
        reports only on records belonging to that context. Appends go
        through the root ledger's lock, so several views may be used
        concurrently.
        """
        return StationLedgerView(
            root=self,
            variable=str(variable),
            whichcast=str(whichcast),
            filetype=str(filetype),
        )

    # -- recording ---------------------------------------------------------

    def note_stage(
        self,
        stage: str,
        count_in: int | None = None,
        count_out: int | None = None,
        note: str = '',
        variable: str | None = None,
        whichcast: str | None = None,
        filetype: str | None = None,
    ) -> None:
        """Record how many stations entered/left a named pipeline stage.

        Either count may be omitted when it is not cheaply known at the
        call site; the summary tolerates ``None`` gaps. The context
        arguments override this ledger's defaults, letting a call site that
        knows which variable it is processing stamp the record correctly
        even when the attached context says otherwise.
        """
        try:
            record = StageCount(
                stage=str(stage),
                count_in=count_in,
                count_out=count_out,
                note=str(note),
                variable=(
                    self.variable if variable is None else str(variable)
                ),
                whichcast=stamp_whichcast(
                    stage,
                    self.whichcast if whichcast is None else str(whichcast),
                ),
                filetype=(
                    self.filetype if filetype is None else str(filetype)
                ),
            )
            with self._lock:
                self.stages.append(record)
                self.stages_run.add(
                    (record.variable, record.whichcast, record.filetype,
                     record.stage))
        except (TypeError, ValueError):  # pragma: no cover - defensive only
            pass

    def drop(
        self,
        station_id: Any,
        stage: str,
        reason: str,
        variable: str | None = None,
        whichcast: str | None = None,
        filetype: str | None = None,
    ) -> None:
        """Record that a single station was dropped at ``stage``.

        Recording is best-effort: a failure to stringify an exotic
        ``station_id`` is swallowed so that a bookkeeping bug can never abort
        a skill run. The exception scope is narrowed to the conversion/append
        errors that can plausibly occur here rather than a blanket catch.
        """
        try:
            record = DropRecord(
                station_id=str(station_id),
                stage=str(stage),
                reason=str(reason),
                variable=(
                    self.variable if variable is None else str(variable)
                ),
                whichcast=stamp_whichcast(
                    stage,
                    self.whichcast if whichcast is None else str(whichcast),
                ),
                filetype=(
                    self.filetype if filetype is None else str(filetype)
                ),
            )
            with self._lock:
                self.drops.append(record)
                self.stages_run.add(
                    (record.variable, record.whichcast, record.filetype,
                     record.stage))
        except (TypeError, ValueError):  # pragma: no cover - defensive only
            pass

    def mark_stage_run(
        self,
        stage: str,
        variable: str | None = None,
        whichcast: str | None = None,
        filetype: str | None = None,
    ) -> None:
        """Declare that this pass executed ``stage``, whether or not it
        produced any record.

        :data:`DROP_ONLY_STAGES` emit nothing when they run cleanly. Without
        this declaration the merging CSV write has no key to supersede, so a
        drop row written by an earlier run survives into a later run that
        assessed the station successfully -- and the pipeline auditor then
        joins that stale reason onto a healthy station (issue #224).
        """
        try:
            key = (
                self.variable if variable is None else str(variable),
                stamp_whichcast(
                    stage,
                    self.whichcast if whichcast is None else str(whichcast),
                ),
                self.filetype if filetype is None else str(filetype),
                str(stage),
            )
            with self._lock:
                self.stages_run.add(key)
        except (TypeError, ValueError):  # pragma: no cover - defensive only
            pass

    # -- reporting ---------------------------------------------------------

    def snapshot(self) -> tuple[list[StageCount], list[DropRecord]]:
        """Return a consistent copy of the recorded stages and drops."""
        with self._lock:
            return list(self.stages), list(self.drops)

    def replacement_keys(self) -> set[tuple[str, str, str, str, str]]:
        """Return the CSV row keys this pass is entitled to supersede.

        One key per ``(variable, whichcast, filetype, stage)`` the pass
        executed, prefixed with the OFS so it matches :func:`_row_key`.
        """
        with self._lock:
            return {
                (self.ofs, variable, whichcast, filetype, stage)
                for variable, whichcast, filetype, stage in self.stages_run
            }

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
        on individual stations and easy to miss in aggregate). Drops at a
        stage listed in :data:`EXPECTED_DROP_STAGES` stay at ``INFO``: a
        station that does not measure the variable is not a loss.
        """
        stages, drops = self.snapshot()
        _log_summary(logger, self._label(), stages, drops)

    def to_csv(self, path: str, merge_existing: bool = True) -> str | None:
        """Write the ledger to ``path`` as CSV.

        Both stage counts and drop records are written, distinguished by the
        ``record_type`` column, so the per-stage in/out tallies survive into
        the file alongside the reasons.

        With ``merge_existing`` (the default) rows already in the file are
        retained unless this ledger *executed* the stage they belong to,
        keyed by ``(ofs, variable, whichcast, filetype, stage)``. That is
        what lets one combined ``station_ledger_{ofs}.csv`` accumulate
        across invocations -- a later ``-ws forecast_b`` run does not erase
        the earlier ``-ws nowcast`` rows, and a run that reused cached
        control files does not erase the matching pass's ``node_match``
        records. Keying on executed stages rather than on the rows this pass
        happened to emit is what stops a clean re-run from inheriting an
        earlier run's ``pairing``/``temporal_overlap`` drops.

        Returns the path on success, ``None`` on failure (best-effort;
        never raises, so a failed write cannot abort a skill run). Cell
        values that begin with a spreadsheet formula trigger (``= + - @`` or
        a leading tab/CR) are prefixed with a single quote so the file is
        safe to open directly in Excel/LibreOffice.
        """
        stages, drops = self.snapshot()
        rows = _build_rows(self.ofs, self.run_start, self.run_end, stages, drops)
        return _write_rows(
            path, rows, merge_existing, self.replacement_keys())


@dataclass
class StationLedgerView:
    """A per-(variable, whichcast, filetype) window onto a shared ledger.

    Recording through the view stamps the context onto each record and
    appends to the root ledger; reporting through the view sees only the
    records belonging to that context (plus the cast-independent records,
    which belong to every cast). Holding no lock of its own, the view is
    deep-copy and pickle safe.
    """

    root: StationLedger
    variable: str = ''
    whichcast: str = ''
    filetype: str = ''

    def __deepcopy__(self, memo: dict) -> StationLedgerView:
        """Return this very instance, keeping the shared root ledger.

        A view is immutable once created, and deep-copying it would deep
        copy the root with it -- forking every record a worker thread then
        writes (issue #224). See :meth:`StationLedger.__deepcopy__`.
        """
        memo[id(self)] = self
        return self

    @property
    def ofs(self) -> str:
        """OFS identifier of the underlying ledger."""
        return self.root.ofs

    def _select(self) -> tuple[list[StageCount], list[DropRecord]]:
        stages, drops = self.root.snapshot()
        return (
            [s for s in stages
             if _matches_context(s, self.variable, self.whichcast, self.filetype)],
            [d for d in drops
             if _matches_context(d, self.variable, self.whichcast, self.filetype)],
        )

    # -- recording ---------------------------------------------------------

    def note_stage(
        self,
        stage: str,
        count_in: int | None = None,
        count_out: int | None = None,
        note: str = '',
        variable: str | None = None,
    ) -> None:
        """Record a stage count under this view's context."""
        self.root.note_stage(
            stage,
            count_in=count_in,
            count_out=count_out,
            note=note,
            variable=self.variable if variable is None else variable,
            whichcast=self.whichcast,
            filetype=self.filetype,
        )

    def drop(
        self,
        station_id: Any,
        stage: str,
        reason: str,
        variable: str | None = None,
    ) -> None:
        """Record a station drop under this view's context.

        ``variable`` overrides the view's own label for call sites that
        process a different variable than the one the view was opened for
        (the model control file writer loops over every variable in one
        call).
        """
        self.root.drop(
            station_id,
            stage=stage,
            reason=reason,
            variable=self.variable if variable is None else variable,
            whichcast=self.whichcast,
            filetype=self.filetype,
        )

    def mark_stage_run(self, stage: str, variable: str | None = None) -> None:
        """Declare that this pass executed ``stage`` under this context."""
        self.root.mark_stage_run(
            stage,
            variable=self.variable if variable is None else variable,
            whichcast=self.whichcast,
            filetype=self.filetype,
        )

    # -- reporting ---------------------------------------------------------

    @property
    def stages(self) -> list[StageCount]:
        """Stage records belonging to this context."""
        return self._select()[0]

    @property
    def drops(self) -> list[DropRecord]:
        """Drop records belonging to this context."""
        return self._select()[1]

    def has_stage(self, stage: str) -> bool:
        """Return True when this context recorded ``stage``."""
        return any(s.stage == stage for s in self.stages)

    @property
    def has_drops(self) -> bool:
        """Return True when this context recorded at least one drop."""
        return bool(self.drops)

    def drops_by_stage(self) -> dict[str, list[DropRecord]]:
        """Group this context's drop records by stage."""
        grouped: dict[str, list[DropRecord]] = {}
        for rec in self.drops:
            grouped.setdefault(rec.stage, []).append(rec)
        return grouped

    def _label(self) -> str:
        parts = [
            p for p in
            (self.root.ofs, self.variable, self.whichcast, self.filetype) if p
        ]
        return ' / '.join(parts) if parts else '(unlabelled)'

    def log_summary(self, logger: logging.Logger) -> None:
        """Log the accounting for this context only."""
        stages, drops = self._select()
        _log_summary(logger, self._label(), stages, drops)

    def replacement_keys(self) -> set[tuple[str, str, str, str, str]]:
        """Executed-stage row keys belonging to this view's context."""
        return {
            key for key in self.root.replacement_keys()
            if key[1] == self.variable
            and key[3] == self.filetype
            and key[2] in (self.whichcast, CAST_ALL)
        }

    def to_csv(self, path: str, merge_existing: bool = True) -> str | None:
        """Write only this context's records to ``path``."""
        stages, drops = self._select()
        rows = _build_rows(
            self.root.ofs, self.root.run_start, self.root.run_end, stages, drops
        )
        return _write_rows(
            path, rows, merge_existing, self.replacement_keys())
