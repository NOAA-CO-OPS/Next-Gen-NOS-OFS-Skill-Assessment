"""
Inventory -> observation control file reconciliation for the station ledger.

The station-drop ledger (:mod:`ofs_skill.model_processing.station_ledger`)
explains why a station stopped progressing through the 1-D skill workflow.
Its earliest stage is the one the workflow itself cannot instrument cheaply:
the reduction from ``inventory_all_{ofs}.csv`` -- every station the pipeline
knows about -- to the observation control file, the stations for which usable
observations were actually retrieved.

That reduction happens two layers below the skill workflow, and only when the
control file is absent. Instrumenting it there would populate the records on
the very first run and silently omit them from every cached run afterwards.
Reconciling the two files after the fact, as this module does, works
identically on fresh and cached runs (issue #224).

Standard library only, and best-effort throughout: a reconciliation problem
is logged at debug level and never aborts a skill run.
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any

from ofs_skill.model_processing.station_ledger import (
    SHORT_VARIABLE_NAMES,
    StationLedger,
    StationLedgerView,
)


def _inventory_flagged(row: dict[str, Any], flag_column: str) -> bool:
    """Return True when the inventory row is flagged for this variable.

    A missing flag column is treated as flagged so an inventory written by
    an older revision (or a hand-made one) still reconciles instead of
    reporting every station as dropped.
    """
    if flag_column not in row:
        return True
    value = row.get(flag_column)
    if value is None:
        return True
    return str(value).strip().upper() == 'TRUE'


def reconcile_inventory(
    ledger: StationLedger | StationLedgerView | None,
    inventory_path: str,
    ctl_station_ids: Any,
    variable: str,
    logger: logging.Logger | None = None,
) -> None:
    """Record the inventory -> observation control file reduction.

    Two distinct causes are recorded separately, because they mean different
    things to the user:

    ``inventory_variable_flag``
        The inventory's ``has_<var>`` column is False, so the station was
        never queried for this variable at all (very common for currents,
        where most tide gauges carry no ADCP).
    ``obs_ctl``
        The station was queried but produced no usable observations for the
        run window, so it never reached the control file.

    Args:
        ledger: Ledger or view to record into; ``None`` is a no-op. A
            context that already holds an ``inventory`` stage is left
            alone, so a second whichcast in the same run does not
            duplicate the records.
        inventory_path: Path to ``inventory_all_{ofs}.csv``.
        ctl_station_ids: Station IDs parsed from the observation control
            file. CO-OPS ADCP virtual bin IDs (``{parent}_b{NN}``) count for
            their parent station.
        variable: Long variable name (e.g. ``water_level``).
        logger: Optional logger for the debug breadcrumbs.
    """
    log = logger or logging.getLogger(__name__)
    if ledger is None:
        return
    if ledger.has_stage('inventory'):
        # Already reconciled for this variable earlier in the run (the
        # inventory and the obs control file are shared by every cast, so
        # the second cast would only duplicate the records).
        log.debug(
            'Station ledger: inventory already reconciled for this context',
        )
        return
    try:
        if not inventory_path or not os.path.isfile(inventory_path):
            log.debug(
                'Station ledger: no inventory file at %s; inventory '
                'reconciliation skipped', inventory_path,
            )
            return
        with open(inventory_path, newline='', encoding='utf-8') as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, csv.Error, UnicodeDecodeError, ValueError):
        log.debug(
            'Station ledger: could not read inventory %s', inventory_path,
            exc_info=True,
        )
        return

    try:
        # Imported here (rather than at module scope) so this module stays
        # importable without the obs_retrieval package.
        from ofs_skill.obs_retrieval.currents_bins_override import (  # pylint: disable=import-outside-toplevel
            split_virtual_currents_id,
        )

        present: set[str] = set()
        for sid in (ctl_station_ids or []):
            text = str(sid).strip().lower()
            if not text:
                continue
            present.add(text)
            parent, bin_num = split_virtual_currents_id(text)
            if bin_num is not None:
                present.add(parent)

        short = SHORT_VARIABLE_NAMES.get(variable, variable)
        flag_column = f'has_{short}'

        # ``inventory_variable_flag`` is drop-only: an inventory in which
        # every station reports the variable produces no rows at all. Declare
        # it so a clean pass still supersedes an earlier run's rows.
        ledger.mark_stage_run('inventory_variable_flag', variable=variable)

        total = 0
        flagged: list[str] = []
        for row in rows:
            station_id = str(row.get('ID', '') or '').strip()
            if not station_id:
                continue
            total += 1
            if _inventory_flagged(row, flag_column):
                flagged.append(station_id)
            else:
                ledger.drop(
                    station_id,
                    stage='inventory_variable_flag',
                    reason=(
                        f'inventory {flag_column} flag is False: station '
                        f'does not report {variable}'
                    ),
                    variable=variable,
                )

        ledger.note_stage(
            'inventory',
            count_in=total,
            count_out=len(flagged),
            note=(
                f'stations in {os.path.basename(inventory_path)} '
                f'({flag_column} True)'
            ),
            variable=variable,
        )

        reached = 0
        for station_id in flagged:
            if station_id.lower() in present:
                reached += 1
                continue
            ledger.drop(
                station_id,
                stage='obs_ctl',
                reason=(
                    f'no {variable} observations retrievable for the run '
                    f'window, or the provider returned no data when the '
                    f'observation control file was built'
                ),
                variable=variable,
            )

        ledger.note_stage(
            'obs_ctl',
            count_in=len(flagged),
            count_out=reached,
            note='inventory stations that reached the obs station ctl file',
            variable=variable,
        )
    # Bookkeeping only: never let a reconciliation problem abort a run.
    except Exception:  # pylint: disable=broad-exception-caught  # pragma: no cover
        log.debug('Station ledger: inventory reconciliation failed',
                  exc_info=True)
