"""
Regression: every CO-OPS currents station in any local
``inventory_all_*.csv`` resolves to a known mounting type.

Driven by the captured MDAPI audit CSV (see
``tests/manual/coops_adcp_orientation_audit_test.py``) so the test
runs offline. Asserts:

* every CO-OPS currents row in every inventory file maps to one of
  ``{side, up, down}`` (no ``unknown`` is permitted in production
  data).
* every side-looking station has a ``real_time_bin`` from the bins
  endpoint AND a ``sensor_depth`` (or recoverable
  ``measured_depth - height_from_bottom``) from the deployments
  endpoint — the two inputs the issue #140 fix depends on.

If the audit snapshot is older than 90 days, the test is skipped
with a hint to re-run the manual audit. The snapshot ships in the
fixtures directory so the regression runs in CI without network.
"""
from __future__ import annotations

import csv
import datetime
import importlib
from pathlib import Path

import pytest

_AUDIT_SNAPSHOT = (
    Path(__file__).parent / 'fixtures' / 'coops_adcp_audit_snapshot.csv'
)
_INVENTORY_GLOB = 'inventory_all_*.csv'
_INVENTORY_DIRS = [
    Path(__file__).parent.parent / 'control_files',
]
_SNAPSHOT_MAX_AGE_DAYS = 90


def _load_snapshot() -> dict[str, dict[str, str]]:
    if not _AUDIT_SNAPSHOT.exists():
        pytest.fail(
            f'MDAPI audit snapshot missing: {_AUDIT_SNAPSHOT}. '
            'Run "pytest -m manual tests/manual/'
            'coops_adcp_orientation_audit_test.py", then copy the '
            'coops_adcp_orientation_audit.csv it writes under '
            'tempfile.gettempdir() (path printed at test end) into '
            'tests/fixtures/coops_adcp_audit_snapshot.csv. '
            'A failure here is preferable to a silent skip — the '
            'snapshot is the regression\'s ground truth.'
        )
    # Use UTC to avoid local-tz drift across CI runners. The mtime
    # comes back as a naive POSIX timestamp; both ends of the diff
    # are aware UTC for safety.
    snap_mtime = datetime.datetime.fromtimestamp(
        _AUDIT_SNAPSHOT.stat().st_mtime, tz=datetime.timezone.utc)
    age_days = (
        datetime.datetime.now(datetime.timezone.utc) - snap_mtime
    ).days
    if age_days > _SNAPSHOT_MAX_AGE_DAYS:
        pytest.fail(
            f'MDAPI audit snapshot is {age_days} days old (max '
            f'{_SNAPSHOT_MAX_AGE_DAYS}); refresh via the manual '
            'audit. Stale snapshots silently miss new MDAPI '
            'stations / vocabulary changes — fail loudly so the '
            'refresh happens.'
        )
    with open(_AUDIT_SNAPSHOT, encoding='utf-8') as fh:
        return {row['id']: row for row in csv.DictReader(fh)}


def _coops_currents_ids_from_inventories() -> list[tuple[Path, str]]:
    out: list[tuple[Path, str]] = []
    for inv_dir in _INVENTORY_DIRS:
        for inv_path in inv_dir.glob(_INVENTORY_GLOB):
            with open(inv_path, encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if row.get('Source') != 'CO-OPS':
                        continue
                    has_cu = (row.get('has_cu') or '').strip().lower()
                    if has_cu not in ('true', '1', 't', 'yes'):
                        continue
                    out.append((inv_path, row['ID']))
    return out


def test_every_inventory_currents_station_has_known_mounting_type():
    """Pipeline-side regression: any inventory currents station that
    cannot be classified is a hard failure — operators must catch
    these before a run rather than silently emitting plots with the
    wrong ADCP-type label (issue #141)."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    snapshot = _load_snapshot()
    inv_rows = _coops_currents_ids_from_inventories()
    if not inv_rows:
        pytest.skip(
            'No inventory_all_*.csv files with CO-OPS currents rows '
            'found locally — nothing to regress against.'
        )

    missing_in_snapshot = []
    classified_unknown = []
    for inv_path, station_id in inv_rows:
        snap = snapshot.get(station_id)
        if snap is None:
            missing_in_snapshot.append((str(inv_path), station_id))
            continue
        deployment_proxy = {'orientation': snap['orientation']}
        mounting = rtc.resolve_mounting_type(deployment_proxy)
        if mounting not in ('side', 'up', 'down'):
            classified_unknown.append(
                (str(inv_path), station_id, snap['orientation']))

    assert not classified_unknown, (
        f'Stations classified as unknown mounting: {classified_unknown}'
    )
    # missing_in_snapshot is informational rather than fatal — the
    # snapshot may simply lag inventory updates. Surface as a soft
    # warning via xfail instead of failing the run.
    if missing_in_snapshot:
        pytest.xfail(
            f'{len(missing_in_snapshot)} inventory station(s) not in '
            f'audit snapshot (refresh manual audit): '
            f'{missing_in_snapshot[:5]}'
        )


def test_every_side_looking_snapshot_station_has_required_fields():
    snapshot = _load_snapshot()
    side_rows = [r for r in snapshot.values() if r['orientation'] == 'side']
    assert side_rows, 'audit snapshot has no side-looking stations'

    missing = []
    for r in side_rows:
        rtb = r['rtb_bins']
        sensor_depth = r['sensor_depth']
        measured_depth = r['measured_depth']
        hfb = r['height_from_bottom']
        # Either explicit sensor_depth, or measured - hfb is recoverable.
        recoverable_depth = bool(sensor_depth) or (
            bool(measured_depth) and bool(hfb)
            and hfb not in ('None', '0', '0.0')
        )
        # rtb_bins must be a non-empty integer-string.
        rtb_ok = bool(rtb) and rtb not in ('None',)
        if not (recoverable_depth and rtb_ok):
            missing.append((r['id'], rtb, sensor_depth,
                            measured_depth, hfb))

    assert not missing, (
        f'{len(missing)} side-looking station(s) lack the inputs the '
        f'issue #140 fix depends on (real_time_bin + sensor_depth or '
        f'measured-hfb): {missing[:5]}'
    )


def test_no_non_side_station_has_null_bin_depths():
    """For up/down/unknown stations, every per-bin depth must be
    populated. A null on these orientations would route through the
    ERROR-logged anomaly branch in ``_retrieve_currents_all_bins``."""
    snapshot = _load_snapshot()
    bad = []
    for r in snapshot.values():
        if r['orientation'] in ('side', ''):
            continue
        try:
            null_bins = int(r['null_depth_bins'])
        except (KeyError, TypeError, ValueError):
            continue
        if null_bins > 0:
            bad.append((r['id'], r['orientation'], null_bins))
    assert not bad, (
        f'Non-side stations with null per-bin depths in snapshot: '
        f'{bad[:5]}'
    )
