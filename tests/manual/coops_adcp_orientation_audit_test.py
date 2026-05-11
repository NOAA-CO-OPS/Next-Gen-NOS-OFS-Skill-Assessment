"""
Live MDAPI audit of every CO-OPS currents station (issues #140, #141).

Produces a CSV at ``<tempfile.gettempdir()>/coops_adcp_orientation_audit.csv``
(``/tmp/...`` on POSIX, ``%TEMP%\\...`` on Windows) with one row per
currents station, recording the deployment-level ``orientation`` and
the per-bin shape needed to validate the pipeline's mounting-type
classification. Asserts the invariants we depend on:

* every station resolves to ``{side, up, down}`` (no empty / unknown)
* ``side`` ⇒ all per-bin ``depth`` are null AND ``real_time_bin`` is
  populated on the bins endpoint AND ``sensor_depth`` is populated on
  the deployments endpoint
* ``up`` ⇒ all per-bin ``depth`` populated AND bin-depth strictly
  decreases as bin number increases (bin 1 is deepest, bin N nearest
  surface)
* ``down`` ⇒ all per-bin ``depth`` populated AND bin-depth strictly
  increases as bin number increases (bin 1 nearest surface, bin N
  deepest)

Marked ``manual`` because it hits the live MDAPI; opt-in only via
``pytest -m manual``.
"""
from __future__ import annotations

import csv
import json
import os
import tempfile
import time
import urllib.error
import urllib.request

import pytest

_BASE = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi'
_AUDIT_OUT = os.path.join(
    tempfile.gettempdir(), 'coops_adcp_orientation_audit.csv')
_FETCH_DELAY_SEC = 0.1
_HTTP_TIMEOUT_SEC = 15
_FETCH_MAX_ATTEMPTS = 4
_FETCH_BACKOFF_SEC = 2.0


def _fetch(url: str) -> dict | None:
    """GET + JSON decode with retry on transient network failure.

    The audit issues ~176 sequential MDAPI calls; any single timeout or
    socket blip would otherwise flip a healthy station's row to
    ``FETCH_FAIL`` and fail the whole run. Retries on URLError /
    timeout / JSONDecodeError with linear backoff. Returns ``None`` only
    when every attempt fails — that is the genuine "metadata
    unavailable" signal the assertion downstream is checking for.
    """
    last_exc: Exception | None = None
    for attempt in range(_FETCH_MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(
                url, timeout=_HTTP_TIMEOUT_SEC
            ) as r:
                return json.load(r)
        except (urllib.error.URLError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt < _FETCH_MAX_ATTEMPTS - 1:
                time.sleep(_FETCH_BACKOFF_SEC * (attempt + 1))
    # Final failure — emit a hint to the test logs so the next CI run
    # can distinguish a real outage from a flaky transient.
    print(f'  _fetch gave up on {url} after '
          f'{_FETCH_MAX_ATTEMPTS} attempts: {last_exc!r}')
    return None


def _classify_bin_depth_ordering(
    bins: list[dict],
) -> str:
    depths: list[float] = [
        float(b['depth']) for b in bins if b.get('depth') is not None
    ]
    if len(depths) < 2:
        return 'n/a'
    if all(depths[i] < depths[i + 1] for i in range(len(depths) - 1)):
        return 'ascending'
    if all(depths[i] > depths[i + 1] for i in range(len(depths) - 1)):
        return 'descending'
    return 'mixed'


@pytest.mark.manual
def test_audit_all_coops_currents_stations() -> None:
    top = _fetch(f'{_BASE}/stations.json?type=currents&units=metric')
    assert top is not None, 'MDAPI stations endpoint unreachable'
    station_ids = [s['id'] for s in top.get('stations', [])]
    assert len(station_ids) > 50, (
        f'Unexpectedly small currents station list: {len(station_ids)}'
    )

    rows = []
    for sid in station_ids:
        dep = _fetch(f'{_BASE}/stations/{sid}/deployments.json?units=metric')
        bins_payload = _fetch(f'{_BASE}/stations/{sid}/bins.json?units=metric')
        if dep is None or bins_payload is None:
            rows.append({'id': sid, 'orientation': 'FETCH_FAIL'})
            continue
        bins = bins_payload.get('bins') or []
        rows.append({
            'id': sid,
            'orientation': dep.get('orientation') or '',
            'sensor_depth': dep.get('sensor_depth'),
            'measured_depth': (
                dep.get('measured_depth') or dep.get('depth')
            ),
            'height_from_bottom': dep.get('height_from_bottom'),
            'n_bins': len(bins),
            'null_depth_bins': sum(
                1 for b in bins if b.get('depth') is None
            ),
            'bin_size': bins_payload.get('bin_size'),
            'center_bin_1_dist': bins_payload.get('center_bin_1_dist'),
            'rtb_bins': bins_payload.get('real_time_bin'),
            'rtb_dep0': (
                (dep.get('deployments') or [{}])[0].get('real_time_bin')
            ),
            'first_bin_depth': bins[0].get('depth') if bins else None,
            'last_bin_depth': bins[-1].get('depth') if bins else None,
            'depth_ordering': _classify_bin_depth_ordering(bins),
        })
        time.sleep(_FETCH_DELAY_SEC)

    with open(_AUDIT_OUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f'\nAudit CSV written to: {_AUDIT_OUT}')

    fetch_failures = [r for r in rows if r['orientation'] == 'FETCH_FAIL']
    assert not fetch_failures, (
        f'Failed to fetch metadata for {len(fetch_failures)} stations: '
        f'{[r["id"] for r in fetch_failures[:5]]}'
    )

    bad_orientation = [
        r for r in rows
        if r['orientation'] not in ('side', 'up', 'down')
    ]
    assert not bad_orientation, (
        f'{len(bad_orientation)} stations have unrecognized orientation: '
        f'{[(r["id"], r["orientation"]) for r in bad_orientation[:10]]}'
    )

    side_violations = [
        r for r in rows
        if r['orientation'] == 'side' and (
            r['null_depth_bins'] != r['n_bins']
            or r['rtb_bins'] is None
            or r['sensor_depth'] is None
        )
    ]
    assert not side_violations, (
        'Side-looking invariant broken (null bin depth + bins.real_time_bin '
        f'+ sensor_depth all required): {side_violations[:5]}'
    )

    up_violations = [
        r for r in rows
        if r['orientation'] == 'up' and (
            r['null_depth_bins'] > 0
            or r['depth_ordering'] != 'descending'
        )
    ]
    assert not up_violations, (
        f'Up-looking invariant broken: {up_violations[:5]}'
    )

    down_violations = [
        r for r in rows
        if r['orientation'] == 'down' and (
            r['null_depth_bins'] > 0
            or r['depth_ordering'] != 'ascending'
        )
    ]
    assert not down_violations, (
        f'Down-looking invariant broken: {down_violations[:5]}'
    )
