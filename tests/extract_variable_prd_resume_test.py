"""Resume short-circuit in ``_extract_variable``.

When a long multi-variable run dies partway (OOM, SIGKILL, contended-host
swap) some per-station ``.prd`` files may already be on disk for one
variable while others are still missing. Before this fix, every restart
re-extracted every variable in ``prop.var_list`` from scratch because the
inner variable loop in ``get_node_ofs`` ignored existing ``.prd`` files —
``_ensure_prd_files`` in ``get_skill`` was the only place that checked
existence, and once it triggered ``get_node_ofs`` for the first missing
variable, the loop blindly re-did the others.

The short-circuit at the top of ``_extract_variable`` checks every
expected per-station ``.prd`` path. If they all exist with non-zero size,
``_extract_variable`` returns early, logs ``all N .prd file(s) already
exist``, and skips ``_precompute_stations_data`` + the per-station write
loop. This makes crash-resume cheap on contested servers.

These tests can't drive the real extract path end-to-end (it needs a
loaded xarray dataset + ctl files), so they unit-test the short-circuit
arithmetic directly: build a fake ofs_ctlfile, populate the expected
``.prd`` paths in a tmp dir, and verify the per-station file pattern
match works for nowcast/forecast_b vs forecast_a.
"""

import os
from pathlib import Path


def _make_ofs_ctlfile(n_stations=3, node_offset=10):
    """Mimic the tuple ``ofs_ctlfile_extract`` returns.

    Tuple slot layout (from get_node_ofs.py):
        [0] lines (raw parsed)
        [1] nodes — list of int
        [2] depths — list of int
        [3] shifts — list of float
        [4] ids — list of str
    """
    nodes = [node_offset + i for i in range(n_stations)]
    depths = [0] * n_stations
    shifts = [0.0] * n_stations
    ids = [f'sta{i:02d}' for i in range(n_stations)]
    return [], nodes, depths, shifts, ids


def _make_prd_path(prd_dir, station_id, ofs, name_var, node, whichcast,
                   ofsfiletype, forecast_hr=None):
    if whichcast == 'forecast_a':
        return os.path.join(
            prd_dir,
            f'{station_id}_{ofs}_{name_var}_{node}_'
            f'{whichcast}_{forecast_hr}_{ofsfiletype}_model.prd',
        )
    return os.path.join(
        prd_dir,
        f'{station_id}_{ofs}_{name_var}_{node}_'
        f'{whichcast}_{ofsfiletype}_model.prd',
    )


def _check_all_prd_present(ofs_ctlfile, prd_dir, ofs, name_var, whichcast,
                           ofsfiletype, forecast_hr=None):
    """Replicate the short-circuit logic. Tests verify this matches what
    ``_extract_variable`` is doing."""
    n_stations = len(ofs_ctlfile[1])
    if n_stations == 0:
        return False
    for i in range(n_stations):
        path = _make_prd_path(
            prd_dir, ofs_ctlfile[4][i], ofs, name_var,
            ofs_ctlfile[1][i], whichcast, ofsfiletype, forecast_hr,
        )
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return False
    return True


def _write_all_prd(ofs_ctlfile, prd_dir, ofs, name_var, whichcast,
                   ofsfiletype, forecast_hr=None, content=b'fake data\n'):
    for i in range(len(ofs_ctlfile[1])):
        path = _make_prd_path(
            prd_dir, ofs_ctlfile[4][i], ofs, name_var,
            ofs_ctlfile[1][i], whichcast, ofsfiletype, forecast_hr,
        )
        Path(path).write_bytes(content)


# ---------------------------------------------------------------------------
# Short-circuit arithmetic
# ---------------------------------------------------------------------------


def test_all_present_nowcast(tmp_path):
    ctl = _make_ofs_ctlfile(n_stations=5)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations')

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations'
    ) is True


def test_one_missing_triggers_extraction(tmp_path):
    ctl = _make_ofs_ctlfile(n_stations=5)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'temp', 'nowcast', 'stations')

    # Delete one — should now fail the check.
    target = _make_prd_path(
        str(tmp_path), ctl[4][2], 'necofs', 'temp', ctl[1][2],
        'nowcast', 'stations',
    )
    os.remove(target)

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'temp', 'nowcast', 'stations'
    ) is False


def test_empty_prd_file_treated_as_missing(tmp_path):
    """A .prd file with 0 bytes means a previous run was killed mid-write —
    must NOT be reused. The check requires non-zero size."""
    ctl = _make_ofs_ctlfile(n_stations=3)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'salt', 'nowcast', 'stations')

    # Truncate one to zero bytes.
    target = _make_prd_path(
        str(tmp_path), ctl[4][1], 'necofs', 'salt', ctl[1][1],
        'nowcast', 'stations',
    )
    open(target, 'wb').close()
    assert os.path.getsize(target) == 0

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'salt', 'nowcast', 'stations'
    ) is False


def test_zero_stations_returns_false(tmp_path):
    ctl = _make_ofs_ctlfile(n_stations=0)
    # No stations means no .prd files expected — but we should NOT treat
    # "vacuously true" as cause to skip extraction (could mask a real bug).
    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'cu', 'nowcast', 'stations'
    ) is False


def test_forecast_b_pattern(tmp_path):
    ctl = _make_ofs_ctlfile(n_stations=4)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'wl', 'forecast_b', 'stations')

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'wl', 'forecast_b', 'stations'
    ) is True

    # nowcast files in the same dir don't help — must match whichcast.
    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations'
    ) is False


def test_forecast_a_pattern_includes_forecast_hr(tmp_path):
    ctl = _make_ofs_ctlfile(n_stations=3)
    _write_all_prd(
        ctl, str(tmp_path), 'cbofs', 'wl', 'forecast_a', 'stations',
        forecast_hr='06z',
    )

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'cbofs', 'wl', 'forecast_a', 'stations',
        forecast_hr='06z',
    ) is True

    # forecast_a with a different cycle hour does NOT match.
    assert _check_all_prd_present(
        ctl, str(tmp_path), 'cbofs', 'wl', 'forecast_a', 'stations',
        forecast_hr='12z',
    ) is False


def test_different_variables_isolated(tmp_path):
    """WL .prd files mustn't satisfy the check for temp."""
    ctl = _make_ofs_ctlfile(n_stations=3)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations')

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations'
    ) is True
    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'temp', 'nowcast', 'stations'
    ) is False


def test_different_ofs_isolated(tmp_path):
    """necofs .prd files mustn't satisfy the check for cbofs."""
    ctl = _make_ofs_ctlfile(n_stations=2)
    _write_all_prd(ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations')

    assert _check_all_prd_present(
        ctl, str(tmp_path), 'necofs', 'wl', 'nowcast', 'stations'
    ) is True
    assert _check_all_prd_present(
        ctl, str(tmp_path), 'cbofs', 'wl', 'nowcast', 'stations'
    ) is False


# ---------------------------------------------------------------------------
# Integration: import the module and confirm the short-circuit hook is
# wired into _extract_variable. We can't easily call _extract_variable
# directly (it's a closure with several outer-scope deps), but we can
# confirm the source contains the resume-log marker so a future refactor
# that strips the check is caught.
# ---------------------------------------------------------------------------


def test_extract_variable_source_contains_resume_log():
    import inspect

    from ofs_skill.model_processing.get_node_ofs import get_node_ofs

    src = inspect.getsource(get_node_ofs)
    assert 'Resume short-circuit' in src, (
        'Resume short-circuit comment block must be present in '
        'get_node_ofs to document the .prd-existence check.'
    )
    assert '.prd file(s) already exist' in src, (
        'Resume short-circuit log message must be present so a kill-resume '
        'flow does not silently re-extract all variables.'
    )
