"""Regression tests for the stale-cache guard on the ctl/obs reuse gates.

Reproduces the reported failure mode: a user runs the pipeline for one date
window, then re-runs for a different window (or different station-owner /
datum) in the SAME working directory without deleting ./data and
./control_files. Before the cache-manifest guard, the second run reused
the first run's control files verbatim -- pinning the wrong station/node set
-- and produced incorrect results.

These tests drive cache_manifest.ensure_fresh the same way the real
gates do (write_obs_ctlfile, write_ofs_ctlfile,
get_station_observations, get_skill, create_1dplot) and assert
that a parameter change forces regeneration while an identical rerun reuses.
"""

import importlib
import os
from types import SimpleNamespace

from ofs_skill.utils import cache_manifest as cm


def _prop(**overrides):
    base = dict(
        ofs='cbofs',
        start_date_full='2026-06-01T00:00:00Z',
        end_date_full='2026-06-02T00:00:00Z',
        ofsfiletype='stations',
        datum='MLLW',
        stationowner='co-ops,ndbc,usgs,chs',
        currents_bins_csv=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _build_ctl(path, signature, base_dir, contents='node depth ... id shift\n'):
    """Mimic a gate writing a ctl and stamping its signature."""
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(contents)
    cm.record_artifact(path, signature, base_dir)


def test_second_window_regenerates_stale_ctl(tmp_path):
    """Run A builds the ctl; run B (new window) must NOT reuse it."""
    ctl = str(tmp_path / 'cbofs_wl_station.ctl')

    # --- Run A: window 2026-06-01..02 ---
    prop_a = _prop()
    sig_a = cm.run_signature(prop_a, variable='water_level')
    _build_ctl(ctl, sig_a, str(tmp_path), contents='RUN_A_STATIONS\n')

    # Same-parameter rerun reuses verbatim (fast path, no regeneration).
    assert cm.ensure_fresh(ctl, sig_a, str(tmp_path), 'obs ctl') is True
    with open(ctl, encoding='utf-8') as handle:
        assert 'RUN_A_STATIONS' in handle.read()

    # --- Run B: different window ---
    prop_b = _prop(start_date_full='2026-07-01T00:00:00Z',
                   end_date_full='2026-07-02T00:00:00Z')
    sig_b = cm.run_signature(prop_b, variable='water_level')

    # The gate sees a signature mismatch, deletes the stale ctl, returns
    # False so the caller rebuilds it.
    assert cm.ensure_fresh(ctl, sig_b, str(tmp_path), 'obs ctl') is False
    assert not os.path.exists(ctl)

    # Caller rebuilds for window B and stamps the new signature.
    _build_ctl(ctl, sig_b, str(tmp_path), contents='RUN_B_STATIONS\n')
    assert cm.ensure_fresh(ctl, sig_b, str(tmp_path), 'obs ctl') is True
    with open(ctl, encoding='utf-8') as handle:
        assert 'RUN_B_STATIONS' in handle.read()


def test_stationowner_change_regenerates(tmp_path):
    """Narrowing -so from all providers to co-ops only rebuilds the ctl."""
    ctl = str(tmp_path / 'cbofs_wl_station.ctl')
    sig_all = cm.run_signature(
        _prop(stationowner='co-ops,ndbc,usgs,chs'), variable='water_level')
    _build_ctl(ctl, sig_all, str(tmp_path), contents='ALL_PROVIDERS\n')

    sig_coops = cm.run_signature(
        _prop(stationowner='co-ops'), variable='water_level')
    assert cm.ensure_fresh(ctl, sig_coops, str(tmp_path), 'obs ctl') is False
    assert not os.path.exists(ctl)


def test_inventory_regenerates_on_window_change(tmp_path):
    """The inventory CSV gate (its own signature shape) also regenerates."""
    inv = str(tmp_path / 'inventory_all_cbofs.csv')

    def inv_sig(start, end, owner):
        return {
            'ofs': cm._normalize('cbofs'),
            'start_date': cm._normalize(start),
            'end_date': cm._normalize(end),
            'stationowner': cm._normalize(owner),
            'currents_bins_csv': cm.file_fingerprint(None),
        }

    sig_a = inv_sig('20260601', '20260602', 'co-ops,ndbc')
    with open(inv, 'w', encoding='utf-8') as handle:
        handle.write('ID,X,Y,Source,Name\n8638610,-76,37,CO-OPS,x\n')
    cm.record_artifact(inv, sig_a, str(tmp_path))
    assert cm.ensure_fresh(inv, sig_a, str(tmp_path), 'inventory') is True

    sig_b = inv_sig('20260701', '20260702', 'co-ops,ndbc')
    assert cm.ensure_fresh(inv, sig_b, str(tmp_path), 'inventory') is False
    assert not os.path.exists(inv)


def test_prd_int_regenerate_on_datum_change(tmp_path):
    """Model .prd and paired .int gates key on datum + whichcast too."""
    prd = str(tmp_path / 's_cbofs_wl_0_nowcast_stations_model.prd')
    intf = str(tmp_path / 'cbofs_wl_s_0_nowcast_stations_pair.int')

    prd_sig = cm.run_signature(
        _prop(datum='MLLW'), variable='water_level',
        extra={'whichcast': 'nowcast'})
    int_sig = cm.run_signature(
        _prop(datum='MLLW'), variable='water_level',
        extra={'whichcast': 'nowcast'})
    for path, sig in ((prd, prd_sig), (intf, int_sig)):
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write('DNUM YEAR MONTH DAY HOUR MINUTE VAL\n')
        cm.record_artifact(path, sig, str(tmp_path))

    # Same params -> reuse.
    assert cm.artifact_is_fresh(prd, prd_sig) is True
    assert cm.artifact_is_fresh(intf, int_sig) is True

    # Change datum -> both read stale.
    prd_sig2 = cm.run_signature(
        _prop(datum='NAVD88'), variable='water_level',
        extra={'whichcast': 'nowcast'})
    int_sig2 = cm.run_signature(
        _prop(datum='NAVD88'), variable='water_level',
        extra={'whichcast': 'nowcast'})
    assert cm.artifact_is_fresh(prd, prd_sig2) is False
    assert cm.artifact_is_fresh(intf, int_sig2) is False


def test_identical_rerun_reuses_all(tmp_path):
    """A byte-for-byte identical rerun reuses every artifact (no churn)."""
    cm.reset_stale_counter()
    prop = _prop()
    paths = {
        'cbofs_wl_station.ctl': cm.run_signature(prop, variable='water_level'),
        'cbofs_wl_model_station.ctl': cm.run_signature(
            prop, variable='water_level'),
    }
    for name, sig in paths.items():
        p = str(tmp_path / name)
        _build_ctl(p, sig, str(tmp_path))
    # Second run, same params: everything fresh, nothing tallied stale.
    for name, sig in paths.items():
        p = str(tmp_path / name)
        assert cm.ensure_fresh(p, sig, str(tmp_path), 'ctl') is True
    assert cm._stale_counter == {}


# ---------------------------------------------------------------------
# An artifact that came out empty must never be certified as good.
#
# Both gates below sit at the end of a build block that can legitimately
# produce nothing -- a provider outage during the one run that rebuilds an
# inventory, a station whose series comes back with no rows. Stamping those
# tells every later run "this file is correct for these parameters", and the
# only way out is deleting control_files/ by hand, which is the manual step
# the manifest exists to remove. The symmetric failure is stamping *nothing*:
# then the gate reads every file as stale and re-fetches the whole station
# set on every run.
# ---------------------------------------------------------------------

def test_empty_inventory_is_not_recorded(tmp_path):
    """A zero-row inventory must not be stamped fresh.

    Drives the production guard, so a regression that goes back to an
    unconditional ``record_artifact`` fails here.
    """
    import logging

    import pandas as pd

    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')

    inventory_path = tmp_path / 'inventory_all_cbofs.csv'
    inventory_path.write_text(
        ',Source,ID,X,Y,Name,has_wl,has_temp,has_salt,has_cu\n',
        encoding='utf-8')
    empty = pd.read_csv(inventory_path)
    signature = {'ofs': 'CBOFS', 'stationowner': 'USGS'}

    recorded = woc.record_inventory_if_populated(
        empty, str(inventory_path), signature, str(tmp_path),
        logging.getLogger('t'))

    assert recorded is False
    assert cm.artifact_is_fresh(str(inventory_path), signature) is False


def test_populated_inventory_is_recorded(tmp_path):
    """The same guard must still stamp an inventory that has stations."""
    import logging

    import pandas as pd

    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')

    inventory_path = tmp_path / 'inventory_all_cbofs.csv'
    inventory_path.write_text(
        ',Source,ID,X,Y,Name,has_wl,has_temp,has_salt,has_cu\n'
        '0,CO-OPS,8638901,-76.3,37.0,Test,True,False,False,False\n',
        encoding='utf-8')
    populated = pd.read_csv(inventory_path)
    signature = {'ofs': 'CBOFS', 'stationowner': 'CO-OPS'}

    recorded = woc.record_inventory_if_populated(
        populated, str(inventory_path), signature, str(tmp_path),
        logging.getLogger('t'))

    assert recorded is True
    assert cm.artifact_is_fresh(str(inventory_path), signature) is True


def test_obs_written_with_rows_is_recorded(tmp_path):
    """A written .obs must carry a signature or it re-fetches every run.

    ``_ensure_obs_files`` treats "no manifest entry" as stale, so dropping
    the stamp does not merely lose an optimization -- it deletes and
    re-fetches every station's observations on every subsequent run.
    """
    obs = tmp_path / '8638901_cbofs_wl_station.obs'
    obs.write_text(
        'Julian days, Year, Month, Day, Hours, Minutes, Water level (m)\n'
        ' 2461127.25000000 2026  8 15  0  0    1.2345\n',
        encoding='utf-8')
    signature = {'ofs': 'CBOFS', 'variable': 'WATER_LEVEL'}
    cm.record_artifact(str(obs), signature, str(tmp_path))

    assert cm.artifact_is_fresh(str(obs), signature) is True
    # And a parameter change still invalidates it.
    assert cm.artifact_is_fresh(
        str(obs), {'ofs': 'CBOFS', 'variable': 'SALINITY'}) is False


def test_fetch_and_format_station_stamps_the_obs_it_writes(tmp_path,
                                                           monkeypatch):
    """Drive the real writer and assert the signature reaches the manifest.

    This is the production path, not a stand-in: a regression that drops
    the ``record_artifact`` call again fails here, whereas the primitive
    tests above would still pass.
    """
    import logging

    import pandas as pd

    # import_module, not ``from ... import``: the package re-exports the
    # function of the same name, which would shadow the module.
    gso = importlib.import_module(
        'ofs_skill.obs_retrieval.get_station_observations')

    stamps = pd.date_range('2026-08-15', periods=6, freq='h')
    monkeypatch.setattr(
        gso, 'retrieve_ndbc_station',
        lambda *a, **k: pd.DataFrame({'DateTime': stamps,
                                      'OBS': [12.5] * len(stamps)}))

    signature = {'ofs': 'CBOFS', 'variable': 'WATER_TEMPERATURE'}
    result = gso._fetch_and_format_station(
        ['44042', '', '', 'NDBC'], ['', '', '', ''],
        'water_temperature', 'temp', 'MLLW', ['MLLW'],
        '20260815', '20260816', '20260815-00:00:00', '20260816-00:00:00',
        'cbofs', str(tmp_path), logging.getLogger('t'), str(tmp_path),
        None, signature,
    )

    obs_path = tmp_path / '44042_cbofs_temp_station.obs'
    assert result == '44042'
    assert obs_path.is_file() and obs_path.stat().st_size > 0
    assert cm.artifact_is_fresh(str(obs_path), signature) is True


def test_fetch_and_format_station_writes_nothing_for_an_empty_series(
        tmp_path, monkeypatch):
    """An all-NaN retrieval must leave no file and no manifest entry."""
    import logging

    import pandas as pd

    # import_module, not ``from ... import``: the package re-exports the
    # function of the same name, which would shadow the module.
    gso = importlib.import_module(
        'ofs_skill.obs_retrieval.get_station_observations')

    monkeypatch.setattr(
        gso, 'retrieve_ndbc_station',
        lambda *a, **k: pd.DataFrame({'DateTime': pd.to_datetime([]),
                                      'OBS': []}))

    signature = {'ofs': 'CBOFS', 'variable': 'WATER_TEMPERATURE'}
    result = gso._fetch_and_format_station(
        ['44042', '', '', 'NDBC'], ['', '', '', ''],
        'water_temperature', 'temp', 'MLLW', ['MLLW'],
        '20260815', '20260816', '20260815-00:00:00', '20260816-00:00:00',
        'cbofs', str(tmp_path), logging.getLogger('t'), str(tmp_path),
        None, signature,
    )

    obs_path = tmp_path / '44042_cbofs_temp_station.obs'
    assert result is None
    assert not obs_path.exists()
    assert cm.artifact_is_fresh(str(obs_path), signature) is False
