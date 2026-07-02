"""Regression test for CHS water-level datum handling in write_obs_ctlfile.

CHS observed water level is labeled with the Great-Lakes datum 'IGLD'. For a
non-Great-Lakes OFS (e.g. stofs_3d_atl) requesting a tidal datum such as MLLW
there is no conversion path, and the underlying coastalmodeling_vdatum package
raises a cryptic ``UnboundLocalError`` ('h_g'). ``_process_chs_station`` must
skip such stations cleanly with an informative warning instead of letting the
crash surface as a misleading "data not found" message.
"""
from __future__ import annotations

import importlib
import logging
from datetime import datetime
from unittest import mock

import pandas as pd

# The package re-exports the ``write_obs_ctlfile`` *function*, shadowing the
# submodule of the same name, so resolve the module via its dotted path.
write_obs_ctlfile = importlib.import_module(
    'ofs_skill.obs_retrieval.write_obs_ctlfile')


def _fake_chs_wl_dataset():
    # data_station['Datum'][1] is read by _process_chs_station, so index 1
    # must carry the datum label CHS assigns to all water-level stations.
    return pd.DataFrame({'Datum': ['IGLD', 'IGLD'], 'WL': [1.1, 1.2]})


def test_chs_water_level_non_gl_skips_when_no_datum_path(caplog):
    """Non-GL OFS requesting MLLW: station is skipped, not crashed."""
    logger = logging.getLogger('chs_datum_skip_test')
    with mock.patch.object(
            write_obs_ctlfile, 'retrieve_chs_station',
            return_value=_fake_chs_wl_dataset()):
        with caplog.at_level(logging.WARNING):
            result = write_obs_ctlfile._process_chs_station(
                id_number='5cebf1e0',
                name='Some Atlantic Canada Station',
                x_value=-65.0,
                y_value=45.0,
                start_date=datetime(2026, 5, 26),
                end_date=datetime(2026, 6, 2),
                variable='water_level',
                name_var='wl',
                datum='MLLW',
                ofs='stofs_3d_atl',
                logger=logger,
            )

    # Skipped cleanly -> empty entry list, no exception propagated.
    assert result == []
    # The warning explains the real reason (no conversion path), not the
    # misleading h_g/'data not found' message.
    msgs = [r.message for r in caplog.records]
    assert any('no datum conversion path' in m for m in msgs), msgs
    assert not any('h_g' in m for m in msgs), msgs
