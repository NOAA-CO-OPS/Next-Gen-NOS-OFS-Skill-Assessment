"""Unit tests for the per-directory cache manifest (stale-cache guard).

The 1D pipeline caches artifacts (*.ctl, inventory_all_{ofs}.csv,
*.obs, *.prd, *_pair.int) under filenames that do not encode the
assessment window or station-selection options. cache_manifest records
the run parameters each artifact was built for in a per-directory index and
lets the reuse gates detect a parameter change and regenerate instead of
serving stale files. These tests exercise the manifest primitives directly
(no network, no model data).
"""

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
        stationowner='co-ops,ndbc',
        currents_bins_csv=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _touch(path, text='data'):
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(text)


class TestRunSignature:
    def test_same_params_equal(self):
        assert cm.run_signature(_prop()) == cm.run_signature(_prop())

    def test_window_change_differs(self):
        a = cm.run_signature(_prop())
        b = cm.run_signature(_prop(end_date_full='2026-06-09T00:00:00Z'))
        assert a != b

    def test_stationowner_order_insensitive(self):
        a = cm.run_signature(_prop(stationowner='co-ops,ndbc'))
        b = cm.run_signature(_prop(stationowner='NDBC,CO-OPS'))
        assert a == b

    def test_datum_change_differs(self):
        a = cm.run_signature(_prop(datum='MLLW'))
        b = cm.run_signature(_prop(datum='NAVD88'))
        assert a != b

    def test_variable_scopes_signature(self):
        a = cm.run_signature(_prop(), variable='water_level')
        b = cm.run_signature(_prop(), variable='currents')
        assert a != b

    def test_extra_whichcast_differs(self):
        a = cm.run_signature(_prop(), extra={'whichcast': 'nowcast'})
        b = cm.run_signature(_prop(), extra={'whichcast': 'forecast_b'})
        assert a != b

    def test_currents_bins_csv_fingerprint(self, tmp_path):
        csv = tmp_path / 'bins.csv'
        _touch(str(csv), 'station_id,bin\ncb0201,2\n')
        sig1 = cm.run_signature(_prop(currents_bins_csv=str(csv)))
        # Rewrite with different content (and size) -> new fingerprint.
        _touch(str(csv), 'station_id,bin\ncb0201,2\ncb0201,3\n')
        os.utime(str(csv), (os.stat(str(csv)).st_atime,
                            os.stat(str(csv)).st_mtime + 5))
        sig2 = cm.run_signature(_prop(currents_bins_csv=str(csv)))
        assert sig1 != sig2


class TestArtifactFreshness:
    def test_missing_file_not_fresh(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        assert cm.artifact_is_fresh(path, cm.run_signature(_prop())) is False

    def test_recorded_then_fresh(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        sig = cm.run_signature(_prop())
        cm.record_artifact(path, sig, str(tmp_path))
        assert cm.artifact_is_fresh(path, sig) is True

    def test_legacy_file_without_entry_not_fresh(self, tmp_path):
        # A pre-upgrade artifact has no manifest entry -> treated as stale.
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        assert cm.artifact_is_fresh(path, cm.run_signature(_prop())) is False

    def test_changed_params_not_fresh(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        cm.record_artifact(path, cm.run_signature(_prop()), str(tmp_path))
        newsig = cm.run_signature(_prop(end_date_full='2026-07-01T00:00:00Z'))
        assert cm.artifact_is_fresh(path, newsig) is False

    def test_index_lives_in_directory(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        cm.record_artifact(path, cm.run_signature(_prop()), str(tmp_path))
        assert (tmp_path / cm.MANIFEST_FILENAME).is_file()

    def test_write_leaves_no_temp_file(self, tmp_path):
        # The atomic write (temp file + os.replace) must not leave the
        # temp artifact behind on success.
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        cm.record_artifact(path, cm.run_signature(_prop()), str(tmp_path))
        leftovers = [p.name for p in tmp_path.iterdir()
                     if p.name.startswith(cm.MANIFEST_FILENAME)
                     and p.name != cm.MANIFEST_FILENAME]
        assert leftovers == []

    def test_rewrite_preserves_prior_entries(self, tmp_path):
        # Recording a second artifact must merge into the existing index
        # (atomic replace of the whole file), not clobber the first entry.
        p1 = str(tmp_path / 'cbofs_wl_station.ctl')
        p2 = str(tmp_path / 'cbofs_temp_station.ctl')
        _touch(p1)
        _touch(p2)
        sig1 = cm.run_signature(_prop(), variable='water_level')
        sig2 = cm.run_signature(_prop(), variable='water_temperature')
        cm.record_artifact(p1, sig1, str(tmp_path))
        cm.record_artifact(p2, sig2, str(tmp_path))
        assert cm.artifact_is_fresh(p1, sig1) is True
        assert cm.artifact_is_fresh(p2, sig2) is True

    def test_corrupt_index_fails_open(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        cm.record_artifact(path, cm.run_signature(_prop()), str(tmp_path))
        # Corrupt the index -> everything reads as stale (safe regeneration).
        _touch(str(tmp_path / cm.MANIFEST_FILENAME), 'not json{{{')
        assert cm.artifact_is_fresh(path, cm.run_signature(_prop())) is False

    def test_forget_artifact(self, tmp_path):
        path = str(tmp_path / 'cbofs_wl_station.ctl')
        _touch(path)
        sig = cm.run_signature(_prop())
        cm.record_artifact(path, sig, str(tmp_path))
        cm.forget_artifact(path, str(tmp_path))
        assert cm.artifact_is_fresh(path, sig) is False


class TestEnsureFresh:
    def test_missing_returns_false_no_tally(self, tmp_path):
        cm.reset_stale_counter()
        path = str(tmp_path / 'x_wl_station.ctl')
        result = cm.ensure_fresh(
            path, cm.run_signature(_prop()), str(tmp_path), 'ctl')
        assert result is False
        # Missing (never built) is not a "stale regenerate" event.
        assert cm._stale_counter == {}

    def test_matching_returns_true(self, tmp_path):
        path = str(tmp_path / 'x_wl_station.ctl')
        _touch(path)
        sig = cm.run_signature(_prop())
        cm.record_artifact(path, sig, str(tmp_path))
        assert cm.ensure_fresh(path, sig, str(tmp_path), 'ctl') is True

    def test_stale_deletes_and_tallies(self, tmp_path):
        cm.reset_stale_counter()
        path = str(tmp_path / 'x_wl_station.ctl')
        _touch(path)
        cm.record_artifact(path, cm.run_signature(_prop()), str(tmp_path))
        newsig = cm.run_signature(_prop(datum='NAVD88'))
        result = cm.ensure_fresh(path, newsig, str(tmp_path), 'ctl')
        assert result is False
        assert not os.path.exists(path)          # deleted
        assert cm._stale_counter.get('ctl') == 1  # tallied
        # Manifest entry dropped too.
        assert cm.artifact_is_fresh(path, newsig) is False

    def test_refuses_delete_outside_base_dir(self, tmp_path):
        # A path resolving outside base_dir must not be deleted.
        inside = tmp_path / 'work'
        inside.mkdir()
        outside = tmp_path / 'outside.ctl'
        _touch(str(outside))
        cm.record_artifact(str(outside), cm.run_signature(_prop()), str(tmp_path))
        newsig = cm.run_signature(_prop(datum='NAVD88'))
        result = cm.ensure_fresh(
            str(outside), newsig, str(inside), 'ctl')
        assert result is False
        assert outside.exists()  # NOT deleted (outside base_dir)


class TestStaleSummary:
    def test_summary_silent_when_none(self):
        cm.reset_stale_counter()
        logged = []
        logger = SimpleNamespace(info=lambda *a, **k: logged.append(a))
        cm.emit_stale_summary('cbofs', logger)
        assert logged == []

    def test_summary_reports_counts(self):
        cm.reset_stale_counter()
        cm.note_stale('ctl')
        cm.note_stale('ctl')
        cm.note_stale('obs')
        logged = []
        logger = SimpleNamespace(info=lambda *a, **k: logged.append(a))
        cm.emit_stale_summary('cbofs', logger)
        assert len(logged) == 1
        # Message mentions the total and the per-kind breakdown.
        rendered = logged[0][0] % logged[0][1:]
        assert '3 stale' in rendered
        assert '2 ctl' in rendered
        assert '1 obs' in rendered
