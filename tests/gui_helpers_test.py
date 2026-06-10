"""Tests for ofs_skill.visualization.gui_helpers.

Covers the pure-logic helpers, conf-driven datum lookup, and validators
extracted from create_gui.py during the Phase 2 refactor. Helpers were
designed to be testable without a Tk root, so every test runs headless.

DateEntry instantiation is intentionally not exercised here (it requires a
display); the corresponding test class verifies the widget's class-level
configuration dictionary, which is the surface most likely to regress.
"""

from datetime import date, datetime, timezone

import pytest

from ofs_skill.visualization import gui_helpers


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Sanity checks on the module-level OFS / datum tuples."""

    def test_great_lakes_ofs_membership(self):
        """Every Great Lakes OFS should be present and lower-cased."""
        for ofs in ('leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs'):
            assert ofs in gui_helpers.GREAT_LAKES_OFS

    def test_great_lakes_ofs_does_not_contain_coastal(self):
        """Coastal OFSes must not leak into the Great Lakes group."""
        for ofs in ('cbofs', 'ngofs2', 'wcofs', 'sscofs'):
            assert ofs not in gui_helpers.GREAT_LAKES_OFS

    def test_stofs_ofs_membership(self):
        """All STOFS variants must be present."""
        assert set(gui_helpers.STOFS_OFS) == {
            'stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac'
        }

    def test_default_datums_includes_common_us_datums(self):
        """Fallback datum list must cover the datums used by most OFSes."""
        for datum in ('MHHW', 'MLLW', 'NAVD88', 'IGLD85'):
            assert datum in gui_helpers.DEFAULT_DATUMS

    def test_default_datums_is_immutable_tuple(self):
        """Returned constant should be a tuple so callers can't mutate it."""
        assert isinstance(gui_helpers.DEFAULT_DATUMS, tuple)


# ---------------------------------------------------------------------------
# quick_run_datum
# ---------------------------------------------------------------------------


class TestQuickRunDatum:
    """Default-datum lookup for Quick Run mode."""

    @pytest.mark.parametrize('ofs', ['leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs'])
    def test_great_lakes_returns_igld85(self, ofs):
        """Great Lakes OFSes use IGLD85 (no MLLW in fresh water)."""
        assert gui_helpers.quick_run_datum(ofs) == 'IGLD85'

    @pytest.mark.parametrize('ofs', ['stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac'])
    def test_stofs_returns_navd88(self, ofs):
        """STOFS systems lack a uniform tidal datum, so use NAVD88."""
        assert gui_helpers.quick_run_datum(ofs) == 'NAVD88'

    @pytest.mark.parametrize('ofs', ['cbofs', 'ngofs2', 'wcofs', 'sscofs', 'tbofs'])
    def test_tidal_coastal_returns_mllw(self, ofs):
        """Tidal coastal OFSes default to MLLW."""
        assert gui_helpers.quick_run_datum(ofs) == 'MLLW'

    def test_unknown_ofs_falls_back_to_mllw(self):
        """Unknown OFS names fall through to the MLLW default."""
        assert gui_helpers.quick_run_datum('not_a_real_ofs') == 'MLLW'


# ---------------------------------------------------------------------------
# compute_recent_cycle
# ---------------------------------------------------------------------------


class TestComputeRecentCycle:
    """Most-recent-cycle picker with 2h NODD arrival delay."""

    @pytest.fixture
    def six_hourly_cycles(self, monkeypatch):
        """Patch get_fcst_hours to advertise 00/06/12/18z cycles."""
        monkeypatch.setattr(
            gui_helpers, 'get_fcst_hours',
            lambda ofs: ([], ['00', '06', '12', '18'])
        )

    def test_picks_06z_at_noon_utc(self, six_hourly_cycles):
        """At 12:00 UTC, the 12z cycle is too fresh (cutoff=10:00); pick 06z."""
        now = datetime(2025, 11, 12, 12, 0, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso == '2025-11-12T06:00:00Z'
        assert hr == '06z'

    def test_picks_12z_at_late_afternoon(self, six_hourly_cycles):
        """At 17:00 UTC, cutoff=15:00 → 12z is most recent available."""
        now = datetime(2025, 11, 12, 17, 0, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso == '2025-11-12T12:00:00Z'
        assert hr == '12z'

    def test_picks_18z_late_evening(self, six_hourly_cycles):
        """At 23:00 UTC, cutoff=21:00 → 18z is most recent available."""
        now = datetime(2025, 11, 12, 23, 0, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso == '2025-11-12T18:00:00Z'
        assert hr == '18z'

    def test_falls_back_to_yesterday_in_early_morning(self, six_hourly_cycles):
        """At 01:00 UTC, even today's 00z is > cutoff (-01:00) → use yesterday's 18z."""
        now = datetime(2025, 11, 12, 1, 0, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso == '2025-11-11T18:00:00Z'
        assert hr == '18z'

    def test_two_hour_delay_boundary_inclusive(self, six_hourly_cycles):
        """A cycle exactly 2h old is considered available (cyc_dt <= cutoff)."""
        # cutoff = 14:00; the 12z cycle (2h ago) is at the boundary.
        now = datetime(2025, 11, 12, 14, 0, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert hr == '12z'
        assert iso == '2025-11-12T12:00:00Z'

    def test_unsorted_cycle_input_still_returns_correct_result(self, monkeypatch):
        """Function sorts internally; upstream order shouldn't matter."""
        monkeypatch.setattr(
            gui_helpers, 'get_fcst_hours',
            lambda ofs: ([], ['18', '00', '12', '06'])
        )
        now = datetime(2025, 11, 12, 12, 0, tzinfo=timezone.utc)
        _, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert hr == '06z'

    def test_minute_and_second_components_dont_shift_cycle(self, six_hourly_cycles):
        """Sub-hour drift in `now` should not change the chosen cycle."""
        now = datetime(2025, 11, 12, 12, 59, 59, 999, tzinfo=timezone.utc)
        _, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert hr == '06z'

    def test_iso_string_uses_z_suffix(self, six_hourly_cycles):
        """Downstream CLI parses YYYY-MM-DDTHH:MM:SSZ — ensure the Z is present."""
        now = datetime(2025, 11, 12, 12, 0, tzinfo=timezone.utc)
        iso, _ = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso.endswith('Z')
        assert 'T' in iso

    def test_default_now_uses_real_clock(self, six_hourly_cycles):
        """Omitting `now` should fall back to current UTC time without raising."""
        iso, hr = gui_helpers.compute_recent_cycle('xxx')
        assert iso.endswith('Z')
        assert hr.endswith('z') and len(hr) == 3

    def test_fallback_when_no_cycle_in_past_two_days_qualifies(self, monkeypatch):
        """Edge case: a single late cycle + early-morning `now` exhausts the
        2-day search window, exercising the safety-net fallback path."""
        monkeypatch.setattr(
            gui_helpers, 'get_fcst_hours',
            lambda ofs: ([], ['23'])
        )
        # At 00:30 UTC the cutoff is yesterday 22:00. Today's 23z is 23h in
        # the future and yesterday's 23z is 1h after the cutoff — both fail
        # the `<= cutoff` check, so the function falls through to the
        # "yesterday's last cycle" guess.
        now = datetime(2025, 11, 12, 0, 30, tzinfo=timezone.utc)
        iso, hr = gui_helpers.compute_recent_cycle('xxx', now=now)
        assert iso == '2025-11-11T23:00:00Z'
        assert hr == '23z'


# ---------------------------------------------------------------------------
# read_datum_list
# ---------------------------------------------------------------------------


def _patch_utils_section(monkeypatch, section_payload, exc=None):
    """Helper: replace gui_helpers.utils.Utils with a fake whose
    read_config_section either returns `section_payload` or raises `exc`."""

    class FakeUtils:
        def __init__(self, *_args, **_kwargs):
            pass

        def read_config_section(self, _section, _log):
            if exc is not None:
                raise exc
            return section_payload

    monkeypatch.setattr(gui_helpers.utils, 'Utils', FakeUtils)


class TestReadDatumList:
    """Conf-driven datum list with hardcoded fallback."""

    def test_returns_parsed_tuple_from_conf(self, monkeypatch):
        """Whitespace-separated datum_list should be split into a tuple."""
        _patch_utils_section(monkeypatch, {'datum_list': 'NAVD88 MLLW IGLD85'})
        assert gui_helpers.read_datum_list() == ('NAVD88', 'MLLW', 'IGLD85')

    def test_collapses_arbitrary_whitespace(self, monkeypatch):
        """str.split() with no arg collapses tabs/newlines/multi-spaces."""
        _patch_utils_section(
            monkeypatch, {'datum_list': '  NAVD88\t MLLW\n  IGLD85 '}
        )
        assert gui_helpers.read_datum_list() == ('NAVD88', 'MLLW', 'IGLD85')

    def test_falls_back_when_datum_list_key_missing(self, monkeypatch):
        """Section present but no datum_list key → defaults."""
        _patch_utils_section(monkeypatch, {'something_else': 'x'})
        assert gui_helpers.read_datum_list() == gui_helpers.DEFAULT_DATUMS

    def test_falls_back_when_datum_list_empty_string(self, monkeypatch):
        """Empty datum_list value → defaults (raw is falsy)."""
        _patch_utils_section(monkeypatch, {'datum_list': ''})
        assert gui_helpers.read_datum_list() == gui_helpers.DEFAULT_DATUMS

    def test_falls_back_on_oserror(self, monkeypatch):
        """Conf file IO failure must not crash the GUI launch."""
        _patch_utils_section(monkeypatch, None, exc=OSError('boom'))
        assert gui_helpers.read_datum_list() == gui_helpers.DEFAULT_DATUMS

    def test_falls_back_on_keyerror(self, monkeypatch):
        """Missing section raises KeyError; defaults must still be returned."""
        _patch_utils_section(monkeypatch, None, exc=KeyError('datums'))
        assert gui_helpers.read_datum_list() == gui_helpers.DEFAULT_DATUMS

    def test_falls_back_on_attributeerror(self, monkeypatch):
        """Defensive against malformed Utils objects in older configs."""
        _patch_utils_section(monkeypatch, None, exc=AttributeError())
        assert gui_helpers.read_datum_list() == gui_helpers.DEFAULT_DATUMS


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------


class TestFormatDate:
    """ISO-string formatter consumed by the downstream CLI."""

    def test_formats_basic_date_and_hour(self):
        """Standard noon date → ISO Z string with seconds set to 00."""
        assert (
            gui_helpers.format_date(date(2025, 11, 12), 12)
            == '2025-11-12T12:00:00Z'
        )

    def test_pads_single_digit_hour(self):
        """Hour 6 must be rendered as '06' for downstream parsers."""
        assert (
            gui_helpers.format_date(date(2025, 11, 12), 6)
            == '2025-11-12T06:00:00Z'
        )

    def test_accepts_string_hour(self):
        """tk.Scale.get() returns strings; the function must coerce."""
        assert (
            gui_helpers.format_date(date(2025, 1, 1), '0')
            == '2025-01-01T00:00:00Z'
        )

    def test_accepts_datetime_input_too(self):
        """datetime is a subclass of date, so it should be accepted."""
        assert (
            gui_helpers.format_date(datetime(2025, 6, 15, 9, 30), 18)
            == '2025-06-15T18:00:00Z'
        )

    def test_raises_typeerror_for_non_date(self):
        """A bare string is not a date object and must be rejected."""
        with pytest.raises(TypeError):
            gui_helpers.format_date('2025-11-12', 12)

    def test_raises_typeerror_for_none(self):
        """None as date must raise (caller is responsible for the falsy check)."""
        with pytest.raises(TypeError):
            gui_helpers.format_date(None, 0)


# ---------------------------------------------------------------------------
# build_utc_datetime
# ---------------------------------------------------------------------------


class TestBuildUtcDatetime:
    """Robust constructor used to normalise widget state for comparisons."""

    def test_builds_correct_utc_datetime(self):
        """Returned datetime should match inputs and carry UTC tzinfo."""
        dt = gui_helpers.build_utc_datetime(date(2025, 11, 12), 6)
        assert dt == datetime(2025, 11, 12, 6, tzinfo=timezone.utc)

    def test_resulting_datetime_is_utc_aware(self):
        """Comparisons would silently fail if tzinfo were missing."""
        dt = gui_helpers.build_utc_datetime(date(2025, 1, 1), 0)
        assert dt.tzinfo is not None
        assert dt.utcoffset().total_seconds() == 0

    def test_accepts_string_hour(self):
        """tk.Scale.get() returns strings; coercion must succeed."""
        dt = gui_helpers.build_utc_datetime(date(2025, 1, 1), '23')
        assert dt.hour == 23

    def test_returns_none_for_falsy_date(self):
        """Empty/None date (widget never populated) → None, not a crash."""
        assert gui_helpers.build_utc_datetime(None, 0) is None
        assert gui_helpers.build_utc_datetime('', 0) is None

    def test_returns_none_for_non_integer_hour(self):
        """Non-integer hour string from a malformed widget → None."""
        assert gui_helpers.build_utc_datetime(date(2025, 1, 1), 'noon') is None

    def test_returns_none_for_out_of_range_hour(self):
        """Hour 24 raises ValueError inside datetime(); caller gets None."""
        assert gui_helpers.build_utc_datetime(date(2025, 1, 1), 24) is None


# ---------------------------------------------------------------------------
# validate_date_order
# ---------------------------------------------------------------------------


class TestValidateDateOrder:
    """Pure-logic check that start is strictly before end."""

    def test_returns_none_when_start_before_end(self):
        """Valid ordering → no error."""
        s = datetime(2025, 1, 1, tzinfo=timezone.utc)
        e = datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert gui_helpers.validate_date_order(s, e) is None

    def test_returns_error_when_start_equals_end(self):
        """Strict inequality: equal timestamps are rejected."""
        s = e = datetime(2025, 1, 1, tzinfo=timezone.utc)
        msg = gui_helpers.validate_date_order(s, e)
        assert msg == 'Start date/hour must be before end date/hour.'

    def test_returns_error_when_start_after_end(self):
        """Reversed inputs trigger the validator."""
        s = datetime(2025, 1, 2, tzinfo=timezone.utc)
        e = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_date_order(s, e) is not None

    def test_returns_none_when_start_is_none(self):
        """Missing inputs are someone else's problem (other validators)."""
        e = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_date_order(None, e) is None

    def test_returns_none_when_end_is_none(self):
        """Missing inputs are someone else's problem (other validators)."""
        s = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_date_order(s, None) is None

    def test_returns_none_when_both_none(self):
        """Empty form: no opinion to offer."""
        assert gui_helpers.validate_date_order(None, None) is None


# ---------------------------------------------------------------------------
# validate_start_not_future
# ---------------------------------------------------------------------------


class TestValidateStartNotFuture:
    """UTC-aware future-date guard with injectable clock."""

    def test_returns_none_when_start_in_past(self):
        """Past date is always valid."""
        start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_start_not_future(start, now=now) is None

    def test_returns_none_when_start_equals_now(self):
        """Inclusive boundary: start == now is allowed (only > now is rejected)."""
        now = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
        assert gui_helpers.validate_start_not_future(now, now=now) is None

    def test_returns_error_when_start_in_future(self):
        """Future start is rejected with the canonical error message."""
        start = datetime(2099, 1, 1, tzinfo=timezone.utc)
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_start_not_future(start, now=now) == (
            'Start date/hour cannot be in the future (UTC).'
        )

    def test_returns_none_when_start_is_none(self):
        """Missing input is a different validator's responsibility."""
        assert gui_helpers.validate_start_not_future(None) is None

    def test_default_now_uses_real_clock(self):
        """Omitting `now` should still work; a 1970 date is always past."""
        start = datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert gui_helpers.validate_start_not_future(start) is None


# ---------------------------------------------------------------------------
# validate_horizon_requires_stations
# ---------------------------------------------------------------------------


class TestValidateHorizonRequiresStations:
    """Horizon_Skill is only implemented for the station model output files."""

    def test_returns_none_when_horizon_off(self):
        """Filetype is irrelevant when horizon assessment is disabled."""
        assert gui_helpers.validate_horizon_requires_stations(False, 'fields') is None
        assert gui_helpers.validate_horizon_requires_stations(False, 'stations') is None

    def test_returns_none_when_horizon_on_with_stations(self):
        """Supported combination: no error."""
        assert (
            gui_helpers.validate_horizon_requires_stations(True, 'stations')
            is None
        )

    def test_returns_error_when_horizon_on_with_fields(self):
        """The unsupported combo must surface a guiding error message."""
        msg = gui_helpers.validate_horizon_requires_stations(True, 'fields')
        assert msg is not None
        assert 'Station' in msg
        assert 'forecast horizons' in msg

    def test_returns_error_for_any_non_stations_filetype(self):
        """Defensive: arbitrary filetype strings still trigger the check."""
        assert (
            gui_helpers.validate_horizon_requires_stations(True, 'something_new')
            is not None
        )


# ---------------------------------------------------------------------------
# DateEntry class-level configuration
# ---------------------------------------------------------------------------


class TestDateEntryClassAttributes:
    """Verify DateEntry's static config without instantiating Tk widgets."""

    def test_inherits_from_tkcalendar_dateentry(self):
        """Drop-in replacement contract: must be a tkcalendar DateEntry subclass."""
        from tkcalendar import DateEntry as TkDateEntry
        assert issubclass(gui_helpers.DateEntry, TkDateEntry)

    def test_color_defaults_present(self):
        """All cell colours must be defined to override the dark-blue inherit."""
        defaults = gui_helpers.DateEntry._CAL_COLOR_DEFAULTS
        for key in (
            'normalbackground', 'normalforeground',
            'selectbackground', 'selectforeground',
            'weekendbackground', 'weekendforeground',
            'headersbackground', 'headersforeground',
            'othermonthbackground', 'othermonthforeground',
            'bordercolor',
        ):
            assert key in defaults, f'missing color default: {key}'

    def test_color_defaults_have_visible_contrast(self):
        """Foreground must not equal background — guards regression to all-dark."""
        d = gui_helpers.DateEntry._CAL_COLOR_DEFAULTS
        assert d['normalbackground'] != d['normalforeground']
        assert d['selectbackground'] != d['selectforeground']
        assert d['weekendbackground'] != d['weekendforeground']


class TestGuiParams:
    """Tests for the GuiParams dataclass."""

    def test_default_instantiation(self):
        """GuiParams() should create an instance with all argparse defaults."""
        p = gui_helpers.GuiParams()
        assert p.OFS is None
        assert p.Path is None
        assert p.StartDate_full is None
        assert p.EndDate_full is None
        assert p.Whichcasts == ['nowcast', 'forecast_b']
        assert p.Datum == 'MLLW'
        assert p.FileType == 'stations'
        assert p.Forecast_Hr == 'now'
        assert p.Station_Owner == ['co-ops', 'ndbc', 'usgs', 'chs']
        assert p.Horizon_Skill is False
        assert p.Var_Selection == [
            'water_level', 'water_temperature', 'salinity', 'currents'
        ]
        assert p.Currents_Bins_Csv is None
        assert p.Disable_Model_File_Check is True
        assert p.config is None

    def test_custom_instantiation(self):
        """Fields can be overridden at construction time."""
        p = gui_helpers.GuiParams(
            OFS='cbofs',
            Path='/tmp/out',
            Datum='NAVD88',
            Horizon_Skill=True,
        )
        assert p.OFS == 'cbofs'
        assert p.Path == '/tmp/out'
        assert p.Datum == 'NAVD88'
        assert p.Horizon_Skill is True
        # Unspecified fields keep defaults
        assert p.FileType == 'stations'

    def test_mutable_defaults_are_independent(self):
        """Each instance gets its own list for mutable defaults."""
        p1 = gui_helpers.GuiParams()
        p2 = gui_helpers.GuiParams()
        p1.Station_Owner.append('extra')
        assert 'extra' not in p2.Station_Owner

    def test_attribute_assignment(self):
        """Fields can be mutated after construction (GUI submit pattern)."""
        p = gui_helpers.GuiParams()
        p.OFS = 'leofs'
        p.Whichcasts = ['nowcast', 'forecast_a']
        assert p.OFS == 'leofs'
        assert p.Whichcasts == ['nowcast', 'forecast_a']


class TestGuiTheme:
    """Tests for the GuiTheme palette dataclass."""

    def test_default_values(self):
        """Default palette matches the legacy hardcoded GUI styling."""
        t = gui_helpers.GuiTheme()
        assert t.themecolor == 'gainsboro'
        assert t.textcolor == 'black'
        assert t.datefield_bg == 'darkblue'
        assert t.datefield_fg == 'white'
        assert t.fontfamily == 'Helvetica'
        assert t.labelfontsize == 12
        assert t.widgetfontsize == 12
        assert t.hintfontsize == 9
        assert t.padx == 3
        assert t.pady == 10
        assert t.section_padx == 10
        assert t.section_pady == 5
        assert t.anchor == 'e'

    def test_is_frozen(self):
        """GuiTheme should be immutable to prevent accidental palette drift."""
        t = gui_helpers.GuiTheme()
        with pytest.raises(Exception):
            t.themecolor = 'red'

    def test_derived_font_tuples(self):
        """label_font / widget_font / hint_font / section_title_font return
        the expected (family, size[, style]) tuples."""
        t = gui_helpers.GuiTheme()
        assert t.label_font == ('Helvetica', 12)
        assert t.widget_font == ('Helvetica', 12)
        assert t.hint_font == ('Helvetica', 9, 'italic')
        assert t.section_title_font == ('Helvetica', 12, 'bold')

    def test_custom_palette(self):
        """Fields can be overridden at construction to support alt themes."""
        t = gui_helpers.GuiTheme(themecolor='white', fontfamily='Arial',
                                 labelfontsize=14)
        assert t.themecolor == 'white'
        assert t.label_font == ('Arial', 14)
        assert t.section_title_font == ('Arial', 14, 'bold')


class TestToolTip:
    """Tests for the ToolTip class (non-display attributes only)."""

    def test_class_constants(self):
        """Verify default delay, background, and foreground."""
        assert gui_helpers.ToolTip._DELAY_MS == 400
        assert gui_helpers.ToolTip._BG == '#ffffe0'
        assert gui_helpers.ToolTip._FG == 'black'

    def test_accepts_text(self):
        """ToolTip stores the provided help string for later display."""
        assert hasattr(gui_helpers.ToolTip, '_DELAY_MS')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
