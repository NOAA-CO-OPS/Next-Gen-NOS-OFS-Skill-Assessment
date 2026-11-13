"""Tests for the modular GUI suite and shared layout/validation helpers.

Covers:
    - Import accessibility of all GUI sub-modules (2D, ice, obs, model)
    - Shared layout / validation / launch helpers in gui_helpers.py
    - GuiValidation class logic
    - Launcher dispatch (_open_selected_gui)
    - Console-script main() entry points in all bin scripts
    - OFS choice tuple consistency across modules

All tests run headless; Tk widget instantiation is avoided via mocking
where needed (the helpers are designed to accept duck-typed widgets).
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ofs_skill.visualization import gui_helpers


# ---------------------------------------------------------------------------
# Module-level imports
# ---------------------------------------------------------------------------


class TestGuiModuleImports:
    """Verify that all GUI sub-modules are importable."""

    def test_create_gui_2d_importable(self):
        mod = importlib.import_module('ofs_skill.visualization.create_gui_2d')
        assert hasattr(mod, 'create_gui_2d')
        assert callable(mod.create_gui_2d)

    def test_create_gui_ice_importable(self):
        mod = importlib.import_module('ofs_skill.visualization.create_gui_ice')
        assert hasattr(mod, 'create_gui_ice')
        assert callable(mod.create_gui_ice)

    def test_create_gui_obs_importable(self):
        mod = importlib.import_module('ofs_skill.visualization.create_gui_obs')
        assert hasattr(mod, 'create_gui_obs')
        assert callable(mod.create_gui_obs)

    def test_create_gui_model_importable(self):
        mod = importlib.import_module('ofs_skill.visualization.create_gui_model')
        assert hasattr(mod, 'create_gui_model')
        assert callable(mod.create_gui_model)

    def test_gui_launcher_importable(self):
        mod = importlib.import_module('bin.visualization.gui_launcher')
        assert hasattr(mod, 'launch')
        assert hasattr(mod, '_open_selected_gui')
        assert hasattr(mod, 'main')


# ---------------------------------------------------------------------------
# ScrollableForm dataclass
# ---------------------------------------------------------------------------


class TestScrollableForm:
    """Tests for the ScrollableForm dataclass."""

    def test_instantiation(self):
        sf = gui_helpers.ScrollableForm(
            container=MagicMock(), canvas=MagicMock(), frame=MagicMock(),
        )
        assert sf.container is not None
        assert sf.canvas is not None
        assert sf.frame is not None

    def test_fields_are_accessible(self):
        container = MagicMock(name='container')
        canvas = MagicMock(name='canvas')
        frame = MagicMock(name='frame')
        sf = gui_helpers.ScrollableForm(
            container=container, canvas=canvas, frame=frame,
        )
        assert sf.container is container
        assert sf.canvas is canvas
        assert sf.frame is frame


# ---------------------------------------------------------------------------
# GuiValidation class
# ---------------------------------------------------------------------------


class TestGuiValidation:
    """Tests for GuiValidation field highlighting logic."""

    def test_initial_state_is_empty(self):
        v = gui_helpers.GuiValidation()
        assert v._invalid_widgets == []
        assert v._dates_touched is False

    def test_mark_invalid_adds_widget(self):
        v = gui_helpers.GuiValidation()
        widget = MagicMock()
        widget.winfo_class.return_value = 'TCombobox'
        widget.configure = MagicMock()

        v.mark_invalid(widget)
        assert widget in v._invalid_widgets
        widget.configure.assert_called_once_with(style='Error.TCombobox')

    def test_mark_invalid_skips_none(self):
        v = gui_helpers.GuiValidation()
        v.mark_invalid(None)
        assert v._invalid_widgets == []

    def test_mark_invalid_skips_duplicate(self):
        v = gui_helpers.GuiValidation()
        widget = MagicMock()
        widget.winfo_class.return_value = 'TEntry'
        v.mark_invalid(widget)
        v.mark_invalid(widget)
        assert v._invalid_widgets.count(widget) == 1

    def test_mark_invalid_skips_unsupported_class(self):
        v = gui_helpers.GuiValidation()
        widget = MagicMock()
        widget.winfo_class.return_value = 'Canvas'
        v.mark_invalid(widget)
        assert widget not in v._invalid_widgets

    def test_clear_invalid_resets_styles(self):
        v = gui_helpers.GuiValidation()
        widget = MagicMock()
        widget.winfo_class.return_value = 'TSpinbox'
        v.mark_invalid(widget)
        assert len(v._invalid_widgets) == 1

        v.clear_invalid()
        assert v._invalid_widgets == []
        widget.configure.assert_called_with(style='TSpinbox')

    def test_clear_invalid_handles_destroyed_widget(self):
        """Destroyed widgets raise TclError; clear_invalid must not crash."""
        import tkinter as tk
        v = gui_helpers.GuiValidation()
        widget = MagicMock()
        widget.winfo_class.return_value = 'TEntry'
        v.mark_invalid(widget)

        widget.winfo_class.side_effect = tk.TclError('widget destroyed')
        v.clear_invalid()
        assert v._invalid_widgets == []

    def test_reset_dates_touched(self):
        v = gui_helpers.GuiValidation()
        v._dates_touched = True
        v.reset_dates_touched()
        assert v._dates_touched is False

    def test_mark_invalid_multiple_widgets(self):
        v = gui_helpers.GuiValidation()
        w1 = MagicMock()
        w1.winfo_class.return_value = 'TCombobox'
        w2 = MagicMock()
        w2.winfo_class.return_value = 'TEntry'
        w3 = MagicMock()
        w3.winfo_class.return_value = 'TLabel'

        v.mark_invalid(w1, w2, w3)
        assert len(v._invalid_widgets) == 3


# ---------------------------------------------------------------------------
# Error-state styling constants
# ---------------------------------------------------------------------------


class TestErrorStyleConstants:
    """Verify the error-state styling constants are correctly defined."""

    def test_error_ttk_classes_tuple(self):
        assert isinstance(gui_helpers._ERROR_TTK_CLASSES, tuple)
        assert 'TCombobox' in gui_helpers._ERROR_TTK_CLASSES
        assert 'TSpinbox' in gui_helpers._ERROR_TTK_CLASSES
        assert 'TEntry' in gui_helpers._ERROR_TTK_CLASSES
        assert 'TLabel' in gui_helpers._ERROR_TTK_CLASSES

    def test_error_colors_are_hex(self):
        assert gui_helpers._ERROR_BG.startswith('#')
        assert gui_helpers._ERROR_BORDER.startswith('#')
        assert gui_helpers._ERROR_LABEL_BG.startswith('#')


# ---------------------------------------------------------------------------
# create_action_buttons (functional signature)
# ---------------------------------------------------------------------------


class TestCreateActionButtons:
    """Verify create_action_buttons signature and return type expectations."""

    def test_function_exists_and_is_callable(self):
        assert callable(gui_helpers.create_action_buttons)

    def test_signature_requires_keyword_args(self):
        import inspect
        sig = inspect.signature(gui_helpers.create_action_buttons)
        params = list(sig.parameters.keys())
        assert 'parent' in params
        assert 'reset_command' in params
        assert 'submit_command' in params
        assert 'submit_text' in params


# ---------------------------------------------------------------------------
# launch_with_progress (functional signature)
# ---------------------------------------------------------------------------


class TestLaunchWithProgress:
    """Verify launch_with_progress signature."""

    def test_function_exists_and_is_callable(self):
        assert callable(gui_helpers.launch_with_progress)

    def test_signature_has_expected_params(self):
        import inspect
        sig = inspect.signature(gui_helpers.launch_with_progress)
        params = list(sig.parameters.keys())
        assert 'root' in params
        assert 'scrollable_frame' in params
        assert 'theme' in params
        assert 'runner' in params
        assert 'params' in params
        assert 'progress_title' in params
        assert 'runner_exception' in params
        assert 'before_run' in params
        assert 'destroy_on_complete' in params


# ---------------------------------------------------------------------------
# show_summary_confirmation (functional signature)
# ---------------------------------------------------------------------------


class TestShowSummaryConfirmation:
    """Verify show_summary_confirmation signature."""

    def test_function_exists(self):
        assert callable(gui_helpers.show_summary_confirmation)

    def test_signature_keyword_only(self):
        import inspect
        sig = inspect.signature(gui_helpers.show_summary_confirmation)
        params = list(sig.parameters.keys())
        assert 'root' in params
        assert 'theme' in params
        assert 'window_title' in params
        assert 'heading' in params
        assert 'rows' in params


# ---------------------------------------------------------------------------
# form_section / form_label / collapsible_section (signatures)
# ---------------------------------------------------------------------------


class TestFormHelperSignatures:
    """Verify the shared form-building helper signatures."""

    def test_form_section_callable(self):
        assert callable(gui_helpers.form_section)

    def test_form_label_callable(self):
        assert callable(gui_helpers.form_label)

    def test_collapsible_section_callable(self):
        assert callable(gui_helpers.collapsible_section)

    def test_configure_gui_styles_callable(self):
        assert callable(gui_helpers.configure_gui_styles)

    def test_setup_scrollable_form_callable(self):
        assert callable(gui_helpers.setup_scrollable_form)

    def test_apply_window_icon_callable(self):
        assert callable(gui_helpers.apply_window_icon)

    def test_set_date_entry_today_callable(self):
        assert callable(gui_helpers.set_date_entry_today)


# ---------------------------------------------------------------------------
# Launcher dispatch logic
# ---------------------------------------------------------------------------


class TestLauncherDispatch:
    """Test _open_selected_gui dispatch logic without opening real GUIs."""

    def test_invalid_key_raises_value_error(self):
        from bin.visualization.gui_launcher import _open_selected_gui
        with pytest.raises(ValueError, match='Unknown GUI module'):
            _open_selected_gui('nonexistent')

    def test_1d_key_dispatches_to_create_gui(self):
        from bin.visualization import gui_launcher
        with patch.object(gui_launcher, '__builtins__', gui_launcher.__builtins__):
            with patch('bin.visualization.create_1dplot._run_pipeline') as mock_run:
                with patch('ofs_skill.visualization.create_gui.create_gui') as mock_gui:
                    gui_launcher._open_selected_gui('1d')
                    mock_gui.assert_called_once()

    def test_2d_key_dispatches_to_create_gui_2d(self):
        from bin.visualization import gui_launcher
        with patch('ofs_skill.visualization.create_gui_2d.create_gui_2d') as mock_gui:
            gui_launcher._open_selected_gui('2d')
            mock_gui.assert_called_once()
            kwargs = mock_gui.call_args
            assert kwargs[1].get('runner') is not None or kwargs[0] != ()

    def test_ice_key_dispatches_to_create_gui_ice(self):
        from bin.visualization import gui_launcher
        with patch('ofs_skill.visualization.create_gui_ice.create_gui_ice') as mock_gui:
            gui_launcher._open_selected_gui('ice')
            mock_gui.assert_called_once()

    def test_obs_key_dispatches_to_create_gui_obs(self):
        from bin.visualization import gui_launcher
        with patch('ofs_skill.visualization.create_gui_obs.create_gui_obs') as mock_gui:
            gui_launcher._open_selected_gui('obs')
            mock_gui.assert_called_once()

    def test_model_key_dispatches_to_create_gui_model(self):
        from bin.visualization import gui_launcher
        with patch('ofs_skill.visualization.create_gui_model.create_gui_model') as mock_gui:
            gui_launcher._open_selected_gui('model')
            mock_gui.assert_called_once()


# ---------------------------------------------------------------------------
# Console-script entry points
# ---------------------------------------------------------------------------


class TestConsoleScriptEntryPoints:
    """Verify that all bin scripts expose a callable main(argv=None)."""

    @pytest.mark.parametrize('module_path', [
        'bin.visualization.create_1dplot',
        'bin.visualization.create_2dplot',
        'bin.skill_assessment.do_iceskill',
        'bin.obs_retrieval.get_station_observations_cli',
        'bin.model_processing.get_node_cli',
        'bin.visualization.gui_launcher',
    ])
    def test_main_function_exists(self, module_path):
        mod = importlib.import_module(module_path)
        assert hasattr(mod, 'main'), f'{module_path} missing main()'
        assert callable(mod.main)

    @pytest.mark.parametrize('module_path', [
        'bin.visualization.create_1dplot',
        'bin.visualization.create_2dplot',
        'bin.skill_assessment.do_iceskill',
        'bin.obs_retrieval.get_station_observations_cli',
        'bin.model_processing.get_node_cli',
    ])
    def test_run_pipeline_function_exists(self, module_path):
        mod = importlib.import_module(module_path)
        assert hasattr(mod, '_run_pipeline'), (
            f'{module_path} missing _run_pipeline()'
        )
        assert callable(mod._run_pipeline)

    @pytest.mark.parametrize('module_path', [
        'bin.visualization.create_1dplot',
        'bin.visualization.create_2dplot',
        'bin.skill_assessment.do_iceskill',
        'bin.obs_retrieval.get_station_observations_cli',
        'bin.model_processing.get_node_cli',
    ])
    def test_main_accepts_argv_keyword(self, module_path):
        """main(argv=...) must accept the argv keyword for testability."""
        import inspect
        mod = importlib.import_module(module_path)
        sig = inspect.signature(mod.main)
        assert 'argv' in sig.parameters


# ---------------------------------------------------------------------------
# GUI module public API surface
# ---------------------------------------------------------------------------


class TestGuiModuleAPIs:
    """Check that each GUI module exposes its public create function."""

    def test_create_gui_2d_signature(self):
        import inspect
        from ofs_skill.visualization.create_gui_2d import create_gui_2d
        sig = inspect.signature(create_gui_2d)
        assert 'runner' in sig.parameters

    def test_create_gui_ice_signature(self):
        import inspect
        from ofs_skill.visualization.create_gui_ice import create_gui_ice
        sig = inspect.signature(create_gui_ice)
        assert 'runner' in sig.parameters

    def test_create_gui_obs_signature(self):
        import inspect
        from ofs_skill.visualization.create_gui_obs import create_gui_obs
        sig = inspect.signature(create_gui_obs)
        assert 'runner' in sig.parameters

    def test_create_gui_model_signature(self):
        import inspect
        from ofs_skill.visualization.create_gui_model import create_gui_model
        sig = inspect.signature(create_gui_model)
        assert 'runner' in sig.parameters


# ---------------------------------------------------------------------------
# OFS choices consistency
# ---------------------------------------------------------------------------


class TestOfsChoicesConsistency:
    """Verify OFS choice tuples are consistent across modules."""

    def test_2d_gui_has_ofs_choices(self):
        from ofs_skill.visualization import create_gui_2d
        assert hasattr(create_gui_2d, '_OFS_CHOICES')
        assert len(create_gui_2d._OFS_CHOICES) > 1

    def test_obs_gui_has_ofs_choices(self):
        from ofs_skill.visualization import create_gui_obs
        assert hasattr(create_gui_obs, '_OFS_CHOICES')
        assert len(create_gui_obs._OFS_CHOICES) > 1

    def test_model_gui_has_ofs_choices(self):
        from ofs_skill.visualization import create_gui_model
        assert hasattr(create_gui_model, '_OFS_CHOICES')
        assert len(create_gui_model._OFS_CHOICES) > 1

    def test_ice_gui_uses_great_lakes_only(self):
        from ofs_skill.visualization import create_gui_ice
        assert hasattr(create_gui_ice, '_ICE_OFS_CHOICES')
        for ofs in gui_helpers.GREAT_LAKES_OFS:
            assert ofs in create_gui_ice._ICE_OFS_CHOICES

    def test_2d_and_obs_ofs_lists_match(self):
        """2D and Obs GUIs should share the same OFS list."""
        from ofs_skill.visualization import create_gui_2d, create_gui_obs
        assert create_gui_2d._OFS_CHOICES == create_gui_obs._OFS_CHOICES

    def test_obs_and_model_ofs_lists_match(self):
        """Obs and Model GUIs should share the same OFS list."""
        from ofs_skill.visualization import create_gui_model, create_gui_obs
        assert create_gui_obs._OFS_CHOICES == create_gui_model._OFS_CHOICES

    def test_placeholder_is_first_element(self):
        """Each OFS tuple starts with a placeholder that is not a real OFS."""
        from ofs_skill.visualization import (
            create_gui_2d,
            create_gui_ice,
            create_gui_model,
            create_gui_obs,
        )
        for mod in (create_gui_2d, create_gui_obs, create_gui_model):
            assert mod._OFS_CHOICES[0] == mod._OFS_PLACEHOLDER
        assert create_gui_ice._ICE_OFS_CHOICES[0] == create_gui_ice._OFS_PLACEHOLDER


# ---------------------------------------------------------------------------
# apply_gui_session (headless integration)
# ---------------------------------------------------------------------------


class TestApplyGuiSessionIntegration:
    """Integration test for apply_gui_session with mock Tk variables."""

    @pytest.fixture
    def session_file(self, tmp_path, monkeypatch):
        path = tmp_path / 'gui_session.json'
        monkeypatch.setattr(gui_helpers, 'SESSION_FILE', path)
        return path

    def test_applies_ofs_to_stringvar_mock(self, session_file):
        gui_helpers.save_gui_session(gui_helpers.GuiSession(
            Path='/data', OFS='cbofs', config='my.conf',
        ))
        ofs_var = MagicMock()
        directory_var = MagicMock()
        config_var = MagicMock()
        session = gui_helpers.apply_gui_session(
            directory_var=directory_var,
            config_var=config_var,
            ofs_var=ofs_var,
            ofs_choices=('Select...', 'cbofs', 'ngofs2'),
            ofs_placeholder='Select...',
        )
        directory_var.set.assert_called_with('/data')
        config_var.set.assert_called_with('my.conf')
        ofs_var.set.assert_called_with('cbofs')

    def test_skips_ofs_not_in_choices(self, session_file):
        gui_helpers.save_gui_session(gui_helpers.GuiSession(OFS='cbofs'))
        ofs_var = MagicMock()
        gui_helpers.apply_gui_session(
            ofs_var=ofs_var,
            ofs_choices=('Select...', 'ngofs2', 'tbofs'),
            ofs_placeholder='Select...',
        )
        ofs_var.set.assert_not_called()

    def test_applies_datum_when_valid(self, session_file):
        gui_helpers.save_gui_session(gui_helpers.GuiSession(Datum='NAVD88'))
        datum_var = MagicMock()
        gui_helpers.apply_gui_session(
            datum_var=datum_var,
            datum_choices=('Select datum...', 'MLLW', 'NAVD88'),
            datum_placeholder='Select datum...',
        )
        datum_var.set.assert_called_with('NAVD88')

    def test_skips_datum_placeholder(self, session_file):
        gui_helpers.save_gui_session(gui_helpers.GuiSession(Datum='Select datum...'))
        datum_var = MagicMock()
        gui_helpers.apply_gui_session(
            datum_var=datum_var,
            datum_choices=('Select datum...', 'MLLW'),
            datum_placeholder='Select datum...',
        )
        datum_var.set.assert_not_called()

    def test_applies_hours(self, session_file):
        gui_helpers.save_gui_session(gui_helpers.GuiSession(
            start_hour=6, end_hour=18,
        ))
        s_hour = MagicMock()
        e_hour = MagicMock()
        gui_helpers.apply_gui_session(s_hour_var=s_hour, e_hour_var=e_hour)
        s_hour.set.assert_called_with(6)
        e_hour.set.assert_called_with(18)


# ---------------------------------------------------------------------------
# pyproject.toml entry point declarations
# ---------------------------------------------------------------------------


class TestPyprojectConsoleScripts:
    """Verify that pyproject.toml declares all expected console scripts."""

    @pytest.fixture
    def pyproject_scripts(self):
        pyproject = Path(__file__).parent.parent / 'pyproject.toml'
        assert pyproject.exists(), 'pyproject.toml not found'
        content = pyproject.read_text()
        in_scripts = False
        scripts = {}
        for line in content.splitlines():
            if line.strip() == '[project.scripts]':
                in_scripts = True
                continue
            if in_scripts:
                if line.startswith('['):
                    break
                if '=' in line:
                    key, value = line.split('=', 1)
                    scripts[key.strip()] = value.strip().strip('"')
        return scripts

    @pytest.mark.parametrize('script_name', [
        'create-1dplot',
        'create-2dplot',
        'do-iceskill',
        'get-station-observations',
        'get-node-ofs',
        'ofs-skill-gui',
    ])
    def test_script_declared(self, pyproject_scripts, script_name):
        assert script_name in pyproject_scripts, (
            f'{script_name} not in [project.scripts]'
        )

    def test_ofs_skill_gui_points_to_launcher(self, pyproject_scripts):
        assert 'gui_launcher:main' in pyproject_scripts.get('ofs-skill-gui', '')


# ---------------------------------------------------------------------------
# Bin wrapper scripts for modular GUIs
# ---------------------------------------------------------------------------


class TestBinWrapperScripts:
    """Verify the thin bin wrapper scripts exist and are importable."""

    @pytest.mark.parametrize('script', [
        'bin/visualization/create_gui_2d.py',
        'bin/visualization/create_gui_ice.py',
        'bin/visualization/create_gui_obs.py',
        'bin/visualization/create_gui_model.py',
        'bin/visualization/gui_launcher.py',
    ])
    def test_script_exists(self, script):
        path = Path(__file__).parent.parent / script
        assert path.exists(), f'{script} not found'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
