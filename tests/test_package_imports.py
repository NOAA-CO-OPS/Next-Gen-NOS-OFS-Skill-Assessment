"""
Test suite to verify package imports after migration.

This ensures that all migrated modules can be imported correctly
and that the package structure is working as expected.
"""

import pytest


class TestPackageStructure:
    """Test that the package structure is correct."""

    def test_top_level_import(self):
        """Test that the top-level package can be imported."""
        import ofs_skill
        assert hasattr(ofs_skill, '__version__')
        assert ofs_skill.__version__ == '1.5.0'

    def test_model_properties_import(self):
        """Test that ModelProperties can be imported from top level."""
        from ofs_skill import ModelProperties
        assert ModelProperties is not None


class TestModelProcessingImports:
    """Test imports from model_processing module."""

    def test_model_processing_module(self):
        """Test that model_processing module can be imported."""
        from ofs_skill import model_processing
        assert model_processing is not None

    def test_model_properties(self):
        """Test ModelProperties class import."""
        from ofs_skill.model_processing import ModelProperties
        prop = ModelProperties()
        assert hasattr(prop, 'ofs')
        assert hasattr(prop, 'datum')

    def test_model_source(self):
        """Test get_model_source function."""
        from ofs_skill.model_processing import get_model_source
        assert get_model_source('cbofs') == 'roms'
        assert get_model_source('ngofs2') == 'fvcom'

    def test_indexing_functions(self):
        """Test indexing module functions."""
        from ofs_skill.model_processing import (
            index_nearest_depth,
            index_nearest_node,
            index_nearest_station,
        )
        assert callable(index_nearest_node)
        assert callable(index_nearest_depth)
        assert callable(index_nearest_station)

    def test_forecast_hours(self):
        """Test get_forecast_hours function."""
        from ofs_skill.model_processing.get_forecast_hours import get_forecast_hours
        fcstlength, fcstcycles = get_forecast_hours('cbofs')
        assert fcstlength == 48
        assert len(fcstcycles) == 4


class TestObsRetrievalImports:
    """Test imports from obs_retrieval module."""

    def test_obs_retrieval_module(self):
        """Test that obs_retrieval module can be imported."""
        from ofs_skill import obs_retrieval
        assert obs_retrieval is not None

    def test_utils(self):
        """Test Utils class import."""
        from ofs_skill.obs_retrieval import Utils
        assert Utils is not None

    def test_station_ctl_file_extract(self):
        """Test station_ctl_file_extract function."""
        from ofs_skill.obs_retrieval import station_ctl_file_extract
        assert callable(station_ctl_file_extract)

    def test_format_functions(self):
        """Test format functions."""
        from ofs_skill.obs_retrieval import scalar, vector
        assert callable(scalar)
        assert callable(vector)

    def test_retrieve_functions(self):
        """Test retrieve functions."""
        from ofs_skill.obs_retrieval import (
            retrieve_ndbc_station,
            retrieve_t_and_c_station,
            retrieve_usgs_station,
        )
        assert callable(retrieve_t_and_c_station)
        assert callable(retrieve_ndbc_station)
        assert callable(retrieve_usgs_station)

    def test_inventory_functions(self):
        """Test inventory functions."""
        from ofs_skill.obs_retrieval import (
            inventory_ndbc_station,
            inventory_t_c_station,
            inventory_usgs_station,
            ofs_inventory_stations,
        )
        assert callable(inventory_t_c_station)
        assert callable(inventory_ndbc_station)
        assert callable(inventory_usgs_station)
        assert callable(ofs_inventory_stations)

    def test_main_observation_functions(self):
        """Test main observation retrieval functions."""
        from ofs_skill.obs_retrieval import (
            get_station_observations,
            write_obs_ctlfile,
        )
        assert callable(get_station_observations)
        assert callable(write_obs_ctlfile)


class TestSkillAssessmentImports:
    """Test imports from skill_assessment module."""

    def test_skill_assessment_module(self):
        """Test that skill_assessment module can be imported."""
        from ofs_skill import skill_assessment
        assert skill_assessment is not None

    def test_get_skill(self):
        """Test get_skill function."""
        from ofs_skill.skill_assessment import get_skill
        assert callable(get_skill)

    def test_skill_functions(self):
        """Test skill calculation functions."""
        from ofs_skill.skill_assessment import (
            skill,
            skill_scalar,
            skill_vector,
        )
        assert callable(skill)
        assert callable(skill_scalar)
        assert callable(skill_vector)

    def test_paired_functions(self):
        """Test paired data functions."""
        from ofs_skill.skill_assessment import (
            paired_scalar,
            paired_vector,
        )
        assert callable(paired_scalar)
        assert callable(paired_vector)


class TestVisualizationImports:
    """Test imports from visualization module."""

    def test_visualization_module(self):
        """Test that visualization module exists (don't import to avoid circular imports)."""
        from pathlib import Path
        viz_path = Path(__file__).parent.parent / 'src' / 'ofs_skill' / 'visualization'
        assert viz_path.exists()
        assert (viz_path / '__init__.py').exists()

    def test_visualization_submodules(self):
        """Test that visualization library modules exist as files."""
        from pathlib import Path
        viz_path = Path(__file__).parent.parent / 'src' / 'ofs_skill' / 'visualization'

        # Check that library module files exist (not CLI scripts)
        assert (viz_path / 'plotting_scalar.py').exists()
        assert (viz_path / 'plotting_vector.py').exists()
        assert (viz_path / 'plotting_functions.py').exists()
        assert (viz_path / 'processing_2d.py').exists()


class TestBackwardCompatibility:
    """Test that old CLI scripts still work."""

    def test_cli_scripts_exist(self):
        """Test that CLI scripts still exist in bin/."""
        from pathlib import Path
        repo_root = Path(__file__).parent.parent

        assert (repo_root / 'bin' / 'visualization' / 'create_1dplot.py').exists()
        assert (repo_root / 'bin' / 'visualization' / 'create_2dplot.py').exists()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
