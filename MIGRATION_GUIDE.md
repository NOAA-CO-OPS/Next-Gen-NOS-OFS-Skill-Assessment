# Package Migration Guide

This guide explains the migration from the old `sys.path` manipulation pattern to the new proper Python package structure.

## What Changed?

The codebase has been refactored to follow Python packaging best practices:

- **Before**: Code used `sys.path.append()` to enable imports
- **After**: Code is now a proper installable package under `src/ofs_skill/`

## For End Users (CLI Usage)

**Good news: Nothing changes!** CLI scripts still work exactly the same way:

```bash
# Still works exactly as before
python ./bin/visualization/create_1dplot.py -p ./ -o cbofs -s 2025-12-15T00:00:00Z -e 2025-12-16T00:00:00Z -d MLLW -ws [Nowcast] -t stations -so NDBC
```

## For Developers (Programmatic Usage)

### Installation

The package can now be installed in editable mode for development:

```bash
pip install -e .
```

Or install from the repository:

```bash
pip install git+https://github.com/NOAA-CO-OPS/NOS_Shared_Cyberinfrastructure_and_Skill_Assessment.git
```

### Old Import Pattern (Deprecated)

```python
# OLD - Don't use this anymore
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from model_processing import model_properties
from obs_retrieval import utils
```

### New Import Pattern (Recommended)

```python
# NEW - Use proper package imports
from ofs_skill.model_processing import ModelProperties
from ofs_skill.obs_retrieval import Utils
from ofs_skill.skill_assessment import get_skill
from ofs_skill.visualization import create_1dplot
```

## Package Structure

```
ofs_skill/
├── model_processing/      # Model data processing
│   ├── ModelProperties
│   ├── get_model_source()
│   ├── get_forecast_hours()
│   └── indexing functions
├── obs_retrieval/         # Observation data retrieval
│   ├── Utils
│   ├── retrieve_*_station()
│   ├── inventory_*_station()
│   └── get_station_observations()
├── skill_assessment/      # Statistical skill metrics
│   ├── get_skill()
│   ├── skill_scalar()
│   ├── skill_vector()
│   └── paired_*()
└── visualization/         # Plotting and visualization
    ├── create_1dplot
    ├── plotting_scalar
    ├── plotting_vector
    └── plotting_functions
```

## Common Usage Examples

### Example 1: Setting up model properties

```python
from ofs_skill.model_processing import ModelProperties

# Create model properties object
props = ModelProperties()
props.ofs = 'cbofs'
props.datum = 'MLLW'
props.start_date_full = '2025-12-15T00:00:00Z'
props.end_date_full = '2025-12-16T00:00:00Z'
```

### Example 2: Retrieving observations

```python
from ofs_skill.obs_retrieval import get_station_observations
import logging

# Set up logger
logger = logging.getLogger(__name__)

# Retrieve station observations
get_station_observations(props, logger)
```

### Example 3: Running skill assessment

```python
from ofs_skill.skill_assessment import get_skill

# Run skill assessment
get_skill(props, logger)
```

### Example 4: Checking model source

```python
from ofs_skill.model_processing import get_model_source

# Get the numerical model framework for an OFS
model_type = get_model_source('cbofs')  # Returns 'roms'
model_type = get_model_source('ngofs2') # Returns 'fvcom'
```

### Example 5: Getting forecast hours

```python
from ofs_skill.model_processing import get_forecast_hours
import numpy as np

# Get forecast cycle information
fcst_length, fcst_cycles = get_forecast_hours('cbofs')
# fcst_length = 48
# fcst_cycles = array([0, 6, 12, 18])
```

## Testing the Migration

Run the test suite to verify imports work correctly:

```bash
pytest tests/test_package_imports.py -v
```

Run the full test suite:

```bash
pytest tests/ -v
```

## Benefits of the New Structure

1. **No more sys.path manipulation** - Cleaner, more maintainable code
2. **Proper package imports** - Standard Python import patterns
3. **Installable package** - Can be installed via pip
4. **Better IDE support** - Auto-completion and type checking work properly
5. **Easier to distribute** - Can be published to PyPI
6. **Backward compatible** - CLI scripts still work the same way

## Troubleshooting

### Import errors after migration

If you see `ModuleNotFoundError: No module named 'ofs_skill'`, make sure you've installed the package:

```bash
pip install -e .
```

### Circular import errors

If you encounter circular imports, check that you're using the new import patterns. Some modules use lazy imports to avoid circular dependencies.

### Missing dependencies

Install all dependencies:

```bash
pip install -e .
```

Or install specific missing packages:

```bash
pip install seaborn tkcalendar searvey s3fs intake intake-xarray
```

## Questions?

For issues or questions about the migration, please:
- Check the [README.md](README.md) for general usage
- Review the test files in `tests/` for examples
- Open an issue on GitHub
- Contact: co-ops.userservices@noaa.gov
