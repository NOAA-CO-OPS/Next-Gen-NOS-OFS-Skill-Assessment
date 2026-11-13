# API Reference

Quick reference for programmatic use of the `ofs_skill` package.

## Installation

```bash
# Install in development mode
pip install -e .

# Or install from GitHub
pip install git+https://github.com/NOAA-CO-OPS/Next-Gen-NOS-OFS-Skill-Assessment.git
```

## Core Modules

### ofs_skill.model_processing

Process and configure OFS model data.

```python
from ofs_skill.model_processing import (
    ModelProperties,           # Main configuration class
    get_model_source,          # Determine model framework (ROMS, FVCOM, etc.)
    get_fcst_hours,        # Get forecast cycle information
    get_datum_offset,          # Calculate datum offsets
    index_nearest_node,        # Find nearest model grid node
    index_nearest_depth,       # Find nearest depth level
    index_nearest_station,     # Find nearest station
)
```

**ModelProperties** - Main configuration class:
```python
props = ModelProperties()
props.ofs = 'cbofs'                           # OFS name
props.datum = 'MLLW'                          # Vertical datum
props.start_date_full = '2025-01-15T00:00:00Z'  # Start date
props.end_date_full = '2025-01-16T00:00:00Z'    # End date
props.path = './'                             # Working directory
props.whichcast = 'nowcast'                   # nowcast, forecast_a, or forecast_b
```

**get_model_source()** - Determine numerical model framework:
```python
model_type = get_model_source('cbofs')   # Returns: 'roms'
model_type = get_model_source('ngofs2')  # Returns: 'fvcom'
model_type = get_model_source('stofs3d') # Returns: 'schism'
```

**get_fcst_hours()** - Get forecast cycle info:
```python
fcst_length, fcst_cycles = get_fcst_hours('cbofs')
# fcst_length: 48 (hours)
# fcst_cycles: array([0, 6, 12, 18])
```

---

### ofs_skill.obs_retrieval

Retrieve and process observational data.

```python
from ofs_skill.obs_retrieval import (
    Utils,                         # Configuration file utilities
    get_station_observations,      # Main observation retrieval function
    retrieve_t_and_c_station,      # Retrieve NOAA CO-OPS data
    retrieve_ndbc_station,         # Retrieve NDBC buoy data
    retrieve_usgs_station,         # Retrieve USGS gauge data
    inventory_t_c_station,         # Inventory CO-OPS stations
    inventory_ndbc_station,        # Inventory NDBC stations
    inventory_usgs_station,        # Inventory USGS stations
    ofs_inventory_stations,        # Create combined station inventory
    station_ctl_file_extract,      # Extract station control file info
    write_obs_ctlfile,            # Write observation control files
    scalar,                        # Format scalar observations
    vector,                        # Format vector observations
)
```

**get_station_observations()** - Retrieve all observations for a model run:
```python
import logging
logger = logging.getLogger(__name__)

get_station_observations(props, logger)
# Downloads observation data for all stations in the OFS domain
# Creates .obs files in data/observations/1d_station/
```

**Utils** - Read configuration files:
```python
utils = Utils()
dir_params = utils.read_config_section('directories', logger)
datum_list = utils.read_config_section('datums', logger)
```

**Retrieve functions** - Get data from specific sources:
```python
from ofs_skill.obs_retrieval import RetrieveProperties

# Set up retrieval parameters
retrieve_input = RetrieveProperties()
retrieve_input.station = '8575512'
retrieve_input.start_date = '20250115'
retrieve_input.end_date = '20250116'
retrieve_input.variable = 'water_level'
retrieve_input.datum = 'MLLW'

# Retrieve from CO-OPS
timeseries = retrieve_t_and_c_station(retrieve_input, logger)

# Retrieve from NDBC
data = retrieve_ndbc_station('20250115', '20250116', '44065', 'water_temperature', logger)
```

---

### ofs_skill.skill_assessment

Calculate statistical skill metrics.

```python
from ofs_skill.skill_assessment import (
    get_skill,          # Main skill assessment function
    skill,              # Calculate all skill metrics
    skill_scalar,       # Skill metrics for scalar variables
    skill_vector,       # Skill metrics for vector variables
    paired_scalar,      # Pair model and observation data (scalar)
    paired_vector,      # Pair model and observation data (vector)
)
```

**get_skill()** - Run complete skill assessment:
```python
import logging
logger = logging.getLogger(__name__)

get_skill(props, logger)
# Calculates skill statistics for all variables
# Creates paired datasets and skill tables
# Outputs results to data/visual/ directory
```

**skill_scalar()** - Calculate scalar skill metrics:
```python
from ofs_skill.skill_assessment import skill_scalar

# df_paired holds the paired series with 'OBS' and 'OFS' columns.
# name_var is the short variable code: 'wl', 'temp', or 'salt'.
metrics = skill_scalar(df_paired, 'wl', station_id, props, logger)
```

Returns a **list** of 19 positional values (not a dict):

```text
[rmse, r_value, bias, bias_perc, nan, cf, cfpf, pof, pofpf,
 nof, nofpf, mdpo, mdpopf, mdno, mdnopf, wof, wofpf, stdev, X1]
```

| Index | Name | Meaning |
| --- | --- | --- |
| 0 | `rmse` | Root mean squared error |
| 1 | `r_value` | Pearson correlation coefficient |
| 2 | `bias` | Mean bias |
| 3 | `bias_perc` | Mean bias as a percentage |
| 4 | -- | Unused slot (always `numpy.nan`; holds `bias_dir` in `skill_vector`) |
| 5-6 | `cf`, `cfpf` | Central frequency and its pass/fail flag |
| 7-8 | `pof`, `pofpf` | Positive outlier frequency and pass/fail |
| 9-10 | `nof`, `nofpf` | Negative outlier frequency and pass/fail |
| 11-12 | `mdpo`, `mdpopf` | Max duration of positive outliers and pass/fail |
| 13-14 | `mdno`, `mdnopf` | Max duration of negative outliers and pass/fail |
| 15-16 | `wof`, `wofpf` | Worst-case outlier frequency and pass/fail |
| 17 | `stdev` | Standard deviation of the bias |
| 18 | `X1` | Error threshold used for the outlier metrics |

**skill_vector()** - Calculate vector skill metrics:
```python
from ofs_skill.skill_assessment import skill_vector

# df_paired holds the paired currents series; name_var is 'cu'.
metrics = skill_vector(df_paired, 'cu', props, logger)
```

Returns a list with the same 19-slot layout as `skill_scalar`, except that
index 4 carries `bias_dir` (mean direction bias) instead of `nan`, and the
speed series supplies `rmse`/`r_value`/`bias`. There is no direction RMSE.

---

### ofs_skill.visualization

Create plots and visualizations.

```python
from ofs_skill.visualization import create_1dplot
from ofs_skill.visualization import plotting_scalar
from ofs_skill.visualization import plotting_vector
from ofs_skill.visualization import plotting_functions
```

**Note**: Visualization modules are typically accessed through their main entry point or used indirectly through the CLI scripts.

**Main functions**:
```python
# Create 1D time series plots
from ofs_skill.visualization.plotting_scalar import oned_scalar_plot
from ofs_skill.visualization.plotting_vector import oned_vector_plot1

# These are typically called by create_1dplot.py CLI script
# For programmatic use, see the CLI scripts as examples
```

---

## Common Workflows

### Complete 1D Skill Assessment Workflow

```python
from ofs_skill.model_processing import ModelProperties
from ofs_skill.obs_retrieval import get_station_observations
from ofs_skill.skill_assessment import get_skill
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Configure model run
props = ModelProperties()
props.ofs = 'cbofs'
props.datum = 'MLLW'
props.start_date_full = '2025-01-15T00:00:00Z'
props.end_date_full = '2025-01-16T00:00:00Z'
props.path = './'
props.whichcast = 'nowcast'
props.stationowner = 'co-ops,ndbc,usgs'
props.var_list = 'water_level,water_temperature,salinity,currents'

# 2. Retrieve observations
logger.info('Retrieving station observations...')
get_station_observations(props, logger)

# 3. Run skill assessment
logger.info('Running skill assessment...')
get_skill(props, logger)

logger.info('Skill assessment complete!')
```

### Check Model Configuration

```python
from ofs_skill.model_processing import (
    get_model_source,
    get_fcst_hours,
)

# Check what type of model an OFS uses
model_framework = get_model_source('cbofs')
print(f"CBOFS uses {model_framework} framework")

# Get forecast cycle information
fcst_length, fcst_cycles = get_fcst_hours('cbofs')
print(f"Forecast length: {fcst_length} hours")
print(f"Forecast cycles: {fcst_cycles}")
```

### Create Station Inventory

```python
from ofs_skill.obs_retrieval import ofs_inventory_stations
import logging

logger = logging.getLogger(__name__)

# Create inventory of all available stations in OFS domain
inventory = ofs_inventory_stations(
    ofs='cbofs',
    start_date='20250115',
    end_date='20250116',
    path='./',
    stationowner='co-ops,ndbc,usgs',
    logger=logger
)

# Inventory saved to: control_files/inventory_all_cbofs.csv
print(f"Found {len(inventory)} stations")
```

---

## Data Structures

### ModelProperties Attributes

Key attributes you'll commonly set:

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `ofs` | str | OFS name | `'cbofs'` |
| `datum` | str | Vertical datum | `'MLLW'`, `'NAVD88'`, `'MSL'` |
| `start_date_full` | str | Start datetime (ISO format) | `'2025-01-15T00:00:00Z'` |
| `end_date_full` | str | End datetime (ISO format) | `'2025-01-16T00:00:00Z'` |
| `path` | str | Working directory | `'./'` or `'/path/to/workdir'` |
| `whichcast` | str | Model run type | `'nowcast'`, `'forecast_a'`, `'forecast_b'` |
| `var_list` | str | Variables to assess | `'water_level,water_temperature,salinity,currents'` |
| `stationowner` | str | Data sources | `'co-ops,ndbc,usgs'` |

---

## Error Handling

```python
from ofs_skill.model_processing import get_model_source

try:
    model_type = get_model_source('unknown_ofs')
except ValueError as e:
    print(f"Error: {e}")
    # Error: Unknown OFS identifier: 'unknown_ofs'
```

---

## Further Reading

- **README.md** - General overview and background
- **development/migration-guide.md** - Migration from old import patterns
- **tests/** - Example usage in test files
- **bin/visualization/** - CLI script examples

## Support

- GitHub Issues: https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/issues
- Email: co-ops.userservices@noaa.gov
