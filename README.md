# The Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software - Prototype
# 1. Overview

This repository contains a prototype of the Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software, currently under development by NOAA's Center for Operational Oceanographic Products and Services (CO-OPS) and Office of Coast Survey (OCS) as part of the Bipartisan Infrastructure Law (BIL) Coastal and Inland Flood Inundation Mapping (CIFIM) project.

NOAA develops and maintains several [Operational Forecast Systems (OFS)](https://tidesandcurrents.noaa.gov/models.html "Operational Forecast System (OFS) NOAA main page") that provide nowcast (past 24 hours to present time) and forecast (up to 120 hours in the future) guidance of water level, current velocity, salinity, water temperature, and ice concentration. OFS are located in coastal waters around the nation, including the Great Lakes, to support critical ports, harbors, and infrastructure. Model predictions and guidance should therefore be as skillful as possible. Oceanographic output from OFSs can be used for, for example, shipping channel navigation, search and rescue, recreational boating and fishing, and tracking of storm effects.

The Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software, explained here, will provide near real-time evaluation of OFS model skill by comparing model guidance to observations at specific point locations (e.g., established buoys and gauges, referred to below as **1D**) and across the entire two-dimensional sea or lake surface of OFS domains using remote sensing products (referred to below as 2D). This new Python-based skill assessment software will replace the [existing Fortran-based NOS skill assessment software](https://tidesandcurrents.noaa.gov/ofs/publications/CS_Techrpt_024_SkillAss_WLsCUs_2006.pdf "Existing skill assessment details"). A map-based interface to view skill assessment results produced by this software will also be available, but is not detailed here.

## Additional resources

:bulb: Links for further reading:

[Main NOAA OFS page](https://tidesandcurrents.noaa.gov/models.html)

[NOAA OFS Publications](https://tidesandcurrents.noaa.gov/ofs/model_publications.html)

[Original OFS skill assessment technical report (2003)](https://tidesandcurrents.noaa.gov/ofs/publications/CS_Techrpt_017_SkillAss_Standards_2003.pdf)

[Original OFS skill assessment GitHub repository](https://github.com/NOAA-CO-OPS/NOS-OFS-Skill-Assessment-Code.git)

## 1.1 Package Structure & Programmatic Usage

This repository is structured as a modern Python package following best practices, making it suitable for both command-line usage and programmatic integration into other projects.

### Installation

You can install this package using either **pip with virtual environments** (recommended for most users) or **conda** (for those who prefer conda workflows).

#### Option 1: pip + venv (Recommended)

This is the standard Python approach and works on all platforms:

**Step 1: Create a virtual environment**
```bash
# Create virtual environment
python -m venv .venv

# Activate on Windows (Git Bash)
source .venv/Scripts/activate

# Activate on Windows (CMD)
.venv\Scripts\activate

# Activate on Linux/macOS
source .venv/bin/activate
```

**Step 2: Install the package**
```bash
# Install in development/editable mode (recommended for development)
pip install -e .

# Or install directly from GitHub
pip install git+https://github.com/NOAA-CO-OPS/NOS_Shared_Cyberinfrastructure_and_Skill_Assessment.git
```

**Step 2a: Configure USGS API Key**

The USGS Water Data API has rate limits:
- **Without API key:** 50 requests/hour
- **With API key:** 1,000 requests/hour

For production use, an API key is required. Here's how to configure it:

*1. Obtain a USGS API key:*

Request a (free) key from the USGS Water Data API at https://api.waterdata.usgs.gov

*2. Configure the key in the conda environment:*

*bash:*
>```bash
># Set the API key as an environment variable in the ofs_dps conda >environment
>conda env config vars set API_USGS_PAT=<your-api-key> -n ofs_dps
>
># Reactivate the environment for the variable to take effect
>conda deactivate
>conda activate ofs_dps
>
># Verify the key is set
>echo $API_USGS_PAT

*Anaconda Powershell:*
>```
>conda activate env_name
>conda env config vars set API_USGS_PAT=<your-api-key>
>conda deactivate
>conda activate env_name
>echo $env:API_USGS_PAT

**Step 3: Run scripts**
```bash
# After installation, you can run scripts directly
python bin/visualization/create_1dplot.py -p ./ -o cbofs ...
```

> **Note:** Always activate your virtual environment before running the software.

#### Option 2: Conda/Miniconda

If you prefer using conda for environment management:

```bash
# Create conda environment from environment.yml
conda env create --name ofs_dps_env --file=environment.yml

# Activate environment
conda activate ofs_dps_env

# Install package in editable mode
pip install -e .
```

For detailed conda setup instructions for beginners, see [Section 3.2](#32-create-an-environment-with-miniconda).

### Package Organization

The codebase is organized into two main sections:

**📁 `bin/` - Command-Line Interface (CLI) Scripts**
- Contains user-facing scripts for running skill assessments
- `bin/visualization/create_1dplot.py` - Generate 1D time series plots
- `bin/visualization/create_2dplot.py` - Generate 2D spatial plots
- These scripts can be called directly from the command line (see [Section 3](#3-installing--running-the-skill-assessment))

**📦 `src/ofs_skill/` - Importable Python Package**
- Contains all reusable library code organized into modules:
  - `model_processing/` - OFS model data processing and configuration
  - `obs_retrieval/` - Observation data retrieval from CO-OPS, NDBC, USGS
  - `skill_assessment/` - Statistical skill metric calculations
  - `visualization/` - Plotting and visualization functions

### Programmatic Usage

The package can now be imported and used programmatically in your own Python scripts:

```python
from ofs_skill.model_processing import ModelProperties, get_model_source
from ofs_skill.obs_retrieval import get_station_observations
from ofs_skill.skill_assessment import get_skill

# Configure and run skill assessment
props = ModelProperties()
props.ofs = 'cbofs'
props.datum = 'MLLW'
props.start_date_full = '2025-01-15T00:00:00Z'
props.end_date_full = '2025-01-16T00:00:00Z'

# Retrieve observations and run skill assessment
get_station_observations(props, logger)
get_skill(props, logger)
```

For detailed API documentation and usage examples, see:
- **[API_REFERENCE.md](API_REFERENCE.md)** - Complete API documentation with examples
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Guide for programmatic usage patterns

### Benefits

✅ **Backward Compatible** - All existing CLI scripts work exactly as before
✅ **Installable Package** - Can be installed via pip for system-wide availability
✅ **Reusable Components** - Import specific functions into your own projects
✅ **Better IDE Support** - Auto-completion and type checking work properly
✅ **Modern Python Practices** - Follows standard package structure conventions

#### NOAA Open Source Disclaimer
<sub><sup>This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ?as is? basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.</sup></sub>

#### License
<sub><sup>Software code created by U.S. Government employees is not subject to copyright in the United States (17 U.S.C. �105). The United States/Department of Commerce reserve all rights to seek and obtain copyright protection in countries other than the United States for Software authored in its entirety by the Department of Commerce. To this end, the Department of Commerce hereby grants to Recipient a royalty-free, nonexclusive license to use, copy, and create derivative works of the Software outside of the United States.</sup></sub>

#### Contact
<sub><sup>Contact: co-ops.userservices@noaa.gov </sup></sub>

![NOAA logo](https://user-images.githubusercontent.com/72229285/216712553-c1e4b2fa-4b6d-4eab-be0f-f7075b6151d1.png)

# 2. OFS & skill assessment background information

The Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software consists of four main modules, each built on modular Python scripts that operate in a pipeline:

👉 __Retrieve observations__, including water level, water temperature, salinity, current velocity, and sea ice concentration from NOAA, USGS, and NDBC stations;

👉 __Process OFS model output__, including nowcasts and forecasts of the oceanographic variables listed above, to match user inputs for time span and location;

👉 __Assess OFS model skill__ against observational data, using skill statistics such as root mean square error, bias, and correlation coefficients;

👉 __Visualize results__ in interactive plots, with time series of model output compared to observations, as well as boxplots of each time series and maps of 2D statistics.

The general skill assessment workflow is illustrated below, with inputs to and outputs from each module. Inputs and outputs are discussed further in subsequent sections.

![general_flow](./readme_images/generalized_flowchart.png)

## 2.1 Nowcasts, forecasts, and skill assessment modes
Each OFS provides guidance over two distinct time periods: a **nowcast** and a **forecast**. Nowcasts and forecasts are predictions about the past, present, and future states of water levels, currents, salinity, temperature, and ice concentration. A nowcast incorporates recent (and often near real-time) observed meteorological and oceanographic data, and covers the period of time from the recent past (e.g., the past few days) to the present. It can also make predictions for locations where observational data are not available. A forecast, on the other hand, incorporates meteorological and oceanographic forecasts to make predictions into the future for times where observational data will not be available. A forecast is usually initiated by the results of a nowcast.

Each OFS runs at specific times each day. For example, the Chesapeake Bay OFS (CBOFS) runs four times each day, at 00Z, 06Z, 12Z, and 18Z. For each of these four model cycles, CBOFS produces nowcast guidance from 6 hours prior to the cycle hour up to the cycle hour, and forecast guidance from the cycle hour to a maximum forecast horizon 48 hours after the cycle hour. For a CBOFS 12Z model cycle, for instance:

![Model cycles](./readme_images/model_cycles_color.png)

Different OFSs can have different model cycles times for each day, and different forecast guidance lengths. For example, the West Coast OFS (WCOFS) has only one model cycle each day at 03Z, and provides forecast guidance out to +72 hours. The Lake Erie OFS (LEOFS) has four cycles each day at 00Z, 06Z, 12Z, and 18Z, and provides forecast guidance out to +120 hours. All OFS supported by the skill assessment package are listed in the table below, along with model cycles and maximum forecast horizons. Recently, the skill assessment was updated to include output from two additional OFS: the 3D Surge and Tide OFS for the Atlantic Basin (STOFS-3D-ATL), and the Northeast Coastal OFS (NECOFS). Note that NECOFS is still in development, and output is not yet publically available.

|OFS|Region|Daily model cycle hours (UTC)|Max forecast horizon (hours)|
|:---|:---|:---|:---|
|CBOFS|Chesapeake Bay|00, 06, 12, 18|48|
|CIOFS|Cook Inlet|00, 06, 12, 18|48|
|DBOFS|Delaware Bay|00, 06, 12, 18|48|
|GoMOFS|Gulf of Maine|00, 06, 12, 18|72|
|LEOFS|Lake Erie|00, 06, 12, 18|120|
|LMHOFS|Lake Michigan & Huron|00, 06, 12, 18|120|
|LOOFS|Lake Ontario|00, 06, 12, 18|120|
|LSOFS|Lake Superior|00, 06, 12, 18|120|
|NECOFS*|Northeast Coastal|00, 06, 12, 18|72|
|NGOFS2|Northern Gulf of America|03, 09, 15, 21|48|
|SFBOFS|San Francisco Bay|03, 09, 15, 21|48|
|SSCOFS|Salish Sea & Columbia River|03, 09, 15, 21|72|
|STOFS-3D-ATL|Atlantic Basin|12|96|
|TBOFS|Tampa Bay|00, 06, 12, 18|48|
|WCOFS|West Coast|03|72|

The skill assessment can run within or across model cycles in three different modes: forecast_a, forecast_b, and nowcast. All modes can be run individually or together (i.e., you can specify more than one mode when starting a skill assessment run), but at least one mode input is required ([Section 3.5](#35-running-the-1d-skill-assessment)).

|Mode|Explanation|
|:---|:---|
|forecast_a|uses OFS forecast output for one complete user-selected forecast cycle. The start and end dates correspond to the start and end of a model forecast cycle.|
|forecast_b|uses OFS forecast output between user-selected start and end dates. The forecast output is stitched together from successive forecast cycles to make a time series of arbitrary lengths, and the start and end dates do not need to correspond to model cycles.|
|nowcast|uses OFS nowcast output between user-selected start and end dates. The nowcast output is stitched together from successive nowcast cycles to make a time series of arbitrary lengths, and the start and end dates do not need to correspond to model cycles.|

The most commonly used skill assessment modes are nowcast and forecast_b. Nowcast and forecast_b modes can easily be run together when using the skill assessment, as you will see in the example run below. No information regarding forecast cycles is needed to run them -- you only need to input the OFS, a start date, and an end date (see [Section 3.5](#35-running-the-1d-skill-assessment)). Between the start and end date, nowcast time series are built by concatenating all available nowcast output from successive model cycles. Forecast time series -- using forecast_b mode -- are built by concatenating the first 6 hours of successive model forecast cycles, until the end date is reached. CBOFS, for example, has a model forecast cycle every 6 hours at 00, 06, 12, and 18Z with a maximum forecast horizon of 48 hours. A CBOFS forecast time series, starting at 00:00Z, would therefore be the first 6 hours of the 00Z cycle, then the first 6 hours of the 06Z cycle, then the first 6 hours of the 12Z cycle – and so on.

For forecast_a, you input the OFS, start date, and model cycle to run the skill assessment. The end date is an optional user input and is determined by the OFS forecast horizon. For example, CBOFS has a forecast horizon of 48 hours. A CBOFS skill assessment run for the 06Z cycle on 01/01/2025 would have an automatic end date of 06Z 01/03/2025, as that is the end of the model forecast cycle.

Forecast_a, therefore, has a hard, pre-defined end date, while forecast_b and nowcast have flexible, user-selected end dates. When running forecast_b and/or nowcast together with forecast_a, this causes a conflict. **The conflict is resolved by defaulting to the forecast_a end date for all run modes.**

### 2.1.1 Assessment of additional forecast horizons and model cycles using the horizon skill add-on
As mentioned above, forecast_b mode builds model time series by concatenating successive model forecast cycles between a start and end date. As a result, the maximum forecast horizon that is assessed for any OFS is equal to the time difference between successive model cycles, and forecast horizons beyond that are not assessed.

An optional add-on to forecast_b mode, called horizon skill, allows the assessment of *all* model forecast cycles and horizons that temporally overlap with a single set of observations, in addition to the standard forecast_b mode. During a given day, for example, there can be up to 20 overlapping model forecast cycles available to compare to a single set of station observations. The horizon skill option retrieves all of them, and compares the different overlapping model cycles (and overlapping forecast horizons) to the same set of observations. Then, it generates additional skill statistics plots, including RMSE and mean error for each model cycle, that are summarized in [Section 3.6.7](#367-forecast-horizon-skill). This overlap approach can more broadly assess how each OFS model is performing on any given day across different cycles and horizons, rather than focusing on a particular model cycle or horizon. It is important to note, however, that forecast horizons are collected and binned from multiple different model forecast cycles.

There are a couple of limitations to using the forecast horizon skill option:
1) A temporal range of 2 days or less between the skill assessment's start and end date must be used to minimize run-time and resource usage. Future updates are aimed at increasing this time window to weeks an months.
2) Model station files are supported, but not field files, as field files are too large and do not provide sufficient temporal resolution (see [Section 2.2](#22-ofs-data-formats--data-retention-schedules) for an explanation of field versus station files).

The horizon skill option can be enabled by using an input argument when calling the skill assessment. For syntax details, see [Section 3.5](#35-running-the-1d-skill-assessment).

## 2.2 OFS data formats & data retention schedules
Nowcast and forecast OFS output is available in multiple data formats. The two primary formats -- and the ones we will use here -- are 'field' files and 'station' files. Field files contain two- or three-dimensional arrays containing spatially distributed model output, with hourly time resolution. Station files contain one-dimensional time series of model output _at selected locations within an OFS_, with 6-minute time resolution.

In terms of the skill assessment software, the functional differences between field and station file formats are:
* **Field files** have 3D model output (latitude x longitude x depth) in a grid across the entire OFS domain, and can be matched with any observation station locations. But they are generally larger in file size and more cumbersome to work with -- the skill assessment runs more slowly with field files than with station files. Field files are required for 2D analysis in the skill assessment.
* **Station files** have 1D time series output only at selected locations within an OFS, which cannot always be matched with all available observation station locations. However, station files are much smaller in size, more nimble to work with, and have a higher 6-minute time resolution. The 1D skill assessment can be run up to 10x faster with station files compared to field files. Station files cannot be used for 2D skill assessment.

When using the skill assessment software, the user can choose which file format and which 'cast' (nowcast or forecast) to use ([Section 3.5](#35-running-the-1d-skill-assessment)). Note that field and station files are supported for all OFS except STOFS-3D-ATL, which currently only runs with field files.

Note that OFS model data, depending on file type and 'cast', has specific data retention time periods, after which the data will no longer be available. Data retention times are listed below. The time periods are relative to today's date.

||Nowcast|Forecast|
|:---|:---|:---|
|__Field files__|2 years|60 days|
|__Station files__|>5 years|>5 years|

## 2.3 Water level vertical datums
When running the skill assessment, a vertical datum for water levels is a required input (see [Section 3.5](#35-running-the-1d-skill-assessment)). The same vertical datum is applied to both observed and modeled water levels to ensure an apples-to-apples comparison.

For observed water levels, whenever possible, data is downloaded using an API that supports datum conversions. CO-OPS water level stations, for example, can reference water levels to many different tidal and geodetic datums prior to download, and thus no local conversion within the skill assessment is needed. Other water level data, such as from USGS stations, is often available in a single pre-defined datum. In that case, if the station-provided datum is different from the datum input by the user, the water level data is converted in the skill assessment. Conversions between datums are made using the [coastalmodeling-vdatum](https://github.com/oceanmodeling/coastalmodeling-vdatum) package.

For modeled water levels, datum conversions are made on-the-fly using netCDF files that contain a 2D grid of conversion factors for a variety of datums including MLLW, MLW, MHW, NAVD88, and xgeoid20b. These netCDF files are available for every OFS (titled `{OFS}_vdatums.nc`) on the [NOAA Open Data Dissemination Amazon S3 bucket](https://noaa-nos-ofs-pds.s3.amazonaws.com/index.html#OFS_Grid_Datum/). Currently,

## 2.4 1D observation data sources
The 1D skill assessment automatically downloads time series of water level, temperature, salinity, and current velocity from NOAA CO-OPS, USGS, and NDBC observation stations that are within the geographic bounds of the user-defined OFS. The user can specify which station providers to use when running the software ([Section 3.5](#35-running-the-1d-skill-assessment)), and the default is to include all three. NDBC station retrieval is completed using the [searvey package](https://github.com/oceanmodeling/searvey).

## 2.5 2D observation data sources
Currently, the 2D skill assessment can handle sea surface temperature (SST) only. It automatically downloads hourly L3C SST satellite products for the user-defined OFS, including from GOES-16, and GOES-18/West, and/or GOES-19/East. GOES-16 was replaced by GOES-19/East in April 2025, and is only used for skill assessment runs prior to that date. Future updates will include analysis using additional remote sensing products, such as L4 NASA SPoRT, as well as support for current velocity, sea surface height, and salinity.

# 3. Installing & running the skill assessment
This software can run in any environment that supports Python, including Windows and Linux. In sections [3.1](#31-download-or-clone-the-github-repository) and [3.2](#32-create-an-environment-with-miniconda), we will set up the skill assessment software using Miniconda. This tutorial is intended for beginning users who have light -- or no -- familiarity with Python, Miniconda/Anaconda, and GitHub.

After installing the skill assessment package, you will find instructions in sections [3.3](#33-updating-the-conf-and-logging-files), [3.4](#34-download-ofs-model-data), and [3.5](#35-running-the-1d-skill-assessment) for how to complete a skill assessment run using example data downloaded from the NOAA OFS Amazon NODD S3 bucket. In the example, nowcast and forecast OFS model guidance are compared against observations of all water level, water temperature, salinity, and currents stations within the Chesapeake Bay Operational Forecast System (CBOFS) over 1 day.

## 3.1 Download or clone the GitHub repository
There are two main ways to create a local copy of the GitHub skill assessment repository: downloading and cloning. Cloning will retain the repository development (commit) history and a direct connection between the local and remote repositories. It is recommended if you'd like to, for example, easily keep your local repository up-to-date with changes to the remote repository, be able to access all development branches, or are developing the software. Downloading does not retain development history or a direct connection between the local and remote repository, and only contains the main code branch. It is better suited for users who are not developing the software, and is easier to set up. However, you will need to re-download the entire package each time it is remotely updated on GitHub.

To clone, you will first need to install [Git Bash](https://git-scm.com/downloads). For NOAA users, it is an approved software package that can be self-installed. After installing Git Bash, [use SSH to clone and create a local repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository#cloning-a-repository). After cloning, you can use Git Bash to navigate the repository, including downloading (pulling) new updates and viewing all branches.

To download, simply click the green 'Code' dropdown button on the main page of the skill assessment repository. In the drop down menu, select Download ZIP, and unzip it to the location where you'd like to run the software.

![How to download repository](./readme_images/download_button.png)

After cloning or downloading, you will see a directory structure like this:

![Directory structure](./readme_images/main_directory_structure.png)

The main, top-level directory that contains 'bin', 'conf', etc. will be referred to below as your `working directory`. The table below summarizes the directories and files that are inside your `working directory`. _(Note --> there is a directory and a file named '.git*' in the main directory, and they're not needed to run the skill assessment. So you can ignore those for now!)_

|Directory or file|Purpose|
|:---|:---|
|bin|directory that holds all of the python code, grouped in four sub-directories that correspond to the four skill assessment modules: observation retrieval, model post-processing, calculation of skill statistics, and visualization.|
|conf|directory that holds software configuration and data files. You will need to edit 'logging.conf' and 'ofs_dps.conf' before running the software (see [Section 3.3](#33-updating-the-conf-and-logging-files)).|
|log|directory that holds logging output in a text file ([Sections 3.3](#33-updating-the-conf-and-logging-files) and [3.6.7](#367-logging)).|
|ofs_extents|directory that holds shapefiles used for defining the spatial extent of each OFS.|
|readme_images|directory that holds the images you see in this README 😎|
|environment.yml|file used to create your Anaconda (or just Conda) environment in [Section 3.2](#32-create-an-environment-with-miniconda)|
|LICENSE.TXT|text file with software license information.|
|README.md|markdown file of this README 😎|

## 3.2 Create an environment with Miniconda
We will use **Miniconda** to manage our Python environment and run the software's Python modules. While some programming platforms (Matlab, for example) are natively equipped to handle complex arrays, equations, and geospatial data, Python is not. As a result, the user must install different packages to the python interface that enable it to handle arrays, equations, and geospatial data, among many other things. Miniconda easily loads and organizes these Python packages for you, and bundles them together into customizable, loadable environments. An environment that loads all Python packages required for the skill assessment software is included below with a set of example data.

Now install Miniconda, which will also install 1) Python, and 2) Anaconda Prompt, a Python command-line interface. Miniconda can be [downloaded freely](https://www.anaconda.com/docs/getting-started/miniconda/install). If you are a NOAA employee, you can self-install from the approved software list, or ask your friendly local IT professional.

After install, you can use the Anaconda Prompt to execute commands, and install the necessary Python packages in an environment. In the Anaconda Prompt, use the `cd` (change directory) command to navigate to your `working directory` where the 'environment.yml' file is located.

Next, in the Anaconda prompt, create a new environment named `ofs_dps_env` from the 'environment.yml' file by entering:
```
conda env create --name ofs_dps_env --file=environment.yml
```
Then activate your new environment:
```
conda activate ofs_dps_env
```
All of the required packages have now been installed and are self-contained in the `ofs_dps_env` environment. ***Always make sure the environment is activated (using the command above) before running the software.***

## 3.3 Updating the conf and logging files
In the `working directory`, there is a sub-directory called 'conf'. In 'conf', there are two **conf**iguration files that must be updated before running the skill assessment software.

### 3.3.1 ofs_dps.conf

'ofs_dps.conf' establishes the directory names and structure that the skill assessment uses to read inputs and write outputs, and sets several key options. By editing the conf file, you can:
1) set your `working directory` (🚨required);
2) provide a list of observation station IDs to assess;
3) add a path to a text file with geographic coordinates for model time series extraction;
4) specify whether to produce static (.png) images of all interactive skill assessment plots.

🚨 Before running the skill assessment, always set your `working directory`. Type the path to your `working directory` in the line that starts with "home=". For example "home=C:/working_directory/".

![ofs_dps_conf_home](./readme_images/ofs_dps_conf.png)

To enable skill assessment of model output at specific observation stations, enter the station ID(s) from any station provider separated by a space in the section titled `[station_IDs]`, and then adjust the input arguments as described in [Section 3.5.1](#351-inputting-a-custom-list-of-station-ids).

A skill assessment module called `get_node_ofs.py` handles the processing of OFS model data to model time series output at station locations. It can be run a-la-carte from the main skill assessment to produce model time series at any observation station location or using custom geographic coordinates. In the `ofs_dps.conf` section called `[user_xy_inputs]`, you can set a path to a text file with geographic coordinates for model time series extraction. Please see [Section 3.7](#37-scripts-with-command-line-interfaces) for more details.

Finally, in the section called [settings], there is an option for the skill assessment to produce static (.png) images of all plots in addition to the standard Plotly interactive plots, if you need graphics for a document or slideshow. The .png images will be saved to `/working_directory/data/model/1d_node/prd_plots/`.

### 3.3.2 logging.conf

The second file, 'logging.conf', determines how the skill assessment collects logging entries -- used mainly for debugging -- as the software runs. You can read more about logging in [Sections 3.6.7](#367-logging) and [5](#5-troubleshooting).

Here, you can choose whether to save logging entries to a text file or print them to screen as the software runs. The default is print to screen. If instead you want to save logging entries to a text file, use a hash (#) to comment out "handlers=screen" in the 'logger_root' section (pink #1 arrow), and "keys=screen" in the 'handlers' section (blue #2 arrow). Then, in the same sections, uncomment the lines that say "handlers=fileHandler" and "keys=fileHandler". If saving logging entries to a text file, 'logging.conf' should look like this:

![ofs_dps_conf](./readme_images/logging_conf.png)

## 3.4 Download OFS model data
After the `working directory` is established in `ofs_dps.conf`, you can retrieve OFS model data from the [NOAA Open Data Dissemination (NODD) Amazon S3 bucket](https://noaa-ofs-pds.s3.amazonaws.com/index.html) using a script called _get_model_data.py_ located in `working_directory/bin/utils/`. This script can download model data for any available time period, file type (fields or stations), and 'cast' (nowcast or forecast). Model output can be retrieved for all OFS listed in [Section 2.1](#21-nowcasts-forecasts-and-skill-assessment-modes), including STOFS-3D-ATL but excluding NECOFS (which is in development).

The script will retrieve all OFS model files needed to run the skill assessment, and automatically organize them into a new `working directory` folder called `example_data`. __After OFS model data is downloaded, do not move or re-organize the files -- otherwise the skill assessment will not be able to find them.__

🚨🚨🚨In the example below, we will use a date range of 07/01/2025 - 07/02/2025 to both retrieve model output files and run the skill assessment. Any date can be used, though, as long as the model output files are retained in the NODD bucket for the input dates (see [Section 2.2](#22-ofs-data-formats--data-retention-schedules) for model file retention lengths). If model output files are not available for the input dates, _get_model_data.py_ will prompt you to change your input dates so that they are in agreement with data retention policies.

To run _get_model_data.py_, first open Anaconda prompt and navigate to your `working directory`. Make sure 1) your conda environment is activated, and 2) that the `ofs_dps.conf` file ([Section 3.3](#33-updating-the-conf-and-logging-files)) is configured with the correct working directory path, and then type:

```
python ./bin/utils/get_model_data.py -p ./ -o cbofs -s 2025-07-01T00:00:00Z -e 2025-07-02T00:00:00Z -ws nowcast -t stations
```
where
* _-o_ is the name of the OFS (in this case, CBOFS)
* _-p_ is the path to `working directory` (which you are currently in, thus it is followed by './')
* _-s_ and _-e_ are the start and end dates, respectively
* _-ws_ is the mode (nowcast or forecast_b -- forecast_a is not supported yet)
* _-t_ is the file format/type (stations or fields).

This will retrieve all CBOFS nowcast station files between 07/01/2025 and 07/02/2025. All input arguments and options are explained in the table below:

| Option explanation | Option syntax | Verbose syntax | Arguments | Required/optional |
|:---:|:---:|:---:|:---:|:---:|
|OFS location<br><sub>([map of OFS locations](https://tidesandcurrents.noaa.gov/models.html))</sub> |-o|--ofs|cbofs, ciofs, dbofs, gomofs, leofs, lmhofs, loofs, lsofs, ngofs2, nyofs, sfbofs, sjofs, sscofs, tbofs, wcofs|required|
|Path to `working directory`|-p|--path|e.g., C:/path/to/working_directory|required|
|Start date for assessment|-s|--StartDate|YYYY-MM-DDTHH:MM:SSZ<br>(e.g. 2025-07-01T00:00:00Z)|required|
|End date for assessment|-e|--EndDate|YYYY-MM-DDTHH:MM:SSZ<br>(e.g. 2025-07-02T00:00:00Z)|required|
|Mode, or 'cast'|-ws|--WhichCast|nowcast OR forecast_b|required<br><sub>use only one whichcast at a time</sub>|
|OFS file format|-t|--FileType|stations OR fields|required<br><sub>use only one file type at a time</sub>|

In the example skill assessment run below ([Section 3.4](#34-running-the-software)), you will use both nowcast and forecast model data. Run the `get_model_data` call one more time to retrieve the forecast station files:

```
python ./bin/utils/get_model_data.py -p ./ -o cbofs -s 2025-07-01T00:00:00Z -e 2025-07-02T00:00:00Z -ws forecast_b -t stations
```

After the program successfully completes, it will display a brief congratulatory message. Now you are ready to run the skill assessment.

### 3.4.1 Additional model data information
For more information on OFS data, including file naming conventions and data structure on the [NOAA Open Data Dissemination Amazon S3 bucket](https://noaa-ofs-pds.s3.amazonaws.com/index.html), see:

https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ofs-pds

In addition to the Amazon S3 bucket, there are several other options for accessing OFS model output:
1) [NOAA CO-OPS Thredds](https://opendap.co-ops.nos.noaa.gov/thredds/catalog/catalog.html)
2) [NCEI Thredds](https://www.ncei.noaa.gov/thredds/catalog/model/model.html)
3) [NOMADS Data at NCEP](https://nomads.ncep.noaa.gov/pub/data/nccf/com/nosofs/prod/)

If you would like to manually download your own model data, the table below lists the files you will need to run the skill assessment between a given start and end date.

||Nowcast|Forecast|
|:---|:---|:---|
|__Field files__|all hourly nowcast files for each model cycle|All OFS except WCOFS: the first 6 hours of data for each model cycle. WCOFS: the first 24 hours of data for each model cycle.|
|__Station files__|all nowcast files for each model cycle|all forecast files for each model cycle|

## 3.5 Running the 1D skill assessment
🚨🚨🚨 It is recommended to delete, rename, or relocate the 'data' and 'control_files' directories from previous skill assessment runs before starting a new skill assessment run. Otherwise you may encounter errors, and the skill assessment may crash (See [Section 5](#5-troubleshooting), Troubleshooting).

First, if you haven't already, make sure the 'ofs_dps_env' environment is activated (type 'conda activate ofs_dps_env'), then navigate to your `working directory`. The following skill assessment run will use the exact example data downloaded in [Section 3.4](#34-download-ofs-model-data) with nowcast and forecast_b modes.

There are two ways to run the skill assessment: using the GUI or a command line interface. Unlike the command line, where you must know all input arguments and options beforehand, you can run the program using a GUI by calling `create_1dplot.py` without input arguments:

```
python ./bin/visualization/create_1dplot.py
```

In the GUI, all input options are displayed, and you can visually select the options you want. Then click 'Run skill assessment', and the program will start.

![GUI](./readme_images/GUI_example.png)


Using the command line, you can more quickly start a skill assessment run by calling the entry point script, `create_1dplot.py` with a command line call and input arguments. For example:

```
python ./bin/visualization/create_1dplot.py -p ./ -o cbofs -s 2025-10-01T00:00:00Z -e 2025-10-02T00:00:00Z -d MLLW -ws nowcast,forecast_b -t stations -so NDBC,USGS,CO-OPS -vs water_level,water_temperature,salinity,currents
```
where
* _-o_ is the name of the OFS (in this case, CBOFS)
* _-p_ is the path to `working directory` (which you are currently in, thus it is followed by './')
* _-s_ and _-e_ are the start and end dates, respectively
* _-d_ is the vertical datum used to retrieve water level observations
* _-ws_ is the mode (nowcast, forecast_a, and/or forecast_b)
* _-t_ is the file format/type (stations or fields).
* _-so_ is the observation station owner/provider (NDBC, USGS, and/or CO-OPS, OR a custom `list` of station IDs)
* _-vs_ is variable selction (water_level, water_temperature, salinity, and/or currents)

So, this run is for CBOFS, from 10/01/2025 to 10/02/2025, using both nowcast and forecast OFS output with station files (and 6-minute time resolution), with a vertical water level datum of mean lower low water, observation retrieval from CO-OPS, NDBC, and USGS stations, and all oceanographic variables selected. All possible input options and arguments are summarized in the table below.


| Option explanation | Option syntax | Verbose syntax | Arguments | Required/optional |
|:---:|:---:|:---:|:---:|:---:|
|Show help messages and exit |-h|--help|none|optional|
|OFS location<br><sub>([map of OFS locations](https://tidesandcurrents.noaa.gov/models.html))</sub> |-o|--ofs|cbofs, ciofs, dbofs, gomofs, leofs, lmhofs, loofs, lsofs, ngofs2, nyofs, sfbofs, sjofs, sscofs, tbofs, wcofs|required|
|Path to `working directory`|-p|--path|C:/path/to/working_directory, or type `./` if you are in your working directory|required|
|Start date for assessment|-s|--StartDate|YYYY-MM-DDTHH:MM:SSZ<br>(e.g. 2025-07-01T00:00:00Z)|required|
|End date for assessment|-e|--EndDate|YYYY-MM-DDTHH:MM:SSZ<br>(e.g. 2025-07-02T00:00:00Z)|optional if using forecast_a,<br>required otherwise|
|Reference tidal datum for water level<br><sub>([List of tidal datums](https://tidesandcurrents.noaa.gov/datum_options.html))</sub>|-d|--datum|MLLW, MLW, MHW, NAVD88, IGLD85, LWD, xgeoid20b|required|
|Mode, or 'cast'|-ws|--WhichCast|nowcast, forecast_a, and/or forecast_b|required<br><sub>(can use one or all)</sub>|
|OFS file format|-t|--FileType|stations OR fields|optional<br><sub>(Choose only one -- default is stations)</sub>|
|Forecast cycle hour<br><sub>(for use only with forecast_a mode)</sub>|-f|--Forecast_Hr|e.g. '06hr', '12hr'|optional<br><sub>(default is '00hr', or closest cycle to '00hr')</sub>|
|Observation station owner selection|-so|--Station_Owner|USGS, NDBC, and/or CO-OPS, or 'list'|optional<br><sub>(Choose one or multiple -- default is CO-OPS, NDBC, USGS)</sub>|
|Enable forecast horizon & model cycle skill|-hs|--Horizon_Skill|None, just include -hs|optional<br><sub>(default is False)</sub>|
|Variable selection|-vs|--Var_Selection|water_level, water_temperature, salinity, currents|optional<br><sub>(default is all variables)</sub>|

Note that, in the above call for CBOFS, the defaults for optional flags `-vs`, `-so`, and `-t` are `water_level,water_temperature,currents`, `co-ops,ndbc,usgs`, and `stations` respectively. So the same run can be achieved using a shorter call:

```
python ./bin/visualization/create_1dplot.py -p ./ -o cbofs -s 2025-10-01T00:00:00Z -e 2025-10-02T00:00:00Z -d MLLW -ws nowcast,forecast_b
```

Input flags that support multiple arguments, like `-ws` or `-vs`, should be formatted so that the different entries are separated by a comma, but no space. For example:

`-ws nowcast,forecast_b`

`-vs water_level,salinity`

`-so co-ops,ndbc`

The skill assessment can be run for time spans for a year or longer if using station files. If using field files, the duration is shorter and depends on available processing power and resources (see [Section 2.2](#22-ofs-data-formats--data-retention-schedules)).

For reference, below are sample calls, using different input combinations.

```
python ./bin/visualization/create_1dplot.py -p ./ -o sscofs -s 2025-10-01T00:00:00Z -e 2025-10-02T00:00:00Z -d NAVD88 -ws nowcast -t fields -so co-ops -vs water_level
```
> _This run is for SSCOFS, from 10/01/2025 to 10/02/2025, using nowcast field output and a vertical datum of NAVD88,
> with skill assessment at CO-OPS water level stations._

```
python ./bin/visualization/create_1dplot.py -p ./ -o leofs -s 2024-08-01T00:00:00Z -e 2025-08-01T00:00:00Z -d IGLD85 -ws nowcast,forecast_b
```
> _This run is for LEOFS, from 08/01/2024 to 08/01/2025, using nowcast and forecast station output and a vertical datum > of IGLD85, with skill assessment for all variables and at all available observation stations (CO-OPS, NDBC, and USGS)._

```
python ./bin/visualization/create_1dplot.py -p ./ -o gomofs -s 2025-11-01T00:00:00Z -e 2025-11-02T00:00:00Z -d MHW -ws nowcast,forecast_b -so list -vs water_level,currents -hs
```
> _This run is for GOMOFS, from 10/01/2025 to 10/02/2025, using nowcast and forecast station output and a vertical datum of MHW,
> with skill assessment for water level and salinity at listed station IDs, and the forecast horizon option enabled._

### 3.5.1 Inputting a custom list of station IDs

By default, the skill assessment will gather ALL observation stations in an OFS for the providers chosen using the `-so` argument. If instead you would like the skill assessment to run only at specific stations (or a single station) you can input a list of station IDs to the configuration file (`ofs_dps.conf`, see [Section 3.3](#33-updating-the-conf-and-logging-files)). The IDs can be a mixture of station providers. In the configuration file, there is a section called `[station_IDs]`. There, you can list IDs separated by a space, and an example is provided in the file. Then, when running the skill assessment, enable the station list option using `-so list`.

## 3.6 1D outputs
During a run, the skill assessment creates a 'data' directory to save all skill-related outputs. Within the data directory, outputs are saved in separate sub-directories depending on output type, including 'observations', 'model', 'skill', and 'visual'.

A 'control_files' directory is also created by the skill assessment in your `working directory`. Here, text files list available observation stations for each oceanographic variable (e.g., temperature, salinity, current velocity, and water level), as well as the spatially matched OFS output locations for each variable. **Typically, you do not need to interact with these files,** but they are useful if, for example, you want to know the latitude and longitude of each observation station, or what OFS grid cell spatially corresponds to each observation station.

The following sections will cover each output directory in detail.

### 3.6.1 Inventory and control files
`/working_directory/control_files`

During a skill assessment run, the first action taken is to create an inventory of the observation stations, including IDs, names, coordinates, and station providers, that will be used to assess model skill. The inventory file for CBOFS, for example, is called `inventory_all_cbofs.csv`. The stations that are included in the inventory file will vary depending on what options are supplied to the `-so` argument, and what stations have data availability for the stated date range.

The control files are used by the software to match observation stations with model output. During a skill assessment run, a control file is created for each oceanographic variable -- temperature (temp), salinity (salt), water level (wl), and/or current velocity (cu) -- for both observations and model output.

Control file types and naming conventions are:

**{OFS}_{variable}_station.ctl**: control file for observation stations, where {OFS} is the user-selected OFS, and {variable} is the oceanographic variable. For example, `cbofs_wl_station.ctl` would be a control file for CBOFS that lists observation stations for water level (wl). Information about each observation station is split into two rows. An example, with the first entry numbered, from the Salish Sea and Columbia River OFS (SSCOFS) is below.

![Example observation ctl file](./readme_images/obs_ctl_example.png)

1. __Station ID__
2. __Station info__, including station ID, variable, OFS, and station provider
3. **Station name**
4. **Latitude & longitude**
5. **Datum shift** (meters) for water level only. Column is assigned 0.0 if no datum shift is needed, 'RANGE' if no conversion is possible because the latitude/longitude are outside of the geographic range of the datum conversion package; and 'UNKNOWN' if no conversion is possible because the provided observation station datum is either unknown or not available to the datum conversion package. For the salinity, temperature, and currents control files, where a datum shift does not apply, the datum shift column is assigned zero.
6. **Observation water depth** in meters below surface. This will always be zero for water level.
7. **User-defined vertical datum**. This column will be assigned zero for non-water level control files where the vertical datum does not apply.

**{OFS}_{variable}_model{_station}.ctl**: control file for model output, where {OFS} is the user-selected OFS, {variable} is the oceanographic variable, and {_station} indicates if the control file was generated using the stations ouptut file format (see [Section 2.1](#21-nowcasts-forecasts-and-skill-assessment-modes)). The {_station} option is left blank if the field file format is used in the skill assessment. For example, `cbofs_wl_model_station.ctl` is a CBOFS control file generated from station files that lists water level model output locations. `cbofs_wl_model.ctl` is a CBOFS control file generated from field files that lists water level output locations. Internally, model control files for field and station file formats both contain the same information and are formatted the same way -- the only difference is the file name. An example control file from SSCOFS using station files is below. Each row contains information on the model location that matches the observation location.

![Example model ctl file](./readme_images/mod_ctl_example.png)

1. **For station files, this is the array index of the model time series** that matches the observation station location. **For field files, this is the node (within the OFS grid)** that matches the observation locations.
2. **Array index of model sigma depth layer** that best matches the observation station water depth.
3. **Latitude & longitude** of the model output location, which should closely match the coordinates of the observation station.
4. **Observation station ID** that corresponds to model output location.
5. **Model time series water depth** in meters below surface. This will always be zero for the water level variable, as it is measured from the water surface.

### 3.6.2 Plots
`/working_directory/data/visual/`

When the software is finished running, interactive plots and maps are saved in '/data/visual/'. Plots of observed and modeled salinity ('salt'), water temperature ('temp'), water levels ('wl'), and current velocity ('cu') are included for each observation station. A file titled

_cbofs_44041_currents_nowcast_forecast_b_stations.html_

is a plot of current velocity for observation station 44041 in the CBOFS OFS that includes both nowcast and forecast model output and uses station files.

The plots are interactive. Clicking or double-clicking on the legend items will hide or show the different time series (forecast, nowcast, observations). Hovering your cursor over the box plots on the right-hand side will reveal statistics, such as mean, max, and min values. And, you can zoom within a plot panel by clicking and dragging.

Plots for scalar variables -- water level, temperature, and salinity -- feature two panels. The top panel shows time series of observational data and model data (nowcast and/or forecast), and tide predictions if plotting water level. The tide predictions are retrieved via API from the nearest CO-OPS station with available data. The distance to the nearest tide station is displayed in the plot's interactive hover info. If no tide prediction is available, then it is not included on the plot.

The bottom panel shows error (model minus observed), along with the target error ranges for a given variable (see [Section 3.6.5](#365-skill-assessment-metrics) for target error range details). To the right of each main panel are box-and-whisker plots showing the distribution of each time series, including minimums, maximums, and quartiles.

![Example skill assessment plot](./readme_images/visuals_example.png)

Plots for current velocity feature three main panels: a top panel with current speed time series in m/s, a middle panel with current direction time series (0-360&deg;), and a bottom panel with error time series between observed and modeled current speed including target error ranges.

![Example vector skill assessment plot](./readme_images/visuals_vector_example.png)

Text files of paired forecast, nowcast, and observational time series used to make the plots are stored in '/working_directory/data/skill/1d_pair/'. You can use these to recreate the plots -- or generate other skill statistics that are not included in this package.

### 3.6.3 Station observations time series
`/working_directory/data/observations/1d_station/`

Time series of observed salinity ('salt'), water temperature ('temp'), current velocity ('cu'), and water level ('wl') are stored as text files (.obs) for each observation station in '/working_directory/data/observations/1d_station/'. For scalar variables temperature (degrees C), salinity (practical salinity units, PSU), and water level (meters relative to chosen datum), the column labels are:

| Days elapsed since Jan. 1 | Year | Month | Day | Time (hours) | Time (minutes) | Observation (e.g. salinity) |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|

For vector variables (current velocity), the observation text file column labels are:

| Days elapsed since Jan. 1 | Year | Month | Day | Time (hours) | Time (minutes) | Current speed (m/s) | Current direction (compass direction, 0-359&deg;) |_u_ (east/west) velocity component|_v_ (north/south) velocity component|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|

### 3.6.4 OFS model time series
`/working_directory/data/model/1d_node/`
`/working_directory/data/model/1d_node/prd_plots/`

Nowcast and/or forecast model time series of salinity, water temperature, current velocity, and water level are stored as text files (.prd) for each CBOFS station in '/working_directory/data/model/1d_node/'. The columns in the model output text files are the same as listed above for the observation data text files.

There is a utility function `/working_directory/utils/plot_model_timeseries.py` that will make a model time series plot of each .prd file and save it to `/working_directory/data/model/1d_node/prd_plots/`. The function takes no input arguments, and can be run as:

```
python ./bin/utils/plot_model_timeseries.py
```

### 3.6.5 Skill assessment metrics
`/working_directory/data/skill/`

In the '/data/skill' folder, there are two sub-directories: 'stats' and '1d_pair'. Skill assessment metrics are stored in '/working_directory/data/skill/stats/'. There, you will find comma-separated files (.csv) with skill metrics and other information for nowcasts and/or forecasts at each OFS station, for each oceanographic variable, calculated over the entire user-defined skill assessment time period.

Several skill assessment statistics described below, including central frequency and positive/negative outlier frequency, quantify the percentage of errors that fall within or outside of a target error range. Error -- also referred to as bias -- is the difference between model-predicted and observed values such that positive values indicate model over-prediction, and negative values under-prediction. For each oceanographic variable, the target error range is:
* Water level: $\pm \textup{0.15 m}$
* Temperature: $\pm \textup{3&deg;C}$
* Salinity: $\pm \textup{3.5 PSU}$
* Current speed: $\pm \textup{0.26 m/s}$

The target error range defines how much error is acceptable. For model performance to be considered acceptable, $\geq90$% of error values should fall within the target range, and $\leq1$% of error values should fall outside of the target error range multiplied by two. These percent thresholds are called the acceptance criteria.

|Column|Units (if applicable)|Explanation|
|:---|:---|:---|
|__ID__|NA|Observation station ID number|
|__Node__|NA|Model node closest to the observation station where model time series was extracted.|
|__obs_water_depth__|meters|Water depth below surface of observation time series|
|__mod_water_depth__|meters|Water depth below surface of model time series|
|__RMSE__|Same as data being analyzed|Root mean square error (RMSE) is the average difference between model predictions and observations.|
|__R__|Dimensionless|The Pearson correlation coefficient (R) measures the linear correlation between model predictions and observations.|
|__Bias__|Same as data being analyzed|Bias, or error, is the mean difference between model predictions and observations. Positive (negative) values indicate model overprediction (underprediction). For currents, which are a vector quantity with both speed and direction, bias is calculated only for speed.|
|__Bias percent__|Percent|Bias percent is the bias divided by the mean of the observations at a given station, multiplied by 100.|
|__Bias direction__|Compass direction, -359-359&deg;|Bias direction is only applicable to current velocity. It is the average difference between model predictions and observations of current direction. Smaller (larger) values indicate higher (lower) model skill in predicting current direction.|
|__Central frequency__|Percent|Central frequency (CF) is the percent of bias/error values that fall within each variable's target error range.|
|__Central frequency pass/fail__|Pass or fail|For a model performance to be considered acceptable, $\geq90$% of errors must fall within the target error range. A 'pass' indicates CF is $\geq90$%, a 'fail' indicates CF is <90%.|
|__Positive outlier frequency__|Percent|Positive outlier frequency (POF) is the percent of bias/error values that is above each variable's target error range multiplied by 2.|
|__Positive outlier frequency pass/fail__|Pass or fail|For a model performance to be considered acceptable, $\leq1$% of errors must fall above the target error range multiplied by two. A 'pass' indicates POF is $\leq1$%, a 'fail' indicates POF is >1%.|
|__Negative outlier frequency__|Percent|Negative outlier frequency (NOF) is the percent of bias/error values that is below each variable's target error range multiplied by 2.|
|__Negative outlier frequency pass/fail__|Pass or fail|For a model performance to be considered acceptable, $\leq1$% of errors must fall below the target error range multiplied by two. A 'pass' indicates NOF is $\leq1$%, a 'fail' indicates NOF is >1%.|
|__Bias standard deviation__|Same as data being analyzed|Bias standard deviation is the standard deviation of all bias/error values.|
|__target_error__|Same as data being analyzed|The variable's target error range|
|__datum__|NA|User-inputted vertical datum (e.g., MLLW, NAVD88), only applicable to water levels. Column is left blank for salinity, temperature, and current velocity.|
|__Y__|degrees|Latitude of station location.|
|__X__|degrees|Longitude of station location.|
|__start_date__|NA|Start date of skill assessment run.|
|__end_date__|NA|End date of skill assessment run.|

In the 'skill/1d_pair' folder, there are text files (.int) with paired observation-model time series data. These text files were used to create the time series plots in /data/visual/. For scalar variables (salinity, water level, and water temperature), the text file columns represent:

| Days elapsed since Jan. 1 | Year | Month | Day | Time (hours) | Time (minutes) | Observation<br>(e.g. salinity) |Model prediction<br>(e.g. salinity)|Bias<br>(predicted minus observed)|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|

For vector variables (current velocity), the columns represent:

| Days elapsed since Jan. 1 | Year | Month | Day | Time (hours) | Time (minutes) | Observed current speed (m/s) |Model-predicted current speed (m/s)|Current speed bias (predicted minus observed, m/s)|Observed current compass direction, 0-359&deg;|Model-predicted current compass direction, 0-359&deg;|Current direction bias (predicted minus observed, -359-359&deg;)|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|

### 3.6.6 Skill maps
`/working_directory/data/visual/`

[Plotly express](https://plotly.com/python/tile-scatter-maps/) maps for each oceanographic variable -- salinity, water level, current velocity, and water temperature -- are produced by the skill assessment. They show the locations of each observation station within an OFS color-mapped by RMSE between model output and observations. The maps are interactive, so hovering the cursor on a station will show other statistics, too, including central frequency, positive/negative outlier frequency, and mean bias/error.

![plotly_express_map](./readme_images/plotly_express_map_example.png)

### 3.6.7 Forecast horizon skill
The following outputs are generated only if the forecast horizon skill add-on is enabled using the *-hs* argument ([Section 2.1.1](#211-assessment-of-additional-forecast-horizons-and-model-cycles-using-the-horizon-skill-add-on) and [Section 3.5](#35-running-the-1d-skill-assessment)).

The forecast horizon skill add-on produces 4 additional plot types that are saved in `/working_directory/data/visual/horizon_visual`:
1) Bar plots of RMSE and mean error across all model cycles and forecast horizons, aggregated in 6-hour bins, for each station and each variable.

![RMSE_bar_plots](./readme_images/rmse_bar_plot_example.png)

2) Bar plots of central frequency across all model cycles and forecast horizons, aggregated in 6-hour bins, for each station and each variable.

![CF_bar_plots](./readme_images/cf_bar_plot_example.png)

3) Time series of observations and all model cycles, with a subplot of model-observation error time series, for each station and each variable.

![cycle_series_plots](./readme_images/model_cycle_series_plot_example.png)

4) Central frequency scorecard plots summarizing all OFS stations and model cycles for each variable.

![cycle_series_plots](./readme_images/scorecard_plot_example.png)

All four plots are generated when running the skill assessment locally, however only plots (1) and (2) are shown on the web application.

In `/working_directory/data/model/1d_node/horizon_model/`, CSV files with time series for all model cycles and observations between the user-defined start and end dates are saved. These can be used to reproduce skill statistics shown in the plots or the plots themselves, or to calculate additional skill statistics for a specific model cycle.

Finally, CSV files of central frequency for all OFS stations and all model cycles are saved in `/working_directory/data/skill/1d_pair/1d_horizon_pair/`. There is one file each for temperature, salinity, current velocity, and water level.

### 3.6.7 Logging
`/working_directory/log/`

Logging, which contains errors, warnings, and other run-time info, is either printed to screen or saved in a text file in /working_directory/log/. See [Sections 3.3](#33-updating-the-conf-and-logging-files) to configure logging options. The log is useful for
* debugging and troubleshooting,
* following along or checking in on run progress,
* understanding the structure of the skill assessment,
* and knowing how long it takes different parts of the software to run.

In the log, you will see 'info' entries and 'error' entries. 'Info' provides general updates during a run, such as when files are saved, data is concatenated, or when the software is downloading observational data. 'Error' entries are largely minor road bumps that the skill assessment encounters. You will see error messages when, for example, observational data from a given station is not available for download, or when the skill assessment cannot match an observation station with a model output location. These more minor errors are handled internally and are not problematic. Some errors, however, do cause the program to crash unexpectedly or exit purposefully -- if this happens, the error may not be logged but will be printed to the screen. See [Section 5](#5-troubleshooting) for troubleshooting, including common errors.

### 3.6.8 Datum report for water levels
`/working_directory/control_files/`

The skill assessment generates a water level datum report/summary that includes the following information for each observation station:
1) the user-specified target datum that the observed and modeleded data is converted to;
2) the source datum that the data was converted from;
3) all datum conversion factors in meters for observed and modeled data;
4) if the conversion between source and target datum was successful ('pass') or unsuccessful ('fail');
5) the reason for failure, if applicable.

In addition, when the datum conversion fails for a given station, a warning label is placed on the station's water level plot indicating that there is a datum offset and the skill results should be interpreted with caution.

## 3.7 Scripts with command line interfaces

While `create_1dplot.py` is the entry point for the skill assessment, there are other scripts within the code base that can be run individually to produce the output listed above without running the entire skill assessment routine. This includes model and/or observation time series and observation station inventories. These scripts with command line interfaces are listed below.

|Script|Explanation|File input|File output|
|:---|:---|:---|:---|
|ofs_inventory_stations.py|Creates an inventory of all observation stations in an OFS.|None|[Inventory .csv file in `/working_directory/control_files/`](#361-inventory-and-control-files)|
|get_station_observations.py|Writes control files and observation time series for each station in an OFS across the date range of your choice.|None|[.obs text files in `working_directory/data/observations/1d_station/`](#363-station-observations-time-series)|
|get_node_ofs.py|Writes control files and model time series across the date range of your choice.|OFS output (field or station files) and either a list of geographic coordinates, or observation control files|[.prd text files in `working_directory/data/model/1d_node/`](#364-ofs-model-time-series)|
|get_skill.py|Writes paired model + observation time series for each station in the OFS and date range of your choice.|.obs and .prd files|[.int text files in `working_directory/data/skill/1d_pair/`](#365-skill-assessment-metrics)|

Input arguments and options can be found in each script. There are two ways to make model time series using `get_node_ofs.py`: by either running with an existing [observation control file](#361-inventory-and-control-files), or by specifying in a text file the geographic coordinates where model time series should be extracted. For the latter option, in the [configuration file, `ofs_dps.conf`](#33-updating-the-conf-and-logging-files), there is a section called [xy_user_input] with instructions on how to format the text file. There is a variable called `user_xy_path` to assign the file's path to. Then, you can run `get_node_ofs.py` with the optional argument `-ui` to make model time series at the inputted coordinates, for example:

```
python ./bin/model_processing/get_node_ofs.py -p ./ -o cbofs -s 11-01-2025T00:00:00Z -e 11-10-2025T00:00:00Z -ws nowcast -d MLLW -ui
```

## 3.8 Running the 2D skill assessment
🚨🚨🚨 It is recommended to delete, rename, or relocate the 'data' and 'control_files' directories from previous skill assessment runs before starting a new skill assessment run. Otherwise you may encounter errors, and the skill assessment may crash (See [Section 5, Troubleshooting](#5-troubleshooting)).

The 2D skill assessment fetches hourly remote sensing observations of sea surface temperature (SST) ([Section 2.5](#25-2d-observation-data-sources)) for a specified OFS, and then compares them to hourly 2D field model output. Interpolation is required to match the satellite and model output resolutions, and the two grids are then superimposed to calculate 2D skill statistics such as RMSE, error, and central frequency.

The extent of 2D satellite coverage is often limited by cloud cover. As such, 2D skill output may also be limited across an OFS and include only areas where there are enough cloud-free data points to calculate skill statistics.

Below is an example of how to run the 2D skill assessment. While the 1D skill assessment example above used model station files -- and can also run with field files -- the 2D skill assessment **requires field files**. So, first download the necessary field files using the same script described in [Section 3.4](#34-download-ofs-model-data). For the example run, we'll use the San Francisco Bay OFS (SFBOFS), a date range of 06/01/25 - 06/02/25, and the same 'casts' (nowcast and forecast_b). The corresponding calls to retrieve the nowcast and forecast field model output are:

```
python ./bin/utils/get_model_data.py -p ./ -o sfbofs -s 2025-06-01T00:00:00Z -e 2025-06-02T00:00:00Z -ws nowcast -t fields
```
```
python ./bin/utils/get_model_data.py -p ./ -o sfbofs -s 2025-06-01T00:00:00Z -e 2025-06-02T00:00:00Z -ws forecast_b -t fields
```

When the downloads are complete, make sure your conda environment is activated ([Section 3.2](#32-create-an-environment-with-miniconda)) and you have navigated to your `working directory`.

🚨 Then, to ensure all output is created, open the file `/bin/visualization/plotting_2d.py`, and search for the variable `make_plotly_maps`. Then set `make_plotly_maps = True`, and save it. This will tell the skill assessment to create plotly maps of all 2D skill statistics and outputs ([Section 3.9.4](#394-1d-and-2d-skill-statistics)). 🚨

Next, download the SST satellite data for SFBOFS from GOES-18/West:

```
python ./bin/obs_retrieval/get_satellite_observations.py -s 2025-06-01T00:00:00Z -e 2025-06-02T00:00:00Z -p ./ -o sfbofs
```

The options for start (_-s_) and end (_-e_) dates, OFS (_-o_), and path to working directory (_-p_) are the same as described in [Section 3.5](#35-running-the-1d-skill-assessment). The hourly satellite data will be concatenated, clipped to the geographic area of SFBOFS, and then saved as a netCDF file called `{OFS}.nc`, where {OFS} is the name of the specified OFS -- in this case, the file will be called `sfbofs.nc`.

When the satellite data download is complete, run the 2D skill assessment **using the same OFS, start date, and end date**:

```
python ./bin/visualization/create_2dplot.py -o sfbofs -p ./ -s 2025-06-01T00:00:00Z -e 2025-06-02T00:00:00Z -ws nowcast,forecast_b
```

All arguments are the same as described in [Section 3.5](#35-running-the-1d-skill-assessment):
* _-o_ is the name of the OFS (in this case, SFBOFS)
* _-p_ is the path to `working directory` (which you are currently in, thus it is followed by './')
* _-s_ and _-e_ are the start and end dates, respectively
* _-ws_ is the mode (nowcast, forecast_a, and/or forecast_b)

As with the 1D skill assessment, you can run nowcast and forecast_b together or separately. Note that forecast_a mode does not work with the 2D skill assessment.

## 3.9 2D outputs
During a run, the skill assessment creates a 'data' directory to save all skill-related outputs. Within the data directory, outputs are saved in separate sub-directories depending on output type, including 'observations', 'model', 'skill', and 'visual'. The following sections will cover 2D outputs in each relevant directory.

### 3.9.1 SST satellite netCDF files
`/working_directory/data/observations/2d_satellite/`

Here you will find the concatenated and clipped netCDF file storing satellite data (e.g., sfbofs.nc), and a concatenated and unclipped netCDF file of all satellite data. A subdirectory holds all raw downloaded satellite data, and the subdirectory is named after the satellite from which it came: `G16` for GOES-16, `G18` for GOES-18/West, and `G19` for GOES-19/East.

### 3.9.2 Hourly and daily-averaged SST satellite JSON maps
`/working_directory/data/observations/2d/`

All hourly SST satellite JSON files are stored here. If the 2D skill assessment is run for 24 hours, then a daily-averaged JSON file will also be generated. The JSON files contain latitude, longitude, and SST values (degrees C), and can be used to make JPEG maps of the satellite data with the script `/bin/utils/plot_leafletJSON.py`, or a JSON visualizer of your choice. The file names contain all relevant information, including the OFS, date, and hour of the satellite data. For example `cbofs_20250701-00z_sst_satellite.json` is a JSON file of satellite data for CBOFS at 00Z on 07/01/2025, and `cbofs_20250701-daily_sst_satellite.json` is a daily-averaged satellite JSON file, as shown below.

![daily_averaged_l3c](./readme_images/daily_l3c_example.png)

### 3.9.3 Hourly and daily-averaged SST model JSON maps
`/working_directory/data/model/2d/`

All hourly SST model JSON files are stored here. If the 2D skill assessment is run for at least 24 hours, then a daily-averaged model JSON file will also be generated. The JSON files contain latitude, longitude, and SST values (degrees C), and can be used to make JPEG maps of the model data with the script `/bin/utils/plot_leafletJSON.py`, or a JSON visualizer of your choice. The file names convention is the same as described above in Section 3.8.2.

### 3.9.4 2D skill statistics
`/working_directory/data/skill/stats/`

Here, you will find skill statistics for SST nowcasts and/or forecasts.

The skill statistics are saved as comma-separated (.csv) files, with separate outputs for nowcast and forecast. Within each file, you will find skill statistics for each hourly time step -- unless it is too cloudy, in which case the entry will be blank. For each hourly time step, the stats are calculated by spatially aggregating across the 2D domain for both satellite and model data. All skill stats are the same as described in [Section 3.6.5](#365-skill-assessment-metrics).

### 3.9.5 Maps
JSON files/maps of skill statistics that are shown on the skill assessment web application are saved in `/working_directory/data/skill/2d/`. Interactive plotly maps of skill statistics, plus model and satellite data, are saved in `/working_directory/data/skill/2d/plotly_maps/`.

Descriptions of file name abbreviations are below. Some plotly maps also have custom color bars, which are colored and categorized by target error or acceptance criteria (see [Section 3.6.5](#365-skill-assessment-metrics) for an explanation of target error and acceptance criteria). The target error for SST is $\pm \textup{3&deg;C}$, the acceptance criteria is 90% for central frequency and 1% for positive and negative outlier frequency.

|Map type|Description|Time slider?|
|:---|:---|:---|
|__obs__|satellite observations at each time step|Yes|
|__mod__|2D model output at each time step|Yes|
|__diffall__|bias/error at each time step|Yes|
|__diffmean__|mean bias/error|No|
|__diffmin__|minimum bias/error|No|
|__diffmax__|maximum bias/error|No|
|__diffstd__|standard deviation of bias/error|No|
|__rmse__|root mean square error|No|
|__cf__|central frequency|No|
|__nof__|negative outlier frequency|No|
|__pof__|positive outlier frequency|No|

Two examples of plotly maps are shown below. In the first, central frequency is color-coded by acceptance criteria: green indicates where the central frequency is $\geq90$% -- where model performance is acceptable -- and red indicates where the threshold has not been met. Plotly maps are interactive, so they supporting zooming, panning, and clicking individual data points, as shown by the inset.

![plotly_express_map](./readme_images/2d_skill_example.png)

In the second map, the mean error is color-coded by target error range in segments of 3&deg;C. Warm colors are model overprediction, and cool colors are model underprediction.

![plotly_express_map](./readme_images/2d_skill_target_example.png)

### 3.9.6 Logging
`/working_directory/log/`

As in the 1D skill assessment, run-time information, warnings, and errors are recorded in a log file ([Section 3.6.7](#367-logging)).

# 4. Great Lakes ice skill assessment
There are four OFSs that provide guidance on Great Lakes conditions: LEOFS (Lake Erie), LMHOFS (Lake Michigan and Huron), LOOFS (Lake Ontario), and LSOFS (Lake Superior). Together, these are referred to as the Great Lakes OFS, or GLOFS. GLOFS model guidance for water level, temperature, and current velocity are assessed by the 'main' skill assessment that is described above. However, GLOFS models also provide guidance on ice conditions during the annual ice season between November and May, including ice concentration and thickness. To assess the skill of OFS ice predictions, there is a dedicated set of ice skill assessment modules within this repository.

Currently, the ice skill assessment compares modeled and observed **ice concentration** and **extent**. Ice concentration is the percentage of lake surface that is covered by ice, where 0% is open water and 100% is completely ice-covered. Ice extent is the percent of a lake's surface area where ice concentration is $\geq10$%. Ice thickness will be added to the skill assessment in a future update.

During a model run, for each day, the ice skill assessment:

1) Downloads ice concentration maps from the [Great Lakes Surface Environmental Analysis (GLSEA)](https://coastwatch.glerl.noaa.gov/satellite-data-products/great-lakes-surface-environmental-analysis-glsea/) for the time period of interest, and clips it to an OFS area;
2) Fetches available nowcast and/or forecast GLOFS guidance of ice concentration for the same time period and OFS area;
3) Produces 1D time series of GLSEA and modeled ice concentration with skill statistics at specified locations within the OFS;
4) Interpolates the model output to the regular GLSEA grid, so they are directly comparable;
5) Produces basin-wide skill statistics and 2D skill statistics maps.

The GLSEA is a daily remote sensing-derived map of Great Lakes surface water temperature and ice concentration, where the ice concentration is provided by the [National Ice Center](https://usicecenter.gov/Products/GreatLakesHome). GLSEA ice concentration information is provided in 10% increments.

The ice skill assessment uses 2D field model output files ([Section 2.2](#22-ofs-data-formats--data-retention-schedules)). While field file output is hourly, GLSEA output is daily. This creates a temporal mismatch that the skill assessment can resolve in two ways. The first way is to choose one daily field file for nowcast (default is hour 6 from the 12Z cycle) and forecast (default is hour 6 from the 06Z cycle) that best matches the 12Z GLSEA timestamp. This option is fast, but only provides assessment of a single model output per day. The second way is to make daily averages of the hourly model field output for each day the skill assessment is run. This second option is statistically more robust, but more time and resource intensive. When running with option #2, the daily averages will be saved as .csv files in `/working_directory/data/model/2D_ice_cover/` to reuse in future runs. To choose between options, use the `-da --DailyAverage` True/False flag when calling the ice skill assessment (see [Section 4.1](#41-running-the-ice-skill-assessment) for how to run the ice skill assessment). All model output needed to run both options can be retrieved using `get_model_data.py`, as explained below in [Section 4.1](#41-running-the-ice-skill-assessment).

## 4.1 Running the ice skill assessment
The following ice skill assessment example will use nowcast mode, and assess ice skill on Lake Erie (LEOFS) from 02-01-2025 to 02-28-2025. First, following the instructions for the main 1D skill assessment, download or clone the skill assessment repositiory ([Section 3.1](#31-download-or-clone-the-repository)), create a Conda environment ([Section 3.2](#32-create-an-environment-with-miniconda)), and update the configuration files ([Section 3.3](#33-updating-the-conf-and-logging-files)). Then, download field model data using _get_model_data.py_ ([Section 3.4](#download-ofs-model-data)) for 02/01 - 02/28/25:

```
python ./bin/utils/get_model_data.py -p ./ -o leofs -s 2025-02-01T00:00:00Z -e 2025-02-28T00:00:00Z -ws nowcast -t fields
```

Next, navigate to the 'conf' directory (`/working_directory/conf/`, [Section 3.1](#31-download-or-clone-the-repository)). There you will find a file called _gl_2d_clim.zip_. This is the 2D Great Lakes climatology in [numpy](https://numpy.org/) (.npy) format that the program uses to calculate model skill statistics ([Section 4.2.1](#421-ice-concentration-skill-score)). Unzip _gl_2d_clim.zip_ to your 'conf' directory.

Make sure the 'ofs_dps_env' environment is activated, then in the Anaconda Prompt, navigate to your `working directory`. You can then start a skill assessment run by typing

```
python ./bin/skill_assessment/do_iceskill.py -p ./ -o leofs -s 2025-02-01T00:00:00Z -e 2025-02-28T00:00:00Z -ws nowcast -da True
```
where
* _-o_ is the name of the OFS (in this case, LEOFS (Lake Erie))
* _-p_ is the path to `working directory` (which you are currently in, thus it is followed by './')
* _-s_ and _-e_ are the start and end dates, respectively, in the same format as the main skill assessment
* _-ws_ is the mode (nowcast and/or forecast_b)
* _-da_ enables a daily average model ice concentration that is assessed against the daily GLSEA ice concentration. If set to False, then a single model output file that corresponds to the daily GLSEA time stamp will be used.

🚨🚨🚨 Note that the ice skill assessment will only run for input dates during the annual ice season between November 1 and May 31. If run outside of those dates, the program will exit. Additionally, in this example we are using nowcast field files that have two-year retention window ([Section 2.2](#22-ofs-data-formats--data-retention-schedules)). If today's date is more than two years after 02/01/2025, please choose another more recent ice season to use in this example.

### 4.1.1 Internal options & flags
There are several variables within _do_iceskill.py_ that control plot and map output, time step, and run mode. These required variables are assigned default values, and can be changed to the specific options listed below.

|<sub>Variable</sub>|<sub>Options</sub>|<sub>Explanation</sub>|
|:---|:---|:---|
|<sub>shouldimakemaps</sub>|<sub>True or False</sub>|<sub>Controls whether or not the program will write static (.png) and JSON maps to file. The default setting is True.</sub>|
|<sub>shouldimakeplots</sub>|<sub>True or False</sub>|<sub>Controls whether or not the program will write plots to file. The default setting is True.</sub>|
|<sub>dailyplotdays</sub>|<sub>number of days (integer)</sub>|<sub>The ice skill assessment can make a map for each day showing the observed and modeled ice concentrations, as well as error between them. 'dailyplotdays' sets the number of days prior to the end date that the program will generate daily maps. For example, the default setting dailyplotdays=10 will generate daily maps for the last 10 days of a skill assessment run. To turn off daily maps, set dailyplotdays=0. To make daily maps for each day, set dailyplotdays equal to the total number of days in your skill assessment run.</sub>|
|<sub>seasonrun</sub>|<sub>'yes' or 'no'</sub>|<sub>'Seasonrun' must be updated to 'no' before running the ice skill assessment. It is intended only for the scheduled runs on the CO-OPS server that generate daily output for the skill assessment webapp. 'Seasonrun' allows the start date to be pinned at the beginning of the ice season (11/01), and the end date to be updated daily. It also handles the uneven availability of nowcast and forecast field files during the ice season, effectively ignoring any missing data files.</sub>|

## 4.2 Ice skill statistics
Statistics that describe the skill of the OFS model guidance relative to observations are described below.

|Statistic|Units|Explanation|
|:---|:---|:---|
|__RMSE__|Same as data being analyzed|Root mean square error (RMSE) is the average difference between model predictions and observations, and is always non-negative. For ice skill, it is calculated in two ways: 1) for all grid cells in an OFS, including cells where there is open water (no modeled or observed ice), and 2) for grid cells where there is either modeled or observed ice (i.e., excluding open water cells). The former RMSE statistic is referred to as 'RMSE', while the latter is 'RMSE, ice only'.|
|__R__|Dimensionless|The Pearson correlation coefficient (R) measures the linear correlation between model predictions and observations.|
|__Mean error__|Same as data being analyzed|Mean error, or bias, is the mean difference between model predictions and observations. Positive (negative) values indicate model overprediction (underprediction).|
|__Max error__|Same as data being analyzed|Max error is the maximum difference between model predictions and observations.|
|__Min error__|Same as data being analyzed|Min error is the minimum difference between model predictions and observations.|
|__Ice days__|Number of days|The cumulative number of days that ice concentration is $\geq10$%.|
|__Ice onset & thaw dates__|Calendar date|Ice onset is the first time during an ice season that the average lake-wide ice concentration exceeds 10% for 5 consecutive days. Ice thaw is the last time during an ice season that the average lake-wide ice concentration drops below 10% for 5 consecutive days.|
|__Skill Score__|Dimensionless|The Skill Score ([Hebert et al., 2015](https://agupubs.onlinelibrary.wiley.com/doi/abs/10.1002/2015JC011283)) evaluates if model guidance or long-term climatology better matches observations of ice concentration. Please see [Section 4.2.1](#421-ice-concentration-skill-score) below for more details.|
|__Critical Success Index__|Dimensionless|The Critical Success Index (CSI) quantifies the spatial overlap between modeled and observed ice extent. Please see [Section 4.2.2](#422-ice-extent-critical-success-index) below for more details.|

### 4.2.1 Ice concentration Skill Score
The **Skill Score** evaluates if model guidance or long-term climatology better matches observations of ice concentration. It is defined as:

$\textup{Skill Score} = 1 - \frac{MSE(mod-obs)}{MSE(clim-obs)}$

where
* **_MSE(mod-obs)_** is the mean squared error (MSE) between modeled and observed ice concentration, and
* **_MSE(clim-obs)_** is the MSE between climatology and observations.

The 2D climatology used in the Skill Score was compiled from >10,000 daily GLSEA datasets between 1995 and 2024. Each day within the ice season (11/01 - 05/31) was averaged across all years, resulting in a daily long-term average for each calendar day.

Possible values and ranges of Skill Scores, and how to interpret them, are listed below. Skill Scores that would indicate skillful OFS model guidance are green, while Skill Scores that would indicate less skillful OFS model guidance are red.

${\color{green}\textup{Skill Score = 1}}$: A 'perfect' model prediction where $MSE(mod-obs)$ equals zero.

${\color{green}\textup{0 < Skill Score < 1}}$: $MSE(mod-obs)$ is less than $MSE(clim-obs)$, indicating model guidance is more skillful than climatology at matching observations.

${\color{red}\textup{Skill Score = 0}}$: $MSE(mod-obs)$ equals $MSE(clim-obs)$, indicating model guidance and climatology are equally skillful at matching observations.

${\color{red}\textup{Skill Score < 0}}$: $MSE(mod-obs)$ is greater than $MSE(clim-obs)$, indicating that climatology is more skillful than model guidance at matching observations.

### 4.2.2 Ice extent Critical Success Index
The Critical Success Index (CSI) quantifies the spatial overlap between modeled and observed ice extent. It is given by:

$\textup{CSI} = \frac{\textup{hits}}{\textup{hits + false alarms + misses}}$

where
* **_hits_** is the total number of grid cells where there is both predicted and observed ice;
* **_false alarms_** is the total number of grid cells where the model predicted ice, but ice was not present in the observations;
* **_misses_** is the total number of grid cells where the model did not predict ice, but ice was present in the observations.

The CSI ranges from 0 to 1. A value of 1 means the model correctly predicted the presence of ice in 100% of the grid cells where observed ice was present, and nowhere else. A value of 0.5, for example, would mean that the model correctly predicted ice in 50% of the cells where either modeled or observed ice was present, and the remaining 50% is false alarms and/or misses.

## 4.3 Ice skill outputs
After the ice skill package runs, it will output a series of plots, maps, and text files. The skill statistics within these files are described in [Section 4.2](#42-ice-skill-statistics). All ice skill outputs are saved in different ice-specific directories, and each directory is housed within the master 'data' directory [Section 3.1](#31-download-or-clone-the-github-repository). Paths to each output type are listed in the sections below.

### 4.3.1 Station plots
`/working_directory/data/visual/ice_visual/time_series/`

Time series of modeled and observed ice concentrations are plotted at CO-OPS, NDBC, and USGS observation stations within an OFS. _Note that these stations do not provide ice concentration observations. Instead, ice concentration time series from the two-dimensional GLSEA dataset and OFS model output are extracted at each station location, which serve as common cross-reference points between the ice and main skill assessments._ An example station time series plot is below.

![1D_ice_example](./readme_images/1d_ice_example.png)

The plot title displays the
* user-selected OFS;
* Node ID, or model node from which the time series was extracted that is closest to the observation station;
* Station ID, or unique identifier of the observation station location where the GLSEA and model time series were extracted;
* and the start and end dates for the skill assessment run.

As in the main 1D skill assessment plot, there two subplots: one showing GLSEA and model time series (top), and another showing model error with 1x and 2x target error ranges. In the plot, GLSEA is shown in tan, and nowcast guidance is dark blue. Shaded areas on either side of the model time series show how the ice concentration varies in the local neighborhood around the model node. The 'neighborhood' is defined here as a radius of 2 model nodes around the model node of interest, and the shaded areas are defined as $\pm$ 1 standard deviation of the mean of the local neighborhood.

The time series used to make the station plots are saved in text file format in `/working_directory/data/skill/1d_ice_pair/`. You can use these to make time series of your own, or calculate different skill statistics.

Skill metrics for each 1D time series are saved in a .csv file in `/working_directory/data/skill/stats/ice_stats/`. The table includes the observation station ID where the time series are extracted, the model node closest to the observation station, several skill statistics ([Sections 3.6.5](#365-skill-assessment-metrics) and [4.2](#42-ice-skill-statistics)), and the start and end date of the skill assessment run.

![skill_table_example](./readme_images/skill_table_example.png)


### 4.3.2 Basin-wide statistics plots
`/working_directory/data/visual/ice_visual/stats/`

The basin-wide plots show time series of ice concentration and extent statistics calculated over all grid cells in an OFS. See [Section 4.2](#42-ice-skill-statistics) for descriptions of all statistics. Text files of the statistics time series used to make these plots, including ice onset and thaw dates, are saved to `/working_directory/data/skill/stats/ice_stats/`.

Ice concentration plots include mean model and observed ice concentrations (with $\pm$ 1 standard deviation about the means), RMSE, and Skill Score for both nowcast and forecast. Ice onset and thaw dates, as well as long-term climatology, are also included on the ice concentration plots. The ice concentration climatology used here is from [GLERL Great Lakes CoastWatch](https://coastwatch.glerl.noaa.gov/statistics/great-lakes-ice-concentration/), and is the average daily ice concentration between 1974-2024 for a given OFS. (_Note that this 1D climatology is different than the 2D climatology used to calculate the Skill Score_ ([Section 4.2.1](#421-ice-concentration-skill-score))_, which is between 1995 - 2024._)

The RMSE is calculated in two ways. In the plot below, the RMSE red line is for all grid cells in an OFS ('RMSE, entire domain'), including cells where there is open water (no modeled or observed ice). RMSE in open water cells is by definition equal to zero, and can bias the RMSE downwards. The brown line, 'RMSE, areas with ice only', is for grid cells where there is either modeled or observed ice, and excludes open water cells. Unless the OFS is fully covered by ice, 'RMSE, areas with ice only' will always be higher than regular 'RMSE, entire domain'.

![Ice_conc_plot_example](./readme_images/ice_concentration_plot_example_scale.png)


Ice extent plots show model and observed ice extents, ice extent error, and Critical Success Index metrics for both nowcast and forecast.

![Ice_extent_plot_example](./readme_images/ice_extent_plot_example_scale.png)

Finally,


### 4.3.3 Static maps
`/working_directory/data/visual/ice_visual/static_maps/`

There are two types of maps produced by the ice skill assessment: daily and summary statistics. Each type is produced separately for nowcast and forecast model guidance.

Daily maps show modeled ice concentration, observed (GLSEA) ice concentration, and ice concentration error (model minus observed) for a single time step. The daily map below shows conditions on Lake Erie for 01/31/2025.

![Ice_daily_map](./readme_images/ice_daily_map_scale.png)

In addition to daily plots, there are four static map files that show summary statistics tabulated across the entire skill assessment run:
* Ice days, showing the total number of days that modeled and observed ice concentration was $\geq10$%;
* Mean ice concentration for both modeled and observed ice, as well as RMSE (example shown below on the left);
* Frequency of hits, misses, and false alarms (see Critical Success Index, [Section 4.2.2](#422-ice-extent-critical-success-index) -- example shown below on the right);
* Mean, max, and min ice concentration errors where error is defined as model minus observation.

![Ice_stats_map](./readme_images/static_plot_examples_diptych.png)

### 4.3.4 JSON maps
`/working_directory/data/visual/ice_visual/json_maps/`

All static maps described above are also available in JSON format.

# 5. Troubleshooting
The most common cause of skill assessment run-time errors and failures is having relict 'data' or 'control_files' directories from a previous skill assessment run. If the program crashes or exits, first try deleting the 'data' and 'control_files' directories from your working directory, and then re-run. If the error persists,
1) check the log file for any 'ERROR' entries. If there is appropriate internal handling of the error, then a log entry will be created for it with a message explaining what went wrong;
2) check if the program is fetching model output files from the correct directory with the correct path;
3) make sure you are calling the program to run from your `working directory`, and that the `working directory` is set correctly in the `ofs_dps.conf` file.

If it is an unexpected error and is not logged, save the error message that was printed to screen and create a [new issue](https://github.com/NOAA-CO-OPS/Next-Gen-NOS-OFS-Skill-Assessment/issues) in the GitHub repository that includes the error and the call used to run the skill assessment. Common errors and troubleshooting advice are listed in the table below.

|Error|Probable explanation|How to fix it|
|:---|:---|:---|
|ValueError: time data '20241031-23:59:00' does not match format '%Y-%m-%dT%H:%M:%SZ'|Start and end dates are sometimes reformatted as the skill assessment runs. If a necessary file is missing, or the program is trying to use an outdated control file, for example, the program can sometimes step backwards to try and correct the problem without properly reformatting the date.|Delete 'control_files' and 'data' directories and restart the run.|
|mask = (timeseries['DateTime'] >= Start_dt_full) & (timeseries['DateTime'] <= End_dt_full), TypeError: 'NoneType' object is not subscriptable|The program is using an old control file.|Delete the 'control_files' directory and run again.|
|ERROR - Model data path Add_model_historical_dir_alternative\cbofs\netcdf/202311 not found. Abort!|The working directory path in `ofs_dps.conf` is incorrect.|Update `ofs_dps.conf` with the correct working directory path ([Section 3.3](#33-updating-the-conf-and-logging-files))|
|'2025-07-21 14:16:27' - INFO - --- Unexpected time error: requested time = 2025-06-01 22:00:00 2025-06-01 22:00:00, model_time = 2025-06-01 22:00:00  ---|Dates between satellite and model data are mismatched in the 2D skill assessment.|Delete the `data` directory, and re-run. If error persists, re-download the satellite and model data, and re-run again.|
|Oops, you are missing model files! The missing files are:...|Model output files needed to run the skill assessment are not found.|Run `get_model_data.py` to download missing model data (see [Section 3.4](#34-download-ofs-model-data) for instructions), delete the data and control files directories, and re-run.|


[def]: #
