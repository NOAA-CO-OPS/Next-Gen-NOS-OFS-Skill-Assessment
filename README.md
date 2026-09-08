**NOTE: Development of this package happens in [this fork](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment), which gets merged to the main branch for every release**

[![CI](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/actions/workflows/ci.yml/badge.svg)](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/actions/workflows/ci.yml)

# The Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software - Prototype

## Overview

This repository contains a prototype of the Next Gen NOS Ocean Forecast Model Skill Assessment and Processing Software, currently under development by NOAA's Center for Operational Oceanographic Products and Services (CO-OPS) and Office of Coast Survey (OCS) as part of the Bipartisan Infrastructure Law (BIL) Coastal and Inland Flood Inundation Mapping (CIFIM) project.

**Developers:** for conda setup (`make setup`), pre-push checks (`make ci-local`), pytest markers, and CI job expectations, see [CONTRIBUTING.md](CONTRIBUTING.md).

NOAA develops and maintains several [Operational Forecast Systems (OFS)](https://tidesandcurrents.noaa.gov/models.html "Operational Forecast System (OFS) NOAA main page") that provide nowcast (past 24 hours to present time) and forecast (up to 120 hours in the future) guidance of water level, current velocity, salinity, water temperature, and ice concentration. OFS are located in coastal waters around the nation, including the Great Lakes, to support critical ports, harbors, and infrastructure. Model predictions and guidance should therefore be as skillful as possible. Oceanographic output from OFSs can be used for, for example, shipping channel navigation, search and rescue, recreational boating and fishing, and tracking of storm effects.

This software provides near real-time evaluation of OFS model skill by comparing model guidance to observations at specific point locations (e.g., established buoys and gauges, referred to as **1D**) and across the entire two-dimensional sea or lake surface of OFS domains using remote sensing products (referred to as **2D**). This new Python-based skill assessment software will replace the [existing Fortran-based NOS skill assessment software](https://tidesandcurrents.noaa.gov/ofs/publications/CS_Techrpt_024_SkillAss_WLsCUs_2006.pdf "Existing skill assessment details"). A map-based interface to view skill assessment results produced by this software will also be available, but is not detailed here.

The general workflow retrieves observations, processes OFS model output, assesses model skill with statistics such as RMSE, bias, and correlation, and visualizes the results in interactive plots and maps:

![general_flow](./readme_images/generalized_flowchart.png)

## Quick start

```bash
# 1. Set up the environment, install the package, and install git hooks
make setup
conda activate ofs_dps

# 2. Create your configuration file and set your working directory
cp conf/ofs_dps.conf.example conf/ofs_dps.conf
#    ...then edit conf/ofs_dps.conf and set home=/path/to/working_directory

# 3a. (Preferred) Ensure use_s3_fallback=True is set in conf/ofs_dps.conf, and the skill
#     assessment routine will read and stream model files from the
#     NODD S3 bucket on demand when local files are missing.
 
# 3b. (Alternate) If you prefer to download model data locally: 
#     Download model data for your OFS and date range
python ./bin/utils/get_model_data.py -p ./ -o cbofs -s 2025-07-01T00:00:00Z -e 2025-07-02T00:00:00Z -ws nowcast -t stations
python ./bin/utils/get_model_data.py -p ./ -o cbofs -s 2025-07-01T00:00:00Z -e 2025-07-02T00:00:00Z -ws forecast_b -t stations

# 4. Run the 1D skill assessment
python ./bin/visualization/create_1dplot.py -p ./ -o cbofs -s 2025-07-01T00:00:00Z -e 2025-07-02T00:00:00Z -d MLLW -ws nowcast,forecast_b
```

Prefer a graphical interface? Run `ofs-skill-gui` to open the [GUI launcher](../../wiki/10.-Graphical-User-Interfaces-(GUI)). Prefer pip/venv or manual conda setup instead of `make setup`? See [Setup and Installation](../../wiki/01.-Setup-and-Installation).

## Documentation

Full documentation lives in the [project wiki](../../wiki):

**Setup**
- [Setup and Installation](../../wiki/01.-Setup-and-Installation) — getting the code, `make setup`, manual pip/conda routes, USGS API key, Windows notes
- [Configuration File Reference](../../wiki/02.-Configuration-File-Reference) — every `conf/ofs_dps.conf` setting, plus `logging.conf`
- [Package Structure and Programmatic Usage](../../wiki/03.-Package-Structure-and-Programmatic-Usage) — repository layout and using `ofs_skill` from your own code (see also [API_REFERENCE.md](API_REFERENCE.md) and [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md))

**User guide**
- [OFS Background and Concepts](../../wiki/04.-OFS-Background-and-Concepts) — supported OFS, nowcasts vs. forecasts, run modes, file formats, data retention, vertical datums, observation data sources
- [Downloading OFS Model Data](../../wiki/05.-Downloading-OFS-Model-Data) — retrieving model output from the NODD S3 bucket
- [Running the 1D Skill Assessment](../../wiki/06.-Running-the-1D-Skill-Assessment) — argument reference, example calls, custom station lists, standalone CLI tools
- [1D Output Reference](../../wiki/07.-1D-Output-Reference) — control files, plots, file formats, skill metrics, maps, datum report
- [2D Skill Assessment](../../wiki/08.-2D-Skill-Assessment) — satellite SST vs. model fields: running the pipeline and its outputs
- [Great Lakes Ice Skill Assessment](../../wiki/09.-Great-Lakes-Ice-Skill-Assessment) — ice concentration and extent skill for the GLOFS models
- [Graphical User Interfaces](../../wiki/10.-Graphical-User-Interfaces-(GUI)) — the GUI launcher and tool GUIs
- [Troubleshooting](../../wiki/11.-Troubleshooting) — common errors and how to fix them

**Development and analysis**
- [Code Development Tips](../../wiki/A.-Code-Development-Tips)
- [Contributing Code: Pull Request Template](../../wiki/B.-Contributing-Code:-Pull-Request-Template)
- [Parallelization and Performance Optimization Guide](../../wiki/C.-Parallelization-and-Performance-Optimization-Guide)
- [Harmonic Analysis](../../wiki/D.-Harmonic-Analysis)
- [CO-OPS ADCP Current Processing](../../wiki/E.-CO‐OPS-ADCP-current-processing)

## Additional resources

:bulb: Links for further reading:

[Main NOAA OFS page](https://tidesandcurrents.noaa.gov/models.html)

[NOAA OFS Publications](https://tidesandcurrents.noaa.gov/ofs/model_publications.html)

[Original OFS skill assessment technical report (2003)](https://tidesandcurrents.noaa.gov/ofs/publications/CS_Techrpt_017_SkillAss_Standards_2003.pdf)

[Original OFS skill assessment GitHub repository](https://github.com/NOAA-CO-OPS/NOS-OFS-Skill-Assessment-Code.git)

[NODD documentation for the NOAA OFS S3 bucket](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ofs-pds)

#### NOAA Open Source Disclaimer
<sub><sup>This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.</sup></sub>

#### License
<sub><sup>Software code created by U.S. Government employees is not subject to copyright in the United States (17 U.S.C. §105). The United States/Department of Commerce reserve all rights to seek and obtain copyright protection in countries other than the United States for Software authored in its entirety by the Department of Commerce. To this end, the Department of Commerce hereby grants to Recipient a royalty-free, nonexclusive license to use, copy, and create derivative works of the Software outside of the United States.</sup></sub>

#### Contact
<sub><sup>Contact: co-ops.userservices@noaa.gov </sup></sub>

![NOAA logo](https://user-images.githubusercontent.com/72229285/216712553-c1e4b2fa-4b6d-4eab-be0f-f7075b6151d1.png)
