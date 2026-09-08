# Architecture

High-level map of the `ofs_skill` package under `src/ofs_skill/`. CLI entry
points live in `bin/` and call into these libraries.

```text
ofs_skill/
├── obs_retrieval/      # Station inventories + time series from CO-OPS, NDBC, USGS, CHS
├── model_processing/   # OFS NetCDF intake, ctl files, datums, indexing, horizon skill
├── skill_assessment/   # Pairing, NOS metrics, 1D/2D skill, skill maps
├── visualization/      # Plotting helpers (CLI plotters stay in bin/visualization/)
├── tidal_analysis/     # Harmonic analysis, prediction, filtering, extrema
├── open_boundary/      # Open-boundary transect processing and plots
└── utils/              # Shared helpers (file headers, coverage checks)
```

## Typical 1D flow

1. **obs_retrieval** — inventory stations in the OFS domain; download observations; write `.obs` / ctl files.
2. **model_processing** — locate/download model output; extract nearest nodes; apply datums; write `.prd` / ctl.
3. **skill_assessment** — pair series; compute RMSE / bias / NOS suite; write skill tables and maps.
4. **visualization** — time-series and summary plots from paired / skill outputs.

2D SST and Great Lakes ice follow related paths (field metrics and ice-specific modules) but reuse the same package boundaries.

## Where to read more

| Need | Place |
|------|--------|
| Generated signatures + docstrings | [API](api/index.md) |
| How to run the tools as a user | [Project wiki](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki) |
| Contribute / CI | [Contributing](development/contributing.md) |
