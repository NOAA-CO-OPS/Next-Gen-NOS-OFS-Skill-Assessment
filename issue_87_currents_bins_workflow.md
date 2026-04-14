# CO-OPS ADCP per-bin currents pipeline

End-to-end workflow for retrieving every ADCP bin's observation data and
resolving an accurate vertical depth for each, including side-looking
(PICS) ADCPs. Covers the implementation landed under issue #87.

---

## Data sources (CO-OPS APIs)

| Call | Endpoint | Purpose |
|------|----------|---------|
| **Datagetter** (per bin) | `GET /api/prod/datagetter?product=currents&station={id}&bin={N}&…` | Pulls the time series for a single bin. Chunked in 30-day windows. Required — without `&bin=N`, only the station's `real_time_bin` is returned. |
| **Bins metadata** | `GET /mdapi/prod/webapi/stations/{id}/bins.json?units=metric` | Lists every bin: `num`, `depth`, `distance`, `is_pics`, `qc_flag`. Authoritative source of bin depth when non-null. |
| **Station metadata** | `GET /mdapi/prod/webapi/stations/{id}.json?units=metric` | Provides `height_from_bottom` (hfb) and `center_bin_1_dist` — needed to compute depth for side-looking ADCPs where `bin.depth` is null. |

All three are cached in-process (`_depth_cache`, `_station_info_cache`) so
each station is hit at most once.

---

## Depth-resolution decision tree

```
for each bin in station metadata:
    if bin.depth is not None:           # downward-looking / per-bin MDAPI depth
        obs_depth = float(bin.depth)    # e.g. cb0402, s08010..s10010
    else:                               # PICS / side-looking ADCP
        # obs_depth temporarily 0.0 in the CTL; resolved later
        hfb = station.height_from_bottom   # meters above channel bottom
        obs_depth = model_h_at_nearest_node - hfb   # see model-CTL step
        obs_depth = max(0.0, obs_depth)   # clamp when hfb > model bathy
```

**Never fall back to `bin.distance` as a depth proxy.** `distance` is the
horizontal range from a side-looking sensor to the bin centre (along the
acoustic beam); treating it as a vertical depth labels a bin 90 m deep in
10 m of water and mis-pairs every sample with the wrong model layer.

---

## Stage-by-stage pipeline

```text
retrieve_t_and_c_station            →  write_obs_ctlfile          →  write_ofs_ctlfile                    →  get_skill (pair)           →  plotting_*
(per-bin time series +                 (obs station.ctl                (model_station.ctl                      (per-bin .int                 (per-bin HTML
 bin/station metadata)                  with 6 fields)                  + back-patched obs depths)              paired files)                 plots)
```

### 1. Retrieval — `src/ofs_skill/obs_retrieval/retrieve_t_and_c_station.py`

* `_get_station_depth(id)` — fetches `/bins.json`; returns `bins[]` with `num`, `depth`, `distance`, `is_pics`.
* `_get_station_info(id)` — fetches `/stations/{id}.json`; returns `height_from_bottom` (hfb), `center_bin_1_dist`.
* `_retrieve_currents_all_bins(id)` — iterates every bin from the metadata, issues one chunked datagetter call per bin (`&bin=N`), returns `dict[int, DataFrame]`. Each DataFrame carries `df.attrs['bin']`, `df.attrs['depth']`, `df.attrs['orientation']`, `df.attrs['height_from_bottom']`.
* `_get_with_retry` — exponential-backoff retries on 403/429/5xx (6 attempts, base 2 s).
* Concurrency: 2 CO-OPS workers at the station level during the currents branch (from `_COOPS_CURRENTS_MAX_WORKERS` in `write_obs_ctlfile.py` and the capped `obs_coops_workers` in `get_station_observations.py`). Lower than other variables because a single ADCP now fans out to N HTTP calls.

### 2. Obs CTL writer — `src/ofs_skill/obs_retrieval/write_obs_ctlfile.py`

One two-line block per bin, with a virtual ID `{parent}_b{NN}`:

```
cb1401_b22 cb1401_b22_cu_cbofs_CO-OPS "Newport News Shipbuilding (bin 22)"
  <lat> <lon> <zdiff> <obs_depth> <shift> <hfb>
```

Columns on the coord line (all 6 always emitted for `cu`):

| # | Field | Meaning |
|---|-------|---------|
| 1 | lat | Station latitude |
| 2 | lon | Station longitude |
| 3 | zdiff | Datum shift (unused for currents — 0.0) |
| 4 | **obs_depth** | Bin depth (m, positive down). Set from `bin.depth` when available, else 0.0 as a placeholder for side-looking ADCPs. |
| 5 | shift | Bias shift (unused for currents — 0.0) |
| 6 | **hfb** | `height_from_bottom` (m). Non-zero only for CO-OPS ADCPs whose MDAPI depth was null. The side-looking resolution marker. |

NDBC / USGS / CHS stations also emit 6 fields (`hfb = 0.00`) to keep the
file rectangular so that `np.array(coord_rows)` works in downstream
indexing.

### 3. Model CTL writer — `src/ofs_skill/model_processing/write_ofs_ctlfile.py`

Order of operations when `name_var == 'cu'`:

```
extract                  = station_ctl_file_extract(obs_ctl)
list_of_nearest_node     = index_nearest_node(extract[-1], …)
_resolve_side_looking_depths(prop, extract, list_of_nearest_node, model, ctl, …)
list_of_nearest_layer,
list_of_depths           = index_nearest_depth(prop, list_of_nearest_node, model, extract[-1], …)
```

`_resolve_side_looking_depths` is the depth-correction pass for PICS /
side-looking ADCPs:

1. For each coord row where field 6 (hfb) > 0 and field 4 (obs_depth) ≤ 0.01:
2. Look up model bathymetry at the nearest node via `_model_bathymetry_at_node`:
   * ROMS: `|h[node]|` (flat index into `h_rho`).
   * FVCOM: `|h[node]|` if present, else deepest `z[:,node,0]`.
   * SCHISM: `|depth[node]|` or bottom of `zcoords`.
3. `obs_depth = max(0.0, water_depth - hfb)`.
4. Mutate field 4 in `extract[-1]` **in place**, so the subsequent `index_nearest_depth` call sees the corrected obs depth and chooses the right vertical model layer.
5. Back-write the updated station.ctl so all downstream readers (pairing, plotting) see the resolved depth.

Each resolution is logged:

```
Side-looking ADCP cb1401_b01: water_depth=13.01 m, hfb=10.30 m -> obs_depth=2.71 m
…
Updated 106 side-looking ADCP obs depths in ./control_files/cbofs_cu_station.ctl
```

### 4. `index_nearest_depth` — `src/ofs_skill/model_processing/indexing.py`

Operates on `extract[-1][idx][3]` (the obs depth). No changes beyond the
coordinate-cache addition for repeated ADCP coords; because the depth is
now correctly populated before this call, nearest-layer selection is
meaningful even for side-looking ADCPs.

### 5. Plot-title annotation — `src/ofs_skill/visualization/plotting_functions.py`

`_build_depth_line` composes:

```
<br>Bin NN — Obs depth -X.X m — Model depth -Y.Y m
```

Obs depth lookup (`_lookup_obs_depth`) order:

1. Virtual-ID + CO-OPS + MDAPI `bins[num]` has explicit `depth` → use it.
2. Otherwise fall through to the obs `{ofs}_cu_station.ctl` (now back-patched for PICS bins).
3. If neither has a value, the Obs-depth span is omitted (Model depth still shown).

Model depth is read from the last column of `{ofs}_cu_model_station.ctl` (`list_of_depths[i]` written by stage 3). Both files are cached
in-process (`_MODEL_CTL_CACHE`, `_OBS_CTL_CACHE`).

For non-virtual stations (NDBC / USGS / CHS) the line drops the `Bin NN`
prefix and shows only `Obs depth — Model depth`.

---

## Worked examples (2026-03-27 nowcast run)

### cb0402 Naval Station Norfolk (downward ADCP, MDAPI depth)

* MDAPI `bins[4].depth = 6.26` — used directly.
* hfb = None (not emitted as side-looking).
* Title: `Bin 04 — Obs depth -6.3 m — Model depth -6.1 m`.

### cb1401 Newport News (side-looking, 48 bins)

* MDAPI bins all have `depth: null`; `distance` ranges 5–193 m along the beam.
* Station meta: `height_from_bottom = 10.30`.
* CBOFS bathy at nearest rho node: `h = 13.01 m`.
* Resolved: all 48 bins → `13.01 − 10.30 = 2.71 m`.
* Title for every bin: `Bin NN — Obs depth -2.7 m — Model depth -2.9 m`.

### cb1301 Chesapeake City (side-looking, 23 bins, near-bottom)

* hfb = 0.18 m.
* CBOFS bathy = 10.70 m.
* Resolved: `10.70 − 0.18 = 10.52 m`.
* Title: `Bin NN — Obs depth -10.5 m — Model depth -10.4 m`.

### cb1001 Cove Point LNG Pier (side-looking, hfb > model bathy)

* hfb = 11.91 m; CBOFS `h = 9.67 m` at the nearest rho node — the model
  doesn't resolve the dredged LNG berth.
* Resolver clamps `max(0, 9.67 − 11.91) = 0.0`.
* All 35 bins pair against the surface model layer.
* Title: `Bin NN — Obs depth -0.0 m — Model depth -0.2 m`.
* Future improvement: swap model bathy for external high-resolution
  bathymetry (NCEI CUDEM) or search a local max-depth neighbourhood
  around the nearest node instead of the point value.

### sfbofs NDBC 46237

* Not a CO-OPS virtual ID → skips the bins-endpoint path.
* Obs depth comes from the `_NDBC` line in `sfbofs_cu_station.ctl`
  (`data_station["DEP01"].mean() = 1.00 m`).
* Title: `Obs depth -1.0 m — Model depth -0.9 m`.

---

## File inventory (issue #87 implementation)

```
src/ofs_skill/obs_retrieval/retrieve_t_and_c_station.py
    _get_station_depth, _get_station_info,
    _retrieve_currents_all_bins, _fetch_currents_chunked, _get_with_retry

src/ofs_skill/obs_retrieval/write_obs_ctlfile.py
    _process_coops_station (6-field coord line with hfb)
    _process_usgs_station / _process_ndbc_station / _process_chs_station
        (6-field coord line for cu; hfb = 0.00 placeholder)
    _COOPS_CURRENTS_MAX_WORKERS = 2

src/ofs_skill/obs_retrieval/get_station_observations.py
    _split_virtual_currents_id (parses {parent}_b{NN})
    obs_coops_workers capped at 2 when variable == 'currents'

src/ofs_skill/model_processing/write_ofs_ctlfile.py
    _model_bathymetry_at_node, _resolve_side_looking_depths

src/ofs_skill/model_processing/indexing.py
    coord_cache shared across fvcom/roms/schism branches

src/ofs_skill/visualization/plotting_functions.py
    _build_depth_line, _lookup_obs_depth,
    _load_obs_station_depths, _load_model_station_depths

tests/coops_currents_bins_test.py        # 10 tests, all passing
```

---

## Operational notes

* Every new or missing bin triggers a datagetter call; a station with 48
  bins requires 48 GETs. Rate-limiting is managed with the 2-worker cap
  and retries; a warm run through all of cbofs currents takes ~3–5 min.
* The obs station.ctl is **rewritten** by `_resolve_side_looking_depths`.
  If you're debugging, the pre- and post-resolution state can be diffed
  by snapshotting the file before running the model CTL step.
* Pre-existing `{ofs}_cu_station.ctl` files produced before this change
  have only 5 coord fields; delete them before the first run on a fresh
  tree so the new writer regenerates the 6-field format.
* `height_from_bottom > model_bathy` is a genuine model-vs-observation
  mismatch, not a bug. The clamp to 0.0 produces a near-surface pair
  rather than a negative depth; an out-of-band note on the plot may be
  worth adding later.
