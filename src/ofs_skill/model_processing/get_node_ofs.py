"""
This is the final model 1d extraction function, it opens the path and looks
 for the model ctl file,
if model ctl file is found, then the script uses it for extracting the model
 timeseries
if model ctl file is not found, all the predefined function for finding the
nearest node and depth are applied and a new model ctl file is created along
with the time series
"""

import copy
import logging
import logging.config
import math
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from ofs_skill.model_processing import do_horizon_skill_utils

# Use new package imports - import directly from modules to avoid circular import
from ofs_skill.model_processing.get_datum_offset import get_datum_offset as get_datum_offset_func
from ofs_skill.model_processing.get_datum_offset import report_datums
from ofs_skill.model_processing.intake_scisa import intake_model
from ofs_skill.model_processing.list_of_files import list_of_dir
from ofs_skill.model_processing.list_of_files import list_of_files as list_of_files_func
from ofs_skill.model_processing.model_format_properties import ModelFormatProperties
from ofs_skill.model_processing.model_source import get_model_source
from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile
from ofs_skill.obs_retrieval import scalar, utils, vector
from ofs_skill.obs_retrieval.utils import get_parallel_config
from ofs_skill.utils import cache_manifest
from ofs_skill.utils.file_headers import (
    SERIES_HEADER_PREFIX,
    series_header,
    strip_model_ctl_header,
)
from ofs_skill.utils.timeseries_coverage import covers_run_window, parse_run_window

logger = logging.getLogger(__name__)

# Serializes / bounds dask.compute() inside _batch_extract across the
# variable threads dispatched from get_node_ofs. Two independent reasons
# to limit concurrency:
#   1. Thread safety: engine='netcdf4' (libnetCDF/HDF5 C bindings) and
#      engine='scipy' hold process-global state and are not safe under
#      concurrent reads → concurrency must be 1 for those engines.
#      engine='h5netcdf' releases the GIL and supports concurrent reads.
#   2. Memory: N threads each materializing a different time-varying var
#      concurrently spikes memory to N× the per-var peak (~4-5 GB per
#      var observed on real NECOFS runs, triggering Windows paging at
#      94% RAM) → even under h5netcdf, concurrency is capped by the
#      parallel_extract_slots config setting (default 2).
# The guard only covers the dask.compute call; the indexing phase,
# formatting, .prd writing, and plotting still parallelize freely
# across variable threads.
_EXTRACT_GUARDS: dict[int, threading.BoundedSemaphore] = {}
_EXTRACT_GUARDS_LOCK = threading.Lock()


def _extract_guard(engine=None, slots=None):
    """Return the semaphore bounding concurrent dask.compute calls.

    ``engine`` is the xarray engine the model dataset was opened with
    (recorded on ``prop.netcdf_engine`` by ``intake_model``). Anything
    other than ``'h5netcdf'`` — including ``None``, when the engine is
    unknown — serializes fully (1 slot). h5netcdf gets ``slots``
    concurrent computes (memory cap, default 2).
    """
    if engine != 'h5netcdf':
        n_slots = 1
    else:
        try:
            n_slots = max(1, int(slots))
        except (TypeError, ValueError):
            n_slots = 2
    with _EXTRACT_GUARDS_LOCK:
        guard = _EXTRACT_GUARDS.get(n_slots)
        if guard is None:
            guard = _EXTRACT_GUARDS[n_slots] = threading.BoundedSemaphore(n_slots)
    return guard


# Per-variable physical bounds used to catch blended SCHISM dry/wet
# sentinel values that NCEP post-processing produces by linearly averaging
# the -999 dry-cell sentinel with real ocean values across the wet/dry
# interface. Anything outside these bounds is treated as invalid and
# replaced with NaN. Water level keeps the looser |val| >= 999 mask
# because datum offsets and units vary by OFS.
_SCHISM_PHYSICAL_BOUNDS = {
    'temp': (-5.0, 50.0),
    'temperature': (-5.0, 50.0),
    'salt': (-1.0, 50.0),
    'salinity': (-1.0, 50.0),
    'currents': (0.0, 10.0),  # speed magnitude
    'currents_uv': (-10.0, 10.0),  # raw u or v component
}


def _mask_schism_sentinels(arr, kind, station_id, ofs, log):
    """Mask SCHISM dry/wet sentinels and blended transitional values.

    SCHISM uses ``-999.0`` as a dry-cell sentinel. NCEP post-processing
    of STOFS-3D-Atl points files can emit values that are linear blends
    of ``-999`` with real ocean values across a dry/wet transition
    (e.g. ``-596.91`` between ``-999`` and ``5 °C``). The previous mask
    of ``|val| >= 999`` caught only the pure sentinels and let the
    blended values pass through into plots and skill statistics.

    This helper masks both: pure-fill (``|val| >= 999``) and blended
    values that fall outside per-variable physical bounds. It logs a
    WARNING when blended values are encountered (so the upstream data
    issue is traceable in the log file) and an INFO when only
    pure-fill values are masked. Clean stations produce no log line.
    """
    arr = np.asarray(arr, dtype=float).copy()
    finite = np.isfinite(arr)
    pure_fill = finite & ((arr <= -999) | (arr >= 999))
    lo, hi = _SCHISM_PHYSICAL_BOUNDS.get(kind, (-999.0, 999.0))
    physical = finite & ((arr < lo) | (arr > hi))
    transitional = physical & ~pure_fill
    n_pure = int(pure_fill.sum())
    n_trans = int(transitional.sum())
    if n_trans:
        bad = arr[transitional]
        log.warning(
            '%s station %s %s: %d blended sentinel values in '
            '[%.2f, %.2f] masked (source: NCEP post-processing '
            'dry/wet interpolation). %d pure-fill values also masked.',
            ofs,
            station_id,
            kind,
            n_trans,
            float(bad.min()),
            float(bad.max()),
            n_pure,
        )
    elif n_pure:
        log.info(
            '%s station %s %s: %d pure-fill (-999) values masked.',
            ofs,
            station_id,
            kind,
            n_pure,
        )
    arr[physical] = np.nan
    return arr


def parse_arguments_to_list(argument, logger):
    """
    takes a string from a user-supplied argument and parses it to a list
    of strings.
    """
    try:
        argument = argument.lower().replace('[', '').replace(']', '').replace(' ', '').split(',')
    except AttributeError:  # If argument is not a string
        logger.info(
            'Input argument (%s) being parsed from str to list is ' 'already a list. Moving on...',
            argument,
        )
        return argument
    try:
        argument[0]
        return argument
    except IndexError:
        logger.error(
            'Cannot parse input argument %s! Correct formatting and ' 'try again.', argument
        )
        sys.exit(-1)


def name_convent(variable):
    """
    change variable names to correspond to model netcdfs
    """
    if variable == 'water_level':
        name_var = 'wl'
        model_var = 'zeta'

    elif variable == 'water_temperature':
        name_var = 'temp'
        model_var = 'temp'

    elif variable == 'salinity':
        name_var = 'salt'
        model_var = 'salinity'

    elif variable == 'currents':
        name_var = 'cu'
        model_var = 'currents'

    return name_var, model_var


def get_time_step(prop, logger):
    """
    Gets the model time step depending on OFS and file type (fields or stations)

    Parameters
    ----------
    prop : program input arguments
    logger : logger

    Returns
    -------
    Time step in integer minutes

    """
    # Define your expected frequency in minutes (e.g. 6 minutes)
    exp_freq = 6
    if prop.ofsfiletype == 'fields':
        if prop.ofs in ['gomofs', 'wcofs', 'ngofs2']:
            exp_freq = 180
        else:
            exp_freq = 60
    return exp_freq


def find_time_gaps(prop, model, logger):
    """
    Look for missing data/time gaps in the model time series.

    Parameters
    ----------
    prop : main input arguments
    model : lazily loaded concatenated model dataset
    logger : logger

    Returns
    -------
    True if gaps detected, False if no gaps detected.

    """

    # Get time name depending on model source
    time_name = 'time'
    if prop.model_source == 'roms':
        time_name = 'ocean_time'

    time_vals = model[time_name].values
    if time_vals.size > 1 and not np.all(np.diff(time_vals) > np.timedelta64(0)):
        # Non-monotonic axis: deltas are meaningless. Treat as "gap" so the
        # caller resamples onto a regular grid (which we guard with sortby).
        return True

    # Calculate time differences between consecutive time steps in minutes
    time_deltas = np.diff(time_vals) / np.timedelta64(1, 'm')

    # Define your expected frequency in minutes (e.g. 6 minutes)
    exp_freq = float(get_time_step(prop, logger))

    # Check for gaps (where the delta is greater than expected)
    gaps = time_deltas != exp_freq

    return bool(np.any(gaps))


def ofs_ctlfile_extract(prop, name_var, model, logger):
    """
    The input here is the path, variable name, and logger.
    Extracts data from an OFS control file. If the file does not exist,
    it generates it first.
    """

    if prop.ofsfiletype == 'fields':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl'
        if (os.path.isfile(filename)) is False and prop.ctl_flag == 0:
            write_ofs_ctlfile(prop, model, logger)
            prop.ctl_flag += 1  # Raise flag -- we've gone through ctl file production
    elif prop.ofsfiletype == 'stations':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl'
        if (os.path.isfile(filename)) is False and prop.ctl_flag == 0:
            write_ofs_ctlfile(prop, model, logger)
            prop.ctl_flag += 1  # Raise flag -- we've gone through ctl file production

    try:
        with open(filename, encoding='utf-8') as file:
            model_ctlfile = file.read()
            lines = model_ctlfile.split('\n')
            # Drop the single header line, if present (legacy files
            # have none)
            lines = strip_model_ctl_header(lines)
            lines = [i.split(' ') for i in lines]
            lines = [list(filter(None, i)) for i in lines]
            nodes = np.array(lines[:-1])[:, 0]
            nodes = [int(i) for i in nodes]
            depths = np.array(lines[:-1])[:, 1]
            depths = [int(i) for i in depths]

            # this is the shift that can be applied to the ofs timeseries,
            # for instance if there is a known bias in the model
            shifts = np.array(lines[:-1])[:, -1]
            shifts = [float(i) for i in shifts]

            # This is the station id, of the nearest station to the mesh node
            ids = np.array(lines[:-1])[:, -2]
            ids = [str(i) for i in ids]

            return lines, nodes, depths, shifts, ids
    except IndexError:
        logger.warning(
            '%s model ctl file is blank -- no ' 'model nodes/stations found! Moving on...', name_var
        )
        return None
    except FileNotFoundError:
        logger.warning(
            '%s model ctl file is missing, probably because there '
            'are no matches between obs and model stations! '
            'Moving on...',
            name_var,
        )
        return None
    except Exception as ex:
        logger.error(
            'Unexpected error when processing %s model ctl ' 'file! Error: %s', name_var, ex
        )
        return None


def roms_nodes(model, node_num):
    """
    This function converts the node from the ofs control file
    into i and j for ROMS
    """
    i_index, j_index = np.unravel_index(int(node_num), np.shape(model['lon_rho']))
    return i_index, j_index


def _preload_static_coords(model, model_source, logger):
    """Force-materialize non-time coordinate vars before parallel fan-out.

    Why this exists: netCDF4's HDF5 backend isn't thread-safe under
    concurrent reads. When the variable fan-out launches 4 threads and each
    one calls ``np.array(model[lon])`` / ``np.array(model[lat])`` etc.
    inside ``index_nearest_node``/``index_nearest_depth``, the threads
    contend on the global HDF5 file lock across all backing files. Symptom:
    one core pinned, others idle, zero log progress for 20+ minutes.

    By pre-loading the static coords in the main thread (sequentially)
    before dispatching, each thread's later access is served from cached
    numpy buffers and never touches the file lock.

    Only the coords actually consumed by indexing.index_nearest_node and
    index_nearest_depth are targeted, to keep the upfront cost bounded.
    """
    candidate_coords = {
        'fvcom': ['lon', 'lat', 'lonc', 'latc', 'siglay', 'h', 'z'],
        'roms': ['lon_rho', 'lat_rho', 'mask_rho', 's_rho', 'h'],
        # SCHISM fields files carry SCHISM_hgrid_node_x/y rather than
        # lon/lat — without them this pre-load found nothing on a fields
        # run (observed: 'Pre-loaded 0 static coord(s)'), leaving the
        # per-station datum lookups in format_waterlevel to contend on the
        # HDF5 file lock once station threading became reachable for fields.
        'schism': [
            'lon',
            'lat',
            'station_name',
            'zcoords',
            'h',
            'SCHISM_hgrid_node_x',
            'SCHISM_hgrid_node_y',
        ],
        'adcirc': ['lon', 'lat'],
    }
    names = candidate_coords.get(model_source, [])
    loaded = []
    for name in names:
        if name not in model.variables:
            continue
        try:
            # .load() materializes AND caches the result back into the
            # dataset, so subsequent thread access reads from a numpy
            # buffer instead of re-running the Dask compute. Plain
            # np.asarray(model[name].values) computes once and discards
            # the buffer — worker threads then re-materialize and deadlock
            # on the HDF5 file lock all over again.
            model[name].load()
            loaded.append(name)
        except Exception as ex:
            logger.debug('Skipping pre-load of %s: %s', name, ex)
    logger.info(
        'Pre-loaded %d static coord(s) before parallel dispatch: %s',
        len(loaded),
        loaded,
    )


def _resample_time_vars_only(model, time_name, time_step, logger):
    """Resample only the data vars that have the time dim; leave static alone.

    ``Dataset.resample(time=...).asfreq()`` aligns *every* variable to the
    new regular time grid, including static mesh vars (lon, lat, siglay,
    h, ...) that intake's nested-combine replicated along the time dim
    during multi-file concat. Materializing those across hundreds of
    NECOFS files costs ~20 min per whichcast.

    By extracting just the time-varying subset, resampling that, and then
    re-attaching the static vars from the original dataset, we sidestep
    the per-file scan of the replicated coords. Output is functionally
    equivalent to the original resample call for downstream consumers.
    """
    time_vars = [name for name in model.data_vars if time_name in model[name].dims]
    static_vars = [name for name in model.data_vars if time_name not in model[name].dims]

    if not time_vars:
        logger.warning(
            'No data_vars carry the %s dim; resample is a no-op.',
            time_name,
        )
        return model

    resampled = model[time_vars].resample({time_name: time_step}).asfreq()

    # Re-attach static data vars (not modified by the resample) so
    # downstream code sees the same dataset shape it expected.
    for name in static_vars:
        resampled[name] = model[name]

    logger.info(
        'Resampled %d time-varying var(s); %d static var(s) re-attached.',
        len(time_vars),
        len(static_vars),
    )
    return resampled


# Default cap on timesteps materialized per dask.compute call inside
# _batch_extract. A single all-at-once compute against a 316-file
# NECOFS dataset (~17,000 timesteps at 6-min cadence) held intermediate
# Dask chunks for every backing file in memory simultaneously, pushing
# Windows into paging at 97% RAM and stretching the WL stage to >16 h.
# Splitting the time axis into ~1000-timestep windows (~4 days of NECOFS
# at 6 min) keeps only the files intersecting each window's slice
# active per compute, capping peak memory at ~1/16 of the previous spike
# while producing bit-for-bit identical output. The cost of the smaller
# window vs the previous 2000 default is ~5% additional wall time per
# variable (more dask.compute invocations with fixed scheduling overhead)
# — worth it on shared hosts where available RAM can swing widely.
_BATCH_EXTRACT_TIME_CHUNK = 1000


# Memory budget for a single dask.compute inside _batch_extract, in bytes.
# Peak resident memory during extraction is roughly
#     (concurrent computes) x (dask worker threads) x (one decompressed chunk)
# and is INDEPENDENT of the time window, so _BATCH_EXTRACT_TIME_CHUNK cannot
# bound it — num_workers is the knob. It only matters for "fat" chunks: a
# SCHISM fields chunk is the whole spatial domain for one timestep (STOFS-3D
# Atl: 3052121 nodes x 49 layers x 4 B = ~598 MB), so an unbounded threaded
# scheduler on a 32-core host would try to hold ~19 GB per compute.
_EXTRACT_MEM_BUDGET_BYTES = 8 * 1024**3
# Never serialize below this: num_workers is also the read-concurrency knob,
# and over NODD/S3 these chunks arrive as range requests. Dropping to 1-2
# workers would trade a memory win for a wall-clock regression.
_EXTRACT_MIN_WORKERS = 4
# Chunks at or below this size are "thin" (stations files, out2d elevation);
# leave those on dask's default scheduling, byte-identical to before.
_EXTRACT_FAT_CHUNK_BYTES = 64 * 1024**2


def _extract_num_workers(model, var_name, logger=None, n_vars=1, slots=None):
    """Cap dask worker threads so fat-chunk extractions cannot exhaust RAM.

    Returns ``None`` (dask's default) for thin chunks, so stations-file
    behaviour is unchanged.

    The budget is a whole-process one, so it is divided by BOTH multipliers
    the naive form misses:

    * ``slots`` — ``_extract_guard`` admits ``parallel_extract_slots``
      concurrent computes under h5netcdf (default 2), which is precisely the
      engine used for the SCHISM fields files this targets.
    * ``n_vars`` — ``_batch_extract_multi`` fuses u and v into ONE compute,
      so two fat chunks are resident per worker, not one.

    Without both, a STOFS-3D-Atl currents run would compute 14 workers from
    an 8 GiB budget and then actually peak near 33 GB.
    """
    try:
        darr = model[var_name].data
        chunksize = getattr(darr, 'chunksize', None)
        if chunksize is None:
            return None
        itemsize = model[var_name].dtype.itemsize
        chunk_bytes = int(np.prod(chunksize)) * int(itemsize)
    except Exception:  # pragma: no cover - defensive; never block extraction
        return None

    if chunk_bytes <= _EXTRACT_FAT_CHUNK_BYTES:
        return None

    concurrent = max(1, int(slots or 1))
    per_worker_bytes = chunk_bytes * max(1, int(n_vars))
    budget = _EXTRACT_MEM_BUDGET_BYTES // concurrent
    workers = int(min(max(_EXTRACT_MIN_WORKERS, budget // per_worker_bytes), 32))
    if logger is not None:
        logger.info(
            'Fat chunks for %s (%.0f MB each, %d var(s), %d concurrent '
            'compute slot(s)); capping dask workers at %d (~%.1f GB peak '
            'across all slots)',
            var_name,
            chunk_bytes / 1024**2,
            n_vars,
            concurrent,
            workers,
            workers * per_worker_bytes * concurrent / 1024**3,
        )
    return workers


def _batch_extract(
    model,
    var_name,
    idx_list,
    dep_list,
    idx_first=False,
    logger=None,
    time_chunk=_BATCH_EXTRACT_TIME_CHUNK,
    engine=None,
    extract_slots=None,
):
    """Extract all stations for a variable via batched dask.compute().

    The time axis is split into windows of ``time_chunk`` steps. Each
    window's per-station selections are computed in one ``dask.compute``
    call; the results are concatenated along time. This bounds peak
    memory to roughly one window's intermediate-chunk footprint, even on
    long multi-month runs that would otherwise materialize hundreds of
    backing files in a single compute graph.

    Parameters
    ----------
    model : xarray.Dataset
        The model dataset
    var_name : str
        Model variable name
    idx_list : list of int
        Station/node indices (2nd or 3rd dim depending on model)
    dep_list : list of int or None
        Depth/layer indices. None for 2D variables.
    idx_first : bool
        If True, indexing is [:, idx, dep] (ROMS order).
        If False, indexing is [:, dep, idx] (FVCOM/SCHISM order).
    logger : logging.Logger or None
        Optional logger for per-chunk progress lines. When provided and
        the time axis is split, each chunk is logged so multi-hour
        compute windows aren't silent.
    time_chunk : int
        Max timesteps per ``dask.compute`` call. Smaller values trade
        more compute invocations for lower peak memory.
    """
    import gc

    import dask

    n = len(idx_list)
    if n == 0:
        return np.empty((0, 0))

    time_dim = model[var_name].dims[0]
    n_time = int(model[var_name].sizes[time_dim])

    def _select(da, t0, t1):
        sliced = da.isel({time_dim: slice(t0, t1)})
        if dep_list is None:
            return [sliced[:, idx_list[i]] for i in range(n)]
        if idx_first:
            return [sliced[:, idx_list[i], dep_list[i]] for i in range(n)]
        return [sliced[:, dep_list[i], idx_list[i]] for i in range(n)]

    probe_da = model[var_name]
    has_dask = hasattr(probe_da.data, 'dask')
    n_workers = _extract_num_workers(model, var_name, logger, slots=extract_slots)

    # Eager (non-Dask) path: per-station numpy materialization. No need
    # to chunk; the previous code path also handled this case in one go.
    if not has_dask:
        if dep_list is None:
            lazy = [probe_da[:, idx_list[i]] for i in range(n)]
        elif idx_first:
            lazy = [probe_da[:, idx_list[i], dep_list[i]] for i in range(n)]
        else:
            lazy = [probe_da[:, dep_list[i], idx_list[i]] for i in range(n)]
        return np.stack([np.array(s) for s in lazy], axis=1)

    # Dask-backed path: window the time axis.
    guard = _extract_guard(engine, extract_slots)
    chunk = max(1, int(time_chunk))
    if n_time <= chunk:
        # Single-window fast path: behaviour matches the pre-chunking
        # implementation exactly when the dataset fits in one window.
        lazy = _select(probe_da, 0, n_time)
        with guard:
            computed = dask.compute(*[s.data for s in lazy], num_workers=n_workers)
        return np.stack(computed, axis=1)

    n_chunks = (n_time + chunk - 1) // chunk
    if logger is not None:
        logger.info(
            'Batch extract %s: time axis %d steps split into %d chunks ' 'of up to %d steps each',
            var_name,
            n_time,
            n_chunks,
            chunk,
        )

    parts = []
    for ci in range(n_chunks):
        t0 = ci * chunk
        t1 = min(n_time, t0 + chunk)
        lazy = _select(probe_da, t0, t1)
        # Bound the dask.compute across variable threads: fully serial
        # for thread-unsafe engines, parallel_extract_slots concurrent
        # computes under h5netcdf (memory cap). See _extract_guard.
        with guard:
            computed = dask.compute(*[s.data for s in lazy], num_workers=n_workers)
        parts.append(np.stack(computed, axis=1))
        if logger is not None:
            logger.info(
                'Batch extract %s chunk %d/%d done (timesteps %d:%d)',
                var_name,
                ci + 1,
                n_chunks,
                t0,
                t1,
            )
        # Drop references to this chunk's intermediate state before the
        # next compute builds its graph, so peak memory tracks one
        # chunk's worth instead of all of them.
        del lazy, computed
        gc.collect()

    return np.concatenate(parts, axis=0)


def _batch_extract_multi(
    model,
    var_names,
    idx_list,
    dep_list,
    idx_first=False,
    logger=None,
    time_chunk=_BATCH_EXTRACT_TIME_CHUNK,
    engine=None,
    extract_slots=None,
):
    """Extract multiple variables that share backing files in one fused compute.

    When two variables (e.g. ``u`` and ``v``) come from the same files,
    issuing one ``dask.compute`` for both lets Dask fuse the per-file
    reads — each chunk of each backing file is read once for both
    variables instead of once per variable. On the user's NECOFS run,
    sequential ``u`` then ``v`` took ~5h 02m for currents; the fused
    path is expected to drop that by roughly 30-40%.

    Same chunking and locking discipline as :func:`_batch_extract`: split
    the time axis into ``time_chunk``-step windows, compute each window
    under the extract guard, ``gc.collect`` between, concatenate.

    Parameters
    ----------
    var_names : list of str
        Model variable names to extract (e.g. ``['u', 'v']``). All must
        share the same dim layout (same ``idx_first`` value, same
        ``dep_list`` semantics).
    Other parameters identical to :func:`_batch_extract`.

    Returns
    -------
    list of np.ndarray
        One ``(time, n_stations)`` array per entry in ``var_names``, in
        the same order.
    """
    import gc

    import dask

    n = len(idx_list)
    if n == 0 or not var_names:
        return [np.empty((0, 0)) for _ in var_names]

    # Use the first variable as the probe for the time axis. All vars
    # must share the same time dim by assumption — checked below.
    probe_var = var_names[0]
    n_workers = _extract_num_workers(
        model, probe_var, logger, n_vars=len(var_names), slots=extract_slots
    )
    time_dim = model[probe_var].dims[0]
    n_time = int(model[probe_var].sizes[time_dim])
    for v in var_names[1:]:
        if model[v].dims[0] != time_dim:
            raise ValueError(
                f'_batch_extract_multi requires var_names to share the '
                f'leading time dim; {probe_var} has {time_dim} but {v} '
                f'has {model[v].dims[0]}'
            )

    def _select_one(da, t0, t1):
        sliced = da.isel({time_dim: slice(t0, t1)})
        if dep_list is None:
            return [sliced[:, idx_list[i]] for i in range(n)]
        if idx_first:
            return [sliced[:, idx_list[i], dep_list[i]] for i in range(n)]
        return [sliced[:, dep_list[i], idx_list[i]] for i in range(n)]

    has_dask = hasattr(model[probe_var].data, 'dask')

    if not has_dask:
        # Eager path — fall back to per-var per-station numpy.
        results = []
        for v in var_names:
            da = model[v]
            if dep_list is None:
                lazy = [da[:, idx_list[i]] for i in range(n)]
            elif idx_first:
                lazy = [da[:, idx_list[i], dep_list[i]] for i in range(n)]
            else:
                lazy = [da[:, dep_list[i], idx_list[i]] for i in range(n)]
            results.append(np.stack([np.array(s) for s in lazy], axis=1))
        return results

    guard = _extract_guard(engine, extract_slots)
    chunk = max(1, int(time_chunk))
    if n_time <= chunk:
        # Single-window fast path: one fused compute over all vars + stations.
        per_var_lazy = [_select_one(model[v], 0, n_time) for v in var_names]
        flat = [s.data for lst in per_var_lazy for s in lst]
        with guard:
            computed = dask.compute(*flat, num_workers=n_workers)
        results = []
        for vi, _ in enumerate(var_names):
            start = vi * n
            results.append(np.stack(computed[start : start + n], axis=1))
        return results

    n_chunks = (n_time + chunk - 1) // chunk
    if logger is not None:
        logger.info(
            'Batch extract %s: time axis %d steps split into %d chunks '
            'of up to %d steps each (fused %d-var compute)',
            '+'.join(var_names),
            n_time,
            n_chunks,
            chunk,
            len(var_names),
        )

    parts_per_var = [[] for _ in var_names]
    for ci in range(n_chunks):
        t0 = ci * chunk
        t1 = min(n_time, t0 + chunk)
        per_var_lazy = [_select_one(model[v], t0, t1) for v in var_names]
        flat = [s.data for lst in per_var_lazy for s in lst]
        with guard:
            computed = dask.compute(*flat, num_workers=n_workers)
        for vi in range(len(var_names)):
            start = vi * n
            parts_per_var[vi].append(np.stack(computed[start : start + n], axis=1))
        if logger is not None:
            logger.info(
                'Batch extract %s chunk %d/%d done (timesteps %d:%d)',
                '+'.join(var_names),
                ci + 1,
                n_chunks,
                t0,
                t1,
            )
        del per_var_lazy, flat, computed
        gc.collect()

    return [np.concatenate(parts, axis=0) for parts in parts_per_var]


def _extract_concurrency_kwargs(prop):
    """Extract-guard kwargs for _batch_extract(_multi) from prop.

    ``netcdf_engine`` is recorded by ``intake_model`` at open time;
    ``extract_slots`` is set from the parallelization config before the
    variable threads are dispatched. Both default to the safe fully
    serialized guard when absent (e.g. pre-loaded model datasets).
    """
    return {
        'engine': getattr(prop, 'netcdf_engine', None),
        'extract_slots': getattr(prop, 'extract_slots', None),
    }


def _fields_scalar_layout(prop, model_var):
    """Resolve the (variable, idx_first) layout for a scalar FIELDS extraction.

    Returns ``(actual_var, idx_first)``, or ``None`` when this combination is
    not expressible as a batched extraction and must stay on the sequential
    per-station path.

    The layouts here mirror ``format_temp_salt``/``format_waterlevel`` exactly
    — that is the contract. SCHISM alone carries three different orders under
    one ``model_source``:

    * STOFS fields  -> ``[:, node, layer]``  (idx_first=True)
    * SECOFS fields -> ``[:, layer, node]``  (idx_first=False)
    * STOFS stations-> ``[:, station]``      (2-D, handled by the is_2d probe)

    Every wrong permutation returns a correctly-shaped array of wrong numbers,
    because both dims are large enough that a transposition does not raise.
    """
    src = prop.model_source
    if src == 'fvcom':
        return (model_var, False)
    if src == 'adcirc':
        # Only water level exists for STOFS-2D-Global; format_temp_salt and
        # format_currents raise ValueError for temp/salt/currents. Returning a
        # layout for those would batch a variable the sequential path refuses
        # to produce.
        return (model_var, False) if model_var == 'zeta' else None
    if src == 'roms':
        # ROMS fields is [:, layer, i, j] via roms_nodes()'s unravel_index —
        # a 4-D selection _batch_extract cannot express. Excluded by policy,
        # not by exception: for node numbers < len(s_rho) the wrong indexing
        # would NOT raise, it would silently return the wrong location.
        return None
    if src == 'schism':
        if 'stofs' in prop.ofs:
            if model_var == 'zeta':
                return ('elevation', False)  # out2d; 2-D, no layer axis
            return ('temperature' if model_var == 'temp' else model_var, True)
        if 'secofs' in prop.ofs:
            # Substring test mirrors format_temp_salt's own `'secofs' in
            # prop.ofs`. NB: SECOFS keeps 'temp' on disk — it must NOT be
            # renamed to 'temperature' the way STOFS is.
            return (model_var, False)
        if model_var == 'zeta':
            # format_waterlevel's SCHISM fields branch is NOT split on
            # stofs/secofs — it handles every SCHISM OFS with a plain 2-D
            # [:, node] read of 'zeta'. So loofs2 fields water level is
            # batchable even though its temp/salt/currents are not.
            return (model_var, False)
        # Other SCHISM (e.g. loofs2) temp/salt has no fields branch in
        # format_temp_salt at all — it would UnboundLocalError. Do not batch
        # what the sequential path cannot do either.
        return None
    return None


def _fields_current_layout(prop):
    """Resolve ``(var_names, idx_first)`` for a FIELDS currents extraction.

    Mirrors ``format_currents``. Returns ``None`` when not batchable.
    """
    src = prop.model_source
    if src == 'fvcom':
        return (['u', 'v'], False)
    if src == 'roms':
        return None  # 4-D [:, layer, i, j] — see _fields_scalar_layout
    if src == 'schism':
        if 'stofs' in prop.ofs:
            return (['horizontalVelX', 'horizontalVelY'], True)
        if prop.ofs in ('secofs',):
            # Exact match, mirroring format_currents' own `prop.ofs in
            # ['secofs']`. format_temp_salt uses a substring test instead;
            # the two are inconsistent upstream, so each layout resolver
            # copies the predicate of the function it mirrors rather than
            # silently picking the looser one.
            return (['u', 'v'], False)
        return None
    return None


def _precompute_current_data(prop, model, ofs_ctlfile, logger):
    """Batch-extract current (u/v) station data in a single Dask compute call.

    Both u and v are computed in one fused dask.compute per time-window,
    so Dask reads each backing file once instead of twice (once for u,
    once for v) — see :func:`_batch_extract_multi`.

    Returns a dict with 'u_data' and 'v_data' numpy arrays.
    """
    n_stations = len(ofs_ctlfile[1])
    indices = [int(ofs_ctlfile[1][i]) for i in range(n_stations)]
    depths = [int(ofs_ctlfile[2][i]) for i in range(n_stations)]
    extract_kwargs = _extract_concurrency_kwargs(prop)

    if prop.ofsfiletype == 'fields':
        layout = _fields_current_layout(prop)
        if layout is None:
            return None
        var_names, idx_first = layout
        u_data, v_data = _batch_extract_multi(
            model,
            var_names,
            indices,
            depths,
            idx_first=idx_first,
            logger=logger,
            **extract_kwargs,
        )
        return {'u_data': u_data, 'v_data': v_data}

    if prop.model_source == 'fvcom':
        u_data, v_data = _batch_extract_multi(
            model, ['u', 'v'], indices, depths, idx_first=False, logger=logger, **extract_kwargs
        )
    elif prop.model_source == 'roms':
        u_data, v_data = _batch_extract_multi(
            model,
            ['u_east', 'v_north'],
            indices,
            depths,
            idx_first=True,
            logger=logger,
            **extract_kwargs,
        )
    elif prop.model_source == 'schism':
        if 'stofs' not in prop.ofs:
            u_data, v_data = _batch_extract_multi(
                model, ['u', 'v'], indices, depths, idx_first=False, logger=logger, **extract_kwargs
            )
        else:
            # STOFS-3D-Atl 2-D currents (no depth dim).
            u_data, v_data = _batch_extract_multi(
                model, ['u', 'v'], indices, None, logger=logger, **extract_kwargs
            )

    return {'u_data': u_data, 'v_data': v_data}


def _precompute_scalar_data(prop, model, ofs_ctlfile, model_var, logger):
    """Batch-extract scalar (temp/salt/water level) station data.

    Returns a dict with 'scalar_data' numpy array.
    """
    n_stations = len(ofs_ctlfile[1])
    indices = [int(ofs_ctlfile[1][i]) for i in range(n_stations)]
    depths = [int(ofs_ctlfile[2][i]) for i in range(n_stations)]

    if prop.ofsfiletype == 'fields':
        # Resolved before the stations-oriented rename block below: that
        # block renames temp->temperature for ALL schism, which KeyErrors on
        # SECOFS (it carries 'temp' on disk). Fields name resolution lives
        # entirely in _fields_scalar_layout.
        layout = _fields_scalar_layout(prop, model_var)
        if layout is None:
            return None
        fields_var, idx_first = layout
        extract_kwargs = _extract_concurrency_kwargs(prop)
        if model[fields_var].ndim < 3:
            scalar_data = _batch_extract(
                model, fields_var, indices, None, logger=logger, **extract_kwargs
            )
        else:
            scalar_data = _batch_extract(
                model,
                fields_var,
                indices,
                depths,
                idx_first=idx_first,
                logger=logger,
                **extract_kwargs,
            )
        return {'scalar_data': scalar_data}

    actual_var = model_var
    if prop.model_source == 'roms' and model_var == 'salinity':
        actual_var = 'salt'
    if prop.model_source == 'schism' and model_var == 'temp':
        actual_var = 'temperature'
    if prop.model_source == 'schism' and model_var == 'zeta':
        actual_var = 'elevation' if prop.ofsfiletype == 'fields' else model_var

    # Some scalars are 2-D (time, station) — e.g. FVCOM/ROMS zeta in
    # stations files. Indexing those with a depth axis raises
    # "too many indices for array", which used to drop the whole run
    # back to slow per-station extraction. Detect dimensionality up front
    # so the batch path stays on for water level too.
    is_2d = model[actual_var].ndim < 3
    extract_kwargs = _extract_concurrency_kwargs(prop)

    if prop.model_source == 'fvcom':
        if is_2d:
            scalar_data = _batch_extract(
                model, actual_var, indices, None, logger=logger, **extract_kwargs
            )
        else:
            scalar_data = _batch_extract(
                model, actual_var, indices, depths, idx_first=False, logger=logger, **extract_kwargs
            )
    elif prop.model_source == 'roms':
        if is_2d:
            scalar_data = _batch_extract(
                model, actual_var, indices, None, logger=logger, **extract_kwargs
            )
        else:
            scalar_data = _batch_extract(
                model, actual_var, indices, depths, idx_first=True, logger=logger, **extract_kwargs
            )
    elif prop.model_source == 'schism':
        if 'stofs' in prop.ofs and model_var in ('temp', 'temperature'):
            scalar_data = _batch_extract(
                model, 'temperature', indices, None, logger=logger, **extract_kwargs
            )
        elif is_2d:
            scalar_data = _batch_extract(
                model, actual_var, indices, None, logger=logger, **extract_kwargs
            )
        else:
            try:
                scalar_data = _batch_extract(
                    model,
                    actual_var,
                    indices,
                    depths,
                    idx_first=False,
                    logger=logger,
                    **extract_kwargs,
                )
            except IndexError:
                scalar_data = _batch_extract(
                    model, actual_var, indices, None, logger=logger, **extract_kwargs
                )

    return {'scalar_data': scalar_data}


def _precompute_stations_data(prop, model, ofs_ctlfile, model_var, logger):
    """Batch-extract all station data in a single Dask compute call.

    Returns a dict with pre-computed numpy arrays keyed by data type.
    Only used for stations files to avoid N separate .compute() calls.

    Uses dask.compute() to batch all station selections into one
    optimized graph execution, so Dask reads each time chunk once
    for all stations instead of once per station. This is critical
    for monthly/yearly runs where many time chunks exist.
    """
    n_stations = len(ofs_ctlfile[1])

    time_var = 'ocean_time' if prop.model_source == 'roms' else 'time'
    model_time = np.array(model[time_var])

    result = {'model_time': model_time}

    if model_var in ('u', 'u_east', 'horizontalVelX', 'currents'):
        # Current variables — need u and v
        current_result = _precompute_current_data(prop, model, ofs_ctlfile, logger)
        if current_result is None:
            return None
        result.update(current_result)
    else:
        # Scalar variables (temp, salt, water level)
        scalar_result = _precompute_scalar_data(prop, model, ofs_ctlfile, model_var, logger)
        if scalar_result is None:
            return None
        result.update(scalar_result)

    logger.info(
        'Pre-computed batch extraction for %d stations, var=%s, filetype=%s',
        n_stations,
        model_var,
        prop.ofsfiletype,
    )
    return result


def format_temp_salt(prop, model, ofs_ctlfile, model_var, i, precomputed=None):
    """
    extract temperature and salinity time series from concatenated model data
    """

    if precomputed is not None:
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        # Sentinel masking is a STATIONS-only behaviour today. The fields
        # code path has never masked, so gating it on ofsfiletype keeps the
        # batch path byte-identical to the sequential one it replaces.
        # (Extending masking to fields is a real but separate fix — it moves
        # published skill numbers and needs cache invalidation.)
        if prop.model_source == 'schism' and prop.ofsfiletype == 'stations':
            model_obs = _mask_schism_sentinels(
                model_obs, model_var, ofs_ctlfile[4][i], prop.ofs, logger
            )
    elif prop.model_source == 'fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs  # + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x siglay x station
            model_time = np.array(model['time'])
            # if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs  # + ofs_ctlfile[3][i]
            # else:
            #    model_obs = None

    elif prop.model_source == 'roms':
        if model_var == 'salinity':
            model_var = 'salt'
        if prop.ofsfiletype == 'fields':
            i_index, j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[2][i]), i_index, j_index])
            model_obs = model_obs  # + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            model_time = np.array(model['ocean_time'])
            # if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
            )
            model_obs = model_obs  # + ofs_ctlfile[3][i]
    elif prop.model_source == 'schism':
        # SECOFS: time x depth x node
        if prop.ofsfiletype == 'fields':
            if 'stofs' in prop.ofs:
                if model_var == 'temp':
                    model_var = 'temperature'
                model_obs = np.array(
                    model[model_var][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
                )
            elif 'secofs' in prop.ofs:
                model_obs = np.array(
                    model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
                )
            model_obs = model_obs
            model_time = np.array(model['time'])
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            if 'stofs' in prop.ofs:
                if model_var == 'temp':
                    model_var = 'temperature'
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            elif 'secofs' in prop.ofs:
                # SECOFS dims: time x siglay x station
                model_obs = np.array(
                    model[model_var][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
                )
            else:
                model_obs = np.array(
                    model[model_var][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
                )
            model_obs = _mask_schism_sentinels(
                model_obs, model_var, ofs_ctlfile[4][i], prop.ofs, logger
            )
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have temp/sal data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Temperature and salinity data are not available for STOFS-2D-Global.')

    data_model = pd.DataFrame(
        {'DateTime': model_time, 'OBS': model_obs}, columns=['DateTime', 'OBS']
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )

    formatted_series = scalar(data_model, start_date, end_date)

    if not formatted_series:
        logger.error(
            'Formatted series is empty in format_temp_salt! If using '
            'custom model file names, make sure input date range and '
            'the date range of your files overlap.'
        )
    return formatted_series


def format_currents(prop, model, ofs_ctlfile, i, precomputed=None):
    """
    extract current velocity time series from concatenated model data
    """

    if precomputed is not None:
        mfp = ModelFormatProperties()
        mfp.model_time = precomputed['model_time']
        # .copy() is load-bearing: these are views into an array shared by
        # every station. _mask_schism_sentinels used to copy internally, but
        # it no longer runs on the fields path, so without an explicit copy
        # any later in-place op here is a cross-station race under the
        # station-level ThreadPoolExecutor.
        u_i = precomputed['u_data'][:, i]
        v_i = precomputed['v_data'][:, i]
        # STATIONS-only, as in format_temp_salt — keeps fields byte-identical.
        if prop.model_source == 'schism' and prop.ofsfiletype == 'stations':
            station_id = ofs_ctlfile[4][i]
            u_i = _mask_schism_sentinels(u_i, 'currents_uv', station_id, prop.ofs, logger)
            v_i = _mask_schism_sentinels(v_i, 'currents_uv', station_id, prop.ofs, logger)
        mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
        mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(len(mfp.model_time))]
        )
    elif prop.model_source == 'fvcom':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])
            v_i = np.array(model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )

            mfp.model_obs = mfp.model_obs  # + ofs_ctlfile[3][i]

        elif prop.ofsfiletype == 'stations':
            # if int(ofs_ctlfile[1][i]) > -999:
            mfp = ModelFormatProperties()
            mfp.model_time = np.array(model['time'])

            u_i = np.array(model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])
            v_i = np.array(model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )

            mfp.model_obs = mfp.model_obs  # + ofs_ctlfile[3][i]

    elif prop.model_source == 'roms':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['ocean_time'])
        if prop.ofsfiletype == 'fields':
            i_index, j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[2][i]), i_index, j_index])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[2][i]), i_index, j_index])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )

            mfp.model_obs = mfp.model_obs  # + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )

            mfp.model_obs = mfp.model_obs  # + ofs_ctlfile[3][i]

    elif prop.model_source == 'schism':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            if 'stofs' in prop.ofs:
                u_i = np.array(
                    model['horizontalVelX'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
                )
                v_i = np.array(
                    model['horizontalVelY'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
                )
            elif prop.ofs in ['secofs']:
                u_i = np.array(model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])
                v_i = np.array(model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )
            mfp.model_obs = mfp.model_obs  # + ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            if 'stofs' in prop.ofs:
                u_i = np.array(model['u'][:, int(ofs_ctlfile[1][i])])
                v_i = np.array(model['v'][:, int(ofs_ctlfile[1][i])])
            else:
                u_i = np.array(model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])
                v_i = np.array(model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])])

            station_id = ofs_ctlfile[4][i]
            u_i = _mask_schism_sentinels(u_i, 'currents_uv', station_id, prop.ofs, logger)
            v_i = _mask_schism_sentinels(v_i, 'currents_uv', station_id, prop.ofs, logger)
            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [
                    math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
                    for t in range(len(np.array(mfp.model_time)))
                ]
            )
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have current data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Current data are not available for STOFS-2D-Global.')

    mfp.data_model = pd.DataFrame(
        {'DateTime': mfp.model_time, 'DIR': mfp.model_ang, 'OBS': mfp.model_obs},
        columns=['DateTime', 'DIR', 'OBS'],
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    formatted_series = vector(mfp.data_model, start_date, end_date)

    if not formatted_series:
        logger.error(
            'Formatted series is empty in format_currents! If using '
            'custom model file names, make sure input date range and '
            'the date range of your files overlap.'
        )

    return formatted_series


def format_waterlevel(prop, model, ofs_ctlfile, model_var, i, logger, precomputed=None):
    """
    extract water level time series from concatenated model data
    """

    id_number = ofs_ctlfile[4][i]
    datum_offset = get_datum_offset_func(prop, int(ofs_ctlfile[1][i]), model, id_number, logger)
    logger.info(f'Datum offset for station {id_number} (node {ofs_ctlfile[1][i]}): {datum_offset}')

    if precomputed is not None:
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        if prop.model_source == 'schism':
            if prop.ofsfiletype == 'fields':
                # The sequential SCHISM fields branch adds the ctl depth
                # column before the datum shift. It is +0.0 in the standard
                # pipeline (index_nearest_depth returns 0.0 for wl) but not
                # for user-supplied or legacy ctl files, so it must not be
                # silently dropped here.
                model_obs = model_obs + ofs_ctlfile[3][i]
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
        elif prop.model_source == 'adcirc':
            # adcirc carries the upper guard too — do not fold it into the
            # generic branch below.
            if datum_offset > -999 and datum_offset < 999:
                model_obs = model_obs - datum_offset
        else:
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
    elif prop.model_source == 'fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            # if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            # else:
            #    model_obs = None
    elif prop.model_source == 'roms':
        if prop.ofsfiletype == 'fields':
            i_index, j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, i_index, j_index])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x stations
            # i_index = roms_station_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            # if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            # else:
            #    model_obs = None
    elif prop.model_source == 'schism':
        if prop.ofsfiletype == 'fields':
            if model_var == 'zeta' and 'stofs' in prop.ofs:
                model_var = 'elevation'  # Using out2d files
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            model_obs = model_obs + ofs_ctlfile[3][i]
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
    elif prop.model_source == 'adcirc':
        model_time = np.array(model['time'])
        model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
        if datum_offset > -999 and datum_offset < 999:
            model_obs = model_obs - datum_offset

    data_model = pd.DataFrame(
        {'DateTime': model_time, 'OBS': model_obs}, columns=['DateTime', 'OBS']
    )

    start_date = (
        str(
            (
                datetime.strptime(prop.start_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                - timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )
    end_date = (
        str(
            (
                datetime.strptime(prop.end_date_full.split('T')[0].replace('-', ''), '%Y%m%d')
                + timedelta(days=2)
            ).strftime('%Y%m%d')
        )
        + '-01:01:01'
    )

    formatted_series = scalar(data_model, start_date, end_date)

    if not formatted_series:
        logger.error(
            'Formatted series is empty in format_waterlevel! If using '
            'custom model file names, make sure input date range and '
            'the date range of your files overlap.'
        )

    return formatted_series, datum_offset


def find_time_variable_name(ds: xr.Dataset) -> str:
    """Scans an xarray Dataset to find the coordinate name representing time

    based on its datetime64 data type.
    """
    # First, check the coordinates (most common place for the time dimension)
    for coord_name in ds.coords:
        if np.issubdtype(ds[coord_name].dtype, np.datetime64):
            return str(coord_name)

    # Fallback: check data variables if it wasn't explicitly marked as a coordinate
    for var_name in ds.data_vars:
        if np.issubdtype(ds[var_name].dtype, np.datetime64):
            return str(var_name)

    raise ValueError('Could not automatically detect a datetime variable in this dataset.')


def has_date_overlap(
    start_date: datetime,
    end_date: datetime,
    ds: xr.Dataset,
) -> bool:
    """Checks for a date overlap by auto-detecting the time variable in an

    xarray Dataset.
    """
    # 1. Auto-detect the time coordinate name
    time_var_name = find_time_variable_name(ds)

    # 2. Extract the lazy-loaded time array using the detected name
    time_array = ds[time_var_name]

    # 3. Convert input Python datetimes to numpy.datetime64
    np_start = np.datetime64(start_date)
    np_end = np.datetime64(end_date)

    # 4. Extract min and max boundaries lazily
    xr_min = time_array.min().values
    xr_max = time_array.max().values

    # 5. Check for overlap
    overlap = (np_start <= xr_max) and (xr_min <= np_end)

    return bool(overlap)


def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    # Start Date and End Date validation

    try:
        start_dt = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (
            f'Please check Start Date - '
            f"'{prop.start_date_full}', End Date -"
            f" '{prop.end_date_full}'. Abort!"
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = (
            f'End Date {prop.end_date_full} '
            f'is before Start Date '
            f'{prop.start_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params['home']

    # Path validation
    ofs_extents_path = utils.resolve_asset_path(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. ' f'Please check Path - ' f"'{prop.path}'. Abort!"
        )
        logger.error(error_message)
        sys.exit(-1)

    # OFS validation
    shapefile = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shapefile):
        error_message = (
            f"Shapefile '{prop.ofs}' " f'is not found at the folder' f' {ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # Whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ['nowcast', 'forecast_a', 'forecast_b', 'hindcast']
    ):
        error_message = f'Please check Whichcast - ' f"'{prop.whichcast}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == 'forecast_a' and prop.forecast_hr is None:
        error_message = 'Forecast_Hr is required if ' 'Whichcast is forecast_a. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # datum validation
    if prop.datum not in prop.datum_list:
        error_message = f'Datum {prop.datum} is not valid. Abort!'
        logger.error(error_message)
        sys.exit(-1)
    # GLOFS datum validation
    if prop.datum.lower() not in ('igld85', 'lwd') and prop.ofs in [
        'leofs',
        'loofs',
        'lmhofs',
        'lsofs',
    ]:
        error_message = f'Use only LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # Non-GLOFS datum validation
    if prop.datum.lower() in ('igld85', 'lwd') and 'l' not in prop.ofs[0]:
        error_message = f'Do not use LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # File type validation
    if prop.ofsfiletype not in ['stations', 'fields']:
        logger.error(
            'Uh-oh, please select a valid model output file type! '
            'You chose %s. The options are "stations" or "fields".',
            prop.ofsfiletype,
        )
    if (prop.ofs == 'stofs_2d_glo') and (prop.ofsfiletype == 'fields'):
        logger.error(
            'STOFS-2D-Global is only available as station output files. Please select "stations" for file type. Exiting...'
        )
        raise NotImplementedError(
            'STOFS-2D-Global fields file processing is not currently implemented.'
        )
    # Warn if using custom lat/lon inputs and stations files
    if prop.ofsfiletype == 'stations' and prop.user_input_location:
        logger.warning(
            'You are using custom lat/lon coordinates for model time '
            'series extraction from stations files. All lats/lons '
            'may not have a matching station output location. '
            'To extract model time series for all lats/lons, try '
            'using field files! Continuing...'
        )
    # Check for user input file if using custom lat/lon inputs
    _conf = getattr(prop, 'config_file', None)
    filepath = utils.Utils(_conf).read_config_section('user_xy_inputs', logger)['user_xy_path']
    if os.path.isfile(filepath) is False and prop.user_input_location:
        logger.error(
            'No user lat & lon inputs found! Please make sure '
            'the path to your input is correct in ofs_dps.conf, or '
            'create a text file with the following columns separated '
            ' by a space: '
            '{location_name} '
            '{latitude (decimal deg)} '
            '{longitude (decimal deg)} '
            '{water depth (m)}'
        )
        sys.exit()
    # Handle variable input argument
    correct_var_list = ['water_level', 'water_temperature', 'salinity', 'currents']
    list_diff = list(set(prop.var_list) - set(correct_var_list))
    if len(list_diff) != 0:
        logger.error(
            'Incorrect inputs to variable selection argument: %s. ' 'Please use %s. Exiting...',
            list_diff,
            correct_var_list,
        )
        sys.exit()
    if prop.ofs == 'stofs_2d_glo':
        if set(prop.var_list) != {'water_level'}:
            logger.warning(
                '"water_level" is the only available variable for STOFS-2D-Global: '
                'Removing other variables from list.'
            )
            prop.var_list = ['water_level']
            # I think we can alter its state here, but maybe there's a reason not to?


def read_custom_filenames(filepath):
    """Read a custom model-file list, one path/URL per line.

    Parameters
    ----------
    filepath : str
        Path to the text file enumerating model file locations.

    Returns
    -------
    list[str]
        One entry per line with trailing newlines stripped.
    """
    with open(filepath) as file:
        lines = file.read().splitlines()
    return lines


def _all_prd_files_complete(prop_local, ofs_ctlfile, name_var, expected_timesteps, logger):
    """Return ``True`` iff every per-station ``.prd`` file for this
    (variable, whichcast, ofsfiletype) combo exists on disk with the
    expected number of data rows.

    A SIGKILL during the per-station write loop can leave one ``.prd``
    truncated to N-1 rows. Without a row-count check the next run would
    happily reuse that partial file and downstream skill would read a
    mis-aligned series. We compare against ``expected_timesteps`` (the
    resampled model time-axis length) and tolerate ``±1`` row to absorb
    a trailing blank line.

    When ``expected_timesteps`` is ``None`` (caller couldn't determine
    it from the dataset), we fall back to the prior length-only check
    so callers stay backwards-compatible.
    """
    n_stations = len(ofs_ctlfile[1])
    if n_stations == 0:
        return False

    def _prd_path(idx):
        if prop_local.whichcast == 'forecast_a':
            return (
                f'{prop_local.data_model_1d_node_path}/'
                f'{ofs_ctlfile[4][idx]}_{prop_local.ofs}_'
                f'{name_var}_{ofs_ctlfile[1][idx]}_'
                f'{prop_local.whichcast}_'
                f'{prop_local.forecast_hr}_'
                f'{prop_local.ofsfiletype}_model.prd'
            )
        return (
            f'{prop_local.data_model_1d_node_path}/'
            f'{ofs_ctlfile[4][idx]}_{prop_local.ofs}_'
            f'{name_var}_{ofs_ctlfile[1][idx]}_'
            f'{prop_local.whichcast}_'
            f'{prop_local.ofsfiletype}_model.prd'
        )

    for i in range(n_stations):
        path = _prd_path(i)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return False
        if expected_timesteps is None:
            # Backwards-compatible fallback: existence + non-zero size only.
            continue
        try:
            with open(path, encoding='utf-8') as fh:
                # Don't count the header line toward the expected
                # timesteps, or a file truncated one data row short
                # would pass this check.
                first = fh.readline()
                row_count = sum(1 for _ in fh)
                if first and not first.startswith(SERIES_HEADER_PREFIX):
                    row_count += 1
        except (OSError, UnicodeDecodeError) as ex:
            logger.warning(
                'Could not read %s for row-count check (%s); treating '
                'as incomplete and re-extracting.',
                path,
                ex,
            )
            return False
        # Tolerance is upward-only: a trailing blank line can produce
        # ``expected_timesteps + 1`` logical rows, but anything short of
        # ``expected_timesteps`` means the prior run was killed
        # mid-write and we must re-extract.
        if row_count < expected_timesteps:
            logger.warning(
                'Resume check: %s has %d rows but expected %d — '
                'previous run likely killed mid-write. Re-extracting.',
                path,
                row_count,
                expected_timesteps,
            )
            return False

    # Row counts alone cannot distinguish a fresh file from one left over
    # by an earlier run of the same window length (daily operational runs
    # produce identical row counts every day). Check that the files
    # actually cover the requested run window before reusing them.
    run_window = parse_run_window(prop_local, logger)
    if run_window is not None:
        for i in range(n_stations):
            path = _prd_path(i)
            if not covers_run_window(path, run_window[0], run_window[1], logger=logger):
                logger.warning(
                    'Resume check: %s does not cover the run window '
                    '%s to %s — likely left over from an earlier run. '
                    'Re-extracting.',
                    path,
                    run_window[0],
                    run_window[1],
                )
                return False
    # NOTE: parameter-signature staleness (datum/station-owner/bins) is
    # enforced upstream by ``get_skill._ensure_prd_files`` via the cache
    # manifest before this resume short-circuit is consulted, so it is not
    # re-checked here. This helper only judges window coverage + row counts,
    # keeping it usable in isolation (and matching its unit tests).
    return True


def get_node_ofs(prop, logger, model_dataset=None):
    """
    This is the final model 1d extraction function, it opens the path and looks
     for the model ctl file,
    if model ctl file is found, then the script uses it for extracting the model
     timeseries
    if model ctl file is not found, all the predefined function for finding the
    nearest node and depth are applied and a new model ctl file is created along
    with the time series.

    Parameters
    ----------
    model_dataset : xarray.Dataset or None
        Pre-loaded model dataset. When provided, skips the expensive
        intake_model() call and reuses this dataset instead.

    Returns
    -------
    xarray.Dataset
        The loaded (and gap-filled) model dataset, so callers can cache it.
    """
    prop.model_source = get_model_source(prop.ofs)
    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting OFS Model process ---')

    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)
    prop.datum_list = (
        utils.Utils(_conf).read_config_section('datums', logger)['datum_list']
    ).split(' ')

    # Check if custom filename input is enabled
    try:
        conf_settings = utils.Utils(_conf).read_config_section('settings', logger)
        use_custom_files = conf_settings.get('use_custom_filenames', 'False').lower() in (
            'true',
            '1',
            'yes',
        )
    except (KeyError, AttributeError, ValueError, OSError) as exc:
        logger.warning(
            'Could not read [settings] for custom-file mode (%s); '
            'proceeding with default model file discovery.',
            exc,
        )
        use_custom_files = False

    # Parse variable selection input to list
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)
    # Parameter validation
    parameter_validation(prop, dir_params, logger)

    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    prop.control_files_path = os.path.join(prop.path, dir_params['control_files_dir'])
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'], dir_params['1d_node_dir']
    )
    prop.data_model_1d_node_path = Path(prop.data_model_1d_node_path).as_posix()
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    # Path to save plotly maps
    prop.plotly_maps = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'], dir_params['visual_maps']
    )
    os.makedirs(prop.plotly_maps, exist_ok=True)

    # Reformat start & end dates
    # Convert ISO format to internal format for processing
    # Use local variables to avoid modifying prop permanently
    start_date_internal = prop.start_date_full.replace('-', '').replace('Z', '').replace('T', '-')
    end_date_internal = prop.end_date_full.replace('-', '').replace('Z', '').replace('T', '-')

    try:
        prop.startdate = (datetime.strptime(start_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d'
        ) + '00'
        prop.enddate = (datetime.strptime(end_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d'
        ) + '23'
    except Exception as e:
        logger.error(f'Problem with date format in get_node_ofs: {e}')
        raise SystemExit(1)

    # Lazy load the model data (or reuse pre-loaded dataset)
    if model_dataset is not None:
        model = model_dataset
        logger.info('Using pre-loaded model dataset (skipping intake_model)')
    else:
        if not use_custom_files:
            dir_list = list_of_dir(prop, logger)
            list_files = list_of_files_func(prop, dir_list, logger)
        else:
            filepath = utils.Utils(_conf).read_config_section('settings', logger)['filename_path']
            try:
                list_files = read_custom_filenames(filepath)
            except FileNotFoundError:
                logger.error(
                    'No custom model filename file found in get_node_ofs! '
                    'Please check the file path and try again.'
                )
                raise SystemExit(1)
            except Exception as e:
                logger.error(
                    'Error when loading custom filenames from file in ' 'get_node_ofs! Error: %s', e
                )
                raise SystemExit(1)

        logging.info('About to start intake_scisa from get_node ...')
        model = intake_model(list_files, prop, logger)
        logging.info('Lazily loaded dataset complete for %s!', prop.whichcast)

        if use_custom_files:
            # Check if dates of loaded model data overlap with user-input dates
            try:
                date_overlap = has_date_overlap(
                    datetime.strptime(start_date_internal.split('-')[0], '%Y%m%d'),
                    datetime.strptime(end_date_internal.split('-')[0], '%Y%m%d'),
                    model,
                )
                if not date_overlap:
                    logger.error(
                        'The date range of the loaded model files '
                        'does not overlap with the start and end '
                        'dates of the skill assessment run. Please '
                        'either disable custom file name loading '
                        'in the conf file, or check that the provided '
                        'filenames are correct. Exiting...'
                    )
                    raise SystemExit(1)
            except Exception as e:
                logger.error(
                    'Cannot verify if the dates in user-supplied '
                    'custom model files overlap with the start and '
                    'end dates of the skill assessment run. Program '
                    'may crash further down the pipeline! Error: %s',
                    e,
                )

    if not model:
        logger.error('No model files or URLs to load in intake! Exiting...')
        raise SystemExit(1)

    # Write filenames to CSV
    try:
        time_name = 'time'
        if prop.model_source == 'roms':
            time_name = 'ocean_time'
        # Format time step
        time_step = str(get_time_step(prop, logger)) + 'min'
        serieskey = model[[time_name, 'filename']].to_dataframe()
        full_date_range = pd.date_range(
            start=serieskey.index.min(), end=serieskey.index.max(), freq=time_step
        )
        serieskey = serieskey.reindex(full_date_range)
        # Write 'key' that lists all model files used to construct
        # time series
        logger.info('Writing model time series filename key!')
        filename = f'{prop.ofs}_{prop.whichcast}_filename_key.csv'
        filepath = Path(os.path.join(prop.data_model_1d_node_path, filename)).as_posix()
        serieskey.to_csv(filepath, index_label='DateTime')
    except KeyError:
        logger.error(
            'No filename variable found in the lazy loaded model '
            'dataset! Cannot write filename time series key. '
            'Moving on...'
        )
    except Exception as ex:
        logger.error('Error writing model time series filename ' 'key: %s', ex)

    # Check for time gaps caused by missing model output files.
    # If there is a gap, resample data to the correct time step and add nans
    # to fill gap
    isgap = find_time_gaps(prop, model, logger)
    if isgap:  # Resample if time gap
        logger.info('Preserving model time series gaps as nans...')
        # xarray.resample requires a monotonic index; sort defensively in
        # case upstream concat left the time axis out of order (forecast_b).
        time_name = 'ocean_time' if prop.model_source == 'roms' else 'time'
        time_vals = model[time_name].values
        if time_vals.size > 1 and not np.all(np.diff(time_vals) > np.timedelta64(0)):
            logger.warning('Time axis was non-monotonic before resample; sorting.')
            model = model.sortby(time_name)
        # Resample only the time-varying data vars. The default
        # Dataset.resample(...).asfreq() touches every variable in the
        # dataset, including static mesh vars (lon, lat, lonc, latc,
        # siglay, h) that intake's nested-combine replicated along the
        # time dim during multi-file concat. Materializing those across
        # 316 NECOFS files costs ~20 min per whichcast. Splitting the
        # dataset and resampling only data_vars that actually depend on
        # time avoids that cost; static vars are re-attached unchanged.
        model = _resample_time_vars_only(model, time_name, time_step, logger)
        logger.info('Resample complete on a %s time axis.', prop.model_source)

    logger.info(
        'Dispatching variable processing for: %s',
        list(prop.var_list),
    )

    prop.ctl_flag = 0  # Need flag to track control file production if
    # user_input_location == True

    def _extract_variable(variable, prop_local):
        """Process a single variable — extractable for parallel dispatch."""
        logger.info('[%s] thread started, reading obs ctl file', variable)
        try:
            name_conventions = name_convent(variable)
            if not prop_local.user_input_location:
                control_file = (
                    f'{prop_local.control_files_path}/{prop_local.ofs}_'
                    f'{name_conventions[0]}_station.ctl'
                )
                if os.path.isfile(control_file) is False:
                    logger.info(
                        '%s is not found. If not providing a custom XY '
                        'input file, then an observation control file '
                        'must be present! Exiting...',
                        control_file,
                    )
                    sys.exit()

                if os.path.isfile(control_file) and os.path.getsize(control_file) > 0: # Gets size of obs ctl file!
                    ofs_ctlfile = ofs_ctlfile_extract(
                        prop_local, name_conventions[0], model, logger
                    )
                    if ofs_ctlfile is None:
                        return
                else:
                    logger.info('%s obs ctl file is blank!', variable)
                    logger.info(
                        'For GLOFS, salt and cu ctl files may be blank. '
                        'If running with a single station provider/owner, '
                        'ctl files may also be blank.'
                    )
                    return
            else:
                ofs_ctlfile = ofs_ctlfile_extract(prop_local, name_conventions[0], model, logger)

            # Resume short-circuit: if every per-station .prd file for this
            # (var, whichcast, ofsfiletype) already exists on disk and the
            # row count matches the resampled model time axis, skip the
            # precompute + per-station write loop entirely. Critical for
            # crash-resume on contested hosts where a partial run leaves
            # WL/temp/etc. fully extracted but salt or cu missing — without
            # this, get_node_ofs's inner var loop re-extracts every variable
            # in prop.var_list on every restart. Row-count validation
            # additionally guards against a SIGKILL during the per-station
            # write loop leaving a single .prd truncated, which a naive
            # existence check would silently reuse.
            # We only short-circuit when not in user_input_location mode
            # (custom xy needs the ctl flag flow) and not in horizon-skill
            # mode (horizon output is per-cycle, separate accounting).
            if not prop_local.user_input_location and not getattr(
                prop_local, 'horizonskill', False
            ):
                n_stations = len(ofs_ctlfile[1])
                # Best-effort: derive the expected per-station row count
                # from the resampled model time axis. Fall back to None
                # (existence-only check) if anything unexpected pops up so
                # the resume path never fails hard on a missing time dim.
                try:
                    time_var = 'ocean_time' if prop_local.model_source == 'roms' else 'time'
                    expected_ts = int(model.sizes[time_var])
                except Exception as ex:  # pylint: disable=broad-except
                    logger.warning(
                        'Resume check: could not determine expected '
                        'timestep count (%s); falling back to '
                        'existence-only check.',
                        ex,
                    )
                    expected_ts = None

                if n_stations > 0 and _all_prd_files_complete(
                    prop_local, ofs_ctlfile, name_conventions[0], expected_ts, logger
                ):
                    logger.info(
                        '[%s] all %d .prd file(s) on disk look complete '
                        '— skipping precompute and per-station writes',
                        variable,
                        n_stations,
                    )
                    return

            # Batch-extract all station data. Applies to BOTH stations and
            # fields files: the win is that each backing chunk is read and
            # decompressed once for all N stations instead of once per
            # station. That matters most for SCHISM fields, where one HDF5
            # chunk is the entire spatial domain for a timestep (~600 MB),
            # so the per-station path re-inflates the whole domain N times.
            precomputed = None
            if prop_local.ofsfiletype in ('stations', 'fields'):
                try:
                    precomputed = _precompute_stations_data(
                        prop_local, model, ofs_ctlfile, name_conventions[-1], logger
                    )
                    if precomputed is None:
                        logger.info(
                            'No batch extraction layout for %s %s %s files; '
                            'using per-station extraction.',
                            prop_local.ofs,
                            prop_local.model_source,
                            prop_local.ofsfiletype,
                        )
                except Exception as ex:
                    logger.warning(
                        'Batch precomputation failed, falling back to '
                        'per-station extraction: %s',
                        ex,
                        exc_info=True,
                    )
                    precomputed = None

            def _process_single_station(
                i, ofs_ctlfile, prop_local, model, name_conventions, precomputed, variable, logger
            ):
                """Process a single station: format data and write .prd file.

                Returns (datum_offset, model_station) for water_level,
                or (None, None) for other variables.
                """
                datum_offset = None
                model_station = None

                if variable in ('salinity', 'water_temperature'):
                    formatted_series = format_temp_salt(
                        prop_local,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i,
                        precomputed=precomputed,
                    )
                elif variable == 'currents':
                    formatted_series = format_currents(
                        prop_local, model, ofs_ctlfile, i, precomputed=precomputed
                    )
                else:
                    formatted_series, datum_offset = format_waterlevel(
                        prop_local,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i,
                        logger,
                        precomputed=precomputed,
                    )
                    model_station = ofs_ctlfile[4][i]

                if (prop_local.whichcast == 'forecast_a' and
                    not prop_local.horizonskill):
                    prd_path_fa = (
                        r''
                        + f'{prop_local.data_model_1d_node_path}'
                          f'/{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_{name_conventions[0]}_'
                          f'{ofs_ctlfile[1][i]}_'
                          f'{prop_local.whichcast}_{prop_local.forecast_hr}_'
                          f'{prop_local.ofsfiletype}_model.prd'
                    )
                    with open(
                        prd_path_fa,
                        'w',
                        encoding='utf-8',
                    ) as output:
                        # Same header as the other .prd branch so the
                        # file format is not cast-dependent; omitted
                        # for empty series to keep blank files 0 bytes.
                        if len(formatted_series) > 0:
                            output.write(series_header(name_conventions[0]))
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_%s_model.prd created ' 'successfully',
                            prop_local.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop_local.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop_local.whichcast,
                            prop_local.forecast_hr,
                            prop_local.ofsfiletype,
                        )
                    cache_manifest.record_artifact(
                        prd_path_fa,
                        cache_manifest.run_signature(
                            prop_local, variable=variable,
                            extra={'whichcast': prop_local.whichcast,
                                   'forecast_hr': prop_local.forecast_hr}),
                        prop.data_model_1d_node_path,
                        logger)
                elif (prop_local.horizonskill and os.path.isfile(
                        f'{prop_local.data_model_1d_node_path}/'
                        f'{ofs_ctlfile[-1][i]}_{prop_local.ofs}_{name_conventions[0]}_'
                        f'{ofs_ctlfile[1][i]}_forecast_b_{prop_local.ofsfiletype}_'
                        f'model.prd'
                    )
                    is True
                ):
                    datecycle = (
                        prop_local.start_date_full.split('T')[0].replace('-', '')
                        + '-'
                        + prop_local.forecast_hr
                        + '-'
                        + 'forecast'
                    )
                    try:
                        df = do_horizon_skill_utils.pandas_processing(
                            name_conventions[0], datecycle, formatted_series
                        )
                    except Exception as e_x:
                        logger.error(
                            'Could not merge datecycle %s! Skipping.' ' Error: %s', datecycle, e_x
                        )
                        return (datum_offset, model_station)
                    filename = (
                        f'{prop_local.ofs}_{ofs_ctlfile[4][i]}_'
                        f'{name_conventions[0]}_fcst_horizons.csv'
                    )
                    # Stash this cycle in an in-memory accumulator keyed by the
                    # station CSV filename. All cycles are merged and written
                    # once at the end of the cycle loop by
                    # do_horizon_skill.make_horizon_series (via
                    # flush_horizon_series), which keeps the horizon workflow
                    # scaling linearly with the number of cycles instead of
                    # re-reading and re-merging the growing CSV on every cycle
                    # (previously O(cycles**2) on disk I/O).
                    try:
                        do_horizon_skill_utils.accumulate_cycle(prop_local, filename, df, datecycle)
                    except Exception as e_x:
                        logger.error(
                            'Could not accumulate forecast horizon '
                            'series for %s at station %s! Error: %s',
                            name_conventions[0],
                            ofs_ctlfile[4][i],
                            e_x,
                        )
                        return (datum_offset, model_station)
                else:
                    prd_path_std = (
                        r''
                        + f'{prop_local.data_model_1d_node_path}/'
                          f'{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_'
                          f'{name_conventions[0]}_{ofs_ctlfile[1][i]}'
                          f'_{prop_local.whichcast}_'
                          f'{prop_local.ofsfiletype}_model.prd'
                    )
                    with open(
                        prd_path_std,
                        'w',
                        encoding='utf-8',
                    ) as output:
                        # Header only when there is data: an empty .prd
                        # must stay 0 bytes so the getsize() blank-file
                        # checks downstream keep working.
                        if len(formatted_series) > 0:
                            output.write(series_header(name_conventions[0]))
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_model.prd created successfully',
                            prop_local.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop_local.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop_local.whichcast,
                            prop_local.ofsfiletype,
                        )
                    cache_manifest.record_artifact(
                        prd_path_std,
                        cache_manifest.run_signature(
                            prop_local, variable=variable,
                            extra={'whichcast': prop_local.whichcast}),
                        prop.data_model_1d_node_path,
                        logger)

                return (datum_offset, model_station)

            # Dispatch station processing — parallel or sequential
            parallel_cfg = get_parallel_config(
                logger,
                config_file=getattr(prop_local, 'config_file', None),
            )
            n_stations = len(ofs_ctlfile[1])
            datum_offsets = []
            model_stations = []

            if parallel_cfg.get('parallel_stations') and n_stations > 1 and precomputed is not None:
                logger.info('Processing %d stations in parallel for %s', n_stations, variable)
                with ThreadPoolExecutor(max_workers=min(n_stations, 8)) as executor:
                    futures = []
                    for i in range(n_stations):
                        prop_copy = copy.copy(prop_local)
                        futures.append(
                            executor.submit(
                                _process_single_station,
                                i,
                                ofs_ctlfile,
                                prop_copy,
                                model,
                                name_conventions,
                                precomputed,
                                variable,
                                logger,
                            )
                        )
                    for f in futures:
                        try:
                            datum_offset, model_station = f.result()
                            if datum_offset is not None:
                                datum_offsets.append(datum_offset)
                            if model_station is not None:
                                model_stations.append(model_station)
                        except Exception as ex:
                            logger.error('Station processing failed for %s: %s', variable, ex)
            else:
                logger.info(
                    'Processing %d stations sequentially for %s '
                    '(parallel_stations=%s, batch precompute=%s)',
                    n_stations,
                    variable,
                    bool(parallel_cfg.get('parallel_stations')),
                    'on' if precomputed is not None else 'off',
                )
                for i in range(n_stations):
                    datum_offset, model_station = _process_single_station(
                        i,
                        ofs_ctlfile,
                        prop_local,
                        model,
                        name_conventions,
                        precomputed,
                        variable,
                        logger,
                    )
                    if datum_offset is not None:
                        datum_offsets.append(datum_offset)
                    if model_station is not None:
                        model_stations.append(model_station)

            # Generate datum report
            if not prop_local.user_input_location:
                datum_filename = f'{prop_local.ofs}_wl_datum_report.csv'
                filepath = os.path.join(prop_local.control_files_path, datum_filename)
                # Check datum report age. Overwrite only if it's > 1 hour old
                try:
                    st = os.stat(filepath)
                    timediffhour = (
                        (datetime.now() - (datetime.fromtimestamp(st.st_mtime))).total_seconds()
                        / 60
                        / 60
                    )
                except FileNotFoundError:
                    timediffhour = 99
                if variable == 'water_level' and timediffhour > 1:
                    # Two code paths land here:
                    #   * First write on a fresh run — happy path. The
                    #     FileNotFoundError above sets timediffhour=99
                    #     as a sentinel; we log at INFO.
                    #   * Stale report (>1h but not the sentinel) — a
                    #     prior vdatum.convert run likely failed silently
                    #     and left the report behind. Warrants attention,
                    #     so log at WARNING.
                    if timediffhour >= 99:
                        logger.info('No datum report found, writing new one.')
                    else:
                        logger.warning(
                            'Stale datum report (%.1fh old, >1h), '
                            'rewriting — a prior vdatum run may have '
                            'failed silently.',
                            timediffhour,
                        )
                    datum_offsets = [model_stations, datum_offsets]
                    report_datums(prop_local, datum_offsets, logger)

        except FileNotFoundError:
            logger.warning(
                'No control file for %s was written because '
                'no model stations matched observation stations.',
                variable,
            )
        except Exception as ex:
            import traceback

            logger.error('Error happened when process %s - %s', variable, str(ex))
            logger.error('Full traceback:\n%s', traceback.format_exc())
        finally:
            logger.info('[%s] thread finished', variable)

    # Dispatch variable processing — parallel or sequential
    parallel_cfg = get_parallel_config(
        logger,
        config_file=getattr(prop, 'config_file', None),
    )
    # Concurrency cap for dask.compute inside _batch_extract, carried on
    # prop so the per-variable deepcopies inherit it (see _extract_guard).
    prop.extract_slots = parallel_cfg['parallel_extract_slots']
    if parallel_cfg['parallel_variables'] and len(prop.var_list) > 1:
        logger.info(
            "Variable threads will extract with engine '%s' "
            '(%s concurrent dask.compute slot(s) when h5netcdf)',
            getattr(prop, 'netcdf_engine', None) or 'unknown -> serialized',
            prop.extract_slots,
        )
        _preload_static_coords(model, prop.model_source, logger)
        logger.info('Processing %d variables in parallel', len(prop.var_list))
        with ThreadPoolExecutor(max_workers=min(len(prop.var_list), 4)) as executor:
            futures = []
            for variable in prop.var_list:
                prop_local = copy.deepcopy(prop)
                prop_local.var_list = [variable]
                # deepcopy would give each variable its own accumulator dict,
                # which flush_horizon_series (called on the outer prop by
                # make_horizon_series) would never see. Re-share the outer
                # prop's accumulator so all variables/cycles append into the
                # same store.
                if hasattr(prop, '_horizon_accumulator'):
                    prop_local._horizon_accumulator = prop._horizon_accumulator
                futures.append(executor.submit(_extract_variable, variable, prop_local))
            for f in futures:
                f.result()  # Raise any exceptions
    else:
        for variable in prop.var_list:
            _extract_variable(variable, prop)

    logger.info('Finished with model data processing!')
    return model
