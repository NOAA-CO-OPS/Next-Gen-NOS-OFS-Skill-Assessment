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

logger = logging.getLogger(__name__)

# Serializes dask.compute() inside _batch_extract across the variable
# threads dispatched from get_node_ofs. NECOFS uses engine='netcdf4'
# which isn't thread-safe under concurrent reads; in addition, four
# threads each materializing a different time-varying var concurrently
# spikes memory to 4× the per-var peak (~4-5 GB observed on real runs,
# triggering Windows paging at 94% RAM). The lock caps memory at one
# variable at a time without breaking the rest of the parallelism (the
# indexing phase, formatting, .prd writing, and plotting still
# parallelize across variable threads).
_BATCH_EXTRACT_LOCK = threading.Lock()


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
    'currents': (0.0, 10.0),       # speed magnitude
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
            ofs, station_id, kind, n_trans,
            float(bad.min()), float(bad.max()), n_pure,
        )
    elif n_pure:
        log.info(
            '%s station %s %s: %d pure-fill (-999) values masked.',
            ofs, station_id, kind, n_pure,
        )
    arr[physical] = np.nan
    return arr


def parse_arguments_to_list(argument, logger):
    '''
    takes a string from a user-supplied argument and parses it to a list
    of strings.
    '''
    try:
        argument = argument.lower().replace('[', '').replace(']','').\
            replace(' ','').split(',')
    except AttributeError: # If argument is not a string
        logger.info('Input argument (%s) being parsed from str to list is '
                     'already a list. Moving on...', argument)
        return argument
    try:
        argument[0]
        return argument
    except IndexError:
        logger.error('Cannot parse input argument %s! Correct formatting and '
                     'try again.', argument)
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
    '''
    Gets the model time step depending on OFS and file type (fields or stations)

    Parameters
    ----------
    prop : program input arguments
    logger : logger

    Returns
    -------
    Time step in integer minutes

    '''
    # Define your expected frequency in minutes (e.g. 6 minutes)
    exp_freq = 6
    if prop.ofsfiletype == 'fields':
        if prop.ofs in ['gomofs', 'wcofs', 'ngofs2']:
            exp_freq = 180
        else:
            exp_freq = 60
    return exp_freq

def find_time_gaps(prop, model, logger):
    '''
    Look for missing data/time gaps in the model time series.

    Parameters
    ----------
    prop : main input arguments
    model : lazily loaded concatenated model dataset
    logger : logger

    Returns
    -------
    True if gaps detected, False if no gaps detected.

    '''

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
    gaps = (time_deltas != exp_freq)

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
            prop.ctl_flag += 1 # Raise flag -- we've gone through ctl file production
    elif prop.ofsfiletype == 'stations':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl'
        if (os.path.isfile(filename)) is False and prop.ctl_flag == 0:
            write_ofs_ctlfile(prop, model, logger)
            prop.ctl_flag += 1 # Raise flag -- we've gone through ctl file production

    try:
        with open(
                filename, encoding='utf-8'
        ) as file:
            model_ctlfile = file.read()
            lines = model_ctlfile.split('\n')
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
        logger.warning('%s model ctl file is blank -- no '
                     'model nodes/stations found! Moving on...',
                     name_var)
        return None
    except FileNotFoundError:
        logger.warning('%s model ctl file is missing, probably because there '
                       'are no matches between obs and model stations! '
                       'Moving on...', name_var)
        return None
    except Exception as ex:
        logger.error('Unexpected error when processing %s model ctl '
                     'file! Error: %s',
                     name_var, ex)
        return None

def roms_nodes(model, node_num):
    """
    This function converts the node from the ofs control file
    into i and j for ROMS
    """
    i_index,j_index = np.unravel_index(int(node_num),np.shape(model['lon_rho']))
    return i_index,j_index


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
        'schism': ['lon', 'lat', 'station_name', 'zcoords', 'h'],
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
        len(loaded), loaded,
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
    time_vars = [name for name in model.data_vars
                 if time_name in model[name].dims]
    static_vars = [name for name in model.data_vars
                   if time_name not in model[name].dims]

    if not time_vars:
        logger.warning(
            'No data_vars carry the %s dim; resample is a no-op.', time_name,
        )
        return model

    resampled = model[time_vars].resample({time_name: time_step}).asfreq()

    # Re-attach static data vars (not modified by the resample) so
    # downstream code sees the same dataset shape it expected.
    for name in static_vars:
        resampled[name] = model[name]

    logger.info(
        'Resampled %d time-varying var(s); %d static var(s) re-attached.',
        len(time_vars), len(static_vars),
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


def _batch_extract(model, var_name, idx_list, dep_list, idx_first=False,
                   logger=None, time_chunk=_BATCH_EXTRACT_TIME_CHUNK):
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
    chunk = max(1, int(time_chunk))
    if n_time <= chunk:
        # Single-window fast path: behaviour matches the pre-chunking
        # implementation exactly when the dataset fits in one window.
        lazy = _select(probe_da, 0, n_time)
        with _BATCH_EXTRACT_LOCK:
            computed = dask.compute(*[s.data for s in lazy])
        return np.stack(computed, axis=1)

    n_chunks = (n_time + chunk - 1) // chunk
    if logger is not None:
        logger.info(
            'Batch extract %s: time axis %d steps split into %d chunks '
            'of up to %d steps each',
            var_name, n_time, n_chunks, chunk,
        )

    parts = []
    for ci in range(n_chunks):
        t0 = ci * chunk
        t1 = min(n_time, t0 + chunk)
        lazy = _select(probe_da, t0, t1)
        # Serialize the dask.compute across variable threads. NetCDF4
        # isn't thread-safe under concurrent reads and 4 simultaneous
        # computes would quadruple peak memory.
        with _BATCH_EXTRACT_LOCK:
            computed = dask.compute(*[s.data for s in lazy])
        parts.append(np.stack(computed, axis=1))
        if logger is not None:
            logger.info(
                'Batch extract %s chunk %d/%d done (timesteps %d:%d)',
                var_name, ci + 1, n_chunks, t0, t1,
            )
        # Drop references to this chunk's intermediate state before the
        # next compute builds its graph, so peak memory tracks one
        # chunk's worth instead of all of them.
        del lazy, computed
        gc.collect()

    return np.concatenate(parts, axis=0)


def _batch_extract_multi(model, var_names, idx_list, dep_list, idx_first=False,
                         logger=None, time_chunk=_BATCH_EXTRACT_TIME_CHUNK):
    """Extract multiple variables that share backing files in one fused compute.

    When two variables (e.g. ``u`` and ``v``) come from the same files,
    issuing one ``dask.compute`` for both lets Dask fuse the per-file
    reads — each chunk of each backing file is read once for both
    variables instead of once per variable. On the user's NECOFS run,
    sequential ``u`` then ``v`` took ~5h 02m for currents; the fused
    path is expected to drop that by roughly 30-40%.

    Same chunking and locking discipline as :func:`_batch_extract`: split
    the time axis into ``time_chunk``-step windows, compute each window
    under ``_BATCH_EXTRACT_LOCK``, ``gc.collect`` between, concatenate.

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

    chunk = max(1, int(time_chunk))
    if n_time <= chunk:
        # Single-window fast path: one fused compute over all vars + stations.
        per_var_lazy = [_select_one(model[v], 0, n_time) for v in var_names]
        flat = [s.data for lst in per_var_lazy for s in lst]
        with _BATCH_EXTRACT_LOCK:
            computed = dask.compute(*flat)
        results = []
        for vi, _ in enumerate(var_names):
            start = vi * n
            results.append(np.stack(computed[start:start + n], axis=1))
        return results

    n_chunks = (n_time + chunk - 1) // chunk
    if logger is not None:
        logger.info(
            'Batch extract %s: time axis %d steps split into %d chunks '
            'of up to %d steps each (fused %d-var compute)',
            '+'.join(var_names), n_time, n_chunks, chunk, len(var_names),
        )

    parts_per_var = [[] for _ in var_names]
    for ci in range(n_chunks):
        t0 = ci * chunk
        t1 = min(n_time, t0 + chunk)
        per_var_lazy = [_select_one(model[v], t0, t1) for v in var_names]
        flat = [s.data for lst in per_var_lazy for s in lst]
        with _BATCH_EXTRACT_LOCK:
            computed = dask.compute(*flat)
        for vi in range(len(var_names)):
            start = vi * n
            parts_per_var[vi].append(np.stack(computed[start:start + n], axis=1))
        if logger is not None:
            logger.info(
                'Batch extract %s chunk %d/%d done (timesteps %d:%d)',
                '+'.join(var_names), ci + 1, n_chunks, t0, t1,
            )
        del per_var_lazy, flat, computed
        gc.collect()

    return [np.concatenate(parts, axis=0) for parts in parts_per_var]


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

    if prop.model_source == 'fvcom':
        u_data, v_data = _batch_extract_multi(
            model, ['u', 'v'], indices, depths,
            idx_first=False, logger=logger)
    elif prop.model_source == 'roms':
        u_data, v_data = _batch_extract_multi(
            model, ['u_east', 'v_north'], indices, depths,
            idx_first=True, logger=logger)
    elif prop.model_source == 'schism':
        if 'stofs' not in prop.ofs:
            u_data, v_data = _batch_extract_multi(
                model, ['u', 'v'], indices, depths,
                idx_first=False, logger=logger)
        else:
            # STOFS-3D-Atl 2-D currents (no depth dim).
            u_data, v_data = _batch_extract_multi(
                model, ['u', 'v'], indices, None, logger=logger)

    return {'u_data': u_data, 'v_data': v_data}


def _precompute_scalar_data(prop, model, ofs_ctlfile, model_var, logger):
    """Batch-extract scalar (temp/salt/water level) station data.

    Returns a dict with 'scalar_data' numpy array.
    """
    n_stations = len(ofs_ctlfile[1])
    indices = [int(ofs_ctlfile[1][i]) for i in range(n_stations)]
    depths = [int(ofs_ctlfile[2][i]) for i in range(n_stations)]

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

    if prop.model_source == 'fvcom':
        if is_2d:
            scalar_data = _batch_extract(model, actual_var, indices, None,
                                         logger=logger)
        else:
            scalar_data = _batch_extract(model, actual_var, indices, depths,
                                         idx_first=False, logger=logger)
    elif prop.model_source == 'roms':
        if is_2d:
            scalar_data = _batch_extract(model, actual_var, indices, None,
                                         logger=logger)
        else:
            scalar_data = _batch_extract(model, actual_var, indices, depths,
                                         idx_first=True, logger=logger)
    elif prop.model_source == 'schism':
        if 'stofs' in prop.ofs and model_var in ('temp', 'temperature'):
            scalar_data = _batch_extract(model, 'temperature', indices, None,
                                         logger=logger)
        elif is_2d:
            scalar_data = _batch_extract(model, actual_var, indices, None,
                                         logger=logger)
        else:
            try:
                scalar_data = _batch_extract(model, actual_var, indices, depths,
                                             idx_first=False, logger=logger)
            except IndexError:
                scalar_data = _batch_extract(model, actual_var, indices, None,
                                             logger=logger)

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
        current_result = _precompute_current_data(
            prop, model, ofs_ctlfile, logger)
        result.update(current_result)
    else:
        # Scalar variables (temp, salt, water level)
        scalar_result = _precompute_scalar_data(
            prop, model, ofs_ctlfile, model_var, logger)
        result.update(scalar_result)

    logger.info('Pre-computed batch extraction for %d stations, var=%s',
                n_stations, model_var)
    return result


def format_temp_salt(prop, model, ofs_ctlfile, model_var, i, precomputed=None):
    """
    extract temperature and salinity time series from concatenated model data
    """

    if precomputed is not None and prop.ofsfiletype == 'stations':
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        if prop.model_source == 'schism':
            model_obs = _mask_schism_sentinels(
                model_obs, model_var, ofs_ctlfile[4][i], prop.ofs, logger)
    elif prop.model_source=='fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]),
                                 int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x siglay x station
            model_time = np.array(model['time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(
                model[model_var][:, int(ofs_ctlfile[2][i]),
                                 int(ofs_ctlfile[1][i])]
            )
            model_obs = model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    model_obs = None

    elif prop.model_source=='roms':
        if model_var=='salinity':
            model_var='salt'
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[2][i]),
                                                  i_index,j_index])
            model_obs = model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            model_time = np.array(model['ocean_time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var]
                                 [:, int(ofs_ctlfile[1][i]),
                                  int(ofs_ctlfile[2][i])])
            model_obs = model_obs #+ ofs_ctlfile[3][i]
    elif prop.model_source=='schism':
        if prop.ofsfiletype == 'fields':
            if model_var=='temp':
               model_var='temperature'
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i]),
                                                  int(ofs_ctlfile[2][i])])
            model_obs = model_obs
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            if 'stofs' in prop.ofs:
                model_var = 'temperature'
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            else:
                model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i]),
                                                      int(ofs_ctlfile[2][i])])
            model_obs = _mask_schism_sentinels(
                model_obs, model_var, ofs_ctlfile[4][i], prop.ofs, logger)
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have temp/sal data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Temperature and salinity data are not available for STOFS-2D-Global.')

    data_model = pd.DataFrame(
        {'DateTime': model_time,
         'OBS': model_obs}, columns=['DateTime', 'OBS']
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

    formatted_series = \
        scalar(data_model, start_date, end_date)
    return formatted_series


def format_currents(prop, model, ofs_ctlfile, i, precomputed=None):
    """
    extract current velocity time series from concatenated model data
    """

    if precomputed is not None and prop.ofsfiletype == 'stations':
        mfp = ModelFormatProperties()
        mfp.model_time = precomputed['model_time']
        u_i = precomputed['u_data'][:, i]
        v_i = precomputed['v_data'][:, i]
        if prop.model_source == 'schism':
            station_id = ofs_ctlfile[4][i]
            u_i = _mask_schism_sentinels(
                u_i, 'currents_uv', station_id, prop.ofs, logger)
            v_i = _mask_schism_sentinels(
                v_i, 'currents_uv', station_id, prop.ofs, logger)
        mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
        mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0
             for t in range(len(mfp.model_time))])
    elif prop.model_source=='fvcom':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(
                model['u'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )
            v_i = np.array(
                model['v'][:, int(ofs_ctlfile[2][i]), int(ofs_ctlfile[1][i])]
            )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
            len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]

        elif prop.ofsfiletype == 'stations':
            #if int(ofs_ctlfile[1][i]) > -999:
            mfp = ModelFormatProperties()
            mfp.model_time = np.array(model['time'])

            u_i = np.array(
                model['u'][:, int(ofs_ctlfile[2][i]),
                           int(ofs_ctlfile[1][i])]
            )
            v_i = np.array(
                model['v'][:, int(ofs_ctlfile[2][i]),
                           int(ofs_ctlfile[1][i])]
            )

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5

            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    mfp.model_obs = None
            #    mfp.model_ang = None

    elif prop.model_source=='roms':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['ocean_time'])
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[2][i]),
                                           i_index,j_index])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[2][i]),
                                            i_index,j_index])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x station x s_rho
            #if int(ofs_ctlfile[1][i]) > -999:
            u_i = np.array(model['u_east'][:, int(ofs_ctlfile[1][i]),
                                           int(ofs_ctlfile[2][i])])
            v_i = np.array(model['v_north'][:, int(ofs_ctlfile[1][i]),
                                            int(ofs_ctlfile[2][i])])

            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])

            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
            #else:
            #    mfp.model_obs = None
            #    mfp.model_ang = None
    elif prop.model_source=='schism':
        mfp = ModelFormatProperties()
        mfp.model_time = np.array(model['time'])
        if prop.ofsfiletype == 'fields':
            u_i = np.array(
                model['horizontalVelX'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
            )
            v_i = np.array(
                model['horizontalVelY'][:, int(ofs_ctlfile[1][i]), int(ofs_ctlfile[2][i])]
            )
            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
            [math.atan2(u_i[t], v_i[t]) / math.pi * 180 % 360.0 for t in range(
            len(np.array(mfp.model_time)))])
            mfp.model_obs = mfp.model_obs #+ ofs_ctlfile[3][i]
        elif prop.ofsfiletype == 'stations':
            if 'stofs' not in prop.ofs:
                u_i = np.array(
                    model['u'][:, int(ofs_ctlfile[2][i]),
                               int(ofs_ctlfile[1][i])]
                )
                v_i = np.array(
                    model['v'][:, int(ofs_ctlfile[2][i]),
                               int(ofs_ctlfile[1][i])]
                )
            else:
                u_i = np.array(
                    model['u'][:, int(ofs_ctlfile[1][i])]
                )
                v_i = np.array(
                    model['v'][:, int(ofs_ctlfile[1][i])]
                )

            station_id = ofs_ctlfile[4][i]
            u_i = _mask_schism_sentinels(
                u_i, 'currents_uv', station_id, prop.ofs, logger)
            v_i = _mask_schism_sentinels(
                v_i, 'currents_uv', station_id, prop.ofs, logger)
            mfp.model_obs = np.array(u_i**2 + v_i**2) ** 0.5
            mfp.model_ang = np.array(
                [math.atan2(u_i[t], v_i[t]) / math.pi * \
                 180 % 360.0 for t in range(
                    len(np.array(mfp.model_time)))])
    elif prop.model_source == 'adcirc':
        if prop.ofs == 'stofs_2d_glo':
            # We raise en exception here for STOFS-2D-Global because it does
            # not have current data, and logic elsewhere should steer users
            # away from calling this function with STOFS-2D-Global.
            raise ValueError('Current data are not available for STOFS-2D-Global.')

    mfp.data_model = pd.DataFrame(
        {'DateTime': mfp.model_time,
         'DIR': mfp.model_ang,
         'OBS': mfp.model_obs},
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
    formatted_series = \
        vector(mfp.data_model, start_date, end_date)

    return formatted_series


def format_waterlevel(prop, model, ofs_ctlfile, model_var,
                      i, logger, precomputed=None):
    """
    extract water level time series from concatenated model data
    """


    id_number = ofs_ctlfile[4][i]
    datum_offset = get_datum_offset_func(
        prop, int(ofs_ctlfile[1][i]), model, id_number, logger)
    logger.info(f'Datum offset for station {id_number} (node {ofs_ctlfile[1][i]}): {datum_offset}')

    if precomputed is not None and prop.ofsfiletype == 'stations':
        model_time = precomputed['model_time']
        model_obs = precomputed['scalar_data'][:, i].copy()
        if prop.model_source == 'schism':
            if datum_offset > -999 and datum_offset < 999:
                sign = 1 if 'stofs' in prop.ofs else -1
                model_obs = model_obs + sign * datum_offset
        else:
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
    elif prop.model_source=='fvcom':
        if prop.ofsfiletype == 'fields':
            model_time = np.array(model['time'])
            model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            model_time = np.array(model['time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:,
                                                  int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            #else:
            #    model_obs = None
    elif prop.model_source=='roms':
        if prop.ofsfiletype == 'fields':
            i_index,j_index = roms_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            model_obs = np.array(model[model_var][:, i_index,j_index])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
        elif prop.ofsfiletype == 'stations':
            # Dimensions: time x stations
            #i_index = roms_station_nodes(model, int(ofs_ctlfile[1][i]))
            model_time = np.array(model['ocean_time'])
            #if int(ofs_ctlfile[1][i]) > -999:
            model_obs = np.array(model[model_var][:,
                                                  int(ofs_ctlfile[1][i])])
            if datum_offset > -999:
                model_obs = model_obs - datum_offset
            #else:
            #    model_obs = None
    elif prop.model_source=='schism':
        if prop.ofsfiletype == 'fields':
            if model_var=='zeta':
               model_var='elevation' # Using out2d files
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
    elif prop.model_source =='adcirc':
        model_time = np.array(model['time'])
        model_obs = np.array(model[model_var][:, int(ofs_ctlfile[1][i])])
        if datum_offset > -999 and datum_offset < 999:
            model_obs = model_obs - datum_offset

    data_model = pd.DataFrame(
        {'DateTime': model_time,
         'OBS': model_obs}, columns=['DateTime', 'OBS']
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

    formatted_series = \
        scalar(data_model, start_date, end_date)

    return formatted_series, datum_offset


def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    # Start Date and End Date validation

    try:
        start_dt = datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(
            prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = f'Please check Start Date - ' \
                        f"'{prop.start_date_full}', End Date -" \
                        f" '{prop.end_date_full}'. Abort!"
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f'End Date {prop.end_date_full} ' \
                        f'is before Start Date ' \
                        f'{prop.start_date_full}. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params['home']

    # Path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = f'ofs_extents/ folder is not found. ' \
                        f'Please check Path - ' \
                        f"'{prop.path}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    # OFS validation
    shapefile = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shapefile):
        error_message = f"Shapefile '{prop.ofs}' " \
                        f'is not found at the folder' \
                        f' {ofs_extents_path}. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # Whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ['nowcast', 'forecast_a', 'forecast_b', 'hindcast']
    ):
        error_message = f'Please check Whichcast - ' \
                        f"'{prop.whichcast}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == 'forecast_a' and prop.forecast_hr is None:
        error_message = 'Forecast_Hr is required if ' \
                        'Whichcast is forecast_a. Abort!'
        logger.error(error_message)
        sys.exit(-1)

    # datum validation
    if prop.datum not in prop.datum_list:
        error_message = f'Datum {prop.datum} is not valid. Abort!'
        logger.error(error_message)
        sys.exit(-1)
    # GLOFS datum validation
    if (prop.datum.lower() not in ('igld85', 'lwd') and prop.ofs in
        ['leofs','loofs','lmhofs','lsofs']):
        error_message = f'Use only LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # Non-GLOFS datum validation
    if (prop.datum.lower() in ('igld85', 'lwd') and 'l' not in prop.ofs[0]):
        error_message = f'Do not use LWD or IGLD85 datums for {prop.ofs}!'
        logger.error(error_message)
        sys.exit()
    # File type validation
    if prop.ofsfiletype not in ['stations','fields']:
        logger.error('Uh-oh, please select a valid model output file type! '
                     'You chose %s. The options are "stations" or "fields".',
                     prop.ofsfiletype)
    if (prop.ofs == 'stofs_2d_glo') and (prop.ofsfiletype == 'fields'):
        logger.error('STOFS-2D-Global is only available as station output files. Please select "stations" for file type. Exiting...')
        raise NotImplementedError('STOFS-2D-Global fields file processing is not currently implemented.')
    # Warn if using custom lat/lon inputs and stations files
    if prop.ofsfiletype == 'stations' and prop.user_input_location:
        logger.warning('You are using custom lat/lon coordinates for model time '
                       'series extraction from stations files. All lats/lons '
                       'may not have a matching station output location. '
                       'To extract model time series for all lats/lons, try '
                       'using field files! Continuing...')
    # Check for user input file if using custom lat/lon inputs
    _conf = getattr(prop, 'config_file', None)
    filepath = (utils.Utils(_conf).read_config_section('user_xy_inputs', logger)
                ['user_xy_path'])
    if os.path.isfile(filepath) is False and prop.user_input_location:
        logger.error('No user lat & lon inputs found! Please make sure '
                     'the path to your input is correct in ofs_dps.conf, or '
                     'create a text file with the following columns separated '
                     ' by a space: '
                     '{location_name} '
                     '{latitude (decimal deg)} '
                     '{longitude (decimal deg)} '
                     '{water depth (m)}')
        sys.exit()
    # Handle variable input argument
    correct_var_list = ['water_level','water_temperature',
                        'salinity','currents']
    list_diff = list(set(prop.var_list) - set(correct_var_list))
    if len(list_diff) != 0:
        logger.error('Incorrect inputs to variable selection argument: %s. '
                     'Please use %s. Exiting...', list_diff,
                     correct_var_list)
        sys.exit()
    if prop.ofs == 'stofs_2d_glo':
        if set(prop.var_list) != {'water_level'}:
            logger.warning('"water_level" is the only available variable for STOFS-2D-Global: '
                           'Removing other variables from list.')
            prop.var_list = ['water_level']
            # I think we can alter its state here, but maybe there's a reason not to?


def _all_prd_files_complete(prop_local, ofs_ctlfile, name_var,
                            expected_timesteps, logger):
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
                row_count = sum(1 for _ in fh)
        except OSError as ex:
            logger.warning(
                'Could not read %s for row-count check (%s); treating '
                'as incomplete and re-extracting.', path, ex)
            return False
        # Tolerance is upward-only: a trailing blank line can produce
        # ``expected_timesteps + 1`` logical rows, but anything short of
        # ``expected_timesteps`` means the prior run was killed
        # mid-write and we must re-extract.
        if row_count < expected_timesteps:
            logger.warning(
                'Resume check: %s has %d rows but expected %d — '
                'previous run likely killed mid-write. Re-extracting.',
                path, row_count, expected_timesteps)
            return False
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
    prop.datum_list = (utils.Utils(_conf).read_config_section('datums', logger)\
                       ['datum_list']).split(' ')
    # Parse variable selection input to list
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)
    # Parameter validation
    parameter_validation(prop, dir_params, logger)

    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir']
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'])
    prop.data_model_1d_node_path = Path(prop.data_model_1d_node_path).as_posix()
    os.makedirs(prop.data_model_1d_node_path, exist_ok = True)

    # Path to save plotly maps
    prop.plotly_maps = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_maps'])
    os.makedirs(prop.plotly_maps, exist_ok=True)

    # Reformat start & end dates
    # Convert ISO format to internal format for processing
    # Use local variables to avoid modifying prop permanently
    start_date_internal = prop.start_date_full.replace('-', '').replace('Z', '').replace('T', '-')
    end_date_internal = prop.end_date_full.replace('-', '').replace('Z', '').replace('T', '-')

    try:
        prop.startdate = (datetime.strptime(
            start_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d') + '00'
        prop.enddate = (datetime.strptime(
            end_date_internal.split('-')[0], '%Y%m%d')).strftime(
            '%Y%m%d') + '23'
    except Exception as e:
        logger.error(f'Problem with date format in get_node_ofs: {e}')
        raise SystemExit(1)

    # Lazy load the model data (or reuse pre-loaded dataset)
    if model_dataset is not None:
        model = model_dataset
        logger.info('Using pre-loaded model dataset (skipping intake_model)')
    else:
        dir_list = list_of_dir(prop, logger)
        list_files = list_of_files_func(prop, dir_list, logger)
        logging.info('About to start intake_scisa from get_node ...')
        model = intake_model(list_files, prop, logger)
        logging.info('Lazily loaded dataset complete for %s!', prop.whichcast)

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
        full_date_range = pd.date_range(start=serieskey.index.min(),
                                        end=serieskey.index.max(), freq=time_step)
        serieskey = serieskey.reindex(full_date_range)
        # Write 'key' that lists all model files used to construct
        # time series
        logger.info('Writing model time series filename key!')
        filename = f'{prop.ofs}_{prop.whichcast}_filename_key.csv'
        filepath = Path(os.path.join(prop.data_model_1d_node_path,
                                     filename)).as_posix()
        serieskey.to_csv(filepath, index_label='DateTime')
    except KeyError:
        logger.error('No filename variable found in the lazy loaded model '
                     'dataset! Cannot write filename time series key. '
                     'Moving on...')
    except Exception as ex:
        logger.error('Error writing model time series filename '
                     'key: %s', ex)

    # Check for time gaps caused by missing model output files.
    # If there is a gap, resample data to the correct time step and add nans
    # to fill gap
    isgap = find_time_gaps(prop, model, logger)
    if isgap: # Resample if time gap
        logger.info('Preserving model time series gaps as nans...')
        # xarray.resample requires a monotonic index; sort defensively in
        # case upstream concat left the time axis out of order (forecast_b).
        time_name = 'ocean_time' if prop.model_source == 'roms' else 'time'
        time_vals = model[time_name].values
        if time_vals.size > 1 and not np.all(
                np.diff(time_vals) > np.timedelta64(0)):
            logger.warning(
                'Time axis was non-monotonic before resample; sorting.')
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
        logger.info('Resample complete on a %s time axis.',
                    prop.model_source)

    logger.info(
        'Dispatching variable processing for: %s',
        list(prop.var_list),
    )

    prop.ctl_flag = 0 #Need flag to track control file production if
                 #user_input_location == True

    def _extract_variable(variable, prop_local):
        """Process a single variable — extractable for parallel dispatch."""
        logger.info('[%s] thread started, reading obs ctl file', variable)
        try:
            name_conventions = name_convent(variable)
            if not prop_local.user_input_location:
                control_file = f'{prop_local.control_files_path}/{prop_local.ofs}_' \
                               f'{name_conventions[0]}_station.ctl'
                if os.path.isfile(control_file) is False:
                    logger.info('%s is not found. If not providing a custom XY '
                                'input file, then an observation control file '
                                'must be present! Exiting...', control_file)
                    sys.exit()
                if os.path.getsize(control_file): # Gets size of obs ctl file!
                    ofs_ctlfile = ofs_ctlfile_extract(
                        prop_local, name_conventions[0], model, logger)
                    if ofs_ctlfile is None:
                        return
                else:
                    logger.info('%s obs ctl file is blank!', variable)
                    logger.info('For GLOFS, salt and cu ctl files may be blank. '
                                'If running with a single station provider/owner, '
                                'ctl files may also be blank.')
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
            if (not prop_local.user_input_location
                    and not getattr(prop_local, 'horizonskill', False)):
                n_stations = len(ofs_ctlfile[1])
                # Best-effort: derive the expected per-station row count
                # from the resampled model time axis. Fall back to None
                # (existence-only check) if anything unexpected pops up so
                # the resume path never fails hard on a missing time dim.
                try:
                    time_var = ('ocean_time'
                                if prop_local.model_source == 'roms'
                                else 'time')
                    expected_ts = int(model.sizes[time_var])
                except Exception as ex:  # pylint: disable=broad-except
                    logger.warning(
                        'Resume check: could not determine expected '
                        'timestep count (%s); falling back to '
                        'existence-only check.', ex)
                    expected_ts = None

                if n_stations > 0 and _all_prd_files_complete(
                        prop_local, ofs_ctlfile, name_conventions[0],
                        expected_ts, logger):
                    logger.info(
                        '[%s] all %d .prd file(s) on disk look complete '
                        '— skipping precompute and per-station writes',
                        variable, n_stations,
                    )
                    return

            # Batch-extract all station data if using stations files
            precomputed = None
            if prop_local.ofsfiletype == 'stations':
                try:
                    precomputed = _precompute_stations_data(
                        prop_local, model, ofs_ctlfile,
                        name_conventions[-1], logger)
                except Exception as ex:
                    logger.warning(
                        'Batch precomputation failed, falling back to '
                        'per-station extraction: %s', ex)
                    precomputed = None

            def _process_single_station(i, ofs_ctlfile, prop_local, model,
                                        name_conventions, precomputed,
                                        variable, logger):
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
                    formatted_series = format_currents(prop_local, model,
                                                       ofs_ctlfile,
                                                       i,
                                                       precomputed=precomputed)
                else:
                    formatted_series, datum_offset = format_waterlevel(
                        prop_local,
                        model,
                        ofs_ctlfile,
                        name_conventions[-1],
                        i, logger,
                        precomputed=precomputed,
                    )
                    model_station = ofs_ctlfile[4][i]

                if (prop_local.whichcast == 'forecast_a' and
                    not prop_local.horizonskill):
                    with open(
                        r''
                        + f'{prop_local.data_model_1d_node_path}'
                          f'/{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_{name_conventions[0]}_'
                          f'{ofs_ctlfile[1][i]}_'
                          f'{prop_local.whichcast}_{prop_local.forecast_hr}_'
                          f'{prop_local.ofsfiletype}_model.prd',
                        'w',
                        encoding='utf-8',
                    ) as output:
                        for line in formatted_series:
                            output.write(str(line) + '\n')
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_%s_model.prd created '
                            'successfully',
                            prop_local.data_model_1d_node_path,
                            ofs_ctlfile[4][i],
                            prop_local.ofs,
                            name_conventions[0],
                            ofs_ctlfile[1][i],
                            prop_local.whichcast,
                            prop_local.forecast_hr,
                            prop_local.ofsfiletype
                        )
                elif (prop_local.horizonskill and os.path.isfile(
                        f'{prop_local.data_model_1d_node_path}/'
                        f'{ofs_ctlfile[-1][i]}_{prop_local.ofs}_{name_conventions[0]}_'
                        f'{ofs_ctlfile[1][i]}_forecast_b_{prop_local.ofsfiletype}_'
                        f'model.prd'
                    ) is True):
                    datecycle = prop_local.start_date_full.split('T')[0].replace('-', '') + \
                        '-' + prop_local.forecast_hr + '-' + 'forecast'
                    try:
                        df = do_horizon_skill_utils.pandas_processing(
                            name_conventions[0],datecycle,formatted_series)
                    except Exception as e_x:
                        logger.error('Could not merge datecycle %s! Skipping.'
                                     'Error: %s', e_x)
                        return (datum_offset, model_station)
                    filename = (f'{prop_local.ofs}_{ofs_ctlfile[4][i]}_'
                    f'{name_conventions[0]}_fcst_horizons.csv')
                    filepath = os.path.join(prop_local.data_horizon_1d_node_path,
                                 filename)
                    if os.path.isfile(filepath):
                        try:
                            df = do_horizon_skill_utils.pandas_merge(filepath, df,
                                                            datecycle,prop_local)
                        except Exception as e_x:
                            logger.error('Could not concat forecast horizon '
                                         'series in pandas for %s at station '
                                         '%s! Error: %s', name_conventions[0],
                                         ofs_ctlfile[4][i], e_x)
                            logger.error('No forecast horizons available!')
                            return (datum_offset, model_station)
                    # Save pandas dataframe with horizon time series
                    try:
                        df.to_csv(filepath, index=False)
                    except Exception as e_x:
                        logger.error("Couldn't save forecast horizons to csv!"
                                     'Error: %s', e_x)
                        return (datum_offset, model_station)
                else:
                    with open(
                        r''
                        +f'{prop_local.data_model_1d_node_path}/{ofs_ctlfile[4][i]}_'
                          f'{prop_local.ofs}_'
                          f'{name_conventions[0]}_{ofs_ctlfile[1][i]}'
                          f'_{prop_local.whichcast}_{prop_local.ofsfiletype}_model.prd',
                        'w',
                        encoding='utf-8',
                    ) as output:
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
                            prop_local.ofsfiletype
                        )

                return (datum_offset, model_station)

            # Dispatch station processing — parallel or sequential
            parallel_cfg = get_parallel_config(
                logger,
                config_file=getattr(prop_local, 'config_file', None),
            )
            n_stations = len(ofs_ctlfile[1])
            datum_offsets = []
            model_stations = []

            if (parallel_cfg.get('parallel_stations')
                    and n_stations > 1 and precomputed is not None):
                logger.info('Processing %d stations in parallel for %s',
                            n_stations, variable)
                with ThreadPoolExecutor(
                        max_workers=min(n_stations, 8)) as executor:
                    futures = []
                    for i in range(n_stations):
                        prop_copy = copy.copy(prop_local)
                        futures.append(executor.submit(
                            _process_single_station, i, ofs_ctlfile,
                            prop_copy, model, name_conventions,
                            precomputed, variable, logger))
                    for f in futures:
                        try:
                            datum_offset, model_station = f.result()
                            if datum_offset is not None:
                                datum_offsets.append(datum_offset)
                            if model_station is not None:
                                model_stations.append(model_station)
                        except Exception as ex:
                            logger.error(
                                'Station processing failed for %s: %s',
                                variable, ex)
            else:
                for i in range(n_stations):
                    datum_offset, model_station = _process_single_station(
                        i, ofs_ctlfile, prop_local, model,
                        name_conventions, precomputed, variable, logger)
                    if datum_offset is not None:
                        datum_offsets.append(datum_offset)
                    if model_station is not None:
                        model_stations.append(model_station)

            # Generate datum report
            if not prop_local.user_input_location:
                datum_filename = (f'{prop_local.ofs}_wl_datum_report.csv')
                filepath = os.path.join(prop_local.control_files_path,
                                        datum_filename)
                # Check datum report age. Overwrite only if it's > 1 hour old
                try:
                    st=os.stat(filepath)
                    timediffhour = (datetime.now() - (datetime.fromtimestamp(
                        st.st_mtime))).total_seconds()/60/60
                except FileNotFoundError:
                    timediffhour = 99
                if (variable == 'water_level' and timediffhour > 1):
                    # Two code paths land here:
                    #   * First write on a fresh run — happy path. The
                    #     FileNotFoundError above sets timediffhour=99
                    #     as a sentinel; we log at INFO.
                    #   * Stale report (>1h but not the sentinel) — a
                    #     prior vdatum.convert run likely failed silently
                    #     and left the report behind. Warrants attention,
                    #     so log at WARNING.
                    if timediffhour >= 99:
                        logger.info(
                            'No datum report found, writing new one.')
                    else:
                        logger.warning(
                            'Stale datum report (%.1fh old, >1h), '
                            'rewriting — a prior vdatum run may have '
                            'failed silently.', timediffhour)
                    datum_offsets = [model_stations, datum_offsets]
                    report_datums(prop_local, datum_offsets, logger)

        except FileNotFoundError:
            logger.warning('No control file for %s was written because '
                         'no model stations matched observation stations.',
                         variable)
        except Exception as ex:
            import traceback
            logger.error('Error happened when process %s - %s',
                         variable,
                         str(ex))
            logger.error('Full traceback:\n%s', traceback.format_exc())
        finally:
            logger.info('[%s] thread finished', variable)

    # Dispatch variable processing — parallel or sequential
    parallel_cfg = get_parallel_config(
        logger,
        config_file=getattr(prop, 'config_file', None),
    )
    if parallel_cfg['parallel_variables'] and len(prop.var_list) > 1:
        _preload_static_coords(model, prop.model_source, logger)
        logger.info('Processing %d variables in parallel', len(prop.var_list))
        with ThreadPoolExecutor(max_workers=min(len(prop.var_list), 4)) as executor:
            futures = []
            for variable in prop.var_list:
                prop_local = copy.deepcopy(prop)
                prop_local.var_list = [variable]
                futures.append(executor.submit(_extract_variable, variable, prop_local))
            for f in futures:
                f.result()  # Raise any exceptions
    else:
        for variable in prop.var_list:
            _extract_variable(variable, prop)

    logger.info('Finished with model data processing!')
    return model
