"""
Model File Validation

Pre-open validation of model netCDF files so that incomplete, corrupt,
or structurally inconsistent files are dropped with a warning instead
of crashing the multi-file open or the extraction that follows
(issue #194: multi-month runs failing on files left behind by a
forecast interruption).

Each local file is opened individually and checked for:

1. **Readability** — the file opens under the engine chosen for the
   batch. Zero-byte files, garbage, and truncated HDF5 files fail here.
2. **A usable time axis** — the concat dimension exists and has at
   least one step. Header-only files written during an interrupted
   forecast fail here.
3. **A strictly increasing time axis** — the full time coordinate is
   loaded and must be strictly increasing. This is the truncation
   detector for NetCDF-3: libnetCDF silently zero-fills reads past
   EOF (no error!), so a file truncated by an interrupted forecast
   opens cleanly and returns zeros for the missing records — including
   time values of 0, which later produce duplicate/non-monotonic
   timestamps that crash resampling (and quietly poison the data
   before that). Since time is a record variable, any truncation deep
   enough to lose data corrupts the time tail, so this check catches
   it. Truncated HDF5 files fail at open instead (check 1).
4. **Data variables present** — a file with coordinates but no data
   variables is dropped.

After the per-file checks, dimension sizes are compared across the
surviving files: a file whose non-time dimensions disagree with the
majority of the batch (e.g. a different number of sigma layers or mesh
nodes) is dropped, since the multi-file combine would otherwise fail.
The ``station`` dimension of stations files is exempt — differing
station counts are legitimate (grid updates) and are handled downstream
by ``remove_extra_stations``, which slices files to the common set.

Remote URLs are passed through unvalidated: per-file remote opens are
expensive, and NODD-served files are the canonical copies. The
h5netcdf open-time fallback in ``intake_model`` still protects that
path.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from collections import Counter
from logging import Logger
from typing import Any

import netCDF4
import numpy as np

_REMOTE_PREFIXES = ('http://', 'https://', 's3://')

# Progress heartbeat interval (files) for the serial validation loop.
_PROGRESS_EVERY = 50

# Static variables larger than this are excluded from the
# static-metadata fingerprint to keep the per-file probe cheap.
# Stations-file static vars (x/y/lon/lat/h/sigma/names) are a few
# hundred to a few thousand elements.
_FINGERPRINT_MAX_ELEMENTS = 200_000


def _is_remote(path: Any) -> bool:
    if not isinstance(path, str):
        return False
    # Unwrap fsspec protocol chaining (simplecache::https://...)
    tail = path.split('::')[-1] if '::' in path else path
    return tail.startswith(_REMOTE_PREFIXES)


# Characters allowed in the cache subdirectory derived from a URL's
# parent directory; anything else is replaced so a hostile or malformed
# URL can never traverse outside the cache root.
_UNSAFE_DIR_CHARS = re.compile(r'[^A-Za-z0-9._-]')


def cached_path_for_url(url: Any, cache_dir: str) -> str:
    """Local cache path for one remote model-file URL.

    The cached copy keeps the URL's basename but is nested under the
    URL's parent directory name::

        .../stofs_2d_glo.20260701/stofs_2d_glo.t00z.points.cwl.nc
        -> <cache_dir>/stofs_2d_glo.20260701/stofs_2d_glo.t00z.points.cwl.nc

    STOFS points filenames are identical in every date directory of the
    NODD bucket, so a cache keyed by basename alone silently serves one
    date's file for every other date — an old cached copy poisons every
    later run, and even a clean multi-day run reads day 1's file for
    days 2..N (issue #267). The parent directory carries the date and
    disambiguates; non-STOFS filenames embed the date anyway and merely
    gain a subdirectory level. Keeping the basename unchanged preserves
    everything downstream that reads filenames (the ``filename``
    coordinate, format sniffing, the filename-key CSV).

    A parent segment that is empty or sanitizes to ``.``/``..`` falls
    back to a flat ``<cache_dir>/<basename>`` path.
    """
    path = str(url).split('::')[-1].split('?')[0].rstrip('/')
    parts = path.split('/')
    basename = parts[-1]
    parent = _UNSAFE_DIR_CHARS.sub('_', parts[-2]) if len(parts) > 1 else ''
    if parent in ('', '.', '..'):
        return os.path.join(cache_dir, basename)
    return os.path.join(cache_dir, parent, basename)


def _static_fingerprint(nc: netCDF4.Dataset,
                        time_name: str | None) -> str:
    """Hash the values of the file's small static (non-time) variables.

    Multi-file concat with ``data_vars='minimal'`` requires static
    variables (station x/y/lon/lat, names, sigma levels, ...) to be
    IDENTICAL across every file in the batch; a model configuration
    change mid-window (e.g. relocated stations after an outage) makes
    xarray raise ``MergeError: conflicting values for variable 'x'``.
    Hashing these values per file lets the batch-consistency step spot
    the change and drop the minority deployment with a clear warning
    instead of crashing the combine. Variables above the size cap are
    skipped to keep the probe cheap.
    """
    md5 = hashlib.md5()
    for name in sorted(nc.variables):
        var = nc.variables[name]
        if time_name and time_name in var.dimensions:
            continue
        if var.size > _FINGERPRINT_MAX_ELEMENTS:
            continue
        try:
            var.set_auto_maskandscale(False)
            payload = np.asarray(var[:]).tobytes()
        except Exception:  # noqa: BLE001 - unreadable static var:
            continue       # other checks decide the file's fate
        md5.update(name.encode())
        md5.update(payload)
    return md5.hexdigest()


def _check_file(
    path: str,
    time_name: str | None,
    fingerprint: bool = False,
) -> tuple[str | None, dict, str | None]:
    """Validate one local file.

    Returns ``(reason, dims, static_fp)`` — ``reason`` is None when the
    file is valid, otherwise a short description of why it was
    rejected; ``dims`` is the file's dimension-size mapping (empty on
    failure); ``static_fp`` is the static-variable fingerprint when
    ``fingerprint`` is True (None otherwise or on failure).

    The probe uses ``netCDF4.Dataset`` directly rather than a full
    ``xr.open_dataset``: libnetCDF reads every on-disk format, and a
    bare header open plus one coordinate read is several times cheaper
    than building an xarray Dataset — this loop runs once per model
    file and dominated start-up on slow archive storage.
    """
    try:
        nc = netCDF4.Dataset(path, 'r')
    except Exception as ex:  # noqa: BLE001 - any open failure = bad file
        return (f'unreadable ({type(ex).__name__}: '
                f'{str(ex)[:120]})'), {}, None
    try:
        dims = {name: len(dim) for name, dim in nc.dimensions.items()}
        if time_name:
            if time_name not in dims:
                return (f"missing '{time_name}' dimension "
                        '(incomplete file?)'), {}, None
            if dims[time_name] < 1:
                return f"empty '{time_name}' dimension", {}, None
        if len(nc.variables) == 0:
            return 'no variables (header-only file?)', {}, None
        if time_name and time_name in nc.variables:
            try:
                raw = nc.variables[time_name][:]
                tvals = np.ma.filled(
                    np.ma.masked_invalid(
                        np.asarray(raw, dtype='float64')), np.nan)
            except Exception as ex:  # noqa: BLE001
                return (f'time coordinate unreadable — truncated file? '
                        f'({type(ex).__name__}: {str(ex)[:120]})'), {}, None
            # Model time axes are strictly increasing within a file.
            # libnetCDF silently zero-fills NetCDF-3 reads past EOF, so
            # a truncated file reads back with a zeroed time tail —
            # detectable here, invisible to the open itself. (NaN /
            # masked time values fail the check too, deliberately.)
            if tvals.size > 1:
                diffs = np.diff(tvals)
                if not np.all(diffs > 0):
                    bad = int(np.argmin(diffs > 0)) + 1
                    return ('time axis not strictly increasing at step '
                            f'{bad} of {tvals.size} — zero-filled or '
                            'truncated file?'), {}, None
        static_fp = (_static_fingerprint(nc, time_name)
                     if fingerprint else None)
        return None, dims, static_fp
    finally:
        nc.close()


def _majority_dims(per_file_dims: list[dict], exempt: set) -> dict:
    """Most common size for every non-exempt dimension in the batch."""
    counts: dict[str, Counter] = {}
    for dims in per_file_dims:
        for name, size in dims.items():
            if name in exempt:
                continue
            counts.setdefault(name, Counter())[size] += 1
    return {name: counter.most_common(1)[0][0]
            for name, counter in counts.items()}


def scrub_cached_copies(
    urlpaths: list,
    cache_dir: str,
    time_name: str | None,
    logger: Logger,
) -> int:
    """
    Delete partial or corrupt cached copies of remote model files.

    A run killed during the model-loading phase can leave a partially
    downloaded file in the fsspec cache (``~/.ofs_cache/s3/``). On the
    next run the cache is trusted as-is and the partial file surfaces
    as an unrelated-looking crash — ``ValueError: buffer size must be
    a multiple of element size`` or ``MergeError: conflicting values
    for variable 'x'`` (a truncated NetCDF-3 file zero-fills its
    static coordinates; see issues #176 / #193).

    For every remote URL whose cached copy already exists, run the same
    integrity probe used for archive files and delete copies that fail
    — unlike archive files, a cached file is safely re-downloadable, so
    removal is the right remedy rather than dropping the file from the
    run.

    Parameters
    ----------
    urlpaths : list of str
        Remote URLs (http(s)/s3) about to be opened through the cache.
        Non-remote entries are ignored.
    cache_dir : str
        The cache directory root; each URL maps to the date-keyed path
        given by ``cached_path_for_url``.
    time_name : str or None
        Name of the time dimension, for the integrity probe.
    logger : Logger
        Logger instance.

    Returns
    -------
    int
        Number of bad cached copies removed.
    """
    removed = 0
    for url in urlpaths:
        if not (isinstance(url, str) and _is_remote(url)):
            continue
        cached = cached_path_for_url(url, cache_dir)
        if not os.path.isfile(cached):
            continue
        reason, _, _ = _check_file(cached, time_name)
        if reason is None:
            continue
        try:
            os.remove(cached)
            removed += 1
            logger.warning(
                'Removed bad cached copy %s (%s) — it will be '
                're-downloaded. A previous run was likely interrupted '
                'while downloading it.', cached, reason)
        except OSError as ex:
            logger.warning(
                'Cached copy %s failed validation (%s) but could not '
                'be removed (%s) — the model open may fail; clear the '
                'cache directory manually.', cached, reason, ex)
    if removed:
        logger.info(
            'Cache scrub removed %d partial/corrupt cached file(s) '
            'from %s', removed, cache_dir)
    return removed


def validate_model_files(
    file_list: list,
    engine: str,
    time_name: str | None,
    ofsfiletype: str,
    logger: Logger,
) -> tuple[list, list[tuple[str, str]], dict[str, dict]]:
    """
    Drop unreadable, incomplete, or structurally inconsistent files.

    Parameters
    ----------
    file_list : list of str
        Model file paths and/or remote URLs about to be opened together.
    engine : str
        xarray engine chosen for the batch (see netcdf_engine.py).
        Currently informational — the probes read headers via netCDF4,
        which handles every on-disk format.
    time_name : str or None
        Name of the concat/time dimension ('time', 'ocean_time', ...).
    ofsfiletype : str
        'stations' or 'fields'. For stations files the station
        dimension is exempt from the consistency check (handled by
        remove_extra_stations downstream).
    logger : Logger
        Logger instance; one WARNING per dropped file plus a summary.

    Returns
    -------
    tuple of (list, list, dict)
        ``(valid_files, dropped, dims_by_path)`` — ``dropped`` holds
        ``(path, reason)`` pairs; ``dims_by_path`` maps each surviving
        LOCAL path to its dimension-size dict, so callers can reuse
        the probed dimensions (e.g. the station-dimension compatibility
        check) instead of re-opening every file. Remote URLs are always
        passed through as valid and do not appear in ``dims_by_path``.

    Raises
    ------
    SystemExit
        If the batch contained files but none survived validation.
    """
    local = [f for f in file_list if isinstance(f, str)
             and not _is_remote(f)]
    if not local:
        return list(file_list), [], {}

    # This pass can take a while on long runs (hundreds of files on
    # slow archive storage), so announce it and report progress —
    # otherwise the run appears hung between obs retrieval and the
    # catalog open.
    start = time.perf_counter()
    logger.info(
        'Validating %d local model file(s) before open '
        '(lightweight header probes) ...', len(local))

    # The probes run serially on purpose: concurrent netCDF4.Dataset
    # open/close across threads races in libnetCDF's global file table
    # ("NetCDF: Not a valid ID"). Serial is fine — the probe is a bare
    # header open plus one coordinate read, so per-file cost is
    # milliseconds locally and I/O-latency-bound on network storage.
    # Static-metadata fingerprinting is only meaningful for stations
    # files, where the batch is concatenated with data_vars='minimal'
    # and static vars must agree exactly.
    fingerprint = ofsfiletype == 'stations'
    checks = []
    for i, path in enumerate(local, 1):
        checks.append(_check_file(path, time_name, fingerprint))
        if i % _PROGRESS_EVERY == 0 and i < len(local):
            logger.info('Validated %d/%d model files (%.0f s elapsed)',
                        i, len(local), time.perf_counter() - start)
    logger.info('Model file validation finished: %d file(s) in %.1f s',
                len(local), time.perf_counter() - start)

    dropped: list[tuple[str, str]] = []
    surviving: dict[str, dict] = {}
    fp_by_path: dict[str, str] = {}
    for path, (reason, dims, static_fp) in zip(local, checks):
        if reason is None:
            surviving[path] = dims
            if static_fp is not None:
                fp_by_path[path] = static_fp
        else:
            dropped.append((path, reason))

    # Batch-level consistency: drop files whose non-time dims disagree
    # with the batch majority (a minority file would crash the combine).
    exempt = {time_name} if time_name else set()
    if ofsfiletype == 'stations':
        exempt.add('station')
    if len(surviving) > 1:
        majority = _majority_dims(list(surviving.values()), exempt)
        for path, dims in list(surviving.items()):
            bad = {name: (size, majority[name])
                   for name, size in dims.items()
                   if name not in exempt and size != majority[name]}
            if bad:
                detail = ', '.join(
                    f'{name}={size} (batch majority {want})'
                    for name, (size, want) in sorted(bad.items()))
                dropped.append(
                    (path, f'dimension mismatch with batch: {detail}'))
                del surviving[path]
                fp_by_path.pop(path, None)

    # Static-metadata consistency: with identical dimensions, files can
    # still disagree on static VALUES (station x/y/lon/lat, names, sigma
    # levels) after a model configuration change mid-window — xarray's
    # minimal-mode concat then dies with "MergeError: conflicting values
    # for variable 'x'". Drop the minority deployment, but only when the
    # batch has a clear majority and every file has the same station
    # count (differing counts are the legitimate remove_extra_stations
    # case, where coordinate arrays trivially differ).
    station_sizes = {dims.get('station') for dims in surviving.values()}
    if (fingerprint and len(fp_by_path) > 1
            and len(station_sizes) == 1):
        fp_counts = Counter(fp_by_path.values())
        top_fp, top_n = fp_counts.most_common(1)[0]
        if len(fp_counts) > 1 and top_n * 2 >= len(fp_by_path):
            for path, fp in sorted(fp_by_path.items()):
                if fp != top_fp:
                    dropped.append((path, (
                        'static station metadata (coordinates/names/'
                        'sigma levels) differs from the batch majority '
                        '— the model configuration likely changed '
                        'mid-window; consider splitting the assessment '
                        'window at the change date')))
                    del surviving[path]
        elif len(fp_counts) > 1:
            logger.warning(
                'Static station metadata varies across %d groups of '
                'files with no clear majority — the multi-file combine '
                'may fail with a MergeError. The assessment window '
                'likely spans multiple model configurations.',
                len(fp_counts))

    if not dropped:
        return list(file_list), [], dict(surviving)

    for path, reason in dropped:
        logger.warning('Skipping model file %s: %s', path, reason)
    logger.warning(
        'Dropped %d of %d model file(s) that failed validation; '
        'continuing with the remaining %d. Skill results will not '
        'include data from the dropped files.',
        len(dropped), len(file_list), len(file_list) - len(dropped))

    dropped_set = {path for path, _ in dropped}
    valid = [f for f in file_list
             if not (isinstance(f, str) and f in dropped_set)]
    if not valid:
        logger.error(
            'All %d model files failed validation — cannot continue. '
            'See the warnings above for per-file reasons.',
            len(file_list))
        raise SystemExit
    return valid, dropped, dict(surviving)
