"""
NetCDF Engine Detection

Chooses the xarray engine for reading model files by detecting each
file's on-disk format from its magic bytes instead of relying on a
hardcoded per-OFS mapping. NetCDF-4/HDF5 files are readable by
``h5netcdf``, which releases the GIL during reads and supports
concurrent access from multiple threads; NetCDF-3 files (classic,
64-bit offset, or CDF-5) require ``netcdf4`` or ``scipy``, whose
underlying C libraries hold process-global state and must be
serialized across threads.

Formats can differ per OFS, per file type, and per source archive —
e.g. stofs_2d_glo publishes NetCDF-3 stations files next to HDF5
fields files in the same cycle, and NECOFS is NetCDF-3 across the
board — so detection is done per batch of files at open time rather
than from a static list.

The strategy is configurable via ``engine_strategy`` in the
``[settings]`` section of ofs_dps.conf:

- ``auto`` (default): sniff the batch; use h5netcdf only when every
  sampled file is HDF5, otherwise fall back to a NetCDF-3-capable
  engine.
- ``netcdf4`` / ``h5netcdf``: force that engine for every open
  (debugging and emergency rollback).
"""

from __future__ import annotations

import logging
import threading
import urllib.request
from collections.abc import Iterable
from typing import Any

from ofs_skill.obs_retrieval import utils

FORMAT_HDF5 = 'hdf5'
FORMAT_NETCDF3 = 'netcdf3'
FORMAT_UNKNOWN = 'unknown'

_HDF5_MAGIC = b'\x89HDF\r\n\x1a\n'
_NETCDF3_MAGICS = (b'CDF\x01', b'CDF\x02', b'CDF\x05')

# The HDF5 superblock is usually at offset 0, but the format also allows
# it at 512 << n. Only local files are probed at the extra offsets; for
# remote files a miss at offset 0 is classified as unknown (which maps
# to the safe netcdf4-family engine).
_HDF5_LOCAL_OFFSETS = (0, 512, 1024, 2048, 4096)

# Custom user-supplied NetCDF-3 files that historically require the
# scipy engine, identified by filename marker (see intake_scisa).
NETCDF3_SCIPY_MARKERS = ('2017_free_run_station',)

# Cap on per-batch sniffs. Batches are homogeneous in practice (one OFS,
# one file type, one date range), so sampling the batch evenly is enough;
# a wrong pick on a pathological mixed batch is caught by the engine
# fallback in intake_scisa.
_MAX_SNIFFS_PER_BATCH = 12

_SNIFF_CACHE: dict[str, str] = {}
_SNIFF_CACHE_LOCK = threading.Lock()

VALID_STRATEGIES = ('auto', 'netcdf4', 'h5netcdf')


def _classify(header: bytes) -> str:
    if header.startswith(_HDF5_MAGIC):
        return FORMAT_HDF5
    if header.startswith(_NETCDF3_MAGICS):
        return FORMAT_NETCDF3
    return FORMAT_UNKNOWN


def _strip_protocol_chain(path: str) -> str:
    """Strip fsspec protocol chaining, e.g. simplecache::https://..."""
    if '::' in path:
        return path.split('::')[-1]
    return path


def _sniff_remote(url: str, logger: logging.Logger) -> str:
    """Classify a remote file from a ranged GET of its first 8 bytes."""
    if url.startswith('s3://'):
        # Public NODD buckets are reachable anonymously over HTTPS.
        bucket, _, key = url[len('s3://'):].partition('/')
        url = f'https://{bucket}.s3.amazonaws.com/{key}'
    request = urllib.request.Request(url, headers={'Range': 'bytes=0-7'})
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return _classify(resp.read(8))
    except Exception as ex:
        logger.debug('Format sniff failed for %s: %s', url, ex)
        return FORMAT_UNKNOWN


def _sniff_local(path: str, logger: logging.Logger) -> str:
    try:
        with open(path, 'rb') as fp:
            fmt = _classify(fp.read(8))
            if fmt != FORMAT_UNKNOWN:
                return fmt
            for offset in _HDF5_LOCAL_OFFSETS[1:]:
                fp.seek(offset)
                if fp.read(8) == _HDF5_MAGIC:
                    return FORMAT_HDF5
            return FORMAT_UNKNOWN
    except OSError as ex:
        logger.debug('Format sniff failed for %s: %s', path, ex)
        return FORMAT_UNKNOWN


def sniff_netcdf_format(path: Any, logger: logging.Logger | None = None) -> str:
    """
    Detect the on-disk NetCDF format of a local path or remote URL.

    Parameters
    ----------
    path : str
        Local file path, or http(s):// / s3:// URL. fsspec protocol
        chains (``simplecache::https://...``) are unwrapped.
    logger : logging.Logger, optional
        Logger for debug output on sniff failures.

    Returns
    -------
    str
        ``'hdf5'``, ``'netcdf3'``, or ``'unknown'``. Sniff failures
        (unreadable file, network error, unrecognized magic) return
        ``'unknown'`` so callers fall back to the netcdf4-family
        engine, which reads every format.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    if not isinstance(path, str):
        return FORMAT_UNKNOWN
    path = _strip_protocol_chain(path)
    with _SNIFF_CACHE_LOCK:
        cached = _SNIFF_CACHE.get(path)
    if cached is not None:
        return cached
    if path.startswith(('http://', 'https://', 's3://')):
        fmt = _sniff_remote(path, logger)
    else:
        fmt = _sniff_local(path, logger)
    # Don't cache failures: a transient network error shouldn't pin a
    # file to 'unknown' for the rest of the run.
    if fmt != FORMAT_UNKNOWN:
        with _SNIFF_CACHE_LOCK:
            _SNIFF_CACHE[path] = fmt
    return fmt


def _sample_paths(file_list: list) -> list:
    """Evenly sample up to _MAX_SNIFFS_PER_BATCH paths from the batch."""
    paths = [f for f in file_list if isinstance(f, str)]
    if len(paths) <= _MAX_SNIFFS_PER_BATCH:
        return paths
    step = (len(paths) - 1) / (_MAX_SNIFFS_PER_BATCH - 1)
    indices = {round(i * step) for i in range(_MAX_SNIFFS_PER_BATCH)}
    return [paths[i] for i in sorted(indices)]


def get_engine_strategy(
    config_file: Any = None,
    logger: logging.Logger | None = None,
) -> str:
    """
    Read ``engine_strategy`` from the [settings] config section.

    Returns ``'auto'`` when the setting is missing or invalid.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    try:
        settings = utils.Utils(config_file=config_file).read_config_section(
            'settings', logger)
    except Exception:
        settings = {}
    strategy = (settings or {}).get('engine_strategy', 'auto').strip().lower()
    if strategy not in VALID_STRATEGIES:
        logger.warning(
            "Invalid engine_strategy '%s' in [settings]; expected one of "
            '%s. Using auto.', strategy, ', '.join(VALID_STRATEGIES))
        strategy = 'auto'
    return strategy


def _netcdf3_engine(ofs: str, file_list: Iterable) -> str:
    """NetCDF-3-capable engine, preserving historical special cases."""
    if ofs == 'stofs_2d_glo':
        return 'scipy'
    if any(marker in f
           for f in file_list if isinstance(f, str)
           for marker in NETCDF3_SCIPY_MARKERS):
        return 'scipy'
    return 'netcdf4'


def resolve_engine(file_list: list, prop: Any, logger: logging.Logger) -> str:
    """
    Choose the xarray engine for a batch of model files.

    With ``engine_strategy=auto`` (default), sniffs the batch's magic
    bytes: every sampled file HDF5 → ``h5netcdf``; anything else →
    ``scipy`` for the historical NetCDF-3 special cases (stofs_2d_glo,
    custom free-run station files) or ``netcdf4`` otherwise. Forced
    strategies skip detection entirely.

    Parameters
    ----------
    file_list : list of str
        Local paths and/or remote URLs about to be opened together.
    prop : ModelProperties
        Provides ``ofs`` and optionally ``config_file``.
    logger : logging.Logger
        Logger instance.

    Returns
    -------
    str
        ``'h5netcdf'``, ``'netcdf4'``, or ``'scipy'``.
    """
    strategy = get_engine_strategy(
        getattr(prop, 'config_file', None), logger)
    if strategy != 'auto':
        logger.info('engine_strategy=%s forced from config', strategy)
        return strategy

    samples = _sample_paths(file_list)
    if not samples:
        return 'netcdf4'
    formats = {sniff_netcdf_format(p, logger) for p in samples}
    if formats == {FORMAT_HDF5}:
        engine = 'h5netcdf'
    else:
        engine = _netcdf3_engine(getattr(prop, 'ofs', ''), file_list)
    logger.info(
        'Detected netCDF format(s) %s across %d sampled file(s) of %d; '
        "using engine '%s'",
        sorted(formats), len(samples), len(file_list), engine)
    return engine
