"""
Per-directory manifest of the run parameters cached artifacts were built for.

Most 1D pipeline artifacts (``*_station.ctl``, ``*_model.ctl``,
``inventory_all_{ofs}.csv``, ``*_station.obs``, ``*_model.prd``,
``*_pair.int``) are named with only ``{ofs}_{var}_...`` and carry no record
of the assessment window or station-selection options they were produced
for. A persistent working directory therefore silently reuses files from an
earlier run whose parameters differ, producing wrong results (issue: users
having to delete ``./data`` and ``./control_files`` between runs).

``timeseries_coverage`` already catches window drift for the *timestamped*
artifacts (``.obs``/``.prd``/``.int``), but the ``.ctl`` and inventory files
have no timestamps to inspect, so a stale station/node pairing is reused
verbatim and poisons everything downstream.

This module records, per artifact directory, the "run signature" each
cached file was built under and lets the reuse gates ask
``artifact_is_fresh(path, signature)``. A file whose recorded signature
differs from the current run's (or that has no record at all -- e.g. a
pre-upgrade file) is treated as stale and regenerated once, after which its
signature is stored.

Design choices:
- **One index per directory** (``.ofs_cache_manifest.json``), not a sidecar
  per file, to keep the artifact directories tidy.
- **Auto-detect and regenerate by default**; no flag required.
- Reads/writes are guarded by a process lock and tolerate a missing or
  corrupt index (fails open into "stale", forcing a safe regeneration).
"""

import contextlib
import json
import os
import threading

from ofs_skill.utils.timeseries_coverage import (
    is_within_directory,
    remove_stale_artifact,
)

# Index filename dropped into each artifact directory. Leading dot keeps it
# out of the way of the pattern-based output globs (``*.obs`` etc.).
MANIFEST_FILENAME = '.ofs_cache_manifest.json'

# Bump when the signature schema changes so old indexes invalidate safely.
MANIFEST_SCHEMA_VERSION = 1

# One lock guards all index read/modify/write cycles. The indexes are tiny
# and written rarely (once per artifact (re)generation), so a single global
# lock is simpler than per-path locks and avoids interleaved writes when the
# variable/station fan-outs touch the same directory.
_manifest_lock = threading.RLock()

# Process-wide tally of artifacts declared stale (parameters changed) during
# the current run, so an entry point can emit a single end-of-stage summary
# instead of one line per file. Keyed by a short artifact-kind label.
_stale_counter: dict = {}
_stale_counter_lock = threading.Lock()


def reset_stale_counter() -> None:
    """Zero the stale-artifact tally at the start of a run/stage."""
    with _stale_counter_lock:
        _stale_counter.clear()


def note_stale(kind: str) -> None:
    """Increment the stale tally for an artifact kind (e.g. ``'ctl'``)."""
    with _stale_counter_lock:
        _stale_counter[kind] = _stale_counter.get(kind, 0) + 1


def emit_stale_summary(ofs, logger) -> None:
    """Log one summary line if any stale artifacts were regenerated.

    Silent when nothing was stale, so an unchanged same-window rerun stays
    quiet. Does not reset the tally -- call :func:`reset_stale_counter` at the
    start of the next run.
    """
    with _stale_counter_lock:
        if not _stale_counter:
            return
        parts = ', '.join(
            f'{count} {kind}' for kind, count
            in sorted(_stale_counter.items()))
        total = sum(_stale_counter.values())
    if logger is not None:
        logger.info(
            'Regenerated %d stale cached artifact(s) for %s due to changed '
            'run parameters (window/options): %s. Delete the working dirs '
            'to force a full rebuild.', total, ofs, parts)


def _normalize(value):
    """Canonicalize a signature value for order-insensitive comparison.

    Lists/tuples/sets (and comma- or space-separated strings) of station
    owners or variables must compare equal regardless of order or case, so
    they are lowercased and sorted. Plain scalars are stringified so
    ``'MLLW'`` and ``MLLW`` compare consistently across the JSON round-trip.
    """
    if value is None:
        return None
    if isinstance(value, list | tuple | set):
        return sorted(str(v).strip().lower() for v in value if str(v).strip())
    text = str(value).strip()
    # A multi-token owner/variable string (e.g. ``'co-ops,ndbc'`` or
    # ``'co-ops ndbc'``) must not depend on token order. A single token
    # (a datum, date, ofs) is returned as-is.
    tokens = [t for t in text.replace(',', ' ').split() if t]
    if len(tokens) > 1:
        return sorted(t.lower() for t in tokens)
    return text

def _normalize_date(value):
    """Canonicalize date strings to ISO format for consistent signature matching.

    Tolerates the YYYYMMDD-HH:MM:SS format leaked by check_model_files.
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if len(text) == 17 and text[8] == '-':
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}T{text[9:]}Z'
    return text


def file_fingerprint(path: str | None) -> list | None:
    """Return ``[path, mtime_ns, size]`` for a file, or None.

    Used to fold an external input file (e.g. the currents-bins override
    CSV) into a run signature: editing that file changes the fingerprint and
    invalidates artifacts built from the old contents. Returns None when the
    path is falsy or unreadable, which compares equal to "no such input".
    """
    if not path:
        return None
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return [os.path.abspath(path), stat.st_mtime_ns, stat.st_size]


def run_signature(prop, *, variable=None, extra=None) -> dict:
    """Build the parameter set that determines a cached artifact's validity.

    Any field that changes the *contents* an artifact should have belongs
    here; a mismatch on any field marks the cached file stale. ``variable``
    scopes per-variable artifacts (obs/model/paired); ``extra`` lets a caller
    add gate-specific keys (e.g. ``whichcast`` for ``.prd``/``.int``).
    """
    sig = {
        'ofs': _normalize(getattr(prop, 'ofs', None)),
        'start_date_full': _normalize_date(getattr(prop, 'start_date_full', None)),
        'end_date_full': _normalize_date(getattr(prop, 'end_date_full', None)),
        'ofsfiletype': _normalize(getattr(prop, 'ofsfiletype', None)),
        'datum': _normalize(getattr(prop, 'datum', None)),
        'stationowner': _normalize(getattr(prop, 'stationowner', None)),
        'currents_bins_csv': file_fingerprint(
            getattr(prop, 'currents_bins_csv', None)),
        'user_input_location': _normalize(getattr(prop, 'user_input_location', False)),
    }

    # Fold in the custom XY file fingerprint if the user override is active
    user_loc = getattr(prop, 'user_input_location', False)
    if str(user_loc).strip().lower() in ('true', '1', 'yes', 't'):
        try:
            import configparser

            from ofs_skill.obs_retrieval.utils import Utils

            _conf = getattr(prop, 'config_file', None)
            if not _conf:
                _conf = Utils().get_config_file()

            parser = configparser.ConfigParser()
            # parser.read safely ignores missing files without crashing
            if _conf and os.path.isfile(_conf):
                parser.read(_conf)

            if parser.has_option('user_xy_inputs', 'user_xy_path'):
                xy_path = parser.get('user_xy_inputs', 'user_xy_path')
                sig['user_xy_path'] = file_fingerprint(xy_path)
            else:
                sig['user_xy_path'] = None
        except Exception:
            # Degrade gracefully if the config section/file is malformed or missing
            sig['user_xy_path'] = None

    if variable is not None:
        sig['variable'] = _normalize(variable)
    if extra:
        for key, value in extra.items():
            sig[key] = file_fingerprint(value) if key.endswith('_file') \
                else _normalize(value)
    return sig

def inventory_signature(ofs, start_date, end_date, stationowner,
                        currents_bins_csv=None) -> dict:
    """Signature for the shared station inventory CSV.

    The inventory is built before a ``prop`` exists, so it cannot go through
    :func:`run_signature`. Keeping its shape here rather than inline at the
    call site means the normalization stays consistent, and callers (tests
    included) do not have to reach into the private ``_normalize`` helpers
    to construct a matching stamp.
    """
    return {
        'ofs': _normalize(ofs),
        'start_date': _normalize_date(start_date),
        'end_date': _normalize_date(end_date),
        'stationowner': _normalize(stationowner),
        'currents_bins_csv': file_fingerprint(currents_bins_csv),
    }


def _index_path(directory: str) -> str:
    return os.path.join(directory, MANIFEST_FILENAME)


def _load_index(directory: str) -> dict:
    """Read the directory's manifest index, or an empty index.

    Fails open: a missing, unreadable, or wrong-version index returns an
    empty ``entries`` map so every artifact reads as "stale" and regenerates
    safely rather than trusting a possibly-corrupt record.
    """
    path = _index_path(directory)
    try:
        with open(path, encoding='utf-8') as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return {'version': MANIFEST_SCHEMA_VERSION, 'entries': {}}
    if not isinstance(data, dict) \
            or data.get('version') != MANIFEST_SCHEMA_VERSION \
            or not isinstance(data.get('entries'), dict):
        return {'version': MANIFEST_SCHEMA_VERSION, 'entries': {}}
    return data


def _write_index(directory: str, index: dict, logger=None) -> None:
    # Write atomically: dump to a temp file in the same directory, then
    # os.replace() over the real index. A crash mid-write can only leave the
    # (discarded) temp file behind, never a half-written index -- so a killed
    # run does not corrupt the manifest and force an unnecessary full-dir
    # regeneration. os.replace is atomic on the same filesystem on both
    # POSIX and Windows.
    path = _index_path(directory)
    tmp_path = f'{path}.{os.getpid()}.tmp'
    try:
        os.makedirs(directory, exist_ok=True)
        with open(tmp_path, 'w', encoding='utf-8') as handle:
            json.dump(index, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except OSError as exc:  # pragma: no cover - defensive
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        if logger is not None:
            logger.warning(
                'Could not write cache manifest %s: %s. Cached-artifact '
                'staleness detection is degraded for this directory.',
                path, exc)


def artifact_is_fresh(artifact_path: str, signature: dict) -> bool:
    """True when the artifact exists and its recorded signature matches.

    Returns False when the file is absent, has no manifest entry (e.g. a
    pre-upgrade file, or one deleted-and-rebuilt outside this module), or was
    recorded under a different run signature. False means "regenerate".
    """
    if not os.path.isfile(artifact_path):
        return False
    directory = os.path.dirname(artifact_path) or '.'
    key = os.path.basename(artifact_path)
    with _manifest_lock:
        index = _load_index(directory)
    recorded = index.get('entries', {}).get(key)
    return recorded == signature


def record_artifact(artifact_path: str, signature: dict, base_dir: str, logger=None) -> None:
    """Store ``signature`` for a freshly (re)generated artifact.

    Called right after a gate writes the artifact so a later same-parameter
    run reuses it. Keyed by basename within the file's own directory index.
    """
    if not is_within_directory(artifact_path, base_dir):
        if logger is not None:
            logger.error(
                'Security error: Artifact path %s escapes base directory %s',
                artifact_path, base_dir
            )
        return

    directory = os.path.dirname(artifact_path) or '.'
    key = os.path.basename(artifact_path)
    with _manifest_lock:
        index = _load_index(directory)
        index.setdefault('entries', {})[key] = signature
        index['version'] = MANIFEST_SCHEMA_VERSION
        _write_index(directory, index, logger)


def forget_artifact(artifact_path: str, base_dir: str, logger=None) -> None:
    """Drop an artifact's manifest entry (e.g. after deleting the file)."""
    if not is_within_directory(artifact_path, base_dir):
        return

    directory = os.path.dirname(artifact_path) or '.'
    key = os.path.basename(artifact_path)
    with _manifest_lock:
        index = _load_index(directory)
        entries = index.get('entries', {})
        if key in entries:
            del entries[key]
            _write_index(directory, index, logger)


def ensure_fresh(artifact_path, signature, base_dir, kind, logger=None):
    """Reuse gate helper: True if the artifact is reusable for this run.

    Combines the manifest check with stale cleanup:

    * Missing file -> returns False (caller regenerates); nothing to delete.
    * Present and signature matches -> returns True (reuse verbatim).
    * Present but signature differs -> deletes the file (bounded to
      ``base_dir``), drops its manifest entry, tallies it for the run
      summary, and returns False so the caller regenerates.
    """
    if not os.path.isfile(artifact_path):
        return False
    if artifact_is_fresh(artifact_path, signature):
        return True
    if logger is not None:
        logger.warning(
            '%s was built for different run parameters than the current '
            'run and is likely left over from an earlier run. Deleting it '
            'so it is regenerated.', artifact_path)
    if remove_stale_artifact(artifact_path, base_dir, logger):
        forget_artifact(artifact_path, base_dir, logger)
        note_stale(kind)
    return False
