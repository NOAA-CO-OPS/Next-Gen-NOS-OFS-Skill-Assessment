"""
Helpers to check whether cached time-series artifacts cover the run window.

The 1D pipeline caches its intermediate artifacts (``*_station.obs``,
``*_model.prd``, ``*_pair.int``) under filenames that do not encode the
assessment start/end dates. A persistent working directory can therefore
serve files left over from an earlier run window. Downstream, the plotting
step crops every series to the current window
(``combine_obs_across_casts``), so a stale artifact renders as a one-point
plot when the two windows are adjacent (daily operational runs share a
boundary timestamp) or as a blank plot when they are disjoint. Rolling
windows (e.g. a 7-day assessment re-run daily) are subtler: a stale file
overlaps most of the new window but is missing exactly the newest data.

These helpers let the existence checks in ``get_skill``, ``get_node_ofs``,
and ``create_1dplot`` detect stale artifacts and trigger regeneration
instead. ``remove_stale_artifact`` is the shared deletion path: it refuses
targets that resolve outside the artifact directory and tolerates a
concurrent run having already deleted the file.
"""

import contextlib
import os
from datetime import UTC, datetime, timedelta

# How far a cached artifact's data may stop short of the reachable run
# window before the file is declared stale. Sized to absorb the nowcast
# cycle spacing (up to 6 h between cycles) plus NODD/CO-OPS publication
# lag, while still catching daily-cadence staleness (a file from
# yesterday's window ends ~24 h short). Data gaps wider than this at the
# very start or end of the window will regenerate on every run; gaps in
# the middle of the window are never penalized.
STALENESS_TOLERANCE = timedelta(hours=12)


def _parse_date(raw):
    """Parse one pipeline date string in either accepted format."""
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y%m%d-%H:%M:%S'):
        try:
            return datetime.strptime(raw, fmt)
        except (TypeError, ValueError):
            continue
    return None


def parse_run_window(prop, logger=None):
    """Parse ``prop.start_date_full``/``prop.end_date_full`` to datetimes.

    Each date is parsed independently, so a run that has rewritten one of
    them to the compact format mid-pipeline still yields a window. Returns
    a ``(start, end)`` tuple, or ``None`` if either date cannot be parsed
    (in which case callers should skip staleness checks).
    """
    start_raw = getattr(prop, 'start_date_full', None)
    end_raw = getattr(prop, 'end_date_full', None)
    start_dt = _parse_date(start_raw)
    end_dt = _parse_date(end_raw)
    if start_dt is not None and end_dt is not None:
        return start_dt, end_dt
    if logger is not None:
        logger.warning(
            'Could not parse run window from start=%s end=%s; '
            'skipping cached-file staleness checks.', start_raw, end_raw)
    return None


def _row_datetime(line):
    """Timestamp of one whitespace-separated data row, or None.

    ``.obs``, ``.prd``, and ``*_pair.int`` files all carry
    ``year month day hour minute`` in columns 1-5 (0-based) after the
    julian-date column. Header rows and malformed lines return None.
    """
    fields = line.split()
    if len(fields) < 6:
        return None
    try:
        return datetime(int(float(fields[1])), int(float(fields[2])),
                        int(float(fields[3])), int(float(fields[4])),
                        int(float(fields[5])))
    except (ValueError, OverflowError):
        return None


def read_first_last_timestamps(path):
    """Return (first, last) data-row timestamps of a cached artifact.

    Returns ``(None, None)`` when the file cannot be read or decoded, or
    contains no parseable data rows.
    """
    first = None
    last = None
    try:
        with open(path, encoding='utf-8') as file_handle:
            for line in file_handle:
                stamp = _row_datetime(line)
                if stamp is None:
                    continue
                if first is None:
                    first = stamp
                last = stamp
    except (OSError, UnicodeDecodeError):
        return None, None
    return first, last


def _has_no_data_rows(path):
    """True when the file opens but holds no parseable data rows.

    Distinguishes an artifact with no usable data — empty, header-only,
    or corrupt content, all safe and useful to regenerate — from one
    that cannot even be opened (an OS-level failure, where deletion and
    regeneration would likely fail too). Decoding errors are tolerated
    per byte so a corrupt binary file still counts as "no data rows".
    """
    try:
        with open(path, encoding='utf-8', errors='replace') as file_handle:
            return all(_row_datetime(line) is None for line in file_handle)
    except OSError:
        return False


def covers_run_window(path, start_dt, end_dt, *, logger=None, now=None):
    """True unless the file's data demonstrably stops short of the window.

    A cached artifact is stale when its first timestamp begins more than
    ``STALENESS_TOLERANCE`` after the window start, or its last timestamp
    ends more than that before ``min(end_dt, now)``. Clamping the end to
    *now* means windows extending into the future (forecast runs) never
    penalize a fresh file for data that cannot exist yet, and comparing
    against the window ends rather than an overlap fraction catches
    rolling-window reuse (a file from yesterday's overlapping window
    always ends ~24 h short) without deleting files that merely have
    mid-window gaps.

    A file that opens but contains no parseable data rows — empty,
    header-only, or corrupt — is stale (returns False): it carries zero
    usable data, so regenerating it can only help. Empty ``.prd`` files
    written during a failed model extraction were previously reused
    forever here, silently producing zero pairs on every subsequent run
    (issue #267).

    Fails open (returns True) when the file cannot even be opened (an
    OS-level error, where regeneration would likely fail too) and when
    the reachable part of the window is shorter than the tolerance (too
    little data can exist to judge either way).
    """
    first, last = read_first_last_timestamps(path)
    if first is None or last is None:
        if _has_no_data_rows(path):
            if logger is not None:
                logger.warning(
                    '%s contains no parseable data rows; treating it as '
                    'stale so it is regenerated.', path)
            return False
        if logger is not None:
            logger.warning(
                'Could not read timestamps from %s; '
                'skipping staleness check for this file.', path)
        return True
    if now is None:
        now = datetime.now(UTC).replace(tzinfo=None)
    effective_end = min(end_dt, now)
    if effective_end - start_dt <= STALENESS_TOLERANCE:
        return True
    if first > start_dt + STALENESS_TOLERANCE:
        return False
    if last < effective_end - STALENESS_TOLERANCE:
        return False
    return True


def is_within_directory(path, directory):
    """True if ``path`` resolves to a location inside ``directory``.

    Cached-artifact paths embed station IDs that originate from external
    metadata services; a deletion must never follow such an ID outside
    the artifact directory.
    """
    try:
        base = os.path.realpath(directory)
        target = os.path.realpath(path)
        return os.path.commonpath([base, target]) == base
    except (OSError, ValueError):
        return False


def remove_stale_artifact(path, base_dir, logger=None):
    """Delete a stale cached artifact; return True if the file is gone.

    Refuses to delete a path that resolves outside ``base_dir`` (returns
    False so callers do not treat the file as regenerable). A file already
    deleted by a concurrent run is not an error.
    """
    if not is_within_directory(path, base_dir):
        if logger is not None:
            logger.warning(
                'Refusing to delete %s: it resolves outside the artifact '
                'directory %s.', path, base_dir)
        return False
    with contextlib.suppress(FileNotFoundError):
        os.remove(path)
    return True
