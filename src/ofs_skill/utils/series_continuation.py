"""
Append-and-dedup machinery for continuation runs (issue #211).

A continuation run (``--Continue_Run``) re-uses the head of an existing
``.obs``/``.prd`` series and only fetches or extracts the missing tail.
This module owns the merge: given the rows already on disk and the rows a
tail fetch just produced, it returns the combined series -- deduplicated
at the seam, sorted, and written back atomically.

The merge is deliberately **text-level**. Both writers emit fixed-width,
self-contained rows (``format_obs_timeseries.format_scalar`` /
``format_vector``, and the model formatters in ``get_node_ofs``), whose
content does not depend on the run window. Merging the formatted lines
rather than re-deriving them from dataframes means an extended file is
byte-for-byte what a from-scratch run would have written, with no
float-formatting drift and no re-rounding of the julian column.

Rows are keyed on the ``year month day hour minute`` columns rather than
the julian column: julian is written at 8 decimals today but files
produced before #200 used 4, and those would never compare equal to a
freshly computed value for the same instant. The minute-resolution key is
the file format's own resolution, so nothing is lost that the file could
have represented.
"""

from __future__ import annotations

import contextlib
import os
import stat
import tempfile

from ofs_skill.utils.file_headers import SERIES_HEADER_PREFIX
from ofs_skill.utils.timeseries_coverage import row_datetime

# Hours of already-retrieved data a continuation run re-fetches before
# the seam. CO-OPS and USGS publish preliminary values in near real time
# and revise them later, so the tail of a previous run is exactly the
# part most likely to have changed; re-fetching a day of it lets the
# revised values replace what is on disk. It also gives the seam check
# real overlap to measure instead of a single boundary sample.
DEFAULT_CONTINUE_OVERLAP_HOURS = 24.0

# How far either side of the seam the gap check looks, and how wide a
# step it tolerates there. Six hours matches the nowcast cycle spacing
# that STALENESS_TOLERANCE is already sized against; anything wider at
# the join means the tail did not actually meet the head, and the
# artifact is regenerated instead.
SEAM_CHECK_WINDOW_HOURS = 12.0
MAX_SEAM_GAP_HOURS = 6.0


def _read_umask_once():
    """Read the process umask, restoring it immediately.

    Called once at import, while the process is still single-threaded.
    ``os.umask`` has no read-only form, so querying it later would set
    the process-wide umask to 0 for an instant -- and any file another
    worker thread created in that window would come out world-writable.
    """
    mask = os.umask(0)
    os.umask(mask)
    return mask


# Mode for a replacement file whose predecessor is gone, matching what a
# plain open(path, 'w') elsewhere in the pipeline would produce.
_DEFAULT_FILE_MODE = 0o666 & ~_read_umask_once()


def read_series_file(path):
    """Split a ``.obs``/``.prd`` file into ``(header, data_lines)``.

    ``header`` is the descriptive first line including its newline, or
    ``None`` for the headerless files written before the header was
    introduced. ``data_lines`` are the remaining lines with trailing
    newlines stripped; blank and unparseable lines are preserved in place
    so a caller that decides not to merge can rewrite the file verbatim.

    Returns ``(None, None)`` when the file cannot be read or decoded.
    """
    try:
        with open(path, encoding='utf-8') as file_handle:
            lines = file_handle.read().splitlines()
    except (OSError, UnicodeDecodeError):
        return None, None
    header = None
    if lines and lines[0].startswith(SERIES_HEADER_PREFIX):
        header = lines[0] + '\n'
        lines = lines[1:]
    return header, lines


def _keyed_rows(lines):
    """Map each parseable row to its timestamp, preserving file order.

    Returns ``(mapping, n_unparseable)``, or ``(None, _)`` when two rows
    share a timestamp -- a file whose rows are not uniquely keyed cannot
    be merged without guessing which one to keep.
    """
    keyed = {}
    unparseable = 0
    for line in lines:
        stamp = row_datetime(line)
        if stamp is None:
            if line.strip():
                unparseable += 1
            continue
        if stamp in keyed:
            return None, unparseable
        keyed[stamp] = line
    return keyed, unparseable


def merge_series_lines(existing_lines, new_lines):
    """Combine an existing series with freshly produced tail rows.

    Returns ``(merged_lines, stats)``, where ``merged_lines`` is sorted
    by timestamp and ``stats`` records what happened
    (``kept``/``replaced``/``added``/``unparseable``/``first``/``last``).
    Rows from ``new_lines`` win on a timestamp collision: the overlap is
    re-fetched precisely so newer provider QC and re-extracted model
    values replace what is on disk.

    Returns ``None`` when either side has duplicate timestamps, so the
    caller can fall back to regenerating the artifact outright rather
    than silently dropping a row.
    """
    existing_keyed, existing_bad = _keyed_rows(existing_lines)
    if existing_keyed is None:
        return None
    new_keyed, new_bad = _keyed_rows(new_lines)
    if new_keyed is None:
        return None

    replaced = sum(1 for stamp in new_keyed if stamp in existing_keyed)
    merged_keyed = dict(existing_keyed)
    merged_keyed.update(new_keyed)

    stamps = sorted(merged_keyed)
    stats = {
        'kept': len(existing_keyed),
        'replaced': replaced,
        'added': len(new_keyed) - replaced,
        'unparseable': existing_bad + new_bad,
        'total': len(stamps),
        'first': stamps[0] if stamps else None,
        'last': stamps[-1] if stamps else None,
    }
    return [merged_keyed[stamp] for stamp in stamps], stats


def max_step_seconds(lines, around=None, window=None):
    """Largest gap, in seconds, between consecutive rows.

    With ``around`` (a timestamp) and ``window`` (a
    :class:`~datetime.timedelta`), only steps whose endpoints fall within
    ``window`` of ``around`` are considered -- that is the seam check:
    a continuation must not leave a hole where the head meets the tail.
    Returns ``0.0`` when there are fewer than two rows in range.
    """
    stamps = [stamp for stamp in (row_datetime(line) for line in lines)
              if stamp is not None]
    largest = 0.0
    for previous, current in zip(stamps, stamps[1:]):
        if around is not None and window is not None:
            if (abs(current - around) > window
                    and abs(previous - around) > window):
                continue
        largest = max(largest, (current - previous).total_seconds())
    return largest


def write_series_file(path, header, lines):
    """Atomically write a ``.obs``/``.prd`` file.

    Writes a temporary file in the destination directory and
    :func:`os.replace`\\ s it into place, so a crash mid-write can never
    leave a truncated artifact behind -- which matters here because the
    staleness check deliberately fails *open* on an unreadable file and
    would otherwise reuse the wreckage on every subsequent run.

    An empty ``lines`` writes a 0-byte file with no header, preserving
    the blank-file contract the ``getsize() > 0`` checks depend on.
    """
    directory = os.path.dirname(path) or '.'
    try:
        existing_mode = stat.S_IMODE(os.stat(path).st_mode)
    except OSError:
        existing_mode = None
    # Leading dot so a temp file left behind by a hard kill is hidden and
    # cannot be mistaken for a data file by the directory scans.
    handle = tempfile.NamedTemporaryFile(
        mode='w', encoding='utf-8', dir=directory,
        prefix='.' + os.path.basename(path) + '.', suffix='.part',
        delete=False)
    try:
        with handle as output:
            if lines and header:
                output.write(header)
            for line in lines:
                output.write(str(line) + '\n')
        # NamedTemporaryFile creates 0600 regardless of umask, and
        # os.replace carries the temp file's mode to the destination --
        # so without this the extended artifact becomes unreadable to
        # everyone but its owner, unlike every other file the pipeline
        # writes.
        os.chmod(handle.name,
                 existing_mode if existing_mode is not None
                 else _DEFAULT_FILE_MODE)
        os.replace(handle.name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(handle.name)
        raise


def merge_and_write(path, new_lines, header, logger=None,
                    max_seam_gap_seconds=None, seam_window=None):
    """Merge ``new_lines`` into the series at ``path`` and rewrite it.

    Returns ``True`` when the file was extended. Returns ``False`` -- and
    leaves the file untouched -- whenever the merge cannot be trusted:
    the file is unreadable, either side has duplicate timestamps, or the
    merged series has a gap wider than ``max_seam_gap_seconds`` near
    ``seam``. The caller is expected to fall back to regenerating the
    artifact over the full window, so refusing here is always safe.

    Refusing is also the right answer for an empty ``new_lines``: a tail
    fetch that came back with nothing must never truncate a good file.
    """
    if not new_lines:
        if logger is not None:
            logger.warning(
                'Continuation produced no new rows for %s; leaving the '
                'existing file untouched.', path)
        return False

    existing_header, existing_lines = read_series_file(path)
    if not existing_lines:
        # Either unreadable, or a 0-byte file recording "this station had
        # no data". Neither is a prefix to build on, and writing here
        # would turn a blank artifact into a header-plus-tail file --
        # breaking the 0-byte contract the getsize() checks rely on.
        if logger is not None:
            logger.warning(
                'Nothing to extend in %s (missing, empty or unreadable); '
                'falling back to a full rewrite.', path)
        return False

    # Anchor the gap check on the join itself -- the last row already on
    # disk. The tail deliberately starts an overlap earlier than that, so
    # anchoring on the tail's first row would place the window on data
    # that is merely being replaced and never look at the join at all.
    seam = row_datetime(existing_lines[-1])

    merged = merge_series_lines(existing_lines, new_lines)
    if merged is None:
        if logger is not None:
            logger.warning(
                'Duplicate timestamps in %s or in its continuation tail; '
                'cannot merge safely, falling back to regeneration.', path)
        return False
    merged_lines, stats = merged

    if (max_seam_gap_seconds is not None and seam is not None
            and seam_window is not None):
        gap = max_step_seconds(merged_lines, around=seam, window=seam_window)
        if gap > max_seam_gap_seconds:
            if logger is not None:
                logger.warning(
                    'Continuation seam for %s leaves a %.0f s gap (limit '
                    '%.0f s); falling back to regeneration.',
                    path, gap, max_seam_gap_seconds)
            return False

    write_series_file(path, header or existing_header, merged_lines)
    if logger is not None:
        logger.info(
            'Extended %s: %d existing row(s), %d replaced, %d appended '
            '(now %d rows, %s to %s).',
            path, stats['kept'], stats['replaced'], stats['added'],
            stats['total'], stats['first'], stats['last'])
    return True
