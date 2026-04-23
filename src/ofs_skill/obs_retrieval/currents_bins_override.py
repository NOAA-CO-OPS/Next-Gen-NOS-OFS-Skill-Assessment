"""
Parser for the optional currents-bins override CSV.

Lets a user pin which ADCP bins are processed and override any of
``depth``, ``orientation``, or display ``name`` on a per-bin basis.
Full end-user docs — schema, CLI usage, worked examples, and the
behaviour matrix — live on the wiki under *CO-OPS ADCP current
processing*:

https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki/CO%E2%80%90OPS-ADCP-current-processing

CSV schema (header row required, column order flexible):

    station_id,bin,depth,orientation,name

* ``station_id`` and ``bin`` are required. ``station_id`` matches the
  parent CO-OPS station ID (not the ``{parent}_b{NN}`` virtual ID).
* ``depth``, ``orientation``, ``name`` are all optional. Empty cells are
  treated as "do not override".
* Rows for the same ``station_id`` with the same ``bin`` replace each
  other — later wins.
* Blank lines and lines whose first non-whitespace character is ``#``
  are skipped, so a shipped example can carry inline comments.

Behaviour at CTL-write time (see ``write_obs_ctlfile._process_coops_station``):

* If a parent station has **any** rows in the CSV, **only** those bins
  are emitted (filter mode). The bin set is also passed into
  :func:`~ofs_skill.obs_retrieval.retrieve_t_and_c_station.retrieve_t_and_c_station`
  via ``only_bins`` so the CO-OPS datagetter is only called for pinned
  bins.
* For each surviving bin, any non-empty CSV field overrides the
  MDAPI-derived value. A user-supplied ``depth`` short-circuits the
  side-looking ``water_depth - height_from_bottom`` resolver by
  forcing ``hfb = 0.00`` in the obs station ctl.
* If the CSV names a bin that the datagetter did not return, a WARNING
  is logged and the row is skipped.
* Parent stations absent from the CSV keep the default "emit all bins"
  behaviour.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from logging import Logger
from pathlib import Path
from typing import Optional

# Shared regex + helper for the ``{parent}_b{NN}`` virtual-ID format used
# throughout the per-bin currents pipeline. Lives here rather than in each
# consumer so the two modules that split these IDs agree on the grammar.
VIRTUAL_CURRENTS_ID_RE = re.compile(r'^(.+)_b(\d+)$')


def split_virtual_currents_id(station_id: str) -> tuple[str, Optional[int]]:
    """Split a CO-OPS currents virtual ID ``{parent}_b{NN}`` into parts.

    Returns ``(parent_id, bin_num)`` for virtual IDs and ``(sid, None)``
    for legacy (non-virtual) IDs — letting the CO-OPS currents path
    transparently handle both.
    """
    m = VIRTUAL_CURRENTS_ID_RE.match(str(station_id))
    if not m:
        return str(station_id), None
    try:
        return m.group(1), int(m.group(2))
    except (TypeError, ValueError):
        return str(station_id), None


@dataclass
class BinSpec:
    """Per-bin override spec from the user CSV."""
    bin: int
    depth: Optional[float] = None
    orientation: Optional[str] = None
    name: Optional[str] = None


def _clean(value: Optional[str]) -> Optional[str]:
    # Strip CR/LF and swap " -> ' so a hostile or typo'd cell cannot
    # split the 2-lines-per-station CTL record or close a quoted label.
    if value is None:
        return None
    s = str(value).strip().replace('\r', '').replace('\n', '').replace('"', "'")
    return s if s else None


def load_currents_bins_csv(
    path: Optional[str], logger: Logger,
) -> dict[str, list[BinSpec]]:
    """Load the currents-bins override CSV into ``{station_id: [BinSpec]}``.

    Returns an empty dict when ``path`` is falsy or the file is missing
    — the caller treats that as "no overrides, use default behaviour".
    Malformed rows are skipped with a WARNING; the entire file is never
    a hard failure.
    """
    if not path:
        return {}
    csv_path = Path(path)
    if not csv_path.is_file():
        logger.warning(
            'Currents bins override CSV not found at %s — proceeding '
            'with default per-bin behaviour.', path)
        return {}

    result: dict[str, list[BinSpec]] = {}
    with open(csv_path, encoding='utf-8', newline='') as fh:
        # Skip blank lines + lines whose first non-whitespace char is ``#``
        # so the example CSV can carry inline comments.
        filtered = (
            line for line in fh
            if line.strip() and not line.lstrip().startswith('#')
        )
        reader = csv.DictReader(filtered)
        if reader.fieldnames is None:
            logger.warning(
                'Currents bins override CSV %s has no header row; '
                'expected station_id,bin[,depth,orientation,name].', path)
            return {}
        required = {'station_id', 'bin'}
        missing = required - {c.strip() for c in reader.fieldnames if c}
        if missing:
            logger.error(
                'Currents bins override CSV %s is missing required '
                'column(s): %s', path, sorted(missing))
            return {}

        row_no = 1  # header is row 1
        for raw in reader:
            row_no += 1
            station_id = _clean(raw.get('station_id'))
            bin_raw = _clean(raw.get('bin'))
            if not station_id or not bin_raw:
                logger.warning(
                    'Currents bins CSV row %d missing station_id/bin; '
                    'skipping.', row_no)
                continue
            try:
                bin_num = int(bin_raw)
            except ValueError:
                logger.warning(
                    'Currents bins CSV row %d: bin=%r is not an integer; '
                    'skipping.', row_no, bin_raw)
                continue

            depth_raw = _clean(raw.get('depth'))
            depth: Optional[float] = None
            if depth_raw is not None:
                try:
                    depth = float(depth_raw)
                except ValueError:
                    logger.warning(
                        'Currents bins CSV row %d: depth=%r is not a '
                        'number; ignoring depth override for station=%s '
                        'bin=%d.', row_no, depth_raw, station_id, bin_num)
                    depth = None

            spec = BinSpec(
                bin=bin_num,
                depth=depth,
                orientation=_clean(raw.get('orientation')),
                name=_clean(raw.get('name')),
            )

            bucket = result.setdefault(station_id, [])
            # Replace any prior spec with the same bin number.
            bucket[:] = [b for b in bucket if b.bin != bin_num]
            bucket.append(spec)

    if result:
        total = sum(len(b) for b in result.values())
        logger.info(
            'Loaded currents-bins overrides from %s: %d station(s), '
            '%d bin row(s) total.', path, len(result), total)
    return result


def bin_spec_lookup(
    overrides: dict[str, list[BinSpec]], station_id: str,
) -> Optional[dict[int, BinSpec]]:
    """Convenience: ``{bin_num: BinSpec}`` for a parent station, or None.

    ``None`` signals "no CSV entries for this station — keep default
    per-bin behaviour." An empty dict would be ambiguous, hence the
    explicit None.
    """
    if station_id not in overrides:
        return None
    return {s.bin: s for s in overrides[station_id]}
