"""
Regression: model-side depth indexing must tolerate ragged station ctl rows.

CO-OPS ADCP currents rows on this branch carry an extra orientation
token (``up``/``down``/``side``) appended to the coord line by the
issue #140/#141 fix. NDBC currents rows do not. A mixed-source ctl
(common for tbofs/cbofs/etc.) therefore produces rows of two widths,
e.g.::

    t02010_b01 t02010_b01_cu_tbofs_CO-OPS "Old Port Tampa (bin 01)"
      27.863 -82.554 0.0  10.76  0.0  0.70  up        # 7 fields
    42098 42098_cu_tbofs_NDBC "Egmont Channel Entrance, FL (214)"
      27.590 -82.931 0.0   1.00  0.0  0.00            # 6 fields

The previous depth-indexing pattern (``np.array(extract)[:, 3][idx]``)
raised ``ValueError: setting an array element with a sequence ...
inhomogeneous shape`` on inputs like this, because NumPy 2.x refuses
to coerce ragged lists into a 2-D array. The fix replaces every site
with positional list indexing — ``extract[idx][3]`` — which has no
column-uniformity requirement.

This test is the smoking-gun shape: a list-of-lists with mismatched
inner lengths, indexed at column 3. It fails with the old pattern and
passes with the new one.
"""
from __future__ import annotations

import numpy as np
import pytest

_RAGGED_COORDS: list[list[str]] = [
    # 10 CO-OPS ADCP bin rows with trailing orientation token (7 fields)
    ['27.863', '-82.554', '0.0', '10.76', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '9.75', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '8.75', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '7.74', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '6.74', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '5.76', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '4.75', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '3.75', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '2.74', '0.0', '0.70', 'up'],
    ['27.863', '-82.554', '0.0',  '1.74', '0.0', '0.70', 'up'],
    # 1 NDBC row, no orientation token (6 fields)
    ['27.590', '-82.931', '0.0',  '1.00', '0.0', '0.00'],
]


def test_old_pattern_raises_on_ragged_rows():
    """Confirms the failure mode that motivated the fix."""
    with pytest.raises(ValueError, match='inhomogeneous shape'):
        _ = np.array(_RAGGED_COORDS)[:, 3]


@pytest.mark.parametrize('idx, expected', [
    (0,  '10.76'),
    (4,  '6.74'),
    (9,  '1.74'),
    (10, '1.00'),   # the NDBC row, with one fewer column than the ADCP rows
])
def test_new_pattern_extracts_depth_per_row(idx, expected):
    """Direct positional indexing tolerates mixed row widths."""
    station_depth = _RAGGED_COORDS[idx][3]
    assert station_depth == expected
    # Also exercises the downstream float() coercion every call site
    # uses immediately after the lookup, so a regression to e.g. tuple
    # indexing of a numpy string scalar would surface here too.
    assert float(station_depth) == float(expected)


def test_indexer_call_sites_use_positional_indexing():
    """Static check: no call site in indexing.py should ever reintroduce
    the np.array(...)[:, 3][idx] pattern that fails on ragged rows."""
    from pathlib import Path
    src = (
        Path(__file__).parent.parent
        / 'src' / 'ofs_skill' / 'model_processing' / 'indexing.py'
    ).read_text()
    forbidden = 'np.array(station_ctl_file_extract)[:, 3]'
    assert forbidden not in src, (
        f'Forbidden pattern reintroduced in indexing.py: {forbidden!r}. '
        'Use positional indexing (station_ctl_file_extract[idx][3]) — '
        'np.array(...) raises on mixed-width rows (CO-OPS ADCP + NDBC).'
    )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
