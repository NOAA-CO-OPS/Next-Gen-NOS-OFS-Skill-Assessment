"""
NOS standard 37 tidal constituent definitions and speeds.

Defines the 37 tidal constituents used by NOS for harmonic analysis and
tidal prediction, matching the ordering in Appendix C of NOAA Technical
Report NOS CS 24 (Zhang et al. 2006).

Constituent speeds are from Schureman (1958) Special Publication No. 98.
Names use UTide-compatible conventions.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# The 37 NOS standard tidal constituents, grouped by type.
# Ordering follows Appendix C of NOS CS 24.
# ---------------------------------------------------------------------------

# -- Semidiurnal (period ~ 12 h) --
_SEMIDIURNAL = [
    'M2', 'S2', 'N2', 'K2', '2N2', 'MU2', 'NU2', 'L2', 'T2', 'R2', 'LDA2',
]

# -- Diurnal (period ~ 24 h) --
_DIURNAL = [
    'K1', 'O1', 'P1', 'Q1', 'J1', 'M1', 'OO1', '2Q1', 'RHO1',
]

# -- Long-period (period > 1 day) --
_LONG_PERIOD = [
    'MF', 'MM', 'SSA', 'SA', 'MSM', 'MSF',
]

# -- Shallow-water / overtides --
_SHALLOW_WATER = [
    'M4', 'M6', 'M8', 'MS4', 'MN4', 'MK3', 'S4', 'S6', '2MK3', '2SM2', 'MO3',
]

NOS_37_CONSTITUENTS: list[str] = (
    _SEMIDIURNAL + _DIURNAL + _LONG_PERIOD + _SHALLOW_WATER
)
"""List of the 37 NOS standard tidal constituents in Appendix C order."""

# ---------------------------------------------------------------------------
# Constituent angular speeds in degrees per hour.
# Source: Schureman (1958) SP98, Table 2.
# ---------------------------------------------------------------------------

CONSTITUENT_SPEEDS: dict[str, float] = {
    # Semidiurnal
    'M2':   28.9841042,
    'S2':   30.0000000,
    'N2':   28.4397295,
    'K2':   30.0821373,
    '2N2':  27.8953548,
    'MU2':  27.9682084,
    'NU2':  28.5125831,
    'L2':   29.5284789,
    'T2':   29.9589333,
    'R2':   30.0410667,
    'LDA2': 29.4556253,
    # Diurnal
    'K1':   15.0410686,
    'O1':   13.9430356,
    'P1':   14.9589314,
    'Q1':   13.3986609,
    'J1':   15.5854433,
    'M1':   14.4966939,
    'OO1':  16.1391017,
    '2Q1':  12.8542862,
    'RHO1': 13.4715145,
    # Long-period
    'MF':    1.0980331,
    'MM':    0.5443747,
    'SSA':   0.0821373,
    'SA':    0.0410686,
    'MSM':   0.4715211,
    'MSF':   1.0158958,
    # Shallow-water / overtides
    'M4':   57.9682084,
    'M6':   86.9523127,
    'M8':  115.9364169,
    'MS4':  58.9841042,
    'MN4':  57.4238337,
    'MK3':  44.0251729,
    'S4':   60.0000000,
    'S6':   90.0000000,
    '2MK3': 42.9271398,
    '2SM2': 31.0158958,
    'MO3':  42.9271398,
}
"""Angular speeds (degrees/hour) for the 37 NOS standard constituents."""

# Mapping from NOS/Fortran names to UTide names where they differ.
# UTide uses "LDA2" for Lambda-2, which matches NOS convention.
NOS_TO_UTIDE: dict[str, str] = {
    'LDA2': 'LDA2',
}
"""Mapping of NOS constituent names to UTide names (identity where equal)."""

# ---------------------------------------------------------------------------
# CO-OPS API ↔ NOS/UTide name mapping.
#
# The CO-OPS Tides & Currents API (product=harcon) returns constituent names
# that occasionally differ from the NOS/UTide convention used in this package.
# This dictionary maps *every* name the API may return to the canonical NOS
# name.  Constituents whose names are identical in both systems are included
# explicitly so the mapping is comprehensive and auditable.
# ---------------------------------------------------------------------------

COOPS_API_NAME_MAP: dict[str, str] = {
    # -- Semidiurnal --
    'M2':   'M2',
    'S2':   'S2',
    'N2':   'N2',
    'K2':   'K2',
    '2N2':  '2N2',
    'MU2':  'MU2',
    'NU2':  'NU2',
    'L2':   'L2',
    'T2':   'T2',
    'R2':   'R2',
    'LAM2': 'LDA2',   # CO-OPS uses "LAM2"; NOS/UTide uses "LDA2"
    'LDA2': 'LDA2',   # Accept NOS name too (idempotent)
    # -- Diurnal --
    'K1':   'K1',
    'O1':   'O1',
    'P1':   'P1',
    'Q1':   'Q1',
    'J1':   'J1',
    'M1':   'M1',
    'OO1':  'OO1',
    '2Q1':  '2Q1',
    'RHO1': 'RHO1',
    'RHO':  'RHO1',   # Alternate short form occasionally seen
    # -- Long-period --
    'MF':   'MF',
    'MM':   'MM',
    'SSA':  'SSA',
    'SA':   'SA',
    'MSM':  'MSM',
    'MSF':  'MSF',
    # -- Shallow-water / overtides --
    'M4':   'M4',
    'M6':   'M6',
    'M8':   'M8',
    'MS4':  'MS4',
    'MN4':  'MN4',
    'MK3':  'MK3',
    'S4':   'S4',
    'S6':   'S6',
    '2MK3': '2MK3',
    '2SM2': '2SM2',
    'MO3':  'MO3',
}
"""Mapping of CO-OPS API constituent names to NOS/UTide names."""


def normalize_constituent_name(name: str) -> str:
    """
    Normalize a constituent name from CO-OPS API to NOS/UTide convention.

    Parameters
    ----------
    name : str
        Constituent name as returned by the CO-OPS API.

    Returns
    -------
    str
        Normalized name in NOS/UTide convention.  If the name is not
        recognized, it is returned unchanged (uppercased).
    """
    cleaned = name.strip().upper()
    return COOPS_API_NAME_MAP.get(cleaned, cleaned)
