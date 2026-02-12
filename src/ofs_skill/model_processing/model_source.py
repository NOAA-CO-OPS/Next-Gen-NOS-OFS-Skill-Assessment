"""
Model Source Detection

This module provides functionality to determine which numerical model
framework is used by a given OFS.
"""

from __future__ import annotations


def get_model_source(ofs: str) -> str:
    """
    Determine the numerical model framework used by an OFS.

    Maps OFS identifiers to their underlying numerical ocean model frameworks.
    Supports ROMS, POM, FVCOM, and SCHISM model types.

    Parameters
    ----------
    ofs : str
        OFS identifier (e.g., 'cbofs', 'ngofs2', 'stofs_3d_atl')

    Returns
    -------
    str
        Model framework: 'roms', 'pom', 'fvcom', or 'schism'

    Raises
    ------
    ValueError
        If the OFS identifier is not recognized

    Examples
    --------
    >>> get_model_source('cbofs')
    'roms'
    >>> get_model_source('ngofs2')
    'fvcom'
    >>> get_model_source('stofs_3d_atl')
    'schism'

    Notes
    -----
    **ROMS** - Regional Ocean Modeling System
    - Used by: CBOFS, DBOFS, GOMOFS, TBOFS, CIOFS, WCOFS

    **POM** - Princeton Ocean Model
    - Used by: NYOFS, SJROFS

    **FVCOM** - Finite Volume Community Ocean Model
    - Used by: NECOFS, NGOFS2, NGOFS, LEOFS, LMHOFS, LOOFS, LSOFS, SFBOFS, SSCOFS

    **SCHISM** - Semi-implicit Cross-scale Hydroscience Integrated System Model
    - Used by: STOFS_3D_ATL, STOFS_3D_PAC, LOOFS-NEXTGEN
    """
    ofs_lower = ofs.lower()

    if ofs_lower in ('cbofs', 'dbofs', 'gomofs', 'tbofs', 'ciofs', 'wcofs'):
        return 'roms'

    elif ofs_lower in ('nyofs', 'sjrofs'):
        return 'pom'

    elif ofs_lower in ('necofs', 'ngofs2', 'ngofs', 'leofs', 'lmhofs', 'loofs',
                       'lsofs', 'sfbofs', 'sscofs'):
        return 'fvcom'

    elif ofs_lower in ('stofs_3d_atl', 'stofs_3d_pac', 'loofs-nextgen'):
        return 'schism'

    else:
        raise ValueError(
            f"Unknown OFS identifier: '{ofs}'. "
            f'Must be one of the recognized NOAA OFS systems.'
        )


# Legacy function name for backward compatibility
def model_source(ofs: str) -> str:
    """
    Legacy function name - use get_model_source() instead.

    .. deprecated::
        Use :func:`get_model_source` instead.
    """
    return get_model_source(ofs)
