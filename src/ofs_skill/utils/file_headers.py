"""
Single source of truth for the descriptive header lines written to (and
detected in) the text data files produced by the skill assessment:
model control files (``*_model[_station].ctl``), observation control
files (``*_station.ctl``), and time-series files (``.obs``/``.prd``).

Writers import the header constants; readers use the ``strip_*`` /
``*_rows_to_skip`` helpers so that detection stays anchored to the
exact text the writers emit. Legacy files without headers pass through
all helpers unchanged.
"""


# Model control file: one header line.
MODEL_CTL_HEADER_PREFIX = 'Node/station index'
MODEL_CTL_HEADER = (
    'Node/station index, Depth layer index, Latitude, Longitude, '
    'Station ID, Model layer depth (m)\n'
)

# Observation control file: two header lines (entries are two lines per
# station, so the header is two lines to preserve even/odd parity).
OBS_CTL_HEADER_PREFIX = 'Station ID'
OBS_CTL_HEADER = (
    'Station ID, Source ID, Station Name\n'
    '  Latitude, Longitude, Target-to-station datum offset (m), '
    'Water depth (m), Station datum (if applicable; zero otherwise)\n'
)

# Time-series files (.obs and .prd): one header line.
SERIES_HEADER_PREFIX = 'Julian days'
_SERIES_TIME_COLS = 'Julian days, Year, Month, Day, Hours, Minutes'
_SERIES_VALUE_LABELS = {
    'cu': 'Current speed (m/s), Current direction (0-359), u, v',
    'wl': 'Water level (m)',
    'temp': 'Water temperature (C)',
    'salt': 'Salinity (PSU)',
}


def series_header(name_var):
    """Return the .obs/.prd header line for a variable.

    Falls back to the raw variable name for variables without a mapped
    label, so an unmapped variable never writes a literal 'None'.
    """
    label = _SERIES_VALUE_LABELS.get(name_var, name_var)
    return f'{_SERIES_TIME_COLS}, {label}\n'


def strip_model_ctl_header(lines):
    """Drop the single model-ctl header line if present (legacy files
    pass through unchanged)."""
    if lines and lines[0].startswith(MODEL_CTL_HEADER_PREFIX):
        return lines[1:]
    return lines


def strip_obs_ctl_header(lines):
    """Drop the two obs-ctl header lines if present (legacy files pass
    through unchanged). Anchored to the line start so a station name
    containing 'Station ID' cannot be mistaken for a header."""
    if lines and lines[0].startswith(OBS_CTL_HEADER_PREFIX):
        return lines[2:]
    return lines


def series_rows_to_skip(path):
    """Number of header rows (0 or 1) to skip when reading a .obs/.prd
    file with pandas."""
    with open(path, encoding='utf-8') as peek:
        first_line = peek.readline()
    return 1 if first_line.startswith(SERIES_HEADER_PREFIX) else 0
