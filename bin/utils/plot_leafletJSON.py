"""
Reads JSON file output from leaflet routine and visualizes as a cartopy plot.

Supports all model variables (SST, SSH, SSS, SSU, SSV) and current vector
quiver maps. Can auto-detect variable from filename or accept explicit
variable flag.

Usage:
    # Scalar map (auto-detects variable from filename)
    python plot_leafletJSON.py -f data/model/2d/cbofs_20260328-00z_sss_model.nowcast.json

    # Current quiver map (provide both u and v files)
    python plot_leafletJSON.py -fu data/model/2d/cbofs_20260328-00z_ssu_model.nowcast.json \
                               -fv data/model/2d/cbofs_20260328-00z_ssv_model.nowcast.json
"""
import argparse
import logging
import os

from ofs_skill.visualization.plotting_2d import (
    load_json_grid,
    plot_2d_current_quiver_map,
    plot_2d_scalar_map,
)


def detect_variable(fname: str) -> str:
    """Detect variable name from JSON filename."""
    for var in ('sst', 'ssh', 'sss', 'ssu', 'ssv'):
        if f'_{var}_' in fname:
            return var
    return 'unknown'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python plot_leafletJSON.py',
        description='Make cartopy plot of leaflet JSON file.',
    )
    parser.add_argument(
        '-f', '--filename',
        help='Path to JSON file to plot (scalar map).',
    )
    parser.add_argument(
        '-fu', '--filename_u',
        help='Path to SSU JSON file (for quiver map).',
    )
    parser.add_argument(
        '-fv', '--filename_v',
        help='Path to SSV JSON file (for quiver map).',
    )
    parser.add_argument(
        '-v', '--variable',
        help='Variable name override (sst, ssh, sss, ssu, ssv).',
    )
    parser.add_argument(
        '-o', '--output',
        help='Output file path (default: input filename + .png).',
    )

    args = parser.parse_args()
    logger = logging.getLogger('plot_leafletJSON')
    logging.basicConfig(level=logging.INFO)

    if args.filename_u and args.filename_v:
        # Quiver map mode
        lons, lats, ssu = load_json_grid(args.filename_u)
        _, _, ssv = load_json_grid(args.filename_v)
        title = os.path.basename(args.filename_u).replace('_ssu_', '_currents_')
        output = args.output or args.filename_u.replace('.json', '_currents.png')
        plot_2d_current_quiver_map(
            lons, lats, ssu, ssv, title, output, logger,
        )
    elif args.filename:
        # Scalar map mode
        lons, lats, data = load_json_grid(args.filename)
        variable = args.variable or detect_variable(args.filename)
        title = os.path.basename(args.filename)
        output = args.output or args.filename + '.png'
        plot_2d_scalar_map(
            lons, lats, data, variable, title, output, logger,
        )
    else:
        parser.error('Provide either -f (scalar) or -fu and -fv (quiver).')
