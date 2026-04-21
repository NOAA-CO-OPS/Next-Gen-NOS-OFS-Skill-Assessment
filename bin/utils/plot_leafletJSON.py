"""
Script Name: plot_leafletJSON.py

Directory Location: bin/utils/

Technical Contact(s): AJK

Abstract:
    Standalone utility to generate static cartopy PNG maps from the 2D JSON
    grid files produced by the leaflet processing pipeline (processing_2d.py).
    Reads the JSON structure (lats, lons, sst keys) and renders a georeferenced
    map with coastlines, land masking, state boundaries, and a colorbar.

    Supports two modes:
      1. Scalar map (-f): Renders a single variable (SST, SSH, SSS, SSU, or
         SSV) as a filled pcolormesh plot. The variable is auto-detected from
         the filename or can be overridden with -v. Each variable uses a
         default colormap and unit label (e.g., SSS -> YlGnBu, psu).
      2. Current quiver map (-fu, -fv): Takes paired SSU and SSV JSON files,
         computes current magnitude for the color background, and overlays
         subsampled quiver arrows showing flow direction and speed.

    Output is saved as PNG at 100 DPI. Output path defaults to the input
    filename with .png appended, or can be specified with -o.

    This script wraps library functions from ofs_skill.visualization.plotting_2d
    (plot_2d_scalar_map, plot_2d_current_quiver_map, load_json_grid) which are
    also called by the automated pipeline in create_2dplot.py when
    static_plots=True in conf/ofs_dps.conf.

Language: Python 3.11

Usage:
    # Scalar map (auto-detects variable from filename)
    python plot_leafletJSON.py -f data/model/2d/cbofs_20260328-00z_sss_model.nowcast.json

    # Scalar map with variable override and custom output path
    python plot_leafletJSON.py -f data/model/2d/cbofs_20260328-00z_sss_model.nowcast.json \
                               -v sss -o ./output/cbofs_sss.png

    # Current quiver map (provide both u and v component files)
    python plot_leafletJSON.py -fu data/model/2d/cbofs_20260328-00z_ssu_model.nowcast.json \
                               -fv data/model/2d/cbofs_20260328-00z_ssv_model.nowcast.json

Arguments:
    -f,  --filename     Path to a single JSON file (scalar map mode)
    -fu, --filename_u   Path to SSU (eastward velocity) JSON file (quiver mode)
    -fv, --filename_v   Path to SSV (northward velocity) JSON file (quiver mode)
    -v,  --variable     Variable name override: sst, ssh, sss, ssu, ssv
    -o,  --output       Output PNG file path (default: input filename + .png)

Revisions:
    Date        Author      Description
    09/2024     AJK         Initial version (SST only, cartopy pcolor)
    03/2026     AJK         Refactored to use plotting_2d library functions,
                            added support for all variables and quiver maps
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
