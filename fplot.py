#!/usr/bin/env python
import argparse
import sys

from typing import List

import numpy as np
import panel as pn

from bokeh.plotting import figure

import fileplotter.file_utils
from fileplotter import csv, data


def parse_args(argv: List[str]) -> dict:
    """Parse commandline arguments in sys.argv list into a dictionary"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-d",
        "--directory",
        default=".",
        help="Directory to watch",
    )
    return vars(parser.parse_args(argv))


def main():
    index = 0
    if "panel_realtime.py" in sys.argv:
        index = sys.argv.index("panel_realtime.py")

    args = parse_args(sys.argv[index + 1 :])
    print(f"Starting fileplotter for folder {args['directory']}")

    pn.extension()
    bokeh_figure = figure(
        sizing_mode="stretch_width", title="File plotter: plot new lines in CSV files"
    )
    bokeh_figure.circle(x=[0], y=[0])
    folder = args["directory"]

    files = fileplotter.file_utils.find_latest_files(folder)
    files = [f.relative_to(folder) for f in files]
    file_list = pn.widgets.MultiSelect(
        name="Files:", options=files, size=8, value=files[0:1]
    )

    column_list = pn.widgets.MultiSelect(name="Columns", size=8)

    def update_column_names(_):
        csv_files = [folder / f for f in file_list.value]
        path_cols = csv.read_column_names(csv_files)
        all_cols = set()
        for cols in path_cols.values():
            all_cols.update(cols)

        col_list = list(all_cols)
        column_list.options = col_list

    update_column_names(None)

    file_list.param.watch(update_column_names, "value")
    control_row = pn.Row(file_list, column_list)
    bk_pane = pn.pane.Bokeh(bokeh_figure)
    column = pn.Column(control_row, bk_pane, width=700)
    column.servable()

    def stream():
        if not hasattr(pn.state, "data_store"):
            pn.state.data_store = {}

        for path in file_list.value:
            data.read_data_file(
                folder / path, column_list.value, pn.state.data_store, bokeh_figure
            )

        pn.io.push_notebook(bk_pane)  # Only needed when running in notebook context

    pn.state.add_periodic_callback(stream, 500)
    return bk_pane, stream


main_pane, stream_cb = main()

# if __name__ == "__main__":
#     pn.serve(main_pane, threaded=True)
