#!/usr/bin/env python
import argparse
import bz2
import gzip
import pathlib
import sys

from typing import Dict, List

import numpy as np
import panel as pn

from bokeh.plotting import figure
from bokeh.models import ColumnDataSource


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


xs = np.arange(1000)
ys = np.random.randn(1000).cumsum()
x, y = xs[-1], ys[-1]

FILE_TYPES = {".csv": open, ".csv.gz": gzip.open, ".csv.bz2": bz2.open}


def find_files(folder: str) -> List[pathlib.Path]:
    p = pathlib.Path(folder)
    files = []
    for t in FILE_TYPES:
        files.extend(p.glob(f"*{t}"))
    return [f.relative_to(folder) for f in files]


def open_file(path: pathlib.Path):
    """Open file and return the file descriptor.

    If the file has known compressed file suffix (gz, bz2), then use gzip or bz2 module to open the file.
    """
    open_func = open
    for t in FILE_TYPES:
        if path.name.endswith(t):
            open_func = FILE_TYPES[t]
    return open_func(path, "r", encoding="utf-8")


def read_column_names(
    folder, files: List[pathlib.Path], separator=","
) -> Dict[pathlib.Path, List[str]]:
    """Read column names for all files.

    The return value is a dict {path: list-of-col-names}
    :param folder: the base folder. All the paths are relative to this.
    :param files: files to read column names"""
    paths = {}
    for path in files:
        f = open_file(folder / path)
        cols = f.readline().split(separator)
        paths[path] = cols
    return paths


def main():
    index = 0
    if "panel_realtime.py" in sys.argv:
        index = sys.argv.index("panel_realtime.py")

    args = parse_args(sys.argv[index + 1 :])
    print(f"Starting fileplotter for folder {args['directory']}")

    pn.extension()
    p = figure(sizing_mode="stretch_width", title="Bokeh streaming example")

    cds = ColumnDataSource(data={"x": xs, "y": ys})

    p.line("x", "y", source=cds)
    folder = args["directory"]
    files = find_files(folder)
    file_list = pn.widgets.MultiSelect(
        name="Files:", options=files, size=8, value=files[0:1]
    )

    column_list = pn.widgets.MultiSelect(name="Columns", size=8)

    def update_column_names(_):
        path_cols = read_column_names(folder, file_list.value)
        all_cols = set()
        for cols in path_cols.values():
            all_cols.update(cols)

        col_list = list(all_cols)
        column_list.options = col_list

    update_column_names(None)

    column_list.param.watch(update_column_names, "value")
    control_row = pn.Row(file_list, column_list)
    bk_pane = pn.pane.Bokeh(p)
    column = pn.Column(control_row, bk_pane, width=700)
    column.servable()

    def stream():
        global x, y
        x += 1
        y += np.random.randn()
        cds.stream({"x": [x], "y": [y]})
        pn.io.push_notebook(bk_pane)  # Only needed when running in notebook context

    pn.state.add_periodic_callback(stream, 100)
    return bk_pane, stream


main_pane, stream = main()

# if __name__ == "__main__":
#     pn.serve(main_pane, threaded=True)
