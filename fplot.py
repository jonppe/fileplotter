#!/usr/bin/env python
import argparse
import itertools
import sys
import traceback

from typing import List


import panel as pn

from bokeh import palettes, models
from bokeh.plotting import figure

import fileplotter.file_utils
from fileplotter import csv, data

from bokeh.plotting import curdoc


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


def onload():
    doc.add_periodic_callback(stream_cb, 500)


def main():
    index = 0
    if "panel_realtime.py" in sys.argv:
        index = sys.argv.index("panel_realtime.py")

    args = parse_args(sys.argv[index + 1 :])
    print(
        f"Starting fileplotter for folder {args['directory']}: {pn.state}, {curdoc()}"
    )

    pn.extension()
    folder = args["directory"]

    files = fileplotter.file_utils.find_latest_files(folder)
    files = [f.relative_to(folder) for f in files]
    file_list = pn.widgets.MultiSelect(
        name="Files:",
        options=files,
        size=8,
        value=files[0:1],
        width=800,
    )

    column_list = pn.widgets.MultiSelect(name="Columns", size=8)

    def update_column_names(_):
        csv_files = [folder / f for f in file_list.value]
        path_cols = csv.read_column_names(csv_files)
        all_cols = set()
        for cols in path_cols.values():
            all_cols.update(cols)

        column_list.options = list(all_cols)
        if hasattr(doc, "data_store"):
            data.clean(doc.data_store, csv_files, column_list.value)

    update_column_names(None)

    file_list.param.watch(update_column_names, "value")
    control_row = pn.Row(file_list, column_list)

    bokeh_figure = figure(
        sizing_mode="stretch_width", title="File plotter: plot new lines in CSV files"
    )
    bokeh_figure.add_layout(
        models.Legend(click_policy="hide", background_fill_alpha=0.3)
    )
    bk_pane = pn.pane.Bokeh(bokeh_figure)

    column = pn.Column(control_row, bk_pane, width=1500)
    column.servable()

    async def stream():
        try:
            if not hasattr(doc, "data_store"):
                doc.data_store = {}
                doc.colors = itertools.cycle(palettes.Category10_10)

            x_max = -1
            new_data_len = -1
            for path in file_list.value:
                x_end, data_len = data.read_data_file(
                    folder / path,
                    column_list.value,
                    doc.data_store,
                    bokeh_figure,
                    str(path),
                    doc,
                )
                x_max = max(x_end, x_max)
                new_data_len = max(new_data_len, data_len)

            # Pan the image right when the newly added data is visible.
            # Bokeh normally does this but only in the 'reset' state (i.e., all data visible).
            # It would be nice to do this on the javascript-side, though.
            x = bokeh_figure.x_range
            if x.start < x_max < x.end - new_data_len:
                bokeh_figure.x_range.start += new_data_len
                bokeh_figure.x_range.end += new_data_len
            pn.io.push_notebook(bk_pane)  # Only needed when running in notebook context

        except Exception as e:
            print(f"Exception in updating data: {e}")
            traceback.print_exc()

    pn.state.onload(onload)
    return bk_pane, stream


# Save curdoc() to make sure all threads see the same document.
doc = curdoc()

main_pane, stream_cb = main()

# if __name__ == "__main__":
#     pn.serve(main_pane, threaded=True)
