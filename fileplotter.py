#!/usr/bin/env python
import argparse
import sys

from typing import List

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

    bk_pane = pn.pane.Bokeh(p)
    bk_pane.servable()

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
