"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import pathlib
from dataclasses import dataclass
from typing import IO, List, Dict, Optional


import panel as pn
from bokeh.models import ColumnDataSource, Line

from fileplotter.csv import read_column_names_file, load_dataframe_continuous

MAX_ROWS = 15000


@dataclass
class OpenFile:
    file: Optional[IO]
    col_names: List[str]
    data: Dict[str, ColumnDataSource]
    lines: Dict[str, Line]
    rows: int


def clean_line(ds: ColumnDataSource, line: Line):
    print("Destroing column")
    ds.destroy()
    line.visible = False


def clean(
    data_store: Dict[pathlib.Path, OpenFile],
    selected_files: List[pathlib.Path],
    selected_cols: List[str],
):
    unused_paths = [p for p in data_store.keys() if p not in selected_files]
    for p in unused_paths:
        for col, ds in data_store[p].data.items():
            clean_line(ds, data_store[p].lines[col])

        data_store.pop(p)


def read_data_file(
    path: pathlib.Path,
    used_columns: List[str],
    old_data: dict,
    bokeh_figure,
    path_description: str,
    doc,
    separator=",",
    max_rows=MAX_ROWS,
):
    try:
        if path in old_data:
            file_info = old_data[path]
            cols = file_info.col_names
        else:
            col_names = read_column_names_file(path, separator)
            file_info = OpenFile(
                file=None, col_names=col_names, data={}, rows=0, lines={}
            )
            old_data[path] = file_info
            cols = None  # Set cols to None so that load_dataframe_continuous() will read them from the file
    except Exception as e:
        print(f"Exception in reading data for {path}: {e}")
        return
    used_columns = [k for k in used_columns if k in file_info.col_names]
    if len(used_columns) == 0:
        return
    df, f = load_dataframe_continuous(
        path,
        usecols=used_columns,
        cols=cols,
        f=file_info.file,
    )
    if len(df) > max_rows:
        df = df.iloc[-max_rows:]
    file_info.file = f
    rows_end = file_info.rows + len(df)
    for col in used_columns:
        new_data = {
            "y": df[col].values,
            "x": list(range(file_info.rows, rows_end)),
        }
        if col in file_info.data:
            print(f"Updating col {col} with {len(df)}")
            ds = file_info.data[col]
            ds.stream(new_data, rollover=max_rows)
        else:
            print(f"New col {col} with {len(df)}")
            ds = ColumnDataSource(new_data)
            file_info.data[col] = ds
            color = next(doc.colors)
            file_info.lines[col] = bokeh_figure.line(
                "x",
                "y",
                source=ds,
                legend_label=f"{path_description}:{col}",
                color=color,
            )

    file_info.rows = rows_end
    return rows_end, len(df)
