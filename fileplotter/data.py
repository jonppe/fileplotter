"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import pathlib
from dataclasses import dataclass
from typing import IO, List, Dict, Optional

from bokeh.models import ColumnDataSource

from fileplotter.csv import MAX_ROWS, read_column_names_file, load_dataframe_continuous


@dataclass
class OpenFile:
    file: Optional[IO]
    col_names: List[str]
    data: Dict[str, ColumnDataSource]
    rows: int


def read_data_file(
    path: pathlib.Path,
    used_columns: List[str],
    old_data: dict,
    bokeh_figure,
    separator=",",
    max_rows=MAX_ROWS,
):
    try:
        if path in old_data:
            file_info = old_data[path]
            cols = file_info.col_names
        else:
            col_names = read_column_names_file(path, separator)
            file_info = OpenFile(file=None, col_names=col_names, data={}, rows=0)
            old_data[path] = file_info
            cols = None
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
            bokeh_figure.line("x", "y", source=ds)

    file_info.rows = rows_end
    return file_info
