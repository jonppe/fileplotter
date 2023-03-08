"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import bz2
import gzip
import io
import pathlib

from typing import Dict, List, Union

import pandas as pd

FILE_TYPES = {".csv": open, ".csv.gz": gzip.open, ".csv.bz2": bz2.open}

MAX_ROWS = 5000


def load_dataframe_continuous(
    path: Union[str, pathlib.Path],
    f=None,
    cols: List[str] = None,
    usecols: List[str] = None,
):
    """Open a csv-file and return pandas dataframe. Allow continuing from previous file position.

    If 'f' and 'cols' are provided, this method will try to continue reading from a previous file position
    and then returns a dataframe with only new data.

    :param path: string or Path to a csv file (potentially compressed)
    :param f: this should be the f returned by a previous call to this method
    :param cols: columns of the csv. This needs to be provided when 'f' is given.
    :param usecols: columns of the csv. This needs to be provided when 'f' is given.

    The method supports opening also csv-files with gz or bz2 compression that are still open in another process
    (and files that have been cut e.g. due to a process crash, power-cut, or failed download).
    """
    path = pathlib.Path(path)
    if f is None:
        f = open_file(path)
    # Save initial position
    initial_pos = f.seek(0, 1)
    try:
        # Go to the end of file. This can raise EOFError on compressed files.
        f.seek(0, 2)
    except EOFError:
        pass
    # the current position should tell the (uncompressed) size of the file (even after EOFError)
    size = f.seek(0, 1)
    # Now, let's go the beginning and then read the data only up to the readable size
    f.seek(initial_pos, 0)
    data = f.read(size - initial_pos)
    stream = io.StringIO(data)
    return pd.read_csv(stream, usecols=usecols, names=cols), f


def load_dataframe(path: Union[str, pathlib.Path]):
    """Open a csv-file and return pandas dataframe.

    The method supports opening also csv-files with gz or bz2 compression that are still open in another process
    (and files that have been cut e.g. due to a process crash, power-cut, or failed download).
    """

    # First try pandas.read_csv() directly since this is probably more efficient
    try:
        return pd.read_csv(path, encoding="utf-8")
    except EOFError as e:
        print(
            f"Could not fully read file {path}. Trying to read as much as possible. ({e})"
        )
    # As a backup, use the more robust method that reads as many bytes as possible
    return load_dataframe_continuous(path)[0]


def open_file(path: pathlib.Path):
    """Open file and return the file descriptor.

    If the file has known compressed file suffix (gz, bz2), then use gzip or bz2 module to open the file.
    """
    open_func = open
    for t in FILE_TYPES:
        if path.name.endswith(t):
            open_func = FILE_TYPES[t]
    return open_func(path, "r", encoding="utf-8")


def read_column_names_file(path: pathlib.Path, separator=","):
    f = open_file(path)
    return f.readline().strip().split(separator)


def read_column_names(
    files: List[pathlib.Path],
    separator=",",
) -> Dict[pathlib.Path, List[str]]:
    """Read column names for all files.

    The return value is a dict {path: list-of-col-names}

    :param files: files to read column names
    """
    paths = {}
    for path in files:
        paths[path] = read_column_names_file(path, separator)
    return paths
