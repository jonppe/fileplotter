"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import bz2
import gzip
import pathlib

from typing import Dict, List


FILE_TYPES = {".csv": open, ".csv.gz": gzip.open, ".csv.bz2": bz2.open}


def find_files(folder: str) -> List[pathlib.Path]:
    p = pathlib.Path(folder)
    files = []
    for t in FILE_TYPES:
        files.extend(p.glob(f"*{t}"))
    return files


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
    files: List[pathlib.Path],
    separator=",",
) -> Dict[pathlib.Path, List[str]]:
    """Read column names for all files.

    The return value is a dict {path: list-of-col-names}

    :param files: files to read column names
    """
    paths = {}
    for path in files:
        # if path in known_files:
        #     f = known_files[path]
        # else:
        f = open_file(path)
        cols = f.readline().split(separator)

        paths[path] = cols
    return paths


def read_data(paths: pathlib.Path, cols: List[str]):
    pass
