"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import pathlib
import time
from typing import List

from fileplotter.csv import FILE_TYPES


def sort_and_limit_files(
    files: List[pathlib.Path], changed_since_secs=60 * 60
) -> List[pathlib.Path]:
    """Take a list of paths and sort them by change-time.

    Sort last-changed files first.
    Drop files that have not been changed since 'changed_since_secs'.
    """
    time_limit = time.time() - changed_since_secs
    files = [(f, f.stat().st_ctime) for f in files]
    files = [(f, t) for f, t in files if t > time_limit]
    files.sort(key=lambda t: t[1], reverse=True)
    return [t[0] for t in files]


def find_latest_files(
    folder: str, changed_since_secs=60 * 60 * 24
) -> List[pathlib.Path]:
    p = pathlib.Path(folder)
    files = []
    for t in FILE_TYPES:
        files.extend(p.glob(f"**/*{t}"))

    return sort_and_limit_files(files, changed_since_secs)
