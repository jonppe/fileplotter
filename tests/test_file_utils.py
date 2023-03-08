"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import time

from fileplotter import csv, file_utils


def test_sort_and_limit_files(tmp_path):
    fnames = ["a.csv", "b.csv.gz", "c.csv"]
    paths = [tmp_path / fname for fname in fnames]
    a, b, c = paths
    for path in paths:
        path.touch()
        time.sleep(0.05)

    paths = file_utils.sort_and_limit_files(paths)
    assert paths[0] == c
    assert paths[2] == a

    b.touch()
    paths = file_utils.sort_and_limit_files(paths)
    assert paths[0] == b
    assert paths[2] == a
