"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""
import pathlib

from fileplotter import csv

TESTDATA = pathlib.Path(__file__).parent / "testdata"


def test_load_dataframe_continuous(tmp_path):
    with open(tmp_path / "x.csv", "a") as write_f:
        write_f.write("A,B,C\n")
        write_f.write("1,2,3\n")
        write_f.write("11,12,13\n")
        write_f.flush()

        cols = ["A", "C"]
        df, read_f = csv.load_dataframe_continuous(tmp_path / "x.csv", usecols=cols)
        assert len(df) == 2
        assert list(df.keys()) == cols
        assert list(df["A"]) == [1, 11]
        assert list(df["C"]) == [3, 13]

        write_f.write("21,22,23\n")
        write_f.write("31,32,33\n")
        write_f.flush()
        original_cols = ["A", "B", "C"]
        df, write_f = csv.load_dataframe_continuous(
            tmp_path / "x.csv", usecols=cols, cols=original_cols, f=read_f
        )
        assert len(df) == 2
        assert list(df.keys()) == cols
        assert list(df["A"]) == [21, 31]
        assert list(df["C"]) == [23, 33]


def test_load_dataframe():
    files = ["a.csv", "a_copy.csv.gz"]
    for fname in files:

        df = csv.load_dataframe(TESTDATA / fname)
        assert len(df) == 2
        assert list(df.keys()) == ["first", "second", "third"]


# def test_read_data()
