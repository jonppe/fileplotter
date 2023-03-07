"""
    Copyright (c) Vaisala Oyj. All rights reserved.
"""

from unittest import mock

from fileplotter import data


def test_read_data_file(tmp_path):
    path = tmp_path / "x.csv"
    with open(path, "a") as write_f:
        write_f.write("A,B,C\n")
        write_f.write("1,2,3\n")
        write_f.write("11,12,13\n")
        write_f.flush()

        cols = ["A", "C", "X"]
        old_data = {}
        figure = mock.MagicMock()
        data.read_data_file(path, cols, old_data, figure)
        assert figure.line.call_count == 2
        assert len(old_data[path].data["A"].data["y"]) == 2
        write_f.write("21,22,23\n")
        write_f.write("31,32,33\n")
        write_f.flush()
        data.read_data_file(path, cols, old_data, figure)
        assert figure.line.call_count == 2
        assert len(old_data[path].data["A"].data["y"]) == 4
