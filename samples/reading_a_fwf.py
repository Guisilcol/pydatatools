"""This script demonstrates how to read a fixed width file using the Python connector"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], '..'))
"""Just to make sure the package is importable"""

from pydatatools import DataTools
import polars as pl

input_file = "./samples/data/fwf.txt"

dtools = DataTools.get_python_connector()
filesystem = dtools.get_filesystem()

lz = filesystem.get_file_to_lf(input_file, 'text')

layout = [
    (0, 20),
    (20, 20),
    (40, 7),
]

for index, (start, length) in enumerate(layout):
    lz = lz.with_columns(
        pl.col("value")
        .str.slice(start, length)
        .str.strip_chars()
        .alias("col" + str(index))
    )

lz = lz.drop("value")
df = lz.collect()

print(df)