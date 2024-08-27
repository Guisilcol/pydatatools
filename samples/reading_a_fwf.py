"""This script demonstrates how to read a fixed width file using the Python connector"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], '..'))
"""Just to make sure the package is importable"""

from pydatatools.datatools import DataTools
import polars as pl

input_file = "./samples/data/fwf.txt"

dtools = DataTools.get_python_connector()
filesystem = dtools.get_filesystem()
lz = filesystem.get_file_to_lf(input_file, 'text')

lz = lz.with_columns(
    pl.col("value").str.slice(0, 20).alias("col1").str.strip_chars(),
    pl.col("value").str.slice(20, 20).alias("col2").str.strip_chars(),
    pl.col("value").str.slice(40, 7).alias("col3").str.strip_chars(),
)

lz = lz.drop("value")
df = lz.collect()

print(df)