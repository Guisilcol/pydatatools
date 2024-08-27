from setuptools import setup, find_packages

setup(
    name="pydatatools",
    version="0.1",
    packages=find_packages(),
    requires=[
        'boto3',
        'polars',
        's3fs'
    ]
)