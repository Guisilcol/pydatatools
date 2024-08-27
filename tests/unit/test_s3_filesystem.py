import tempfile
from unittest.mock import Mock

import pytest
import polars as pl

from pydatatools.engine.python.filesystem import S3Filesystem

@pytest.fixture
def s3_filesystem():
    s3_cli = Mock()
    return S3Filesystem(s3_cli=s3_cli)

@pytest.fixture
def temp_dir():
    return tempfile.TemporaryDirectory()

def test_get_file_to_lf(s3_filesystem: S3Filesystem, temp_dir: tempfile.TemporaryDirectory):
    filepath = f'{temp_dir.name}/file.text'
    
    data = [
        'VALUE_1    VALUE_2    VALUE_3',
        'VALUE_4    VALUE_5    VALUE_6',
    ]
    
    with open(filepath, 'w') as f:
        f.write('\n'.join(data))
    
    lf = s3_filesystem.get_file_to_lf(filepath, file_format='text')
    
    assert lf.collect_schema().names() == ['value']
    assert lf.collect().shape == (2, 1)