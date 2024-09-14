from datetime import datetime
from unittest.mock import MagicMock
from typing import List
from pytest import fixture, MonkeyPatch
from unittest.mock import patch
import tempfile
import os
import glob

import polars as pl

from pydatatools.engine.python.catalog import GlueCatalog
from pydatatools.interfaces.catalog import TableMetadataTypeDef, ColumnMetadataTypeDef, PartitionMetadataTypeDef

@fixture
def aws_env_vars(monkeypatch: MonkeyPatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'KEY')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'SECRET')

@fixture
def catalog():
    return GlueCatalog(s3_cli=MagicMock(), glue_cli=MagicMock())

def test_get_table_metadata_with_csv_files(catalog: GlueCatalog, aws_env_vars):
    ######################
    # MOCK RETURN VALUES #
    ######################
    get_table_return_value = {
        'Table': {
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'},
                    {'Name': 'col2', 'Type': 'int'},
                ],
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'Location': 's3://bucket/prefix/',
                'SerdeInfo': {
                    'Parameters': {
                        'field.delim': ','
                    },
                }
            },
        }
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    expected_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'csv',
        'raw': get_table_return_value
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.glue_cli.get_table.return_value = get_table_return_value
    table_metadata = catalog.get_table_metadata('db', 'table')
    
    assert table_metadata == expected_value, 'Table metadata is not as expected'

def test_get_table_metadata_with_parquet_files(catalog: GlueCatalog, aws_env_vars):
    ######################
    # MOCK RETURN VALUES #
    ######################
    get_table_return_value = {
        'Table': {
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'},
                    {'Name': 'col2', 'Type': 'int'},
                ],
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'Location': 's3://bucket/prefix/'
            },
        }
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    expected_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': None,
        'file_extension': 'parquet',
        'raw': get_table_return_value
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.glue_cli.get_table.return_value = get_table_return_value
    table_metadata = catalog.get_table_metadata('db', 'table')
    
    assert table_metadata == expected_value, 'Table metadata is not as expected'

def test_get_table_metadata_with_partitions_columns(catalog: GlueCatalog, aws_env_vars):
    ######################
    # MOCK RETURN VALUES #
    ######################
    get_table_return_value = {
        'Table': {
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'},
                    {'Name': 'col2', 'Type': 'int'},
                ],
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'Location': 's3://bucket/prefix/'
            },
            'PartitionKeys': [
                {'Name': 'partition_col1', 'Type': 'string'},
                {'Name': 'partition_col2', 'Type': 'int'},
            ],
        }
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    expected_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': None,
        'file_extension': 'parquet',
        'raw': get_table_return_value
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.glue_cli.get_table.return_value = get_table_return_value
    table_metadata = catalog.get_table_metadata('db', 'table')
    
    assert table_metadata == expected_value, 'Table metadata is not as expected'
    
def test_get_table_to_lazyframe_using_csv_files(catalog: GlueCatalog, aws_env_vars):
    with tempfile.TemporaryDirectory() as temp_dir:
        
        ######################
        # MOCK RETURN VALUES #
        ######################
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ',',
            'file_extension': 'csv',
            'raw': {}
        }
        
        iterable_csv_content = [
            'col1,col2',
            'a,1',
            'b,2',
        ]
        
        csv_content = '\n'.join(iterable_csv_content)
        
        with open(temp_dir + '/file.csv', 'w') as f:
            f.write(csv_content)
        
        ###################
        # EXPECTED VALUES #
        ###################
        
        expected_lf = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################
        
        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
            result = catalog.get_table_to_lazyframe('db', 'table')
            assert result.collect().equals(expected_lf.collect()), 'LazyFrame is not as expected'
  
def test_get_table_to_lazyframe_using_parquet_files(catalog: GlueCatalog, aws_env_vars):
    with tempfile.TemporaryDirectory() as temp_dir:
        
        ######################
        # MOCK RETURN VALUES #
        ######################
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ',',
            'file_extension': 'parquet',
            'raw': {}
        }
                        
        pl.DataFrame({'col1': ['a', 'b'], 'col2': [1, 2]}).write_parquet(temp_dir + '/file1.parquet')
        pl.DataFrame({'col1': ['a', 'b'], 'col2': [3, 4]}).write_parquet(temp_dir + '/file2.parquet', compression='snappy')
        ###################
        # EXPECTED VALUES #
        ###################
        expected_lf = pl.LazyFrame({
            'col1': ['a', 'b', 'a', 'b'],
            'col2': [1, 2, 3, 4],
        })
        
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################
        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
            result = catalog.get_table_to_lazyframe('db', 'table')
            assert result.collect().equals(expected_lf.collect()), 'LazyFrame is not as expected'
            
def test_get_table_to_lazyframe_using_where_clause(catalog: GlueCatalog, aws_env_vars):
    with tempfile.TemporaryDirectory() as temp_dir:
        
        ######################
        # MOCK RETURN VALUES #
        ######################
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ',',
            'file_extension': 'parquet',
            'raw': {}
        }
                        
        pl.DataFrame({'col1': ['a', 'b'], 'col2': [1, 2]}).write_parquet(temp_dir + '/file1.parquet')
        pl.DataFrame({'col1': ['a', 'b'], 'col2': [3, 4]}).write_parquet(temp_dir + '/file2.parquet', compression='snappy')
        ###################
        # EXPECTED VALUES #
        ###################
        expected_lf = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################
        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
            where = 'col2 < 3'
            result = catalog.get_table_to_lazyframe('db', 'table', where)
            assert result.collect().equals(expected_lf.collect()), 'LazyFrame is not as expected'
            
def test_get_last_partition_to_lazyframe(catalog: GlueCatalog, aws_env_vars):
    with tempfile.TemporaryDirectory() as temp_dir:
        
        ######################
        # MOCK RETURN VALUES #
        ######################
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            ],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            ],
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ',',
            'file_extension': 'parquet',
            'raw': {}
        }
        
        get_last_partition_return_value: PartitionMetadataTypeDef = {
            'values': ['partition1'],
            'location': temp_dir,
            'creation_time': datetime(2021, 1, 1),
        }
          
        pl.DataFrame({'col1': ['a', 'b'], 'col2': [1, 2]}).write_parquet(temp_dir + '/file.parquet')
        ###################
        # EXPECTED VALUES #
        ###################
        expected_lf = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################
        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'get_last_partition', return_value=get_last_partition_return_value):
            result = catalog.get_last_partition_to_lazyframe('db', 'table')
            assert result.collect().equals(expected_lf.collect()), 'LazyFrame is not as expected'

def test_get_columns_table(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'csv',
        'raw': {}
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    expected_value: List[ColumnMetadataTypeDef] = [
        {'name': 'col1', 'type': 'string', 'is_partition_key': False},
        {'name': 'col2', 'type': 'int', 'is_partition_key': False},
    ]
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_columns_table('db', 'table')
        assert result == expected_value, 'Columns are not as expected'
        
def test_get_all_partitions(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_paginator_return_value = MagicMock()
    get_paginator_return_value.paginate.return_value = [
        {
            'Partitions': [
                {
                    'Values': ['2021-01-01'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/2021-01-01',
                    },
                    'CreationTime': datetime(2021, 1, 1),
                },
                {
                    'Values': ['2021-01-02'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/2021-01-02',
                    },
                    'CreationTime': datetime(2021, 1, 2),
                },
            ]
        }
    ]
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value: List[PartitionMetadataTypeDef] = [
        {
            'values': ['2021-01-01'],
            'location': 's3://bucket/prefix/2021-01-01',
            'creation_time': datetime(2021, 1, 1),
        },
        {
            'values': ['2021-01-02'],
            'location': 's3://bucket/prefix/2021-01-02',
            'creation_time': datetime(2021, 1, 2),
        },
    ]
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    catalog.glue_cli.get_paginator.return_value = get_paginator_return_value
    result = catalog.get_all_partitions('db', 'table')
    
    assert result == expected_value, 'Partitions are not as expected'
    
def test_get_partitioned_columns(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'csv',
        'raw': {}
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    expected_value: List[ColumnMetadataTypeDef] = [
        {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
        {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
    ]
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_partitioned_columns('db', 'table')
        assert result == expected_value, 'Partitioned columns are not as expected'  

def test_get_last_partition_sorting_by_alphanumeric_and_1_level_partition(catalog: GlueCatalog):    
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'parquet',
        'raw': {}
    }
    
    get_paginator_return_value = MagicMock()
    get_paginator_return_value.paginate.return_value = [
        {
            'Partitions': [
                {
                    'Values': ['A'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=A',
                    },
                    'CreationTime': datetime(2021, 1, 1),
                },
                {
                    'Values': ['B'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=B',
                    },
                    'CreationTime': datetime(2021, 1, 2),
                },
            ]
        }
    ]
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value: PartitionMetadataTypeDef = {
        'values': ['B'],
        'location': 's3://bucket/prefix/partition_col1=B',
        'creation_time': datetime(2021, 1, 2),
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    catalog.glue_cli.get_paginator.return_value = get_paginator_return_value
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_last_partition('db', 'table', 'alphanumeric')
        assert result == expected_value, 'Last partition is not as expected'

def test_get_last_partition_sorting_by_alphanumeric_and_2_levels_partition(catalog: GlueCatalog):    
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'parquet',
        'raw': {}
    }
    
    get_paginator_return_value = MagicMock()
    get_paginator_return_value.paginate.return_value = [
        {
            'Partitions': [
                {
                    'Values': ['A', 1],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=A/partition_col2=1',
                    },
                    'CreationTime': datetime(2021, 1, 1),
                },
                {
                    'Values': ['A', 2],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=B/partition_col2=2',
                    },
                    'CreationTime': datetime(2021, 1, 2),
                },
            ]
        }
    ]
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value: PartitionMetadataTypeDef = {
        'values': ['A', 2],
        'location': 's3://bucket/prefix/partition_col1=B/partition_col2=2',
        'creation_time': datetime(2021, 1, 2),
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    catalog.glue_cli.get_paginator.return_value = get_paginator_return_value
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_last_partition('db', 'table', 'alphanumeric')
        assert result == expected_value, 'Last partition is not as expected'
        
def test_get_last_partition_sorting_by_createtime_and_1_level_partition(catalog: GlueCatalog):    
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'parquet',
        'raw': {}
    }
    
    get_paginator_return_value = MagicMock()
    get_paginator_return_value.paginate.return_value = [
        {
            'Partitions': [
                {
                    'Values': ['A'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=A',
                    },
                    'CreationTime': datetime(2021, 1, 2),
                },
                {
                    'Values': ['B'],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=B',
                    },
                    'CreationTime': datetime(2021, 1, 1),
                },
            ]
        }
    ]
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value: PartitionMetadataTypeDef = {
        'values': ['A'],
        'location': 's3://bucket/prefix/partition_col1=A',
        'creation_time': datetime(2021, 1, 2),
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    catalog.glue_cli.get_paginator.return_value = get_paginator_return_value
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_last_partition('db', 'table', 'createtime')
        assert result == expected_value, 'Last partition is not as expected'
        
def test_get_last_partition_sorting_by_createtime_and_2_levels_partition(catalog: GlueCatalog):    
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'parquet',
        'raw': {}
    }
    
    get_paginator_return_value = MagicMock()
    get_paginator_return_value.paginate.return_value = [
        {
            'Partitions': [
                {
                    'Values': ['A', 1],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=A/partition_col2=1',
                    },
                    'CreationTime': datetime(2021, 1, 2),
                },
                {
                    'Values': ['B', 2],
                    'StorageDescriptor': {
                        'Location': 's3://bucket/prefix/partition_col1=B/partition_col2=2',
                    },
                    'CreationTime': datetime(2021, 1, 1),
                },
            ]
        }
    ]
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value: PartitionMetadataTypeDef = {
        'values': ['A', 1],
        'location': 's3://bucket/prefix/partition_col1=A/partition_col2=1',
        'creation_time': datetime(2021, 1, 2),
    }
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    catalog.glue_cli.get_paginator.return_value = get_paginator_return_value
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        result = catalog.get_last_partition('db', 'table', 'createtime')
        assert result == expected_value, 'Last partition is not as expected'
        
def test_get_last_partition_when_table_not_have_partition_column(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_table_metadata_return_value: TableMetadataTypeDef = {
        'columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'string', 'is_partition_key': False},
            {'name': 'col2', 'type': 'int', 'is_partition_key': False},
        ],
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': ',',
        'file_extension': 'parquet',
        'raw': {}
    }
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    # ValueError is expected when table does not have partition columns
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value):
        try:
            catalog.get_last_partition('db', 'table', 'createtime')
        except ValueError:
            pass
        else:
            assert False, 'ValueError was not raised'
            
def test_get_partition_count(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_all_partitions_return_value: List[PartitionMetadataTypeDef] = ['partition1', 'partition2']
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value = 2
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, 'get_all_partitions', return_value=get_all_partitions_return_value):
        result = catalog.get_partition_count('db', 'table')
        assert result == expected_value, 'Partition count is not as expected'
        
def test_check_if_table_exists_when_exists(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES # 
    ######################
    
    glue_cli_get_table_return = {}
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.glue_cli.get_table.return_value = glue_cli_get_table_return
    result = catalog.check_if_table_exists('db', 'table')
    assert result, 'Table exists check is not as expected'

def test_check_if_table_exists_when_not_exists(catalog: GlueCatalog):
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    class EntityNotFoundException(Exception):
        pass
    
    catalog.glue_cli.exceptions.EntityNotFoundException = EntityNotFoundException
    catalog.glue_cli.get_table.side_effect = EntityNotFoundException
    result = catalog.check_if_table_exists('db', 'table')
    assert not result, 'Table exists check is not as expected'

def test_check_empty_partition_when_have_files(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_partition_metadata_return_value: PartitionMetadataTypeDef = {
        'location': 's3://bucket/prefix/partition1'
    }
    
    get_s3_files_return_value = ['file1', 'file2']
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value = False
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, '_get_partition_metadata', return_value=get_partition_metadata_return_value), \
         patch.object(catalog, '_get_all_s3_keys_from_prefix', return_value=get_s3_files_return_value):
        result = catalog.check_empty_partition('db', 'table', 'partition1')
        assert result == expected_value, 'Empty partition check is not as expected'
        
def test_check_empty_partition_when_not_have_files(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_partition_metadata_return_value: PartitionMetadataTypeDef = {
        'location': 's3://bucket/prefix/partition1'
    }
    
    get_s3_files_return_value = []
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value = True
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    with patch.object(catalog, '_get_partition_metadata', return_value=get_partition_metadata_return_value), \
         patch.object(catalog, '_get_all_s3_keys_from_prefix', return_value=get_s3_files_return_value):
        result = catalog.check_empty_partition('db', 'table', 'partition1')
        assert result == expected_value, 'Empty partition check is not as expected'
       
def test_get_partitions_files(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    glue_cli_get_partition_return_value = {
        'Partition': {
            'StorageDescriptor': {
                'Location': 's3://bucket/prefix/partition1',
            },
        }
    }
    
    _get_all_s3_keys_from_prefix_mock = MagicMock()
    _get_all_s3_keys_from_prefix_mock.return_value = ['prefix/partition1/file1', 'prefix/partition1/file2']
    

    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value = [
        'bucket/prefix/partition1/file1',
        'bucket/prefix/partition1/file2',
    ]
    
    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.glue_cli.get_partition.return_value = glue_cli_get_partition_return_value
    
    with patch.object(catalog, '_get_all_s3_keys_from_prefix', _get_all_s3_keys_from_prefix_mock):
        result = catalog.get_partition_files('db', 'table', 'partition1')
        assert result == expected_value, 'Partitions files are not as expected'

def test_get_partition_size(catalog: GlueCatalog):
    ######################
    # MOCK RETURN VALUES #
    ######################
    
    get_partition_files_return_value = ['prefix/partition/file1', 'prefix/partition/file2']

    s3_cli_head_object_return_values = [
        {'ContentLength': 100},
        {'ContentLength': 200},
    ]
    
    
    ###################
    # EXPECTED VALUES #
    ###################
    
    expected_value = 300 / 1024**2

    ###################################
    # ASSERTIONS AND MOCKS ATRIBUTION #
    ###################################
    
    catalog.s3_cli.head_object.side_effect = s3_cli_head_object_return_values
    
    with patch.object(catalog, 'get_partition_files', return_value=get_partition_files_return_value):
        result = catalog.get_partition_size('db', 'table', 'partition1')
        assert result == expected_value, 'Partition size is not as expected'

def test_put_frame_to_table_when_lazyframe_and_not_have_partitioned_columns_and_uses_csv_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'file_extension': 'csv',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ';',
        }
        
        update_partitions_return_value = None
        
        frame = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = os.listdir(temp_dir)
        
        assert len(files) == 1, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_lazyframe_and_have_partitioned_columns_and_uses_csv_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},    
            ],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
            ],
            'file_extension': 'csv',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ';',
        }
        
        update_partitions_return_value = None
        
        frame = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
            'partition_col1': ['A', 'B'],
            'partition_col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = glob.glob(temp_dir + '/**/*.csv', recursive=True)
        assert len(files) == 2, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_lazyframe_and_not_have_partitioned_columns_and_uses_parquet_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'file_extension': 'parquet',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': None,
        }
        
        update_partitions_return_value = None
        
        frame = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = os.listdir(temp_dir)
        
        assert len(files) == 1, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_lazyframe_and_have_partitioned_columns_and_uses_parquet_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},    
            ],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
            ],
            'file_extension': 'parquet',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': None,
        }
        
        update_partitions_return_value = None
        
        frame = pl.LazyFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
            'partition_col1': ['A', 'B'],
            'partition_col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = glob.glob(temp_dir + '/**/*.parquet', recursive=True)
        assert len(files) == 2, 'Number of files is not as expected'        
        
def test_put_frame_to_table_when_dataframe_and_not_have_partitioned_columns_and_uses_csv_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'file_extension': 'csv',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ';',
        }
        
        update_partitions_return_value = None
        
        frame = pl.DataFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = os.listdir(temp_dir)
        
        assert len(files) == 1, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_dataframe_and_have_partitioned_columns_and_uses_csv_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},    
            ],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
            ],
            'file_extension': 'csv',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': ';',
        }
        
        update_partitions_return_value = None
        
        frame = pl.DataFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
            'partition_col1': ['A', 'B'],
            'partition_col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = glob.glob(temp_dir + '/**/*.csv', recursive=True)
        assert len(files) == 2, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_dataframe_and_not_have_partitioned_columns_and_uses_parquet_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'file_extension': 'parquet',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': None,
        }
        
        update_partitions_return_value = None
        
        frame = pl.DataFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = os.listdir(temp_dir)
        
        assert len(files) == 1, 'Number of files is not as expected'
        
def test_put_frame_to_table_when_dataframe_and_have_partitioned_columns_and_uses_parquet_files(catalog: GlueCatalog):
    with tempfile.TemporaryDirectory() as temp_dir:
    
        ######################
        # MOCK RETURN VALUES #
        ######################
        
        get_table_metadata_return_value: TableMetadataTypeDef = {
            'columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
            ],
            'partitioned_columns': [
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},    
            ],
            'all_columns': [
                {'name': 'col1', 'type': 'string', 'is_partition_key': False},
                {'name': 'col2', 'type': 'int', 'is_partition_key': False},
                {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
                {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
            ],
            'file_extension': 'parquet',
            'database': 'db',
            'table': 'table',
            'location': temp_dir,
            'delimiter': None,
        }
        
        update_partitions_return_value = None
        
        frame = pl.DataFrame({
            'col1': ['a', 'b'],
            'col2': [1, 2],
            'partition_col1': ['A', 'B'],
            'partition_col2': [1, 2],
        })
        
        database = 'db'
        table = 'table'
        compression = 'snappy'
        type = 'overwrite'
        
        def _create_and_open_file(filepath: str, mode: str):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            return open(filepath, mode)
        
        filesystem_mock = MagicMock()
        filesystem_mock.open = _create_and_open_file
                
        ###################################
        # ASSERTIONS AND MOCKS ATRIBUTION #
        ###################################

        with patch.object(catalog, 'get_table_metadata', return_value=get_table_metadata_return_value), \
             patch.object(catalog, 'update_partitions', return_value=update_partitions_return_value), \
             patch.object(catalog, '_get_filesystem', return_value=filesystem_mock):
                 
            catalog.put_frame_to_table(frame, database, table, compression, type)
            
        files = glob.glob(temp_dir + '/**/*.parquet', recursive=True)
        assert len(files) == 2, 'Number of files is not as expected'        
        
def test_delete_partitions(catalog: GlueCatalog):    
    get_partition_files_mock = MagicMock()
    get_partition_files_mock.return_value = ['bucket/prefix/partition1=partition1/file1', 'bucket/prefix/partition2=partition2/file2']
    
    database = 'db'
    table = 'table'
    partitions = [('partition1', ), ('partition2', )]
    
    
    with patch.object(catalog, 'get_partition_files', get_partition_files_mock):
        catalog.delete_partitions(database, table, partitions)
    
    catalog.s3_cli.delete_objects.assert_any_call(
        Bucket='bucket',
        Delete={
            'Objects': [
                {'Key': 'prefix/partition1=partition1/file1'},
                {'Key': 'prefix/partition2=partition2/file2'},
            ]
        }
    )
    
    catalog.glue_cli.delete_partition.assert_any_call(
        DatabaseName='db',
        TableName='table',
        PartitionValues=('partition1', )
    )
    
    catalog.glue_cli.delete_partition.assert_any_call(
        DatabaseName='db',
        TableName='table',
        PartitionValues=('partition2', )
    )
    
def test_check_table_schema_when_frame_match(catalog: GlueCatalog):
    
    get_table_metadata_mock = MagicMock()
    get_table_metadata_mock.return_value = {
        'columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col2', 'type': 'smallint', 'is_partition_key': False},
            
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col2', 'type': 'smallint', 'is_partition_key': False},
        ],
        'file_extension': 'parquet',
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': None,
    }
    
    frame = pl.DataFrame({
        'col1': [1],
        'col2': [1],
    })
    
    database = 'db'
    table = 'table'
    
    with patch.object(catalog, 'get_table_metadata', get_table_metadata_mock):
        assert catalog.check_table_schema(frame, database, table), 'Table schema check is not as expected'

def test_check_table_schema_when_frame_not_match(catalog: GlueCatalog):
    get_table_metadata_mock = MagicMock()
    get_table_metadata_mock.return_value = {
        'columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col', 'type': 'string', 'is_partition_key': False},
            
        ],
        'partitioned_columns': [],
        'all_columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col', 'type': 'string', 'is_partition_key': False},
        ],
        'file_extension': 'parquet',
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': None,
    }
    
    frame = pl.DataFrame({
        'col1': [1],
        'col2': [1.0],
    })
    
    database = 'db'
    table = 'table'
    
    with patch.object(catalog, 'get_table_metadata', get_table_metadata_mock):
        assert not catalog.check_table_schema(frame, database, table), 'Table schema check is not as expected'
    
def test_get_location(catalog: GlueCatalog):
    get_table_metadata_mock = MagicMock()
    get_table_metadata_mock.return_value = TableMetadataTypeDef({
        'location': 's3://bucket/prefix/',
    })
    
    with patch.object(catalog, 'get_table_metadata', get_table_metadata_mock):
        assert catalog.get_location('db', 'table') == 's3://bucket/prefix/', 'Table location is not as expected'
        
def test_update_partitions(catalog: GlueCatalog):
    get_table_metadata_mock = MagicMock()
    get_all_partitions_mock = MagicMock()
    _get_all_s3_keys_from_prefix_mock = MagicMock()
    catalog.glue_cli.create_partition = MagicMock()
    catalog.glue_cli.batch_delete_partition = MagicMock()
    
    get_table_metadata_mock.return_value = TableMetadataTypeDef({
        'columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col2', 'type': 'smallint', 'is_partition_key': False},
        ],
        'partitioned_columns': [
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'all_columns': [
            {'name': 'col1', 'type': 'tinyint', 'is_partition_key': False},
            {'name': 'col2', 'type': 'smallint', 'is_partition_key': False},
            {'name': 'partition_col1', 'type': 'string', 'is_partition_key': True},
            {'name': 'partition_col2', 'type': 'int', 'is_partition_key': True},
        ],
        'file_extension': 'parquet',
        'database': 'db',
        'table': 'table',
        'location': 's3://bucket/prefix/',
        'delimiter': None,
        'raw': {
            'StorageDescriptor': {}
        }
    })
    
    get_all_partitions_mock.return_value = [
        PartitionMetadataTypeDef({'values': ['A', 1], 'location': 's3://bucket/prefix/partition_col1=A/partition_col2=1'}),
        PartitionMetadataTypeDef({'values': ['B', 2], 'location': 's3://bucket/prefix/partition_col1=B/partition_col2=2'}),
        PartitionMetadataTypeDef({'values': ['C', 3], 'location': 's3://bucket/prefix/partition_col1=C/partition_col2=3'}),
    ]
    
    _get_all_s3_keys_from_prefix_mock.return_value = [
        'prefix/partition_col1=A/partition_col2=1/file1', 
        'prefix/partition_col1=B/partition_col2=2/file2'
    ]
    
    
    with patch.object(catalog, 'get_table_metadata', get_table_metadata_mock), \
         patch.object(catalog, 'get_all_partitions', get_all_partitions_mock), \
         patch.object(catalog, '_get_all_s3_keys_from_prefix', _get_all_s3_keys_from_prefix_mock):
        
        catalog.update_partitions('db', 'table')
