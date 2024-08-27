from datetime import datetime
from typing import List, Optional, Union, Tuple, Literal
from os import environ, path
from uuid import uuid4

import boto3
import s3fs
import polars as pl

from pydatatools.interfaces.catalog import ColumnMetadata, TableMetadata, PartitionMetadata, CatalogIntegratedWithPolars

class GlueCatalog(CatalogIntegratedWithPolars):
    """A class to represent Glue Catalog"""
    
    def __init__(self, s3_cli = boto3.client('s3'), glue_cli = boto3.client('glue')):
        """Initialize GlueCatalog with S3 and Glue clients."""
        self.s3_cli = s3_cli
        self.glue_cli = glue_cli
    
    def _break_iterable_in_chunks(self, iterable: List, chunk_size: int) -> List[List]:
        """Breaks an iterable into chunks of size chunk_size."""
        return [iterable[i:i + chunk_size] for i in range(0, len(iterable), chunk_size)]
        
    def _get_partition_metadata(self, database: str, table: str, partition: Tuple[str, ...]) -> PartitionMetadata:
        """Get metadata for a specific partition."""
        response = self.glue_cli.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=list(partition)
        )
        
        return {
            'values': partition,
            'location': response['Partition']['StorageDescriptor']['Location']
        }
        
    def _scan_file_to_lf(self, key: str, metadata: TableMetadata) -> pl.LazyFrame:
        """Scan a file from S3 into a Polars LazyFrame."""
        storage_options = {
            "aws_access_key_id": environ.get('AWS_ACCESS_KEY_ID'),
            "aws_secret_access_key": environ.get('AWS_SECRET_ACCESS_KEY'),
            "aws_region": environ.get('AWS_REGION', environ.get('AWS_DEFAULT_REGION')),
        }
                
        if metadata['file_extension'] == 'parquet':
            return pl.scan_parquet(key, storage_options=storage_options)
        
        elif metadata['file_extension'] == 'csv':
            return pl.scan_csv(key, storage_options=storage_options, separator=metadata['delimiter'])
        
        raise ValueError(f"Extension {metadata['file_extension']} not supported. For now, only 'parquet' and 'csv' are supported in AWS Glue Catalog")
    
    def _get_s3_files(self, bucket: str, prefix: str) -> List[str]:
        """Get a list of all files under a specific S3 prefix."""
        paginator = self.s3_cli.get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
        )
        
        files = []
        for page in response:
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
                
        return files
   
    def _purge_s3_prefix(self, bucket: str, prefix: str) -> None:
        """Delete all files under a specific S3 prefix."""
        files = self._get_s3_files(bucket, prefix)
        
        if len(files) > 0:
            chunks = self._break_iterable_in_chunks(files, 1000)
            
            for files in chunks:
                self.s3_cli.delete_objects(
                    Bucket=bucket,
                    Delete={
                        'Objects': [{'Key': file} for file in files]
                    }
                )
    
    def _purge_all_partitions(self, database: str, table: str) -> None:
        """Delete all partitions of a table."""
        partitions = self.get_all_partitions(database, table)
        
        if len(partitions) > 0:
            chunks = self._break_iterable_in_chunks(partitions, 25)
            
            for chunk in chunks:
                self.glue_cli.batch_delete_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionsToDelete=[{'Values': x['values']} for x in chunk]
                )
    
    def _get_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(anon=False)
        
    def _generate_filename(self, extension: str, datetime: datetime = datetime.now()) -> str:
        return f"{datetime.strftime('%Y_%m_%d_%H_%M_%S')}.{extension}"
        
    def get_table_metadata(self, database: str, table: str) -> TableMetadata:
        """Retrieve metadata for a specific table."""
        response = self.glue_cli.get_table(
            DatabaseName=database,
            Name=table
        )
        
        storage_descriptor = response['Table']['StorageDescriptor']
        input_format = storage_descriptor['InputFormat']
        
        file_type_mapping = {
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'parquet',
            'org.apache.hadoop.mapred.TextInputFormat': 'csv'
        }
        
        file_extension = file_type_mapping.get(input_format)
        
        if not file_extension:
            raise ValueError(f"The {input_format} is not supported")
        
        delimiter = (
            storage_descriptor
            .get('SerdeInfo', {})
            .get('Parameters', {})
            .get('field.delim')
        )
         
        columns = [
            ColumnMetadata({
                'name': col['Name'],
                'type': col['Type'],
                'is_partition_key': False
            }) for col in storage_descriptor['Columns']]
        
        partition_keys = [
            ColumnMetadata({
                'name': col['Name'],
                'type': col['Type'],
                'is_partition_key': True
            }) for col in response['Table'].get('PartitionKeys', [])] 
        
        all_columns = columns + partition_keys
        
        return TableMetadata({
            'database': database,
            'table': table,
            'location': storage_descriptor['Location'],
            'columns': columns,
            'partitioned_columns': partition_keys,
            'all_columns': all_columns,
            'file_extension': file_extension,
            'delimiter': delimiter,
            'raw': response
        })
   
    def get_table_to_lazyframe(self, database: str, table: str, where: Optional[str] = None, columns: Optional[List] = None) -> pl.LazyFrame:
        """Retrieve table as a Polars LazyFrame."""
        metadata = self.get_table_metadata(database, table)
        glob_pattern = f"{metadata['location']}/**/*.{metadata['file_extension']}"
        
        lf = self._scan_file_to_lf(glob_pattern, metadata)
        lf = lf.select(columns) if columns else lf
        
        if where:
            sql = f'select * from self where {where}' 
            return lf.sql(sql)

        return lf
        
    def get_last_partition_to_lazyframe(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime'] = 'alphanumeric') -> pl.LazyFrame:
        """Retrieve the last partition of a table as a Polars LazyFrame."""
        last_partition = self.get_last_partition(database, table, sort_by)     
        metadata = self.get_table_metadata(database, table)
        return self._scan_file_to_lf(last_partition['location'], metadata)
    
    def get_columns_table(self, database: str, table: str) -> List[str]:
        """Get all columns of a table."""
        return self.get_table_metadata(database, table)['all_columns']
    
    def get_all_partitions(self, database: str, table: str) -> List[PartitionMetadata]:
        """Get all partitions of a table."""
        paginator = self.glue_cli.get_paginator('get_partitions')
        response = paginator.paginate(
            DatabaseName=database,
            TableName=table
        )
        
        partitions: List[PartitionMetadata] = []
        for page in response:
            for partition in page.get('Partitions', []):
                partitions.append({
                    'values': partition['Values'],
                    'location': partition['StorageDescriptor']['Location'],
                    'creation_time': partition['CreationTime']
                })
        
        return partitions
    
    def get_partitioned_columns(self, database: str, table: str) -> List[ColumnMetadata]:
        """Get partitioned columns of a table."""
        return self.get_table_metadata(database, table)['partitioned_columns']
    
    def get_last_partition(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime']) -> Optional[PartitionMetadata]:
        """Get the last partition of a table."""
        metadata = self.get_table_metadata(database, table)
        if not metadata['partitioned_columns']:
            raise ValueError("Table has no partition columns")
        
        partitions = self.get_all_partitions(database, table)
        
        if sort_by == 'alphanumeric':
            partitions = sorted(partitions, key=lambda x: x['values'])
            
        elif sort_by == 'createtime':
            partitions = sorted(partitions, key=lambda x: x['creation_time'])
        
        last_partition = partitions[-1]
        
        return last_partition
    
    def get_partition_count(self, database: str, table: str) -> int:
        """Get the number of partitions in a table."""
        partitions = self.get_all_partitions(database, table)
        return len(partitions)
    
    def check_if_table_exists(self, database: str, table: str) -> bool:
        """Check if a table exists in the Glue catalog."""
        try:
            self.glue_cli.get_table(
                DatabaseName=database,
                Name=table
            )
            return True
            
        except self.glue_cli.exceptions.EntityNotFoundException:
            return False

    def check_empty_partition(self, database: str, table: str, partition: Tuple[str, ...]) -> bool:
        """Check if a partition is empty."""
        partition_location = self._get_partition_metadata(database, table, partition)['location']        
        bucket, prefix = partition_location.split('/', 1)
        
        files = self._get_s3_files(bucket, prefix)
        
        return len(files) == 0
        
    def get_partition_files(self, database: str, table: str, partition: Tuple[str, ...]) -> List[str]:
        """Get all files in a specific partition."""
        response = self.glue_cli.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=list(partition)
        )
        
        partition_location: str = response['Partition']['StorageDescriptor']['Location']
        partition_location = partition_location.replace('s3://', '', 1)
        bucket, prefix = partition_location.split('/', 1) 
        
        return [f'{bucket}/{key}' for key in self._get_s3_files(bucket, prefix)]
    
    def get_partition_size(self, database: str, table: str, partition: Tuple[str, ...]) -> float:
        """Get the size of a specific partition in MB."""
        files = self.get_partition_files(database, table, partition)
        total_size = 0
        
        for file in files:
            bucket, key = file.split('/', 1)  

            response = self.s3_cli.head_object(
                Bucket=bucket,
                Key=key
            )
            
            total_size += response['ContentLength']
        
        return total_size / 1024**2
    
    def put_frame_to_table(self,    frame: Union[pl.LazyFrame, pl.DataFrame], 
                                    database: str, 
                                    table: str, 
                                    compression: Optional[str] = 'snappy', 
                                    type: Literal['overwrite', 'append'] = 'overwrite'
                                    ) -> None:
                                    
        metadata = self.get_table_metadata(database, table)
        table_columns = [x['name'] for x in metadata['all_columns']]
        partition_columns = [x['name'] for x in metadata['partitioned_columns']]
        file_extension = metadata['file_extension']
        
        if type == 'overwrite':
            bucket, prefix = metadata['location'].replace('s3://', '', 1).split('/', 1)
            self._purge_s3_prefix(bucket, prefix)
            self._purge_all_partitions(database, table)

        frame = frame.select(table_columns)
        
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect(streaming=True)
        
        have_partitioned_columns = len(metadata['partitioned_columns']) > 0
        if have_partitioned_columns:
            partition_columns = [x['name'] for x in metadata['partitioned_columns']]
            groups = frame.group_by(partition_columns)
            
            for partition_values, df in groups: 
                partition_name_and_value = list(zip(partition_columns, partition_values))
                
                base_uri = metadata['location']
                partition_uri = '/'.join([f'{name}={value}' for name, value in partition_name_and_value])
                filename = self._generate_filename(file_extension)
                
                filepath = f'{base_uri}/{partition_uri}/{filename}'
                
                with self._get_filesystem().open(filepath, 'wb') as f:
                    if file_extension == 'csv':
                        df.write_csv(f, separator=metadata['delimiter'])
                    elif file_extension == 'parquet':
                        df.write_parquet(f, compression=compression)
                    
        else: 
            base_uri = metadata['location']
            filename = self._generate_filename(file_extension)
            filepath = f'{base_uri}/{filename}'
            
            with self._get_filesystem().open(filepath, 'wb') as f:
                if file_extension == 'csv':
                    frame.write_csv(f, separator=metadata['delimiter'])
                elif file_extension == 'parquet':
                    frame.write_parquet(f, compression=compression)        
        
        self.update_partitions(database, table)
        
    def delete_partitions(self, database: str, table: str, partitions: List[Tuple[str, ...]]) -> None:
        """Delete specific partitions from a table."""
        for partition in partitions:
            
            files = self.get_partition_files(database, table, partition)
            
            if len(files) > 0:
                bucket, _ = files[0].split('/', 1)
                files_without_bucket = [file.split('/', 1)[1] for file in files]
                
                chunks = self._break_iterable_in_chunks(files_without_bucket, 1000)
                for chunk in chunks:
                    self.s3_cli.delete_objects(
                        Bucket=bucket,
                        Delete={
                            'Objects': [{'Key': file} for file in chunk]
                        }
                    )
                
            self.glue_cli.delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionValues=partition
            )

    def check_table_schema(self, frame: Union[pl.LazyFrame, pl.DataFrame], database: str, table: str) -> bool:
        """
        Validate whether the DataFrame schema matches the table schema in Glue.
        """
        
        # TODO: Implement dataype mapping and datatype schema validation
        
        metadata = self.get_table_metadata(database, table)
        columns = metadata['all_columns']
        
        frame_datatypes = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
        
        #polars_to_glue_mapping = {
        #    "Int8": "tinyint",
        #    "Int16": "smallint",
        #    "Int32": "int",
        #    "Int64": "bigint",
        #    "UInt8": "tinyint",
        #    "UInt16": "smallint",
        #    "UInt32": "int",
        #    "UInt64": "bigint",
        #    "Float32": "float",
        #    "Float64": "double",
        #    "Utf8": "string",
        #    "Bool": "boolean",
        #    "Date": "date",
        #    "Datetime": "timestamp",
        #    "Time": "string", 
        #    "Duration": "bigint", 
        #    "List": "array",  
        #    "Struct": "struct", 
        #}

        for column in columns:
            column_name = column['name']
            #column_type = column['type']
            
            if column_name not in frame_datatypes:
                return False
    
        return True

    def get_location(self, database: str, table: str) -> str:
        """Get the S3 location of a table."""
        return self.get_table_metadata(database, table)['location']
        
    def update_partitions(self, database: str, table: str) -> None:
        """Update partitions in the Glue catalog to match S3."""
        metadata = self.get_table_metadata(database, table)
        
        if not metadata['partitioned_columns']:
            return
        
        current_partitions = self.get_all_partitions(database, table)
        current_partitions_values = [x['values'] for x in current_partitions]
        
        bucket, prefix = metadata['location'].replace('s3://', '', 1).split('/', 1)
        files = self._get_s3_files(bucket, prefix)
        
        partitions_values_in_s3 = []
        for key in files:
            location_with_slash_at_end = prefix 
            if not location_with_slash_at_end.endswith('/'):
                location_with_slash_at_end += '/'
                
            partition_dir: str = key.replace(location_with_slash_at_end, '', 1).rsplit('/', 1)[0]
            
            str_partition_values: List[str] = partition_dir.split('/')
            partition_values: List[Tuple[str, str]] = []
            for str_partition_value in str_partition_values:
                partition_value = str_partition_value.split('=')
                partition_values.append((tuple(partition_value)))
            
            partitions_values_in_s3.append(partition_values)
        
        unique_partitions_values_in_s3 = list(set(tuple(x) for x in partitions_values_in_s3))
        
        partitions_to_delete = []
        for partition in current_partitions_values:
            if partition not in unique_partitions_values_in_s3:
                partitions_to_delete.append(partition)
                
        partitions_to_create = []
        for partition in unique_partitions_values_in_s3:
            if partition not in current_partitions_values:
                partitions_to_create.append(partition)
                
        chunks = self._break_iterable_in_chunks(partitions_to_delete, 25)
        for chunk in chunks:
            self.glue_cli.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=[{'Values': x} for x in chunk]
            )
                
        for partition in partitions_to_create:
            partition_input = {
                'Values': [x[1] for x in partition],
                'StorageDescriptor': metadata['raw'].get('StorageDescriptor', {})
            }
            
            self.glue_cli.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput=partition_input
            )

    def truncate_table(self, database: str, table: str) -> None:
        """Truncate a table by deleting all partitions and files."""
        self._purge_all_partitions(database, table)
        
        bucket, prefix = self.get_table_metadata(database, table)['location'] \
                            .replace('s3://', '', 1) \
                            .split('/', 1)
        
        self._purge_s3_prefix(bucket, prefix)