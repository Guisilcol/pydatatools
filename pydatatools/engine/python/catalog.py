from datetime import datetime
from typing import List, Optional, Union, Tuple, Literal
from os import environ

import boto3
import s3fs
import polars as pl

from pydatatools.interfaces.catalog import ColumnMetadataTypeDef, TableMetadataTypeDef, PartitionMetadataTypeDef, CatalogIntegratedWithPolars

class GlueCatalog(CatalogIntegratedWithPolars):
    """A class to represent Glue Catalog integration with Polars."""

    def __init__(self, s3_cli=boto3.client('s3'), glue_cli=boto3.client('glue')):
        """
        Initialize GlueCatalog with S3 and Glue clients.

        Args:
            s3_cli: Boto3 S3 client.
            glue_cli: Boto3 Glue client.
        """
        self.s3_cli = s3_cli
        self.glue_cli = glue_cli

    ############################
    # Private methods: Helpers #
    ############################

    def _break_iterable_in_chunks(self, iterable: List, chunk_size: int) -> List[List]:
        """
        Breaks an iterable into chunks of size chunk_size.

        Args:
            iterable: List of elements to be chunked.
            chunk_size: Size of each chunk.

        Returns:
            List of chunked elements.
        """
        return [iterable[i:i + chunk_size] for i in range(0, len(iterable), chunk_size)]

    def _get_filesystem(self) -> s3fs.S3FileSystem:
        """
        Get an S3 filesystem instance.

        Returns:
            S3 filesystem instance.
        """
        return s3fs.S3FileSystem(anon=False)

    def _generate_filename(self, extension: str, datetime: datetime = datetime.now()) -> str:
        """
        Generate a filename with a timestamp.

        Args:
            extension: File extension.
            datetime: Datetime object.

        Returns:
            Generated filename string.
        """
        return f"{datetime.strftime('%Y_%m_%d_%H_%M_%S')}.{extension}"

    def _get_bucket_and_prefix(self, s3_path: str) -> Tuple[str, str]:
        """
        Get the bucket and prefix from an S3 path.

        Args:
            s3_path: S3 path string.

        Returns:
            Tuple with the bucket and prefix.
        """
        bucket, prefix = s3_path.replace('s3://', '', 1).split('/', 1)
        return bucket, prefix

    #############################
    # Private methods: Metadata #
    #############################

    def _get_partition_metadata(self, database: str, table: str, partition: Tuple[str, ...]) -> PartitionMetadataTypeDef:
        """
        Get metadata for a specific partition.

        Args:
            database: Name of the Glue database.
            table: Name of the Glue table.
            partition: Partition values tuple.

        Returns:
            Metadata for the specified partition.
        """
        response = self.glue_cli.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=list(partition)
        )

        return {
            'values': partition,
            'location': response['Partition']['StorageDescriptor']['Location']
        }

    def _map_serde_library_to_file_extension(self, serde_library: str) -> Optional[str]:
        """
        Map a SerDe library to a file extension.

        Args:
            serde_library: SerDe library string.

        Returns:
            File extension string.
        """
        file_type_mapping = {
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'parquet',
            'org.apache.hadoop.mapred.TextInputFormat': 'csv'
        }

        return file_type_mapping.get(serde_library)

    ############################
    # Private methods: Readers #
    ############################

    def _scan_file_to_lf(self, key: str, metadata: TableMetadataTypeDef) -> pl.LazyFrame:
        """
        Scan a file from S3 into a Polars LazyFrame.

        Args:
            key: S3 object key.
            metadata: Table metadata.

        Returns:
            Polars LazyFrame.
        """
        storage_options = {
            "aws_access_key_id":        environ.get('AWS_ACCESS_KEY_ID'),
            "aws_secret_access_key":    environ.get('AWS_SECRET_ACCESS_KEY'),
            "aws_region":               environ.get('AWS_REGION', environ.get('AWS_DEFAULT_REGION')),
        }

        if metadata['file_extension'] == 'parquet':
            return pl.scan_parquet(key, storage_options=storage_options)

        elif metadata['file_extension'] == 'csv':
            return pl.scan_csv(key, storage_options=storage_options, separator=metadata['delimiter'])

        raise ValueError(f"Extension {metadata['file_extension']} not supported. For now, only 'parquet' and 'csv' are supported in AWS Glue Catalog")

    def _get_all_s3_keys_from_prefix(self, bucket: str, prefix: str) -> List[str]:
        """
        Get a list of all files under a specific S3 prefix.

        Args:
            bucket: S3 bucket name.
            prefix: S3 prefix path.

        Returns:
            List of S3 object keys.
        """
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

    ############################
    # Private methods: Writers #
    ############################
    
    def _write_partitioned_df(self, df: pl.DataFrame, metadata: TableMetadataTypeDef) -> None:
        partition_columns = [x['name'] for x in metadata['partitioned_columns']]
        file_extension = metadata['file_extension']
        groups = df.group_by(partition_columns)

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
                    df.write_parquet(f, compression='snappy')

    def _write_non_partitioned_df(self, df: pl.DataFrame, metadata: TableMetadataTypeDef) -> None:
        base_uri = metadata['location']
        filename = self._generate_filename(metadata['file_extension'])
        filepath = f'{base_uri}/{filename}'

        with self._get_filesystem().open(filepath, 'wb') as f:
            if metadata['file_extension'] == 'csv':
                df.write_csv(f, separator=metadata['delimiter'])
            elif metadata['file_extension'] == 'parquet':
                df.write_parquet(f, compression='snappy')

    ############################
    # Private methods: Purgers #
    ############################

    def _purge_s3_prefix(self, bucket: str, prefix: str) -> None:
        """
        Delete all files under a specific S3 prefix.

        Args:
            bucket: S3 bucket name.
            prefix: S3 prefix path.
        """
        files = self._get_all_s3_keys_from_prefix(bucket, prefix)

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
        """
        Delete all partitions of a table.

        Args:
            database: Glue database name.
            table: Glue table name.
        """
        partitions = self.get_all_partitions(database, table)

        if len(partitions) == 0: return
        
        chunks = self._break_iterable_in_chunks(partitions, 25)

        for chunk in chunks:
            self.glue_cli.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=[{'Values': x['values']} for x in chunk]
            )

    ############################
    # Public methods: Metadata #
    ############################

    def get_table_metadata(self, database: str, table: str) -> TableMetadataTypeDef:
        """
        Retrieve metadata for a specific table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            Table metadata.
        """
        response = self.glue_cli.get_table(
            DatabaseName=database,
            Name=table
        )

        storage_descriptor = response['Table']['StorageDescriptor']
        input_format = storage_descriptor['InputFormat']

        file_extension = self._map_serde_library_to_file_extension(input_format)

        if not file_extension:
            raise ValueError(f"The {input_format} is not supported")

        delimiter = (
            storage_descriptor
            .get('SerdeInfo', {})
            .get('Parameters', {})
            .get('field.delim')
        )

        columns = [
            ColumnMetadataTypeDef({
                'name': col['Name'],
                'type': col['Type'],
                'is_partition_key': False
            }) for col in storage_descriptor['Columns']]

        partition_keys = [
            ColumnMetadataTypeDef({
                'name': col['Name'],
                'type': col['Type'],
                'is_partition_key': True
            }) for col in response['Table'].get('PartitionKeys', [])]

        all_columns = columns + partition_keys

        return TableMetadataTypeDef({
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

    def get_columns_table(self, database: str, table: str) -> List[str]:
        """
        Get all columns of a table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            List of column names.
        """
        return self.get_table_metadata(database, table)['all_columns']

    def get_partitioned_columns(self, database: str, table: str) -> List[ColumnMetadataTypeDef]:
        """
        Get partitioned columns of a table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            List of partitioned column metadata.
        """
        return self.get_table_metadata(database, table)['partitioned_columns']

    def check_if_table_exists(self, database: str, table: str) -> bool:
        """
        Check if a table exists in the Glue catalog.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            True if table exists, False otherwise.
        """
        try:
            self.glue_cli.get_table(
                DatabaseName=database,
                Name=table
            )
            return True

        except self.glue_cli.exceptions.EntityNotFoundException:
            return False

    def get_location(self, database: str, table: str) -> str:
        """
        Get the S3 location of a table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            S3 location of the table.
        """
        return self.get_table_metadata(database, table)['location']

    ##############################
    # Public methods: Partitions #
    ##############################
    
    def check_empty_partition(self, database: str, table: str, partition: Tuple[str, ...]) -> bool:
        """
        Check if a partition is empty.

        Args:
            database: Glue database name.
            table: Glue table name.
            partition: Partition values tuple.

        Returns:
            True if partition is empty, False otherwise.
        """
        partition_location = self._get_partition_metadata(database, table, partition)['location']
        bucket, prefix = self._get_bucket_and_prefix(partition_location)

        files = self._get_all_s3_keys_from_prefix(bucket, prefix)

        return len(files) == 0

    def get_partition_size(self, database: str, table: str, partition: Tuple[str, ...]) -> float:
        """
        Get the size of a specific partition in MB.

        Args:
            database: Glue database name.
            table: Glue table name.
            partition: Partition values tuple.

        Returns:
            Size of the partition in MB.
        """
        files = self.get_partition_files(database, table, partition)
        total_size = 0

        for file in files:
            bucket, key = self._get_bucket_and_prefix(file)

            response = self.s3_cli.head_object(
                Bucket=bucket,
                Key=key
            )

            total_size += response['ContentLength']

        return total_size / 1024 ** 2

    def delete_partitions(self, database: str, table: str, partitions: List[Tuple[str, ...]]) -> None:
        """
        Delete specific partitions from a table.

        Args:
            database: Glue database name.
            table: Glue table name.
            partitions: List of partition values tuples to delete.
        """
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

    def update_partitions(self, database: str, table: str) -> None:
        """
        Update partitions in the Glue catalog to match S3.

        Args:
            database: Glue database name.
            table: Glue table name.
        """
        metadata = self.get_table_metadata(database, table)

        if not metadata['partitioned_columns']:
            return

        # Get current values ​​from Glue partitions
        current_partitions = set(tuple(p['values']) for p in self.get_all_partitions(database, table))

        # Get files from S3
        bucket, prefix = self._get_bucket_and_prefix(metadata['location'])
        files = self._get_all_s3_keys_from_prefix(bucket, prefix)

        # Extract partition values ​​from S3 keys
        location = prefix if prefix.endswith('/') else prefix + '/'
        partitions_in_s3 = set()
        for key in files:
            partition_dir = key.replace(location, '', 1).rsplit('/', 1)[0]
            partition_values = tuple(part.split('=')[1] for part in partition_dir.split('/'))
            partitions_in_s3.add(partition_values)

        # Determine partitions to delete and create
        partitions_to_delete = current_partitions - partitions_in_s3
        partitions_to_create = partitions_in_s3 - current_partitions

        # Delete partitions in batches of 25
        for chunk in self._break_iterable_in_chunks(list(partitions_to_delete), 25):
            self.glue_cli.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=[{'Values': list(p)} for p in chunk]
            )

        # Create new partitions
        for partition in partitions_to_create:
            partition_input = {
                'Values': list(partition),
                'StorageDescriptor': metadata['raw'].get('StorageDescriptor', {})
            }
            self.glue_cli.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput=partition_input
            )

    def get_all_partitions(self, database: str, table: str) -> List[PartitionMetadataTypeDef]:
        """
        Get all partitions of a table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            List of partition metadata.
        """
        paginator = self.glue_cli.get_paginator('get_partitions')
        response = paginator.paginate(
            DatabaseName=database,
            TableName=table
        )

        flatted_partitions = [
            partition 
            for page in response 
            for partition in page.get('Partitions', [])
        ]
        
        partition_metadata = [
            {
                'values': p['Values'],
                'location': p['StorageDescriptor']['Location'],
                'creation_time': p['CreationTime']
            } 
            for p in flatted_partitions
        ]
        
        return partition_metadata
    
    def get_last_partition(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime']) -> Optional[PartitionMetadataTypeDef]:
        """
        Get the last partition of a table.

        Args:
            database: Glue database name.
            table: Glue table name.
            sort_by: Sort partitions by 'alphanumeric' or 'createtime'.

        Returns:
            Metadata of the last partition.
        """
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
        """
        Get the number of partitions in a table.

        Args:
            database: Glue database name.
            table: Glue table name.

        Returns:
            Number of partitions.
        """
        partitions = self.get_all_partitions(database, table)
        return len(partitions)

    ###########################
    # Public methods: Readers #
    ###########################

    def get_table_to_lazyframe(self, database: str, table: str, where: Optional[str] = None, columns: Optional[List] = None) -> pl.LazyFrame:
        """
        Retrieve table as a Polars LazyFrame.

        Args:
            database: Glue database name.
            table: Glue table name.
            where: Optional SQL-like filter condition.
            columns: Optional list of columns to select.

        Returns:
            Polars LazyFrame with the table data.
        """
        metadata = self.get_table_metadata(database, table)
        glob_pattern = f"{metadata['location']}/**/*.{metadata['file_extension']}"

        lf = self._scan_file_to_lf(glob_pattern, metadata)
        lf = lf.select(columns) if columns else lf

        if where:
            sql = f'select * from self where {where}'
            return lf.sql(sql)

        return lf

    def get_last_partition_to_lazyframe(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime'] = 'alphanumeric') -> pl.LazyFrame:
        """
        Retrieve the last partition of a table as a Polars LazyFrame.

        Args:
            database: Glue database name.
            table: Glue table name.
            sort_by: Sort partitions by 'alphanumeric' or 'createtime'.

        Returns:
            Polars LazyFrame with the last partition data.
        """
        last_partition = self.get_last_partition(database, table, sort_by)
        metadata = self.get_table_metadata(database, table)
        return self._scan_file_to_lf(last_partition['location'], metadata)

    def get_partition_files(self, database: str, table: str, partition: Tuple[str, ...]) -> List[str]:
        """
        Get all files in a specific partition.

        Args:
            database: Glue database name.
            table: Glue table name.
            partition: Partition values tuple.

        Returns:
            List of file paths in the partition.
        """
        response = self.glue_cli.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=list(partition)
        )

        partition_location: str = response['Partition']['StorageDescriptor']['Location']
        bucket, prefix = self._get_bucket_and_prefix(partition_location)

        return [f'{bucket}/{key}' for key in self._get_all_s3_keys_from_prefix(bucket, prefix)]

    ###########################
    # Public methods: Writers #
    ###########################

    def put_frame_to_table(self,
                           frame: Union[pl.LazyFrame, pl.DataFrame],
                           database: str,
                           table: str,
                           compression: Optional[str] = 'snappy',
                           type: Literal['overwrite', 'append'] = 'overwrite') -> None:
        """
        Put a Polars DataFrame or LazyFrame to a Glue table.

        Args:
            frame: Polars LazyFrame or DataFrame to put into the table.
            database: Glue database name.
            table: Glue table name.
            compression: Compression type ('snappy' by default).
            type: Write type ('overwrite' or 'append').
        """
        metadata = self.get_table_metadata(database, table)
        table_columns = [x['name'] for x in metadata['all_columns']]

        # Is the type of write 'overwrite'?
        if type == 'overwrite':
            self.truncate_table(database, table)

        frame = frame.select(table_columns)

        # Is the frame a LazyFrame?
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect(streaming=True)

        # Have partitioned columns?
        if metadata['partitioned_columns']:
            self._write_partitioned_df(frame, metadata)

        else:
            self._write_non_partitioned_df(frame, metadata)

        self.update_partitions(database, table)

    def check_table_schema(self, frame: Union[pl.LazyFrame, pl.DataFrame], database: str, table: str) -> bool:
        """
        Validate whether the DataFrame schema matches the table schema in Glue.

        Args:
            frame: Polars LazyFrame or DataFrame to validate.
            database: Glue database name.
            table: Glue table name.

        Returns:
            True if the schemas match, False otherwise.
        """
        # TODO: Implement datatype mapping and datatype schema validation

        metadata = self.get_table_metadata(database, table)
        columns = metadata['all_columns']

        frame_datatypes = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema

        # polars_to_glue_mapping = {
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
        # }

        for column in columns:
            column_name = column['name']
            # column_type = column['type']

            if column_name not in frame_datatypes:
                return False

        return True

    def truncate_table(self, database: str, table: str) -> None:
        """
        Truncate a table by deleting all partitions and files.

        Args:
            database: Glue database name.
            table: Glue table name.
        """
        self._purge_all_partitions(database, table)

        location = self.get_location(database, table)
        bucket, prefix = self._get_bucket_and_prefix(location)

        self._purge_s3_prefix(bucket, prefix)
