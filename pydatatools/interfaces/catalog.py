from datetime import datetime as Datetime
from typing import List, Optional, Union, Tuple, TypedDict, Literal
from abc import ABC, abstractmethod
import polars as pl

class ColumnMetadataTypeDef(TypedDict):
    """A dictionary type for metadata of a column."""
    name: str
    type: str
    is_partition_key: bool

class TableMetadataTypeDef(TypedDict):
    """A dictionary type for metadata of a table."""
    database: str
    table: str 
    location: str
    columns: List[ColumnMetadataTypeDef]
    partitioned_columns: List[ColumnMetadataTypeDef]
    all_columns: List[ColumnMetadataTypeDef]
    file_extension: str
    delimiter: Optional[str]
    raw: dict

class PartitionMetadataTypeDef(TypedDict):
    """A dictionary type for metadata of a partition."""
    values: List[str]
    location: str
    creation_time: Datetime

class CatalogIntegratedWithPolars(ABC):
    """An abstract base class for Catalog operations."""
    
    @abstractmethod
    def get_table_metadata(self, database: str, table: str) -> TableMetadataTypeDef:
        """Retrieve metadata for a specific table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_table_to_lazyframe(self, database: str, table: str, where: Optional[str] = None, columns: Optional[List] = None) -> pl.LazyFrame:
        """Retrieve table as a Polars LazyFrame."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_last_partition_to_lazyframe(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime'] = 'alphanumeric') -> pl.LazyFrame:
        """Retrieve the last partition of a table as a Polars LazyFrame."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_columns_table(self, database: str, table: str) -> List[str]:
        """Get all columns of a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_all_partitions(self, database: str, table: str) -> List[PartitionMetadataTypeDef]:
        """Get all partitions of a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_partitioned_columns(self, database: str, table: str) -> List[ColumnMetadataTypeDef]:
        """Get partitioned columns of a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_last_partition(self, database: str, table: str, sort_by: Literal['alphanumeric', 'createtime']) -> Optional[PartitionMetadataTypeDef]:
        """Get the last partition of a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_partition_count(self, database: str, table: str) -> int:
        """Get the number of partitions in a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def check_if_table_exists(self, database: str, table: str) -> bool:
        """Check if a table exists in the Glue catalog."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def check_empty_partition(self, database: str, table: str, partition: Tuple[str, ...]) -> bool:
        """Check if a partition is empty."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_partition_files(self, database: str, table: str, partition: Tuple[str, ...]) -> List[str]:
        """Get all files in a specific partition."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_partition_size(self, database: str, table: str, partition: Tuple[str, ...]) -> float:
        """Get the size of a specific partition in MB."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def put_frame_to_table(self, frame: Union[pl.LazyFrame, pl.DataFrame], database: str, table: str, 
                           compression: Literal['lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'], 
                           type: Literal['overwrite', 'append'],
                           **kwargs) -> None:
        """Write a DataFrame or LazyFrame to a table in S3."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def delete_partitions(self, database: str, table: str, partitions: List[Tuple[str, ...]]) -> None:
        """Delete specific partitions from a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def check_table_schema(self, frame: Union[pl.LazyFrame, pl.DataFrame], database: str, table: str) -> bool:
        """
        Validate whether the DataFrame schema matches the table schema in Glue.
        """
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def get_location(self, database: str, table: str) -> str:
        """Get the S3 location of a table."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def update_partitions(self, database: str, table: str) -> None:
        """Update partitions in the Glue catalog to match S3."""
        raise NotImplementedError("This method must be implemented by a subclass")

    @abstractmethod
    def truncate_table(self, database: str, table: str) -> None:
        """Truncate a table by deleting all partitions."""
        raise NotImplementedError("This method must be implemented by a subclass")