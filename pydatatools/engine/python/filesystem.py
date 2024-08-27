from os import environ
from typing import Literal

import boto3
import polars as pl

from pydatatools.interfaces.filesystem import FilesystemIntegratedWithPolars

class S3Filesystem(FilesystemIntegratedWithPolars):
    """A class to interact with S3 as a filesystem integrated with Polars."""

    def __init__(self, s3_cli=boto3.client('s3')) -> None:
        """
        Initialize S3Filesystem with an S3 client.

        Args:
            s3_cli: Boto3 S3 client instance.
        """
        self.s3_cli = s3_cli

    def _get_storage_creds(self, env_vars: dict = environ) -> dict:
        """
        Retrieve AWS storage credentials from environment variables.

        Args:
            env_vars: Dictionary containing environment variables.

        Returns:
            A dictionary with AWS credentials and region information.
        """
        aws_access_key_id = env_vars.get('AWS_ACCESS_KEY_ID', '')
        aws_secret_access_key = env_vars.get('AWS_SECRET_ACCESS_KEY', '')

        regions = [env_vars.get('AWS_REGION'), env_vars.get('AWS_DEFAULT_REGION')]
        aws_region = next((item for item in regions if item is not None), '')

        storage_options = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region,
        }

        return storage_options

    def get_file_to_lf(self, filepath: str, 
                             file_format: Literal['text'], 
                             reader_function_kwargs: dict = {}) -> pl.LazyFrame:
        """
        Retrieve a file from S3 as a Polars LazyFrame.

        Args:
            filepath: Path to the file in S3.
            file_format: Format of the file (only 'text' is supported).
            reader_function_kwargs: Additional arguments for the Polars reader function.

        Returns:
            A Polars LazyFrame containing the file data.

        Raises:
            ValueError: If the file format is not supported.
        """
        creds = self._get_storage_creds()

        if file_format == 'text':
            SEPARATOR = reader_function_kwargs.get('sep', '\u001F')
            lf = pl.scan_csv(filepath, separator=SEPARATOR, 
                             storage_options=creds, has_header=False, **reader_function_kwargs)
            
            lf = lf.rename({'column_1': 'value'})
            return lf

        raise ValueError(f'File format {file_format} not supported.')

    def delete_file(self):
        """Placeholder for deleting a single file from S3."""
        pass

    def delete_files(self):
        """Placeholder for deleting multiple files from S3."""
        pass

    def delete_folder(self):
        """Placeholder for deleting a folder from S3."""
        pass

    def get_all_files(self):
        """Placeholder for retrieving all files from a folder in S3."""
        pass

    def get_file_as_buffer(self):
        """Placeholder for retrieving a file from S3 as a buffer."""
        pass

    def get_file_as_string(self):
        """Placeholder for retrieving a file from S3 as a string."""
        pass

    def get_file_to_df(self):
        """Placeholder for retrieving a file from S3 as a Polars DataFrame."""
        pass

    def put_df_to_file(self):
        """Placeholder for writing a Polars DataFrame to a file in S3."""
        pass
