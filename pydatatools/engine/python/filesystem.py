from os import environ
from typing import Literal

import boto3
import polars as pl

from pydatatools.interfaces.filesystem import FilesystemIntegratedWithPolars

class S3Filesystem(FilesystemIntegratedWithPolars):
    
    def __init__(self, s3_cli = boto3.client('s3')) -> None:
        self.s3_cli = s3_cli
        
    def _get_storage_creds(self, env_vars: dict = environ) -> dict:      
        aws_access_key_id       = env_vars.get('AWS_ACCESS_KEY_ID', '')
        aws_secret_access_key   = env_vars.get('AWS_SECRET_ACCESS_KEY', '')
        
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
                             reader_function_kwargs: dict = {}
    ) -> pl.LazyFrame:
        
        creds = self._get_storage_creds()
        
        if file_format == 'text':
            SEPARATOR = reader_function_kwargs.get('sep', '\u001F')
            lf = pl.scan_csv(filepath, separator=SEPARATOR, 
                storage_options=creds, has_header=False, **reader_function_kwargs)
            
            lf = lf.rename({'column_1': 'value'})
            return lf
            
        raise ValueError(f'File format {file_format} not supported.')
    
    def delete_file(self):
        pass
    
    def delete_files(self):
        pass
    
    def delete_folder(self):
        pass
    
    def get_all_files(self):
        pass
    
    def get_file_as_buffer(self):
        pass
    
    def get_file_as_string(self):
        pass
    
    def get_file_to_df(self):
        pass
    
    def put_df_to_file(self):
        pass