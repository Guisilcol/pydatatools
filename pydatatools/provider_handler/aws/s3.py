
import boto3
from typing import List
from mypy_boto3_s3 import S3Client

class S3:
    def __init__(self, s3_cli: S3Client = boto3.client('s3')) -> None:
        """
        Initialize S3Filesystem with an S3 client.

        Args:
            s3_cli: Boto3 S3 client instance.
        """
        self.s3_cli = s3_cli
        
    def get_keys(self, bucket: str, prefix: str) -> List[str]:
        """
        Retrieve keys from an S3 bucket with a given prefix.

        Args:
            bucket: S3 bucket name.
            prefix: Prefix for the keys.

        Returns:
            A list of keys in the bucket with the given prefix.
        """

        paginator = self.s3_cli.get_paginator('list_objects_v2')
        iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)         

        keys = [
            key 
            for page in iterator
            for obj in page.get('Contents', [])
            for key in [obj['Key']]
        ]
        
        return keys
    
    def delete_keys(self, bucket: str, keys: List[str]) -> None:
        """
        Delete keys from an S3 bucket.

        Args:
            bucket: S3 bucket name.
            keys: List of keys to delete.
        """
        
        batch_size = 1000
        for i in range(0, len(keys), batch_size):    
            delete_keys = [{'Key': key} for key in keys[i:i+batch_size]]
            self.s3_cli.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            
    def delete_prefix(self, bucket: str, prefix: str) -> None:
        """
        Delete files with a common prefix in S3.
        
        Args:
            bucket: S3 bucket name.
            prefix: Prefix for the keys.
        """
        keys = self.get_keys(bucket, prefix)
        self.delete_keys(bucket, keys)  