from abc import ABC, abstractmethod
from typing import Any, Optional, List 

class FilesystemIntegratedWithPolars(ABC):
    
    def __init__(self) -> None:
        raise NotImplementedError('Method __init__ must be implemented in subclass.')
          
    @abstractmethod
    def get_file_as_buffer(self, filepath: str) -> object:
        raise NotImplementedError('Method get_file_as_buffer must be implemented in subclass.')
    
    @abstractmethod
    def get_file_as_string(self, filepath: str) -> str:
        raise NotImplementedError('Method get_file_as_string must be implemented in subclass.')
     
    @abstractmethod
    def get_file_to_lf(self, filepath: str, file_format: str) -> object:
        raise NotImplementedError('Method get_file_to_df must be implemented in subclass.')
    
    @abstractmethod
    def get_file_to_df(self, filepath: str, file_format: str) -> object:
        raise NotImplementedError('Method get_file_to_df must be implemented in subclass.')
    
    @abstractmethod
    def put_df_to_file(self, df: object, filepath: str, file_format: str) -> None:
        raise NotImplementedError('Method put_df_to_file must be implemented in subclass.')
    
    @abstractmethod
    def get_all_files(self, directory: str) -> list:
        raise NotImplementedError('Method get_all_files must be implemented in subclass.')
    
    @abstractmethod
    def delete_file(self, filepath: str) -> None:
        raise NotImplementedError('Method delete_file must be implemented in subclass.')
    
    @abstractmethod
    def delete_files(self, filepaths: list, pattern: Optional[str]) -> None:
        raise NotImplementedError('Method delete_files must be implemented in subclass.')
    
    @abstractmethod
    def delete_folder(self, directory: str) -> None:
        raise NotImplementedError('Method delete_files must be implemented in subclass.')
    
    
    