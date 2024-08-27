from abc import ABC, abstractmethod

class Connector(ABC):
    """Abstract class for connectors."""

    def __init_(self): 
        raise NotImplementedError('Class Connector is abstract and cannot be instantiated.')
    
    @abstractmethod
    def get_catalog(self):
        """Connect method to be implemented in subclass."""
        raise NotImplementedError('Method get_catalog must be implemented in subclass.')
    
    @abstractmethod
    def get_filesystem(self):
        """Connect method to be implemented in subclass."""
        raise NotImplementedError('Method get_filesystem must be implemented in subclass.')
    
    @abstractmethod
    def get_odbc(self):
        """Connect method to be implemented in subclass."""
        raise NotImplementedError('Method get_odbc must be implemented in subclass.')
    
    @abstractmethod
    def get_sql(self):
        """Connect method to be implemented in subclass."""
        
    @abstractmethod
    def get_data_quality(self):
        """Connect method to be implemented in subclass."""
        raise NotImplementedError('Method get_data_quality must be implemented in subclass.')