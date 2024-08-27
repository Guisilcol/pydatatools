from pydatatools.engine.python.catalog import GlueCatalog
from pydatatools.engine.python.filesystem import S3Filesystem
from pydatatools.interfaces.connector import Connector

class PythonConnector(Connector):
    def __init__(self, catalog = GlueCatalog(), filesystem = S3Filesystem()):
        self.catalog = catalog
        self.filesystem = filesystem
        
    def get_catalog(self):
        return self.catalog
    
    def get_filesystem(self):
        raise self.filesystem
    
    def get_odbc(self):
        raise NotImplementedError()
    
    def get_sql(self):
        raise NotImplementedError()
    
    def get_data_quality(self):
        raise NotImplementedError()
    
    