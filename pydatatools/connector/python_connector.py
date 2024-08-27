from pydatatools.engine.python.catalog import GlueCatalog
from pydatatools.engine.python.filesystem import S3Filesystem
from pydatatools.interfaces.connector import Connector

class PythonConnector(Connector):
    """
    PythonConnector is a class that implements the Connector interface.
    It provides methods to interact with a catalog and a filesystem.
    """

    def __init__(self, catalog = GlueCatalog(), filesystem = S3Filesystem()):
        """
        Initializes PythonConnector with a catalog and a filesystem.

        :param catalog: an instance of GlueCatalog, defaults to GlueCatalog()
        :param filesystem: an instance of S3Filesystem, defaults to S3Filesystem()
        """
        self.catalog = catalog
        self.filesystem = filesystem
        
    def get_catalog(self) -> GlueCatalog:
        """
        Returns the catalog associated with this connector.

        :return: an instance of GlueCatalog
        """
        return self.catalog
    
    def get_filesystem(self) -> S3Filesystem:
        """
        Returns the filesystem associated with this connector.

        :return: an instance of S3Filesystem
        """
        return self.filesystem
    
    def get_odbc(self) -> None:
        """
        Method to get ODBC connection.
        This method is not implemented in this class and should be overridden in subclasses.

        :raises NotImplementedError: always
        """
        raise NotImplementedError()
    
    def get_sql(self) -> None:
        """
        Method to get SQL connection.
        This method is not implemented in this class and should be overridden in subclasses.

        :raises NotImplementedError: always
        """
        raise NotImplementedError()
    
    def get_data_quality(self) -> None:
        """
        Method to get data quality.
        This method is not implemented in this class and should be overridden in subclasses.

        :raises NotImplementedError: always
        """
        raise NotImplementedError()