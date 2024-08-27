from pydatatools.connector.python_connector import PythonConnector

class DataTools:
    
    @staticmethod
    def get_python_connector() -> PythonConnector:
        return PythonConnector()