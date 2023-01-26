
import yaml
from sqlalchemy import create_engine,inspect
import psycopg2
DBAPI = 'psycopg2'

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self,creds_file):
        creds_file = 'db_creds.yaml'
        with open(creds_file,'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        print (data)
        return data

    def init_db_engine(self,creds_file):
        data = self.read_db_creds(creds_file)
        engine = create_engine(f"postgresql+{DBAPI}://{data['RDS_USER']}:{data['RDS_PASSWORD']}@{data['RDS_HOST']}:{data['RDS_PORT']}/{data['RDS_DATABASE']}")
        print(engine)
        return engine  



