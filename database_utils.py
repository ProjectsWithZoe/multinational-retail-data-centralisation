
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
        connection_string = f"postgresql+{DBAPI}://{data['RDS_USER']}:{data['RDS_PASSWORD']}@{data['RDS_HOST']}:{data['RDS_PORT']}/{data['RDS_DATABASE']}"
        engine = create_engine(connection_string)  
        print(engine)
        return engine  

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print(table_names)

    def read_data(self, engine, table_name):
        with engine.connect() as connect:
            cursor = connect.connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            info = cursor.fetchall()
            print(info)
            return info

    #engine = create_engine(f'postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}')


db1 = DatabaseConnector()
db1.list_db_tables(db1.init_db_engine(creds_file='db_creds.yaml'))

